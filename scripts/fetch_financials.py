"""
拉全市场 A 股的 fina_indicator (财务指标) 到本地 SQLite, 供 /screen 选股使用.

数据源: tushare pro.fina_indicator(ts_code, ...)  (积分要求: 500)
更新频率: 每周日凌晨 (财报披露窗口跨日, 周更够细)
存储: /app/data/financials.db  表 fina_indicators

字段说明:
  ts_code              股票代码 (000001.SZ)
  ann_date             公告日期
  end_date             报告期截止 (20231231=年报, 20260331=Q1, 20260630=半年, 20260930=Q3)
  dt_profit_to_holder  扣除非经常性损益后归属母公司股东的净利润 (元)
  roe                  净资产收益率 (摊薄) %
  roe_dt               扣非净资产收益率 %
  q_dtprofit           单季度扣非净利润 (元) - 用于季度同比时取用
"""

import os
import sqlite3
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import tushare as ts

DB_PATH = Path(os.environ.get("FINANCIALS_DB", "/app/data/financials.db"))
STOCK_META_DB = Path(os.environ.get("STOCK_META_DB", "/app/data/stock_meta.db"))
TOKEN = os.environ.get("TUSHARE_TOKEN", "")
START_DATE = "20210101"  # 拉 5 年覆盖任何 "近三年一期" + 同比基期
SLEEP = float(os.environ.get("FINA_SLEEP", "0.15"))  # 6.6 calls/sec, 远低于 tushare 默认限额

# tushare fina_indicator 接口的"真实"字段名.
# 注意: 扣非净利润在 tushare 里叫 profit_dedt, 不是 dt_profit_to_holder!
# 用错名字 tushare 会返回空列 -> 入库全 NULL -> 选股增速恒为 None -> 0 命中.
TS_FIELDS = [
    "ts_code", "ann_date", "end_date",
    "profit_dedt",  # 扣除非经常性损益后的归母净利润 (扣非净利润)
    "roe",          # ROE %
    "roe_dt",       # 扣非 ROE %
    "q_dtprofit",   # 单季扣非净利润 (用于 Q 级别同比)
]

# tushare 字段名 -> 本地 SQLite 列名 (列名沿用历史命名, 与 app.py 一致)
TS_TO_DB = {"profit_dedt": "dt_profit_to_holder"}

# 本地表的列名 (入库 / 选库都用这套)
FIELDS = [
    "ts_code", "ann_date", "end_date",
    "dt_profit_to_holder",  # 扣非归母净利润 (来自 tushare profit_dedt)
    "roe",                   # ROE %
    "roe_dt",                # 扣非 ROE %
    "q_dtprofit",            # 单季扣非净利润 (用于 Q 级别同比)
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("fetch_financials")


def _get_stock_list() -> list[str]:
    """从 stock_meta.db 取上市状态为 L 的 ts_code 列表"""
    if not STOCK_META_DB.exists():
        raise RuntimeError(f"stock_meta.db 不存在: {STOCK_META_DB}  请先 build_stock_meta.py")
    conn = sqlite3.connect(STOCK_META_DB)
    df = pd.read_sql("SELECT ts_code FROM stock_meta WHERE list_status = 'L'", conn)
    conn.close()
    return df["ts_code"].tolist()


def _init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fina_indicators (
            ts_code TEXT NOT NULL,
            ann_date TEXT,
            end_date TEXT NOT NULL,
            dt_profit_to_holder REAL,
            roe REAL,
            roe_dt REAL,
            q_dtprofit REAL,
            PRIMARY KEY (ts_code, end_date)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_end ON fina_indicators(end_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_code ON fina_indicators(ts_code)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fetch_progress (
            ts_code TEXT PRIMARY KEY,
            last_fetched TEXT,
            n_rows INTEGER
        )
    """)
    conn.commit()
    conn.close()


def _upsert_rows(df: pd.DataFrame):
    if df.empty:
        return 0
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # 确保只取需要的列, 缺的补 None
    for f in FIELDS:
        if f not in df.columns:
            df[f] = None
    sub = df[FIELDS].copy()
    cur.executemany(
        "INSERT OR REPLACE INTO fina_indicators "
        "(ts_code, ann_date, end_date, dt_profit_to_holder, roe, roe_dt, q_dtprofit) "
        "VALUES (?,?,?,?,?,?,?)",
        sub.itertuples(index=False, name=None),
    )
    conn.commit()
    conn.close()
    return len(sub)


def fetch_all(start_date: str = START_DATE, force: bool = False):
    if not TOKEN:
        raise RuntimeError("TUSHARE_TOKEN 未设置")
    ts.set_token(TOKEN)
    pro = ts.pro_api()

    _init_db()
    codes = _get_stock_list()
    log.info(f"要拉 {len(codes)} 只股票财务数据 (start_date={start_date})")

    # 已处理过的: 7 天内拉过的跳过 (除非 force)
    conn = sqlite3.connect(DB_PATH)
    if not force:
        done = pd.read_sql(
            "SELECT ts_code FROM fetch_progress "
            "WHERE last_fetched > datetime('now', '-7 days')", conn
        )["ts_code"].tolist()
    else:
        done = []
    conn.close()
    todo = [c for c in codes if c not in done]
    log.info(f"实际要拉: {len(todo)} (skip {len(codes) - len(todo)} 7 天内已拉)")

    t0 = time.time()
    ok_cnt = fail_cnt = total_rows = 0

    for i, ts_code in enumerate(todo, 1):
        try:
            df = pro.fina_indicator(
                ts_code=ts_code,
                start_date=start_date,
                end_date=datetime.now().strftime("%Y%m%d"),
                fields=",".join(TS_FIELDS),
            )
            if df is not None and not df.empty:
                df = df.rename(columns=TS_TO_DB)  # profit_dedt -> dt_profit_to_holder
            n = _upsert_rows(df) if df is not None else 0
            total_rows += n

            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO fetch_progress VALUES (?, datetime('now'), ?)",
                (ts_code, n),
            )
            conn.commit()
            conn.close()
            ok_cnt += 1
        except Exception as e:
            fail_cnt += 1
            log.warning(f"{ts_code}: {e}")

        if i % 100 == 0 or i == len(todo):
            el = time.time() - t0
            rate = i / el if el > 0 else 0
            eta = (len(todo) - i) / rate if rate > 0 else 0
            log.info(
                f"[{i}/{len(todo)}] ok={ok_cnt} fail={fail_cnt} rows={total_rows} "
                f"rate={rate:.1f}/s eta={eta/60:.1f}min"
            )
        time.sleep(SLEEP)

    log.info(f"DONE: ok={ok_cnt} fail={fail_cnt} total_rows={total_rows} "
             f"elapsed={(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    force = "--force" in sys.argv
    fetch_all(force=force)
