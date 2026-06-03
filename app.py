"""
quantinvest Phase 1: K-line viewer with pinyin-initial search.

Endpoints:
  GET /                        -> index page
  GET /api/health              -> health check
  GET /api/search?q=xxx        -> stock search (code OR pinyin initials OR name substring)
  GET /api/kline?code=xxx&days=N -> OHLCV for ECharts candlestick
"""

import os
import json
import sqlite3
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import tushare as ts
from flask import Flask, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

PORT = int(os.environ.get("PORT", "5055"))
HOST = os.environ.get("HOST", "0.0.0.0")
QLIB_DATA_PATH = Path(os.environ.get("QLIB_DATA_PATH", "/app/qlib_data/cn_data"))
PARQUET_DIR = Path(os.environ.get("PARQUET_DIR", "/app/qlib_data/csv_tmp/tushare_daily"))
STOCK_META_DB = os.environ.get("STOCK_META_DB", "/app/data/stock_meta.db")
FINANCIALS_DB = os.environ.get("FINANCIALS_DB", "/app/data/financials.db")
# qlib 预测结果路径 (默认 ./data; 方案B 下指向 PC↔NAS 共享目录, 见 docker-compose)
PREDICT_JSON = Path(os.environ.get("PREDICT_JSON", str(Path(STOCK_META_DB).parent / "predictions.json")))
# 是否在本机(容器)做预测计算. 默认 0 = 计算在 PC 上跑、NAS 只读展示
PREDICT_COMPUTE_HERE = (os.environ.get("PREDICT_COMPUTE_HERE", "0") == "1")
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")
DAILY_HOUR = int(os.environ.get("DAILY_UPDATE_HOUR", "21"))
DAILY_MINUTE = int(os.environ.get("DAILY_UPDATE_MINUTE", "0"))

# 全局锁: 同一时刻只允许一个 backfill 任务跑, 避免重复 tushare 请求 + bin 写竞态
_backfill_lock = threading.Lock()
_trade_cal_cache: dict[str, bool] = {}  # YYYY-MM-DD -> is_trading_day

# bin 字段列表 (跟 update_daily.py 保持一致)
BIN_FIELDS = ["open", "close", "high", "low", "volume", "change", "factor", "adj"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("app")

app = Flask(__name__)

# ---------- qlib bin reader (no qlib package needed) ----------

def _read_calendar() -> list[str]:
    p = QLIB_DATA_PATH / "calendars" / "day.txt"
    if not p.exists():
        return []
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_bin(code: str, field: str) -> tuple[int, np.ndarray]:
    """Return (start_idx_in_calendar, values_float32)."""
    p = QLIB_DATA_PATH / "features" / code / f"{field}.day.bin"
    if not p.exists():
        return -1, np.array([], dtype=np.float32)
    arr = np.fromfile(p, dtype="<f4")
    if arr.size == 0:
        return -1, arr
    return int(arr[0]), arr[1:]


def _write_bin(code: str, field: str, start_idx: int, values: np.ndarray):
    """Atomically write a bin file: header float32(start_idx) + values."""
    stock_dir = QLIB_DATA_PATH / "features" / code
    stock_dir.mkdir(parents=True, exist_ok=True)
    p = stock_dir / f"{field}.day.bin"
    arr = np.hstack([np.float32(start_idx), values.astype("<f4")]).astype("<f4")
    tmp = p.with_suffix(".bin.tmp")
    arr.tofile(tmp)
    tmp.replace(p)


def _qlib_code_to_ts(code: str) -> str:
    """sh600519 -> 600519.SH ; sz000001 -> 000001.SZ ; bj832317 -> 832317.BJ"""
    return f"{code[2:]}.{code[:2].upper()}"


# ============================================================
#  TUSHARE / BACKFILL
# ============================================================

def _tushare_api():
    if not TUSHARE_TOKEN:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts.set_token(TUSHARE_TOKEN)
    return ts.pro_api()


def _is_trading_day(date_str: str) -> bool:
    """date_str: YYYY-MM-DD. Cached per process."""
    if date_str in _trade_cal_cache:
        return _trade_cal_cache[date_str]
    try:
        pro = _tushare_api()
        ymd = date_str.replace("-", "")
        df = pro.trade_cal(exchange="SSE", start_date=ymd, end_date=ymd)
        is_open = bool(len(df) > 0 and int(df.iloc[0]["is_open"]) == 1)
        _trade_cal_cache[date_str] = is_open
        return is_open
    except Exception as e:
        log.warning(f"trade_cal check failed for {date_str}: {e}")
        return False


def _fetch_one_day_parquet(date_str: str) -> bool:
    """Try fetch one day's full-market daily+adj_factor parquet from tushare.
    Returns True if parquet now exists (either fetched or already there)."""
    ymd = date_str.replace("-", "")
    p = PARQUET_DIR / f"{ymd}.parquet"
    if p.exists():
        return True
    try:
        pro = _tushare_api()
        daily = pro.daily(trade_date=ymd)
        if daily is None or daily.empty:
            log.info(f"tushare daily empty for {ymd}, data not yet published")
            return False
        adj = pro.adj_factor(trade_date=ymd)
        if adj is None or adj.empty:
            adj = pd.DataFrame(columns=["ts_code", "trade_date", "adj_factor"])
        merged = daily.merge(
            adj[["ts_code", "trade_date", "adj_factor"]],
            on=["ts_code", "trade_date"], how="left",
        )
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(p, index=False)
        log.info(f"fetched {ymd}.parquet from tushare ({len(merged)} rows)")
        return True
    except Exception as e:
        log.warning(f"tushare fetch failed for {ymd}: {e}")
        return False


def _append_dates_to_stock_bin(code: str, new_dates_ymd: list[str]) -> int:
    """Append given trading days' rows (from parquets) to one stock's bin files.
    Returns count of dates successfully appended. Handles qfq adjustment incl.
    rescaling old values if a new adj_factor exceeds the historical max."""
    ts_code = _qlib_code_to_ts(code)

    # collect new rows
    rows = []
    for ymd in new_dates_ymd:
        p = PARQUET_DIR / f"{ymd}.parquet"
        if not p.exists():
            continue
        df = pd.read_parquet(p)
        r = df[df["ts_code"] == ts_code]
        if r.empty:
            continue
        rows.append(r.iloc[0].to_dict())
    if not rows:
        return 0

    cal = _read_calendar()
    cal_set = set(cal)

    # append new dates to calendar in order
    new_cal_dates = []
    for row in rows:
        d_ymd = str(row["trade_date"])
        d_iso = f"{d_ymd[:4]}-{d_ymd[4:6]}-{d_ymd[6:8]}"
        if d_iso not in cal_set:
            cal.append(d_iso)
            cal_set.add(d_iso)
            new_cal_dates.append(d_iso)
    if new_cal_dates:
        cal_sorted = sorted(cal)
        (QLIB_DATA_PATH / "calendars" / "day.txt").write_text(
            "\n".join(cal_sorted) + "\n", encoding="utf-8")
        cal = cal_sorted

    # get current adj history for this stock
    start_idx, adj_v = _read_bin(code, "adj")
    if adj_v.size == 0:
        # legacy stock with no adj.day.bin — caller should do full rebuild
        return -1

    bin_last_idx = start_idx + adj_v.size - 1
    cal_idx_map = {d: i for i, d in enumerate(cal)}

    # 各新行按日历位置归位, 只保留比 bin 末尾更新的交易日
    row_by_idx: dict[int, dict] = {}
    for row in rows:
        d_ymd = str(row["trade_date"])
        d_iso = f"{d_ymd[:4]}-{d_ymd[4:6]}-{d_ymd[6:8]}"
        ci = cal_idx_map.get(d_iso)
        if ci is not None and ci > bin_last_idx:
            row_by_idx[ci] = row
    if not row_by_idx:
        return 0
    newer = sorted(row_by_idx)
    max_new_idx = newer[-1]

    # 若 [bin_last_idx+1, max_new_idx] 间有缺口 (该股停牌、日历却有交易日),
    # 简单尾部 append 会让数据与日期错位 -> 返回 -1, 交调用方全量重建 (已正确填充停牌).
    if len(newer) != (max_new_idx - bin_last_idx):
        log.info(f"[{code}] append 检测到停牌缺口, 改走全量重建以保证日期对齐")
        return -1

    old_max = float(adj_v.max())
    overall_max = max(old_max, max(float(r.get("adj_factor") or 1.0) for r in row_by_idx.values()))

    # 除权日新 adj 超过历史最大值时, 回头按比例缩放历史 qfq (保持前复权口径一致)
    if overall_max > old_max + 1e-9:
        scale = old_max / overall_max
        log.info(f"[{code}] rescaling historical qfq by {scale:.6f} (new adj_factor > old max)")
        for field in ("open", "close", "high", "low"):
            si, vals = _read_bin(code, field)
            if vals.size > 0:
                _write_bin(code, field, si, vals * scale)

    # 连续 append 新交易日 (已确认无缺口)
    field_tails: dict[str, list[float]] = {f: [] for f in
        ("open", "close", "high", "low", "volume", "change", "factor", "adj")}
    for ci in newer:
        row = row_by_idx[ci]
        adj_now = float(row.get("adj_factor") or 1.0)
        ratio = adj_now / overall_max
        field_tails["open"].append(float(row["open"]) * ratio)
        field_tails["close"].append(float(row["close"]) * ratio)
        field_tails["high"].append(float(row["high"]) * ratio)
        field_tails["low"].append(float(row["low"]) * ratio)
        field_tails["volume"].append(float(row.get("vol", row.get("volume", 0))))
        field_tails["change"].append(float(row.get("pct_chg", 0)) / 100.0)
        field_tails["factor"].append(1.0)
        field_tails["adj"].append(adj_now)

    for field, new_vals in field_tails.items():
        si, vals = _read_bin(code, field)
        if vals.size == 0:
            continue
        merged = np.concatenate([vals, np.array(new_vals, dtype="<f4")])
        _write_bin(code, field, si, merged)

    return len(newer)


def _full_rebuild_one_stock(code: str) -> dict:
    """从所有 parquet 重建单只股票的所有 8 个 bin 文件 (慢但保证正确).

    用于以下场景:
      - 该股 bin 缺 adj 字段 (旧数据)
      - 该股 bin 数据落后 parquet 很多天
      - 数据有损坏
    """
    ts_code = _qlib_code_to_ts(code)
    parquet_files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not parquet_files:
        return {"ok": False, "message": "无 parquet 文件"}

    log.info(f"[{code}] full rebuild from {len(parquet_files)} parquets ...")
    rows = []
    for p in parquet_files:
        try:
            df = pd.read_parquet(p, columns=None)
        except Exception as e:
            log.warning(f"  skip {p.name}: {e}")
            continue
        r = df[df["ts_code"] == ts_code]
        if not r.empty:
            rows.append(r.iloc[0].to_dict())
    if not rows:
        return {"ok": False, "message": "parquet 里没有该股票 (可能未上市或代码错误)"}

    sdf = pd.DataFrame(rows)
    sdf["trade_date"] = pd.to_datetime(sdf["trade_date"], format="%Y%m%d")
    sdf = sdf.sort_values("trade_date").reset_index(drop=True)

    # 确保日历覆盖所有这只股票出现过的日期
    cal = _read_calendar()
    cal_set = set(cal)
    stock_dates = set(sdf["trade_date"].dt.strftime("%Y-%m-%d"))
    new_in_cal = stock_dates - cal_set
    if new_in_cal:
        cal = sorted(cal_set | new_in_cal)
        (QLIB_DATA_PATH / "calendars" / "day.txt").write_text(
            "\n".join(cal) + "\n", encoding="utf-8")

    cal_idx = {d: i for i, d in enumerate(cal)}
    sdf["cal_idx"] = sdf["trade_date"].dt.strftime("%Y-%m-%d").map(cal_idx)

    # qfq 计算
    sdf["adj_factor"] = sdf.get("adj_factor", pd.Series([1.0] * len(sdf))).fillna(1.0).astype("float32")
    max_adj = float(sdf["adj_factor"].max())
    ratio = sdf["adj_factor"] / max_adj
    n_trade = len(sdf)

    start_idx = int(sdf["cal_idx"].iloc[0])
    last_idx = int(sdf["cal_idx"].iloc[-1])

    vol_src = sdf.get("vol", sdf.get("volume", pd.Series([0] * n_trade)))
    chg_src = sdf.get("pct_chg", pd.Series([0] * n_trade))
    g = pd.DataFrame({
        "cal_idx": sdf["cal_idx"].astype(int).values,
        "open":   (sdf["open"] * ratio).astype("float32").values,
        "close":  (sdf["close"] * ratio).astype("float32").values,
        "high":   (sdf["high"] * ratio).astype("float32").values,
        "low":    (sdf["low"] * ratio).astype("float32").values,
        "volume": vol_src.astype("float32").values,
        "change": (chg_src.astype("float32") / 100.0).values,
        "adj":    sdf["adj_factor"].astype("float32").values,
    })
    # !! 关键: qlib bin 假设从 start_idx 起逐日连续. reindex 到 [start_idx, last_idx]
    # 连续区间, 停牌日前向填充, 否则停牌后所有值与日期错位 (历史复权价全错).
    g = (g.drop_duplicates("cal_idx", keep="last")
           .set_index("cal_idx")
           .reindex(range(start_idx, last_idx + 1)))
    susp = g["close"].isna()
    g["close"] = g["close"].ffill()
    for c in ("open", "high", "low"):       # 停牌日 O=H=L=前一日收盘
        g[c] = g[c].where(~susp, g["close"])
    g["adj"] = g["adj"].ffill()
    g["volume"] = g["volume"].fillna(0.0)
    g["change"] = g["change"].fillna(0.0)
    g["factor"] = np.float32(1.0)

    for field in ("open", "close", "high", "low", "volume", "change", "factor", "adj"):
        _write_bin(code, field, start_idx, g[field].to_numpy(dtype="<f4"))

    n = len(g)
    first_iso, last_iso = cal[start_idx], cal[last_idx]
    log.info(f"[{code}] full rebuild done: {n} 天 ({n_trade} 交易 + {n - n_trade} 停牌填充), "
             f"{first_iso} ~ {last_iso}")
    return {"ok": True, "n_days": n, "first": first_iso, "last": last_iso}


def _get_today_iso() -> str:
    """today in Asia/Shanghai timezone (the container uses TZ=Asia/Shanghai)."""
    return datetime.now().strftime("%Y-%m-%d")


def ensure_freshness_for_stock(code: str) -> dict:
    """Try to ensure this stock's bin contains data up to the latest available day.
    Returns a status dict the caller can include in the response.

    Logic:
      - today's market_open ∈ [9:30, 15:00) → 用昨天数据为最新, 显示"今日交易中"
      - today's after_close (>= 15:00) + 今日是交易日 → 尝试拉今天 parquet, 成功就 append
      - 周末/节假日 → 用最近交易日数据
    """
    with _backfill_lock:
        return _ensure_freshness_inner(code)


def _ensure_freshness_inner(code: str) -> dict:
    now = datetime.now()
    today_iso = now.strftime("%Y-%m-%d")
    cal = _read_calendar()
    if not cal:
        return {"status": "no_calendar", "message": "qlib 日历为空, 请先运行 update_daily.py"}

    # find stock's bin last date
    start_idx, close_v = _read_bin(code, "close")
    if close_v.size == 0:
        return {"status": "stock_not_in_db", "message": "该股票 bin 不存在, 需 update_daily.py 重建"}
    bin_last_idx = start_idx + close_v.size - 1
    bin_last_date = cal[bin_last_idx] if bin_last_idx < len(cal) else cal[-1]

    # ---- step 1: 判断时段 + 若可拉今日 parquet 就先下 ----
    is_today_trade = _is_trading_day(today_iso)
    after_close = now.hour >= 15  # CST, container TZ should be set to Asia/Shanghai
    today_status = ""
    if is_today_trade and after_close:
        if _fetch_one_day_parquet(today_iso):
            today_status = "今日数据已发布"
        else:
            today_status = "今日交易日, 但 tushare 暂未发布数据 (一般 16:00 后才有)"
    elif is_today_trade and not after_close:
        today_status = "今日交易时段中, 行情未结算, 显示截至昨日数据"
    else:
        today_status = "今日非交易日"

    # ---- step 2: 永远检查 bin 是否落后于现有 parquet (不依赖今日是否拉得到) ----
    bin_last_ymd = bin_last_date.replace("-", "")
    existing_ymds = sorted(p.stem for p in PARQUET_DIR.glob("*.parquet"))
    missing_to_fill = [ymd for ymd in existing_ymds if ymd > bin_last_ymd]

    if not missing_to_fill:
        return {"status": "up_to_date", "message": today_status,
                "bin_last_date": bin_last_date}

    n_added = _append_dates_to_stock_bin(code, missing_to_fill)
    if n_added < 0:
        # 增量 append 失败 (无 adj 字段) → 自动 fallback 到全量单股重建
        log.info(f"[{code}] append failed, falling back to full single-stock rebuild")
        rebuilt = _full_rebuild_one_stock(code)
        if rebuilt.get("ok"):
            return {
                "status": "rebuilt",
                "message": f"{today_status} (已自动重建该股全部历史)",
                "n_days": rebuilt["n_days"],
                "bin_last_date": rebuilt["last"],
                "bin_first_date": rebuilt["first"],
            }
        else:
            return {"status": "rebuild_failed", "message":
                    f"重建失败: {rebuilt.get('message', '未知错误')}"}

    new_bin_last_idx = bin_last_idx + n_added
    new_bin_last = _read_calendar()[new_bin_last_idx] if n_added > 0 else bin_last_date
    return {
        "status": "appended" if n_added > 0 else "no_change",
        "message": today_status,
        "appended_dates": n_added,
        "bin_last_date": new_bin_last,
    }


def load_ohlcv(code: str, last_n_days: int | None = None, adjust: str = "qfq") -> dict:
    """读取 OHLCV.

    bin 文件存的是**前复权** (qfq) 价格 + adj.day.bin 存真实 adj_factor.
    支持三种 adjust:
      - 'qfq' (default): 前复权, 直接返回 bin 数据
      - 'hfq':           后复权 = qfq * max(adj). 形态相同, 数值放大
      - 'none' / 'raw':  不复权 = qfq * max(adj) / adj. 还原成交易当日真实价
    """
    cal = _read_calendar()
    if not cal:
        return {"dates": [], "open": [], "high": [], "low": [], "close": [], "volume": [], "adjust": adjust}

    start_idx, open_v = _read_bin(code, "open")
    _,         close_v = _read_bin(code, "close")
    _,         high_v = _read_bin(code, "high")
    _,         low_v = _read_bin(code, "low")
    _,         vol_v = _read_bin(code, "volume")
    _,         adj_v = _read_bin(code, "adj")  # 可能为空 (旧数据未重建)
    if open_v.size == 0:
        return {"dates": [], "open": [], "high": [], "low": [], "close": [], "volume": [], "adjust": adjust}

    n = min(open_v.size, close_v.size, high_v.size, low_v.size, vol_v.size)
    end_idx = start_idx + n
    dates = cal[start_idx:end_idx]

    o = open_v[:n].astype(np.float64)
    c = close_v[:n].astype(np.float64)
    h = high_v[:n].astype(np.float64)
    l = low_v[:n].astype(np.float64)
    v = vol_v[:n].astype(np.float64)

    # 复权变换 (bin 里默认是 qfq)
    actual_adjust = adjust
    if adjust != "qfq":
        if adj_v.size >= n:
            adj_arr = adj_v[:n].astype(np.float64)
            adj_arr[adj_arr == 0] = 1.0  # 防御
            max_adj = float(adj_arr.max())
            if adjust == "hfq":
                o, c, h, l = o * max_adj, c * max_adj, h * max_adj, l * max_adj
            else:  # none / raw
                ratio = max_adj / adj_arr
                o, c, h, l = o * ratio, c * ratio, h * ratio, l * ratio
        else:
            # 旧 bin 没 adj 字段, fallback 返回 qfq + 标识
            actual_adjust = "qfq"

    # 截取最近 N 天 (last_n_days <= 0 或 None 表示全部历史)
    if last_n_days and last_n_days > 0 and len(dates) > last_n_days:
        offset = len(dates) - last_n_days
        dates = dates[offset:]
        o, c, h, l, v = o[offset:], c[offset:], h[offset:], l[offset:], v[offset:]

    return {
        "dates": dates,
        "open": [round(float(x), 4) for x in o],
        "high": [round(float(x), 4) for x in h],
        "low": [round(float(x), 4) for x in l],
        "close": [round(float(x), 4) for x in c],
        "volume": [int(x) for x in v],
        "adjust": actual_adjust,
        "adjust_requested": adjust,
    }


# ---------- routes ----------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/screen")
def screen_page():
    return render_template("screen.html")


@app.route("/pattern")
def pattern_page():
    return render_template("pattern.html")


# ============================================================
#  /api/screen  基本面选股
# ============================================================

def _get_recent_periods(today: datetime | None = None) -> list[tuple[str, str]]:
    """返回 [过去3个年报 + 1个最新已披露季报] 的 (label, end_date) 列表, 旧→新.

    披露规则:
      - 年报: 4月底前 (5月起算最新一份是去年)
      - Q1:   4月底前 (5月起最新一份是当年Q1)
      - 半年: 8月底前 (9月起最新一份是当年中报)
      - Q3:   10月底前 (11月起最新一份是当年Q3)
    """
    today = today or datetime.now()
    y, m = today.year, today.month
    latest_annual_y = (y - 1) if m >= 5 else (y - 2)
    annuals = [(str(yr), f"{yr}1231") for yr in range(latest_annual_y - 2, latest_annual_y + 1)]
    if m >= 11:
        q_label, q_end = f"{y}Q3", f"{y}0930"
    elif m >= 9:
        q_label, q_end = f"{y}Q2", f"{y}0630"
    elif m >= 5:
        q_label, q_end = f"{y}Q1", f"{y}0331"
    else:
        q_label, q_end = f"{y-1}Q3", f"{y-1}0930"
    return annuals + [(q_label, q_end)]


def _qlib_to_ts_code(code: str) -> str:
    return f"{code[2:]}.{code[:2].upper()}"


def _ts_to_qlib_code(ts_code: str) -> str:
    code, exch = ts_code.split(".")
    return f"{exch.lower()}{code}"


def _yoy_growth(now: float | None, before: float | None) -> float | None:
    """同比增长率, before<=0 时返回 None (避免负数为分母时方向反掉)."""
    if now is None or before is None or pd.isna(now) or pd.isna(before):
        return None
    try:
        if before <= 0:
            return None
        return (now - before) / abs(before) * 100.0
    except (TypeError, ValueError):
        return None


@app.route("/api/screen/config")
def screen_config():
    """返回当前 '过去3年1期' 的标签 (供前端动态生成表单)"""
    periods = _get_recent_periods()
    return jsonify({
        "periods": [{"label": l, "end_date": e} for l, e in periods],
        "today": datetime.now().strftime("%Y-%m-%d"),
    })


@app.route("/api/screen")
def screen_query():
    """筛选符合条件的股票.

    查询参数:
      growth_min     扣非净利润同比增速最低值 (%), 默认 20
      growth_periods 启用的报告期 (逗号分隔标签, 如 "2023,2024,2025,2026Q1"), 默认全部启用
      roe_min        ROE 最低值 (%), 默认 8
      roe_period     ROE 取哪一期的值, 默认最新年报 (e.g. "2025")
      q_growth_on    是否启用 "最新一期单季扣非增速" 筛选 (1/0), 默认 0
      q_growth_min   单季扣非增速最低值 (%), 默认 0
      limit          返回多少条, 默认 200
    """
    periods = _get_recent_periods()
    period_map = dict(periods)  # label → end_date
    all_period_labels = [l for l, _ in periods]

    growth_min = float(request.args.get("growth_min", "20"))
    enabled_growth = (request.args.get("growth_periods") or ",".join(all_period_labels)).split(",")
    enabled_growth = [p.strip() for p in enabled_growth if p.strip() in period_map]
    roe_min = float(request.args.get("roe_min", "8"))
    roe_period = request.args.get("roe_period", all_period_labels[-2])  # 默认最新年报
    if roe_period not in period_map:
        roe_period = all_period_labels[-2]
    q_growth_on = (request.args.get("q_growth_on") or "0") == "1"
    q_growth_min = float(request.args.get("q_growth_min", "0"))
    limit = int(request.args.get("limit", "200"))

    # 最新一期 = periods 里最后一个 (那个季报标签, 如 2026Q1), 用于单季扣非同比
    latest_q_label = all_period_labels[-1]

    # 拉所有需要的 end_date 数据 (含同比基期: each period 的去年同期)
    needed_ends = set()
    for label in enabled_growth + [roe_period, latest_q_label]:
        ed = period_map[label]
        needed_ends.add(ed)
        # 同比基期 = 去年同期 (年报: y-1 → y-2; Qx: y-1 Qx)
        py_end = f"{int(ed[:4]) - 1}{ed[4:]}"
        needed_ends.add(py_end)

    if not Path(FINANCIALS_DB).exists():
        return jsonify({
            "error": "财务数据库不存在, 请先运行 docker exec quantinvest python scripts/fetch_financials.py",
            "hits": [],
        }), 503

    conn = sqlite3.connect(FINANCIALS_DB)
    placeholders = ",".join("?" * len(needed_ends))
    fina_df = pd.read_sql(
        f"SELECT ts_code, end_date, dt_profit_to_holder, roe, q_dtprofit FROM fina_indicators "
        f"WHERE end_date IN ({placeholders})",
        conn, params=list(needed_ends),
    )
    conn.close()
    if fina_df.empty:
        return jsonify({"hits": [], "message": "财务表为空"})

    # 转为 dict: ts_code → end_date → {dt_profit, roe}
    by_code: dict[str, dict] = {}
    for r in fina_df.itertuples(index=False):
        # pandas 把 SQL NULL 读成 NaN(float), 不是 None; 统一清成 None,
        # 否则 NaN 会骗过下游的 `is None` 判断, 还会被 jsonify 序列化成非法 JSON 的 NaN.
        by_code.setdefault(r.ts_code, {})[r.end_date] = {
            "dt_profit": None if pd.isna(r.dt_profit_to_holder) else float(r.dt_profit_to_holder),
            "roe": None if pd.isna(r.roe) else float(r.roe),
            "q_dtprofit": None if pd.isna(r.q_dtprofit) else float(r.q_dtprofit),
        }

    # 关联股票名/行业
    meta = pd.read_sql("SELECT ts_code, name, industry FROM stock_meta WHERE list_status = 'L'",
                       sqlite3.connect(STOCK_META_DB))
    meta_map = {r.ts_code: (r.name, r.industry) for r in meta.itertuples(index=False)}

    # 应用筛选
    hits = []
    for ts_code, periods_data in by_code.items():
        ok = True
        growth_metrics = {}
        for label in enabled_growth:
            ed = period_map[label]
            py = f"{int(ed[:4]) - 1}{ed[4:]}"
            now_v = periods_data.get(ed, {}).get("dt_profit")
            past_v = periods_data.get(py, {}).get("dt_profit")
            g = _yoy_growth(now_v, past_v)
            growth_metrics[label] = g
            if g is None or g < growth_min:
                ok = False
                break
        if not ok:
            continue

        roe_end = period_map[roe_period]
        roe_v = periods_data.get(roe_end, {}).get("roe")
        if roe_v is None or roe_v < roe_min:
            continue

        # 最新一期单季扣非净利润同比 (单季 vs 去年同季单季)
        q_end = period_map[latest_q_label]
        q_py = f"{int(q_end[:4]) - 1}{q_end[4:]}"
        q_now = periods_data.get(q_end, {}).get("q_dtprofit")
        q_past = periods_data.get(q_py, {}).get("q_dtprofit")
        q_growth = _yoy_growth(q_now, q_past)
        if q_growth_on and (q_growth is None or q_growth < q_growth_min):
            continue

        name, industry = meta_map.get(ts_code, ("", ""))
        hits.append({
            "ts_code": ts_code,
            "code": _ts_to_qlib_code(ts_code),
            "name": name,
            "industry": industry,
            "growth": {k: round(v, 2) if v is not None else None for k, v in growth_metrics.items()},
            "q_growth": round(q_growth, 2) if q_growth is not None else None,
            "roe": round(roe_v, 2),
            "roe_period": roe_period,
        })

    hits.sort(key=lambda x: x["roe"], reverse=True)
    return jsonify({
        "hits": hits[:limit],
        "total_matched": len(hits),
        "criteria": {
            "growth_min": growth_min,
            "growth_periods": enabled_growth,
            "roe_min": roe_min,
            "roe_period": roe_period,
            "q_growth_period": latest_q_label,
            "q_growth_on": q_growth_on,
            "q_growth_min": q_growth_min,
        },
        "all_periods": [{"label": l, "end_date": e} for l, e in periods],
    })


# ============================================================
#  /api/pattern  欧奈尔杯柄形态选股 (周线)
# ============================================================

# 日历→周(W-FRI) 的映射, 进程内缓存一次 (全市场共用同一日历, 避免每股重复解析)
_WEEK_CACHE: dict | None = None


def _week_buckets() -> dict:
    global _WEEK_CACHE
    if _WEEK_CACHE is None:
        cal = _read_calendar()
        per = pd.to_datetime(cal).to_period("W-FRI")
        wid, _ = pd.factorize(per, sort=False)        # 日历各日所属周的递增 id
        wend = [str(per[np.where(wid == k)[0][0]].end_time.date())
                for k in range(int(wid.max()) + 1)] if len(cal) else []
        _WEEK_CACHE = {"wid": wid.astype(np.int64), "wend": wend, "n": len(cal), "cal": cal}
    return _WEEK_CACHE


def _daily_ohlc(code: str, days: int = 800) -> dict | None:
    """直接读 bin(前复权) 取最近 days 个交易日的日线. 返回 numpy 数组 dict 或 None."""
    si, close = _read_bin(code, "close")
    if close.size < 120:
        return None
    _, high = _read_bin(code, "high")
    _, low = _read_bin(code, "low")
    _, vol = _read_bin(code, "volume")
    n = min(close.size, high.size, low.size, vol.size)
    off = max(0, n - days)
    cal = _week_buckets()["cal"]
    if si + n > len(cal):
        return None
    return {
        "dates": cal[si + off:si + n],
        "high": high[off:n].astype(float),
        "low": low[off:n].astype(float),
        "close": close[off:n].astype(float),
        "volume": vol[off:n].astype(float),
    }


def _weekly_ohlc(code: str, days: int = 900) -> dict | None:
    """直接读 bin(前复权) + numpy reduceat 重采样成周线(周五收盘). 快路径, 无 pandas resample."""
    si, close = _read_bin(code, "close")
    if close.size < 60:
        return None
    _, high = _read_bin(code, "high")
    _, low = _read_bin(code, "low")
    _, vol = _read_bin(code, "volume")
    n = min(close.size, high.size, low.size, vol.size)
    off = max(0, n - days)                              # 只取最近 days 个交易日
    s, e = si + off, si + n
    wb = _week_buckets()
    if e > wb["n"]:
        return None
    wid = wb["wid"][s:e]
    if wid.size < 60:
        return None
    close, high, low, vol = close[off:n], high[off:n], low[off:n], vol[off:n]

    starts = np.concatenate([[0], np.nonzero(np.diff(wid))[0] + 1])
    ends = np.append(starts[1:], wid.size)
    if starts.size < 12:
        return None
    return {
        "dates": [wb["wend"][wid[st]] for st in starts],
        "high": np.maximum.reduceat(high, starts).astype(float),
        "low": np.minimum.reduceat(low, starts).astype(float),
        "close": close[ends - 1].astype(float),
        "volume": np.add.reduceat(vol, starts).astype(float),
    }


def _detect_cup_handle(w: dict, p: dict) -> dict | None:
    """在周线上检测"当前正在成形"的杯柄形态. 柄部结束于最新一周, 故命中即"当下".

    返回最佳形态的指标 dict, 或 None. 评分以"突破就绪度"为主
    (现价离买点越近 + 柄部缩量越明显 + 杯沿越对称, 分越高).
    """
    H, L, C, V = w["high"], w["low"], w["close"], w["volume"]
    dates = w["dates"]
    n = len(C)
    if n < p["cup_min"] + 4:
        return None
    cur = n - 1
    best = None
    # 柄 = 最近 hl 周 (R 为右杯沿)
    for hl in range(1, p["handle_max"] + 1):
        R = cur - hl
        if R < p["cup_min"]:
            continue
        RH = float(H[R])
        if RH <= 0:
            continue
        handle_hi = float(H[R + 1:].max())
        handle_lo = float(L[R + 1:].min())
        if handle_hi > RH * 1.005:                 # 柄不应创新高(突破右杯沿)
            continue
        handle_depth = (RH - handle_lo) / RH
        if handle_depth > p["handle_depth_max"]:    # 柄回撤要浅
            continue
        pivot = RH * (1 + p["pivot_buffer"])        # 买点 = 右杯沿 + 缓冲
        cclose = float(C[cur])
        dist = (pivot - cclose) / pivot             # >0 在买点下方, <0 已突破
        if dist > p["near_pivot_max"] or dist < -p["above_pivot_max"]:
            continue                                # 离买点太远 / 已冲太高都不算"就绪"
        # 杯 = 右杯沿 R 之前 cup_len 周, 左杯沿 Lidx
        for cup_len in range(p["cup_min"], min(p["cup_max"], R) + 1):
            Lidx = R - cup_len
            if Lidx < 1:
                break
            LH = float(H[Lidx])
            if LH <= 0:
                continue
            rim = max(LH, RH)
            rim_diff = abs(RH - LH) / rim           # 左右杯沿要接近
            if rim_diff > p["rim_tol"]:
                continue
            seg = L[Lidx:R + 1]
            bottom = float(seg.min())
            bottom_pos = Lidx + int(np.argmin(seg))
            depth = (rim - bottom) / rim
            if not (p["cup_depth_min"] <= depth <= p["cup_depth_max"]):
                continue
            rel = (bottom_pos - Lidx) / cup_len      # U型: 底部居中, 非 V 型急跌
            if not (0.2 <= rel <= 0.8):
                continue
            mid = bottom + 0.5 * (rim - bottom)      # 柄应在杯的上半部
            if handle_lo < mid:
                continue
            pw = min(p["prior_bars"], Lidx)          # 前期涨势 >= prior_gain_min
            if pw < 4:
                continue
            prior_low = float(C[Lidx - pw:Lidx].min())
            if prior_low <= 0 or (C[Lidx] - prior_low) / prior_low < p["prior_gain_min"]:
                continue
            cup_vol = float(V[Lidx:R + 1].mean())
            handle_vol = float(V[R + 1:].mean())
            dryup = (cup_vol - handle_vol) / cup_vol if cup_vol > 0 else 0.0
            readiness = 1.0 if dist < 0 else max(0.0, 1 - dist / p["near_pivot_max"])
            shape = 1 - rim_diff / p["rim_tol"]
            score = 100 * (0.55 * readiness + 0.25 * max(0.0, dryup) + 0.20 * shape)
            if best is None or score > best["_score"]:
                best = {
                    "_score": score,
                    "score": round(score, 1),
                    "pivot": round(pivot, 2),
                    "close": round(cclose, 2),
                    "dist_pct": round(dist * 100, 2),
                    "cup_depth_pct": round(depth * 100, 1),
                    "cup_weeks": cup_len,
                    "handle_weeks": hl,
                    "handle_depth_pct": round(handle_depth * 100, 1),
                    "vol_dryup_pct": round(dryup * 100, 1),
                    "left_rim": dates[Lidx],
                    "bottom": dates[bottom_pos],
                    "right_rim": dates[R],
                    "ret": (cclose / float(C[cur - p["rs_lookback"]]) - 1) * 100
                           if cur >= p["rs_lookback"] else None,
                }
    if best:
        best.pop("_score", None)
    return best


@app.route("/api/pattern")
def pattern_query():
    """扫全市场, 找当前正在成形杯柄、价已逼近买点的股票, 按就绪度排序."""
    tf = request.args.get("tf", "w")  # 'w' 周线 / 'd' 日线
    # 与周期相关的默认值 (杯/柄长度单位 = 该周期的 bar 数; 前期涨势/RS 回溯也按周期换算)
    if tf == "d":
        tf_def = {"cup_min": "35", "cup_max": "325", "handle_max": "25",
                  "prior_bars": 150, "rs_lookback": 130}
    else:
        tf_def = {"cup_min": "7", "cup_max": "65", "handle_max": "5",
                  "prior_bars": 30, "rs_lookback": 26}
    p = {
        "cup_min": int(request.args.get("cup_min", tf_def["cup_min"])),
        "cup_max": int(request.args.get("cup_max", tf_def["cup_max"])),
        "handle_max": int(request.args.get("handle_max", tf_def["handle_max"])),
        "cup_depth_min": float(request.args.get("cup_depth_min", "12")) / 100,
        "cup_depth_max": float(request.args.get("cup_depth_max", "50")) / 100,
        "handle_depth_max": float(request.args.get("handle_depth_max", "15")) / 100,
        "near_pivot_max": float(request.args.get("near_pivot_max", "8")) / 100,
        "above_pivot_max": float(request.args.get("above_pivot_max", "5")) / 100,
        "prior_gain_min": float(request.args.get("prior_gain_min", "30")) / 100,
        "rim_tol": 0.08,
        "pivot_buffer": 0.01,
        "prior_bars": tf_def["prior_bars"],
        "rs_lookback": tf_def["rs_lookback"],
    }
    ex_st = (request.args.get("ex_st", "1") == "1")
    ex_new = (request.args.get("ex_new", "1") == "1")
    ex_board = (request.args.get("ex_board", "1") == "1")  # 北交所/科创板
    min_amount = float(request.args.get("min_amount", "5000")) * 1e4  # 万元 -> 元
    limit = int(request.args.get("limit", "0"))  # 0 = 全部命中都返回

    if not Path(STOCK_META_DB).exists():
        return jsonify({"error": "stock_meta.db 不存在", "hits": []}), 503
    meta = pd.read_sql(
        "SELECT code, ts_code, name, industry, list_date FROM stock_meta WHERE list_status='L'",
        sqlite3.connect(STOCK_META_DB),
    )
    today = datetime.now()
    one_year_ago = (today - timedelta(days=365)).strftime("%Y-%m-%d")

    hits = []
    scanned = 0
    for r in meta.itertuples(index=False):
        code, name = r.code, (r.name or "")
        if ex_st and "ST" in name.upper():
            continue
        if ex_board and (code.startswith("bj") or code.startswith("sh688")):
            continue
        if ex_new and r.list_date and r.list_date > one_year_ago:
            continue
        w = _daily_ohlc(code) if tf == "d" else _weekly_ohlc(code)
        if w is None:
            continue
        # 流动性: 估算近期日均成交额 (成交额 = close*成交量(手)*100). 周线 sum/5 得日均.
        if tf == "d":
            avg_daily_amount = float((w["close"][-60:] * w["volume"][-60:]).mean()) * 100
        else:
            avg_daily_amount = float((w["close"][-12:] * w["volume"][-12:]).mean()) * 100 / 5.0
        if avg_daily_amount < min_amount:
            continue
        scanned += 1
        det = _detect_cup_handle(w, p)
        if det is None:
            continue
        det.update({"code": code, "ts_code": r.ts_code, "name": name,
                    "industry": r.industry or ""})
        hits.append(det)

    # RS: 近 ~26 周收益在命中股内的百分位 (1-99)
    rets = sorted(h["ret"] for h in hits if h.get("ret") is not None)
    for h in hits:
        v = h.get("ret")
        if v is None or not rets:
            h["rs"] = None
        else:
            rank = sum(1 for x in rets if x <= v) / len(rets)
            h["rs"] = int(round(1 + rank * 98))

    hits.sort(key=lambda x: x["score"], reverse=True)
    return jsonify({
        "hits": hits[:limit] if limit > 0 else hits,
        "total_matched": len(hits),
        "scanned": scanned,
        "tf": tf,
        "today": today.strftime("%Y-%m-%d"),
    })


# ============================================================
#  /api/predict  qlib 下一交易日买入清单
# ============================================================

_predict_job = {"running": False, "status": "", "started": None}


@app.route("/predict")
def predict_page():
    return render_template("predict.html")


@app.route("/api/predict")
def api_predict():
    if not PREDICT_JSON.exists():
        msg = ("尚无预测结果。计算在 PC 端进行: 在 PC 上运行 scripts/run_predict_pc.ps1 "
               "生成 predictions.json 到共享目录") if not PREDICT_COMPUTE_HERE else \
              "尚无预测结果, 点「更新数据并预测」生成"
        return jsonify({"hits": [], "job": _predict_job,
                        "compute_here": PREDICT_COMPUTE_HERE, "message": msg})
    try:
        data = json.loads(PREDICT_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        return jsonify({"hits": [], "job": _predict_job,
                        "compute_here": PREDICT_COMPUTE_HERE, "message": f"读取预测失败: {e}"})
    data["job"] = _predict_job
    data["compute_here"] = PREDICT_COMPUTE_HERE
    return jsonify(data)


def _run_predict_job(retrain: bool):
    global _predict_job
    _predict_job = {"running": True,
                    "status": ("更新数据 + 重训模型 + 预测中..." if retrain else "更新数据 + 预测中..."),
                    "started": datetime.now().strftime("%H:%M:%S")}
    try:
        from scripts.predict_qlib import update_and_predict
        update_and_predict(retrain=retrain)
        _predict_job = {"running": False, "status": "完成",
                        "started": _predict_job["started"]}
    except Exception as e:
        log.exception("predict job failed")
        _predict_job = {"running": False, "status": f"失败: {e}",
                        "started": _predict_job.get("started")}


@app.route("/api/predict/run")
def api_predict_run():
    if not PREDICT_COMPUTE_HERE:
        return jsonify({"ok": False,
                        "message": "本实例不在本机计算; 预测由 PC 端生成, 此页只展示结果"})
    if _predict_job.get("running"):
        return jsonify({"ok": False, "message": "已有预测任务在运行中"})
    retrain = (request.args.get("retrain", "0") == "1")
    threading.Thread(target=_run_predict_job, args=(retrain,), daemon=True).start()
    return jsonify({"ok": True, "message": "已启动: 更新数据并预测" + ("(含重训)" if retrain else "")})


@app.route("/api/health")
def health():
    return jsonify({
        "ok": True,
        "qlib_data": str(QLIB_DATA_PATH),
        "calendar_days": len(_read_calendar()),
        "time": datetime.now().isoformat(timespec="seconds"),
    })


@app.route("/api/search")
def search():
    q = (request.args.get("q") or "").strip().lower()
    if not q:
        return jsonify({"hits": []})

    conn = sqlite3.connect(STOCK_META_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    # priority: exact code prefix > exact pinyin prefix > name substring
    like_q = f"{q}%"
    name_like = f"%{q}%"
    cur.execute("""
        SELECT code, ts_code, name, industry, list_status
        FROM stock_meta
        WHERE list_status = 'L'
          AND (LOWER(code) LIKE ?
               OR LOWER(pinyin_initials) LIKE ?
               OR name LIKE ?)
        ORDER BY
          CASE WHEN LOWER(code) LIKE ? THEN 0
               WHEN LOWER(pinyin_initials) LIKE ? THEN 1
               ELSE 2 END,
          code
        LIMIT 20
    """, (like_q, like_q, name_like, like_q, like_q))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify({"hits": rows})


@app.route("/api/kline")
def kline():
    code = (request.args.get("code") or "").strip().lower()
    days_str = request.args.get("days", "0")  # 0 = 全部历史
    adjust = (request.args.get("adjust") or "qfq").lower()
    refresh = (request.args.get("refresh") or "1") != "0"  # 默认开
    if adjust not in ("qfq", "hfq", "none", "raw"):
        adjust = "qfq"
    try:
        days = int(days_str)
    except ValueError:
        days = 0
    if not code:
        return jsonify({"error": "code required"}), 400

    # 先尝试补到最新数据 (按时段判断是否拉今日 parquet)
    freshness = None
    if refresh:
        try:
            freshness = ensure_freshness_for_stock(code)
        except Exception as e:
            log.exception(f"ensure_freshness failed: {e}")
            freshness = {"status": "error", "message": str(e)}

    data = load_ohlcv(code, last_n_days=days if days > 0 else None, adjust=adjust)
    if not data["dates"]:
        return jsonify({"error": f"no data for {code}"}), 404

    # stock display name from meta
    name = ""
    try:
        conn = sqlite3.connect(STOCK_META_DB)
        cur = conn.cursor()
        cur.execute("SELECT name FROM stock_meta WHERE code = ?", (code,))
        row = cur.fetchone()
        name = row[0] if row else ""
        conn.close()
    except Exception:
        pass

    return jsonify({"code": code, "name": name, "freshness": freshness, **data})


# ---------- scheduler ----------

def run_daily_update():
    log.info("=== daily update triggered by scheduler ===")
    try:
        from scripts.update_daily import main as update_main
        update_main()
        log.info("=== daily update succeeded ===")
        try:
            from scripts.build_stock_meta import main as meta_main
            meta_main(force=False)  # refresh weekly per STOCK_META_REFRESH_DAYS
        except Exception as e:
            log.warning(f"stock_meta refresh skipped: {e}")
        # 数据更新后, 自动预测下一交易日买入清单 (仅当本机负责计算; 方案B由PC算)
        if PREDICT_COMPUTE_HERE:
            try:
                from scripts.predict_qlib import predict_and_save
                predict_and_save()
            except Exception as e:
                log.warning(f"qlib 预测跳过: {e}")
    except Exception as e:
        log.exception(f"daily update failed: {e}")


def run_weekly_predict_retrain():
    log.info("=== weekly qlib retrain + predict triggered ===")
    try:
        from scripts.predict_qlib import update_and_predict
        update_and_predict(retrain=True)
        log.info("=== weekly qlib retrain succeeded ===")
    except Exception as e:
        log.exception(f"weekly qlib retrain failed: {e}")


def run_weekly_financials_update():
    log.info("=== weekly financials update triggered ===")
    try:
        from scripts.fetch_financials import fetch_all
        fetch_all()
        log.info("=== weekly financials update succeeded ===")
    except Exception as e:
        log.exception(f"financials update failed: {e}")


def init_scheduler():
    sched = BackgroundScheduler(timezone="Asia/Shanghai")
    sched.add_job(
        run_daily_update,
        CronTrigger(hour=DAILY_HOUR, minute=DAILY_MINUTE),
        id="daily_update",
        max_instances=1,
        coalesce=True,
    )
    # 每周一凌晨 02:00 跑财务数据更新 (财报披露集中在工作日, 周更细够用)
    sched.add_job(
        run_weekly_financials_update,
        CronTrigger(day_of_week="mon", hour=2, minute=0),
        id="weekly_financials",
        max_instances=1,
        coalesce=True,
    )
    # 每周日凌晨 03:00 重训 qlib 预测模型 (仅当本机负责计算; 方案B由PC算, 默认不挂)
    if PREDICT_COMPUTE_HERE:
        sched.add_job(
            run_weekly_predict_retrain,
            CronTrigger(day_of_week="sun", hour=3, minute=0),
            id="weekly_predict_retrain",
            max_instances=1,
            coalesce=True,
        )
    sched.start()
    log.info(f"scheduler started: daily update at {DAILY_HOUR:02d}:{DAILY_MINUTE:02d}, "
             f"weekly financials Mon 02:00, weekly qlib retrain Sun 03:00 (Asia/Shanghai)")


def boot_stock_meta():
    """On container start, build stock_meta.db if missing."""
    try:
        from scripts.build_stock_meta import main as meta_main
        meta_main(force=False)
    except Exception as e:
        log.warning(f"initial stock_meta build skipped: {e}")


if __name__ == "__main__":
    boot_stock_meta()
    init_scheduler()
    log.info(f"starting Flask on {HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
