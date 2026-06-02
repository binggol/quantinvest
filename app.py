"""
quantinvest Phase 1: K-line viewer with pinyin-initial search.

Endpoints:
  GET /                        -> index page
  GET /api/health              -> health check
  GET /api/search?q=xxx        -> stock search (code OR pinyin initials OR name substring)
  GET /api/kline?code=xxx&days=N -> OHLCV for ECharts candlestick
"""

import os
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

    old_max = float(adj_v.max())
    new_adjs = [float(r.get("adj_factor") or 1.0) for r in rows]
    overall_max = max(old_max, max(new_adjs))

    # rescale historical bins if new max > old max (rare ex-dividend day)
    if overall_max > old_max + 1e-9:
        scale = old_max / overall_max
        log.info(f"[{code}] rescaling historical qfq by {scale:.6f} (new adj_factor > old max)")
        for field in ("open", "close", "high", "low"):
            si, vals = _read_bin(code, field)
            if vals.size > 0:
                _write_bin(code, field, si, vals * scale)

    # build new tail values per field
    cal_idx_map = {d: i for i, d in enumerate(cal)}
    field_tails: dict[str, list[float]] = {f: [] for f in
        ("open", "close", "high", "low", "volume", "change", "factor", "adj")}
    new_cal_indices = []

    for row in rows:
        d_ymd = str(row["trade_date"])
        d_iso = f"{d_ymd[:4]}-{d_ymd[4:6]}-{d_ymd[6:8]}"
        idx_in_cal = cal_idx_map[d_iso]
        new_cal_indices.append(idx_in_cal)
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

    # append to each bin file
    for field, new_vals in field_tails.items():
        si, vals = _read_bin(code, field)
        if vals.size == 0:
            continue
        # verify contiguity: new_cal_indices should be > si + vals.size - 1
        merged = np.concatenate([vals, np.array(new_vals, dtype="<f4")])
        _write_bin(code, field, si, merged)

    return len(rows)


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

    start_idx = int(sdf["cal_idx"].iloc[0])
    n = len(sdf)

    field_values = {
        "open":   (sdf["open"] * ratio).astype("float32").values,
        "close":  (sdf["close"] * ratio).astype("float32").values,
        "high":   (sdf["high"] * ratio).astype("float32").values,
        "low":    (sdf["low"] * ratio).astype("float32").values,
        "volume": sdf.get("vol", sdf.get("volume", pd.Series([0] * n))).astype("float32").values,
        "change": (sdf.get("pct_chg", pd.Series([0] * n)).astype("float32") / 100.0).values,
        "factor": np.ones(n, dtype="float32"),
        "adj":    sdf["adj_factor"].astype("float32").values,
    }

    # 注意: cal_idx 可能不连续 (例如该股有停牌日, 那些日子日历有但该股没数据).
    # qlib bin 格式假设连续, 所以这里需要用日历做"填充" -- 简化: 对于停牌日, 用前一日值填充.
    # 如果第一日 cal_idx 在中间, start_idx 跳过前面没数据的日期, 是正确的.
    contiguous_indices = list(range(start_idx, start_idx + (int(sdf["cal_idx"].iloc[-1]) - start_idx) + 1))
    if len(contiguous_indices) != n:
        # 填充: 用前一日值. 这是 qlib bin 的常见做法.
        full_field_values = {f: [] for f in field_values}
        sdf_by_idx = {int(r["cal_idx"]): r for _, r in sdf.iterrows()}
        last_vals = {f: 0.0 for f in field_values}
        for ci in contiguous_indices:
            if ci in sdf_by_idx:
                for f in field_values:
                    arr = field_values[f]
                    # find original position
                    pos = list(sdf["cal_idx"].values).index(ci)
                    last_vals[f] = float(arr[pos])
            for f in field_values:
                full_field_values[f].append(last_vals[f])
        field_values = {f: np.array(v, dtype="float32") for f, v in full_field_values.items()}

    for field, vals in field_values.items():
        _write_bin(code, field, start_idx, vals)

    last_iso = sdf["trade_date"].iloc[-1].strftime("%Y-%m-%d")
    first_iso = sdf["trade_date"].iloc[0].strftime("%Y-%m-%d")
    log.info(f"[{code}] full rebuild done: {n} 天, {first_iso} ~ {last_iso}")
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
    if now is None or before is None:
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
        by_code.setdefault(r.ts_code, {})[r.end_date] = {
            "dt_profit": r.dt_profit_to_holder,
            "roe": r.roe,
            "q_dtprofit": r.q_dtprofit,
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
    except Exception as e:
        log.exception(f"daily update failed: {e}")


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
    sched.start()
    log.info(f"scheduler started: daily update at {DAILY_HOUR:02d}:{DAILY_MINUTE:02d}, "
             f"weekly financials at Mon 02:00 (Asia/Shanghai)")


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
