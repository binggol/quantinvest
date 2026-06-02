"""
Daily update: download today's tushare daily data + rebuild qlib bin.

Idempotent — safe to run multiple times. Skips dates already downloaded.
Mirrors the logic of Z:\\claude\\qlib\\scripts\\download_tushare.py +
build_qlib_bin.py but self-contained (no qlib python package needed).
"""

import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import tushare as ts

TOKEN = os.environ.get("TUSHARE_TOKEN", "")
QLIB_DATA_PATH = Path(os.environ.get("QLIB_DATA_PATH", "/app/qlib_data/cn_data"))
PARQUET_DIR = Path(os.environ.get("PARQUET_DIR", "/app/qlib_data/csv_tmp/tushare_daily"))

CALENDARS_DIR = QLIB_DATA_PATH / "calendars"
INSTRUMENTS_DIR = QLIB_DATA_PATH / "instruments"
FEATURES_DIR = QLIB_DATA_PATH / "features"

# 注: factor 字段保持 1.0 (RD-Agent / qlib Alpha158 的兼容约定),
# 新增 adj 字段存真实 adj_factor, 供 quantinvest 切换复权用
FIELDS = ["open", "close", "high", "low", "volume", "change", "factor", "adj"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("update_daily")


def _pro():
    if not TOKEN:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts.set_token(TOKEN)
    return ts.pro_api()


def _ts_code_to_qlib(ts_code: str) -> str:
    code, exch = ts_code.split(".")
    return f"{exch.lower()}{code}"


# ---------- step 1: download today's parquet ----------

def download_recent(pro, end: str | None = None, lookback_days: int = 5, sleep: float = 0.4):
    """Download daily + adj_factor parquet for any missing dates in the last N days."""
    end = end or datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - pd.Timedelta(days=lookback_days)).strftime("%Y%m%d")
    log.info(f"checking trade calendar {start} ~ {end}")
    cal = pro.trade_cal(exchange="SSE", start_date=start, end_date=end, is_open="1")
    dates = sorted(cal["cal_date"].astype(str).tolist())

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    todo = [d for d in dates if not (PARQUET_DIR / f"{d}.parquet").exists()]
    log.info(f"to download: {len(todo)} (skipping {len(dates) - len(todo)} already done)")

    for td in todo:
        try:
            daily = pro.daily(trade_date=td)
            if daily is None or daily.empty:
                log.warning(f"{td}: empty daily")
                continue
            time.sleep(sleep)
            adj = pro.adj_factor(trade_date=td)
            if adj is None or adj.empty:
                adj = pd.DataFrame(columns=["ts_code", "trade_date", "adj_factor"])
            merged = daily.merge(
                adj[["ts_code", "trade_date", "adj_factor"]],
                on=["ts_code", "trade_date"], how="left",
            )
            merged.to_parquet(PARQUET_DIR / f"{td}.parquet", index=False)
            log.info(f"  saved {td} ({len(merged)} rows)")
            time.sleep(sleep)
        except Exception as e:
            log.error(f"{td}: failed: {e}")


# ---------- step 2: build qlib bin ----------

def build_qlib_bin():
    """Rebuild calendar, instruments, and per-stock bin files from all parquet."""
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"no parquet files in {PARQUET_DIR}")
    log.info(f"loading {len(files)} parquet files ...")

    dfs = []
    for i, f in enumerate(files, 1):
        dfs.append(pd.read_parquet(f))
        if i % 1000 == 0:
            log.info(f"  loaded {i}/{len(files)}")
    df = pd.concat(dfs, ignore_index=True)
    log.info(f"total rows: {len(df):,}")

    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df["code"] = df["ts_code"].map(_ts_code_to_qlib)

    # calendar
    cals = sorted(df["trade_date"].dt.strftime("%Y-%m-%d").unique())
    CALENDARS_DIR.mkdir(parents=True, exist_ok=True)
    (CALENDARS_DIR / "day.txt").write_text("\n".join(cals) + "\n", encoding="utf-8")
    log.info(f"calendar: {len(cals)} days, {cals[0]} ~ {cals[-1]}")

    cal_idx = {d: i for i, d in enumerate(cals)}
    df["cal_idx"] = df["trade_date"].dt.strftime("%Y-%m-%d").map(cal_idx)

    # qfq prices: raw * adj_factor / max(adj_factor) per stock
    df["adj_factor"] = df["adj_factor"].fillna(1.0)
    max_adj = df.groupby("code")["adj_factor"].transform("max")
    qfq_ratio = df["adj_factor"] / max_adj
    for c in ("open", "close", "high", "low"):
        df[c] = (df[c] * qfq_ratio).astype("float32")
    df["volume"] = df["vol"].astype("float32") if "vol" in df.columns else df["volume"].astype("float32")
    df["change"] = (df["pct_chg"].astype("float32") / 100.0) if "pct_chg" in df.columns else 0.0
    df["factor"] = np.float32(1.0)
    df["adj"] = df["adj_factor"].astype("float32")  # 真实 adj_factor, 用于复权计算

    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    codes = df["code"].unique()
    log.info(f"writing per-stock bin files for {len(codes)} stocks ...")
    instruments = []
    for i, code in enumerate(codes, 1):
        sub = df[df["code"] == code].sort_values("cal_idx")
        if sub.empty:
            continue
        start_idx = int(sub["cal_idx"].iloc[0])
        stock_dir = FEATURES_DIR / code
        stock_dir.mkdir(parents=True, exist_ok=True)
        for field in FIELDS:
            values = sub[field].values.astype("<f")
            arr = np.hstack([np.float32(start_idx), values]).astype("<f")
            (stock_dir / f"{field}.day.bin").write_bytes(arr.tobytes())
        s = sub["trade_date"].iloc[0].strftime("%Y-%m-%d")
        e = sub["trade_date"].iloc[-1].strftime("%Y-%m-%d")
        instruments.append(f"{code}\t{s}\t{e}")
        if i % 500 == 0:
            log.info(f"  wrote {i}/{len(codes)}")

    INSTRUMENTS_DIR.mkdir(parents=True, exist_ok=True)
    (INSTRUMENTS_DIR / "all.txt").write_text("\n".join(instruments) + "\n", encoding="utf-8")
    log.info(f"instruments: {len(instruments)} stocks -> {INSTRUMENTS_DIR / 'all.txt'}")


def main():
    t0 = time.time()
    pro = _pro()
    download_recent(pro)
    build_qlib_bin()
    log.info(f"DONE in {(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    main()
