"""
Build stock_meta.db (SQLite) from tushare stock_basic.
Schema: code (qlib format), ts_code, name, industry, list_date, pinyin_initials
Run once at container startup (if DB missing) and every N days for refresh.
"""

import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import tushare as ts
from pypinyin import lazy_pinyin, Style

DB_PATH = os.environ.get("STOCK_META_DB", "/app/data/stock_meta.db")
TOKEN = os.environ.get("TUSHARE_TOKEN", "")
REFRESH_DAYS = int(os.environ.get("STOCK_META_REFRESH_DAYS", "7"))


def ts_code_to_qlib(ts_code: str) -> str:
    """000001.SZ -> sz000001 ;  600000.SH -> sh600000 ;  832317.BJ -> bj832317"""
    code, exch = ts_code.split(".")
    return f"{exch.lower()}{code}"


def pinyin_initials(name: str) -> str:
    """'贵州茅台' -> 'gzmt'; non-CN chars passed through lowercased."""
    if not name:
        return ""
    parts = lazy_pinyin(name, style=Style.FIRST_LETTER, errors="ignore")
    return "".join(p[:1] for p in parts).lower()


def needs_refresh() -> bool:
    if not Path(DB_PATH).exists():
        print(f"[stock_meta] DB missing at {DB_PATH}, will build")
        return True
    mtime = datetime.fromtimestamp(Path(DB_PATH).stat().st_mtime)
    age = datetime.now() - mtime
    if age > timedelta(days=REFRESH_DAYS):
        print(f"[stock_meta] DB age {age.days}d > {REFRESH_DAYS}d, will refresh")
        return True
    print(f"[stock_meta] DB fresh ({age.days}d old), skip refresh")
    return False


def fetch_basic() -> pd.DataFrame:
    if not TOKEN:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts.set_token(TOKEN)
    pro = ts.pro_api()
    print("[stock_meta] fetching stock_basic from tushare ...", flush=True)
    chunks = []
    for status in ("L", "D", "P"):  # 上市 / 退市 / 暂停
        df = pro.stock_basic(
            exchange="", list_status=status,
            fields="ts_code,symbol,name,industry,list_date",
        )
        if df is not None and not df.empty:
            df["list_status"] = status
            chunks.append(df)
    df = pd.concat(chunks, ignore_index=True)
    print(f"[stock_meta] got {len(df)} rows", flush=True)

    df["code"] = df["ts_code"].map(ts_code_to_qlib)
    df["pinyin_initials"] = df["name"].fillna("").map(pinyin_initials)
    df["list_date"] = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce")
    df["list_date"] = df["list_date"].dt.strftime("%Y-%m-%d").fillna("")
    df["industry"] = df["industry"].fillna("")
    return df[["code", "ts_code", "name", "industry", "list_date", "pinyin_initials", "list_status"]]


def write_db(df: pd.DataFrame):
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS stock_meta")
    cur.execute("""
        CREATE TABLE stock_meta (
            code TEXT PRIMARY KEY,
            ts_code TEXT,
            name TEXT,
            industry TEXT,
            list_date TEXT,
            pinyin_initials TEXT,
            list_status TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO stock_meta VALUES (?,?,?,?,?,?,?)",
        df.itertuples(index=False, name=None),
    )
    cur.execute("CREATE INDEX idx_pinyin ON stock_meta(pinyin_initials)")
    cur.execute("CREATE INDEX idx_name ON stock_meta(name)")
    cur.execute("CREATE INDEX idx_code ON stock_meta(code)")
    conn.commit()
    conn.close()
    print(f"[stock_meta] wrote {len(df)} rows -> {DB_PATH}", flush=True)


def main(force: bool = False):
    if not force and not needs_refresh():
        return
    df = fetch_basic()
    write_db(df)


if __name__ == "__main__":
    force = "--force" in sys.argv
    main(force=force)
