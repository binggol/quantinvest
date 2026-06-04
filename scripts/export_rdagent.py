"""
把 C:\rdagent 的 RD-Agent 产出 (有效因子 + 最终买入清单 + 报告) 汇成一个 JSON,
写到与 NAS 共享的目录, quantinvest 的 /rdagent 页读取展示。

在 PC 上运行 (Windows 或 WSL 均可):
  python scripts/export_rdagent.py
路径可用环境变量覆盖:
  RDAGENT_FINAL_DIR (默认 C:/rdagent/final)
  SHARED_DIR        (默认 Z:/claude/qlib/data/csv_tmp)
"""

import os
import re
import json
import glob
from pathlib import Path


def _resolve(p: str) -> Path:
    """Windows 盘符路径在 WSL/Linux 下转成 /mnt/x。"""
    p = str(p).replace("\\", "/")
    if os.name != "nt" and re.match(r"^[A-Za-z]:", p):
        p = "/mnt/" + p[0].lower() + p[2:]
    return Path(p)


FINAL_DIR = _resolve(os.environ.get("RDAGENT_FINAL_DIR", "C:/rdagent/final"))
SHARED = _resolve(os.environ.get("SHARED_DIR", "Z:/claude/qlib/data/csv_tmp"))
OUT = SHARED / "rdagent.json"


def _latest(pattern: str):
    files = sorted(FINAL_DIR.glob(pattern))
    return files[-1] if files else None


def main():
    import pandas as pd
    if not FINAL_DIR.exists():
        raise SystemExit(f"找不到 RD-Agent 产出目录: {FINAL_DIR}")

    # 有效因子
    factors = []
    fp = FINAL_DIR / "effective_factors.json"
    if fp.exists():
        factors = json.loads(fp.read_text(encoding="utf-8-sig"))

    # 最新的最终买入清单 (final_buy_list_YYYYMMDD.csv)
    blp = _latest("final_buy_list_*.csv")
    hits, as_of = [], None
    if blp is not None:
        m = re.search(r"(\d{8})", blp.name)
        if m:
            d = m.group(1)
            as_of = f"{d[:4]}-{d[4:6]}-{d[6:]}"
        df = pd.read_csv(blp)
        for r in df.itertuples(index=False):
            d = r._asdict() if hasattr(r, "_asdict") else dict(zip(df.columns, r))
            hits.append({
                "rank": int(d.get("rank_final", 0)),
                "code": str(d.get("instrument", "")),
                "ts_code": str(d.get("ts_code", "")),
                "name": str(d.get("name", "")),
                "industry": str(d.get("industry", "")),
                "score": round(float(d.get("score", 0)), 4),
                "close": None if pd.isna(d.get("close")) else round(float(d.get("close", 0)), 2),
                "alloc_yuan": None if pd.isna(d.get("alloc_yuan")) else int(float(d.get("alloc_yuan", 0))),
            })

    # 报告文本 (最新)
    report = ""
    rp = _latest("report_*.txt")
    if rp is not None:
        report = rp.read_text(encoding="utf-8-sig")

    out = {
        "as_of": as_of,
        "source": "RD-Agent SOTA 因子 + LGBModel",
        "factors": factors,
        "n_factors": len(factors),
        "hits": hits,
        "report": report,
    }
    SHARED.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[export_rdagent] 写出 {len(hits)} 只 / {len(factors)} 因子, as_of={as_of} -> {OUT}")


if __name__ == "__main__":
    main()
