"""
qlib 预测: 用 LightGBM + (可插拔)因子集, 预测"下一交易日收益"并排序, 产出买入清单.

设计:
  - 因子集可插拔: 默认用内置 BASE_FACTORS; 若存在 data/factors.json (RD-Agent 产出的
    qlib 表达式因子), 则**追加**进来一起用 -> RD-Agent 挖的有效因子直接喂给预测。
  - 标签 = 下一交易日收益 Ref($close,-1)/$close - 1。
  - 模型每周重训 (train_and_save), 每日数据更新后预测 (predict_and_save)。
  - 产出 data/predictions.json, 供 /api/predict 读取展示。

可单独运行:
  python scripts/predict_qlib.py --train     # 重训并保存模型, 然后预测
  python scripts/predict_qlib.py             # 用已存模型预测 (无模型则自动先训)
"""

import os
import sys
import json
import pickle
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

QLIB_DATA_PATH = os.environ.get("QLIB_DATA_PATH", "/app/qlib_data/cn_data")
STOCK_META_DB = os.environ.get("STOCK_META_DB", "/app/data/stock_meta.db")
DATA_DIR = Path(os.environ.get("PREDICT_DATA_DIR", os.path.dirname(STOCK_META_DB) or "/app/data"))
MODEL_PATH = DATA_DIR / "qlib_model.pkl"
FACTORS_JSON = DATA_DIR / "factors.json"          # RD-Agent 产出的因子 (可选)
PRED_JSON = DATA_DIR / "predictions.json"
TRAIN_START = os.environ.get("PREDICT_TRAIN_START", "2020-01-01")
TOPN = int(os.environ.get("PREDICT_TOPN", "50"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [predict] %(message)s")
log = logging.getLogger("predict_qlib")

LABEL = ["Ref($close,-1)/$close-1"]
LABEL_NAME = ["LABEL0"]

# 内置基础因子 (Alpha158 思路, 不含 vwap, 因为我们的 bin 无 vwap 字段)
BASE_FACTORS = [
    ("$close/Ref($close,5)-1",  "ROC5"),
    ("$close/Ref($close,10)-1", "ROC10"),
    ("$close/Ref($close,20)-1", "ROC20"),
    ("$close/Ref($close,60)-1", "ROC60"),
    ("Mean($close,5)/$close",   "MA5R"),
    ("Mean($close,10)/$close",  "MA10R"),
    ("Mean($close,20)/$close",  "MA20R"),
    ("Mean($close,60)/$close",  "MA60R"),
    ("Std($close/Ref($close,1)-1,5)",  "VOL5"),
    ("Std($close/Ref($close,1)-1,20)", "VOL20"),
    ("$volume/(Mean($volume,5)+1)",    "VR5"),
    ("$volume/(Mean($volume,20)+1)",   "VR20"),
    ("Std($volume,20)/(Mean($volume,20)+1)", "VSTD20"),
    ("($close-Min($low,5))/(Max($high,5)-Min($low,5)+1e-12)",   "RSV5"),
    ("($close-Min($low,20))/(Max($high,20)-Min($low,20)+1e-12)", "RSV20"),
    ("($high-$low)/$close",            "RANGE"),
    ("Mean(($high-$low)/$close,5)",    "RANGE5"),
    ("Mean($close/Ref($close,1)-1,5)",  "RET5"),
    ("Mean($close/Ref($close,1)-1,20)", "RET20"),
    ("$close/Ref($close,1)-1",         "RET1"),
    ("Max($high,5)/$close",            "HI5R"),
    ("Min($low,5)/$close",             "LO5R"),
    ("Corr($close,Log($volume+1),10)", "CORR10"),
    ("Mean($volume,5)/(Mean($volume,60)+1)", "VRLONG"),
]


def _load_factors() -> tuple[list[str], list[str]]:
    """基础因子 + (若有) RD-Agent 产出的 data/factors.json。

    factors.json 格式: [{"name": "...", "expr": "qlib 表达式"}, ...]
    """
    exprs = [e for e, _ in BASE_FACTORS]
    names = [n for _, n in BASE_FACTORS]
    if FACTORS_JSON.exists():
        try:
            extra = json.loads(FACTORS_JSON.read_text(encoding="utf-8"))
            for i, f in enumerate(extra):
                expr, nm = f.get("expr"), f.get("name") or f"RD{i}"
                if expr and nm not in names:
                    exprs.append(expr)
                    names.append(nm)
            log.info(f"已并入 RD-Agent 因子 {len(extra)} 个, 总因子 {len(exprs)}")
        except Exception as e:
            log.warning(f"读取 {FACTORS_JSON} 失败, 仅用基础因子: {e}")
    return exprs, names


def _init_qlib():
    import qlib
    # kernels: 数据特征计算的并行进程数 (默认=CPU核数). 容器/Linux 直接多进程;
    # 可用环境变量 QLIB_KERNELS 调小以控内存。
    kw = {}
    k = os.environ.get("QLIB_KERNELS")
    if k:
        kw["kernels"] = int(k)
    qlib.init(provider_uri=QLIB_DATA_PATH, region="cn", **kw)


def _build_dataset(latest: str):
    from qlib.data.dataset import DatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import QlibDataLoader

    exprs, names = _load_factors()
    cal = _calendar()
    # 切分: train -> valid(最后~40交易日的前一段) -> test(最新一日, 用于预测)
    valid_start = cal[max(0, len(cal) - 45)]
    train_end = cal[max(0, len(cal) - 46)]
    loader = QlibDataLoader(config={"feature": (exprs, names), "label": (LABEL, LABEL_NAME)})
    from qlib.data.dataset.processor import RobustZScoreNorm, Fillna, DropnaLabel, CSRankNorm
    infer_proc = [RobustZScoreNorm(fit_start_time=TRAIN_START, fit_end_time=train_end,
                                   fields_group="feature", clip_outlier=True),
                  Fillna(fields_group="feature")]
    learn_proc = [DropnaLabel(), CSRankNorm(fields_group="label")]
    handler = DataHandlerLP(
        instruments="all", start_time=TRAIN_START, end_time=latest,
        data_loader=loader, infer_processors=infer_proc, learn_processors=learn_proc,
        process_type=DataHandlerLP.PTYPE_A,
    )
    # test 段取最近若干交易日 (日历可能比实际数据多 1 天, 预测时再挑真正有数据的最后一天)
    seg = {"train": (TRAIN_START, train_end), "valid": (valid_start, cal[-2]),
           "test": (cal[max(0, len(cal) - 6)], cal[-1])}
    return DatasetH(handler, segments=seg)


def _calendar() -> list[str]:
    from qlib.data import D
    return [str(x.date()) for x in D.calendar()]


def _new_model():
    from qlib.contrib.model.gbdt import LGBModel
    return LGBModel(loss="mse", num_leaves=64, learning_rate=0.05,
                    num_threads=4, early_stopping_rounds=50, num_boost_round=500)


def train_and_save():
    _init_qlib()
    latest = _calendar()[-1]
    log.info(f"训练数据 {TRAIN_START} ~ {latest} (test={latest})")
    ds = _build_dataset(latest)
    model = _new_model()
    model.fit(ds)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    log.info(f"模型已保存 -> {MODEL_PATH}")
    return model, ds, latest


def _universe_filter(scores: pd.Series) -> pd.DataFrame:
    """scores: index=qlib instrument(SH600519), value=分数. 关联元数据 + 剔除 ST/科创北交/次新/低流动性。"""
    import sqlite3
    from datetime import timedelta
    meta = pd.read_sql("SELECT code, ts_code, name, industry, list_date FROM stock_meta "
                       "WHERE list_status='L'", sqlite3.connect(STOCK_META_DB))
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    # qlib instrument(SH600519) -> qlib bin code(sh600519)
    df = scores.rename("score").reset_index()
    df.columns = ["inst", "score"]
    df["code"] = df["inst"].str.lower()
    df = df.merge(meta, on="code", how="left")
    df = df[df["name"].notna()]
    df = df[~df["name"].str.upper().str.contains("ST", na=False)]
    df = df[~(df["code"].str.startswith("bj") | df["code"].str.startswith("sh688"))]
    df = df[~((df["list_date"].notna()) & (df["list_date"] > one_year_ago))]
    return df


def predict_and_save(model=None, ds=None, latest=None):
    if model is None:
        if not MODEL_PATH.exists():
            log.info("无已存模型, 先训练")
            return _train_then_predict()
        _init_qlib()
        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)
        latest = _calendar()[-1]
        ds = _build_dataset(latest)
    pred = model.predict(ds, segment="test")
    if isinstance(pred, pd.DataFrame):
        pred = pred.iloc[:, 0]
    # test 段是最近几天; 取真正有数据的最后一天
    if isinstance(pred.index, pd.MultiIndex):
        last_dt = pred.dropna().index.get_level_values(0).max()
        latest = str(pd.Timestamp(last_dt).date())
        pred = pred.xs(last_dt, level=0)
    pred = pred.dropna().sort_values(ascending=False)

    df = _universe_filter(pred)
    df = df.sort_values("score", ascending=False).head(TOPN).reset_index(drop=True)

    # 现价 (qfq 最新收盘)
    from qlib.data import D
    insts = df["inst"].tolist()
    if insts:
        px = D.features(insts, ["$close"], start_time=latest, end_time=latest)
        px = px["$close"].droplevel(1) if isinstance(px.index, pd.MultiIndex) else px["$close"]
        df["close"] = df["inst"].map(lambda x: float(px.get(x, float("nan"))))
    else:
        df["close"] = float("nan")

    hits = [{
        "rank": i + 1, "code": r.code, "ts_code": r.ts_code, "name": r["name"],
        "industry": r.industry or "", "score": round(float(r.score), 4),
        "close": None if pd.isna(r.close) else round(float(r.close), 2),
    } for i, r in df.iterrows()]

    today = datetime.now().strftime("%Y-%m-%d")
    data_note = ("数据已更新至今日 (收盘)" if latest >= today
                 else f"数据截至 {latest} (今日盘中/数据未发布/非交易日)")
    out = {
        "as_of": latest,                 # 基于哪一天的数据预测
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_note": data_note,
        "label": "next_day_return",
        "topn": TOPN,
        "n_factors": len(_load_factors()[0]),
        "hits": hits,
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PRED_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"预测完成: {len(hits)} 只, 基于 {latest} -> {PRED_JSON}")
    return out


def _train_then_predict():
    model, ds, latest = train_and_save()
    return predict_and_save(model, ds, latest)


def _run_update():
    """跑 tushare -> bin 更新, 保证预测基于最新数据。"""
    try:
        from scripts.update_daily import main as update_main
    except ImportError:
        import update_daily
        update_main = update_daily.main
    log.info("先从 tushare 更新数据到最新 ...")
    update_main()


def main():
    if "--update" in sys.argv:
        _run_update()
    if "--train" in sys.argv:
        _train_then_predict()
    else:
        predict_and_save()


# 供 app.py 调度/按钮调用
def update_and_predict(retrain: bool = False):
    _run_update()
    return _train_then_predict() if retrain else predict_and_save()


if __name__ == "__main__":
    main()
