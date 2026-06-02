# quantinvest

A股 K线浏览器 · Phase 1 · Flask + ECharts + 内置 21:00 自动更新

## 功能（Phase 1）

- 网页输入 **股票代码 / 名称 / 拼音首字母**（如 `600519` / `茅台` / `gzmt`）即时搜索
- ECharts 标准 K 线 + 成交量 + MA5/10/20/60
- 每晚 21:00 自动从 tushare 增量下载 + 重建 qlib bin（同一份数据 RD-Agent 也直接复用）

## 选股（基本面）

页面 `/screen`，按"过去三年一期"自动判定报告期，筛选条件：

- **扣非净利润同比增速 ≥ X%**（可按报告期勾选，逐期都满足才命中）
- **ROE ≥ X%**（可选取哪一期）
- **最新一期单季扣非增速 ≥ X%**（可选开关，默认关闭，仅勾选才参与筛选）

结果表额外展示一列 `单季Δ`（最新一期单季扣非净利润同比）。

⚠️ **字段名坑（务必注意）**：tushare `fina_indicator` 里"扣非净利润"的字段名是
**`profit_dedt`**，**不是** `dt_profit_to_holder`。用错名字 tushare 会静默返回空列 →
入库全 NULL → 选股增速恒为 None → **0 命中**。`scripts/fetch_financials.py` 用
`TS_FIELDS`（含 `profit_dedt`）向 tushare 请求，拉回后 rename 成本地列名
`dt_profit_to_holder`。单季扣非用 `q_dtprofit`。

数据每周一 02:00 自动刷新；首次或改了字段后需手动 `--force` 重拉（见下方「手动操作」）。

## 数据布局

容器内：
```
/app/qlib_data/cn_data/      ← qlib bin (calendars/instruments/features)
/app/qlib_data/csv_tmp/      ← tushare 原始 parquet
/app/data/stock_meta.db      ← SQLite 股票元数据 + 拼音首字母索引
/app/data/financials.db      ← SQLite 财务指标 (fina_indicator) 供选股
```

宿主（群晖 Synology）：
```
/volume1/docker/obsidian/vaults/claude/qlib/data/cn_data       ← 与 RD-Agent 共享
/volume1/docker/obsidian/vaults/claude/qlib/data/csv_tmp       ← 与 RD-Agent 共享
./data                                                          ← quantinvest 私有
```

## 部署到群晖 Docker

```bash
# 同步项目到群晖 (git pull / scp / Synology Drive)
cd /volume1/docker/quantinvest

# 第一次: 准备 .env (从模板复制并填 token)
cp .env.example .env
# 编辑 .env 确认 TUSHARE_TOKEN

# 启动
docker compose up -d --build

# 查看日志 (首启会自动构建 stock_meta.db, ~30s)
docker compose logs -f quantinvest
```

浏览器开 `http://<群晖IP>:5055`

## 端口

- 内部：`5055`
- 与本机 PC 端 RD-Agent 流程（pdfduibi 5000、rdagent ui 19899）完全错开

## 定时更新

容器启动后内置 APScheduler：
- 每天 21:00 (Asia/Shanghai) 自动跑 `scripts/update_daily.py`
- 每 7 天自动刷一次 `stock_meta.db`（新股 / 行业调整 / 退市）

不依赖群晖任务计划，不依赖宿主 cron。

## 手动操作

```bash
# 立即触发一次更新
docker exec quantinvest python scripts/update_daily.py

# 重建股票元数据 (强制刷新)
docker exec quantinvest python scripts/build_stock_meta.py --force

# 拉/重拉财务数据 (选股用). --force 会绕过 "7天内已拉则跳过" 的逻辑, 全量重拉
docker exec quantinvest python scripts/fetch_financials.py --force

# 检查财务库覆盖率 (各报告期有多少行、扣非净利润有多少非空)
docker exec quantinvest python -c "import sqlite3,pandas as pd; c=sqlite3.connect('/app/data/financials.db'); print(pd.read_sql('SELECT end_date, COUNT(*) n, COUNT(dt_profit_to_holder) has_profit FROM fina_indicators GROUP BY end_date ORDER BY end_date DESC LIMIT 8', c))"
```

## 路线图

- [x] **Phase 1** K线浏览 + 搜索 + 每日自动更新
- [ ] Phase 2：指标叠加（MACD、RSI、KDJ、布林带）
- [ ] Phase 3：自选股 / 看板
- [ ] Phase 4：因子值叠加 K 线（RD-Agent SOTA 因子可视化）
- [ ] Phase 5：多日板块热力图 / 行业轮动
