# 在 PC 上跑 qlib 下一交易日预测 (方案B)。
# 结果 predictions.json 写到与 NAS 共享的 Z 盘 csv_tmp 目录, NAS 容器直接读取展示。
# 预测只读取 bin 数据, 不需要 tushare token (数据由 NAS 每晚更新)。
#
# 用法:
#   powershell -ExecutionPolicy Bypass -File scripts\run_predict_pc.ps1           # 用已存模型预测(日常)
#   powershell -ExecutionPolicy Bypass -File scripts\run_predict_pc.ps1 -Train    # 重训+预测(每周一次)
param([switch]$Train)

$ErrorActionPreference = "Stop"
$proj = Split-Path -Parent $PSScriptRoot

$env:QLIB_DATA_PATH      = "Z:\claude\qlib\data\cn_data"
$env:PARQUET_DIR         = "Z:\claude\qlib\data\csv_tmp\tushare_daily"
$env:PREDICT_DATA_DIR    = "Z:\claude\qlib\data\csv_tmp"            # 与 NAS 共享, 写 predictions.json/模型
$env:STOCK_META_DB       = Join-Path $proj "data\stock_meta.db"    # PC 本地股票元数据
$env:QLIB_KERNELS        = "8"
$env:PREDICT_TRAIN_START = "2020-01-01"

if (-not (Test-Path $env:STOCK_META_DB)) {
  Write-Host "缺少 stock_meta.db, 先在 PC 上构建一次 (需要 token + pypinyin):" -ForegroundColor Yellow
  Write-Host "  pip install pypinyin"
  Write-Host "  `$env:TUSHARE_TOKEN='你的token'; `$env:STOCK_META_DB='$($env:STOCK_META_DB)'; python scripts\build_stock_meta.py --force"
  exit 1
}

Set-Location $proj
if ($Train) {
  Write-Host "[run_predict_pc] 重训 + 预测 ..." -ForegroundColor Cyan
  python scripts\predict_qlib.py --train
} else {
  Write-Host "[run_predict_pc] 预测 (用已存模型) ..." -ForegroundColor Cyan
  python scripts\predict_qlib.py
}
Write-Host "[run_predict_pc] 完成, predictions.json 已写入 $($env:PREDICT_DATA_DIR)" -ForegroundColor Green
