# Run the qlib next-day prediction on the PC (Plan B).
# Writes predictions.json to the NAS-shared folder; the NAS container reads & displays it.
# Prediction only reads bin data (no tushare token needed; the NAS updates data nightly).
# Uses a UNC path (does NOT depend on the Z: drive mapping).
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\run_predict_pc.ps1           # predict (daily)
#   powershell -ExecutionPolicy Bypass -File scripts\run_predict_pc.ps1 -Train    # retrain + predict (weekly)
param([switch]$Train)

$ErrorActionPreference = "Stop"
$proj = Split-Path -Parent $PSScriptRoot

$shared = $env:SHARED_DIR
if (-not $shared) { $shared = "\\192.168.0.106\docker\obsidian\vaults\claude\qlib\data\csv_tmp" }
$qlibData = (Split-Path $shared -Parent) + "\cn_data"

$env:QLIB_DATA_PATH      = $qlibData
$env:PARQUET_DIR         = Join-Path $shared "tushare_daily"
$env:PREDICT_DATA_DIR    = $shared
$env:STOCK_META_DB       = Join-Path $proj "data\stock_meta.db"
$env:QLIB_KERNELS        = "8"
$env:PREDICT_TRAIN_START = "2020-01-01"

if (-not (Test-Path $env:STOCK_META_DB)) {
  Write-Host "missing stock_meta.db, build it once on the PC (needs token + pypinyin):" -ForegroundColor Yellow
  Write-Host "  pip install pypinyin"
  Write-Host "  `$env:TUSHARE_TOKEN='<your token>'; `$env:STOCK_META_DB='$($env:STOCK_META_DB)'; python scripts\build_stock_meta.py --force"
  exit 1
}

Set-Location $proj
if ($Train) {
  Write-Host "[run_predict_pc] retrain + predict ..." -ForegroundColor Cyan
  python scripts\predict_qlib.py --train
} else {
  Write-Host "[run_predict_pc] predict (saved model) ..." -ForegroundColor Cyan
  python scripts\predict_qlib.py
}
Write-Host "[run_predict_pc] done -> $($env:PREDICT_DATA_DIR)\predictions.json" -ForegroundColor Green
