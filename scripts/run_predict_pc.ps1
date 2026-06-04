# Run the qlib next-day prediction on the PC (Plan B).
# Writes predictions.json to the NAS-shared folder; the NAS container reads & displays it.
# Prediction only reads bin data (no tushare token needed; the NAS updates data nightly).
# Uses a UNC path (does NOT depend on the Z: drive mapping).
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\run_predict_pc.ps1            # predict (daily)
#   powershell -ExecutionPolicy Bypass -File scripts\run_predict_pc.ps1 -Update    # 先拉tushare最新数据再预测
#   powershell -ExecutionPolicy Bypass -File scripts\run_predict_pc.ps1 -Train     # retrain + predict (weekly)
param([switch]$Train, [switch]$Update)

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
$pargs = @()
if ($Update) {
  if (-not $env:TUSHARE_TOKEN -and (Test-Path "$proj\data\.tushare_token")) {
    $env:TUSHARE_TOKEN = (Get-Content "$proj\data\.tushare_token" -Raw).Trim()
  }
  if ($env:TUSHARE_TOKEN) { $pargs += "--update" }
  else { Write-Host "[run_predict_pc] -Update 需要 tushare token (data\.tushare_token 或 \$env:TUSHARE_TOKEN), 跳过更新" -ForegroundColor Yellow }
}
if ($Train) { $pargs += "--train" }
Write-Host "[run_predict_pc] running: predict_qlib.py $pargs" -ForegroundColor Cyan
python scripts\predict_qlib.py @pargs
Write-Host "[run_predict_pc] done -> $($env:PREDICT_DATA_DIR)\predictions.json" -ForegroundColor Green
