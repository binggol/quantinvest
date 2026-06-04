# PC resident watcher (Plan B + web button).
# Watches the shared folder for predict_request.json (written by the NAS web button),
# runs the prediction, writes predict_status.json, deletes the request.
# Uses a UNC path so it does NOT depend on the Z: drive mapping (works in any shell,
# including elevated PowerShell where mapped drives are not visible).
#
# Start (keep the window open):
#   powershell -ExecutionPolicy Bypass -File scripts\watch_predict_pc.ps1
# Ctrl+C to stop.

$ErrorActionPreference = "Continue"
$proj = Split-Path -Parent $PSScriptRoot

# Shared dir = NAS qlib data 'csv_tmp', via UNC. Adjust the IP if your NAS changes,
# or override with  $env:SHARED_DIR  before launching.
$shared = $env:SHARED_DIR
if (-not $shared) { $shared = "\\192.168.0.106\docker\obsidian\vaults\claude\qlib\data\csv_tmp" }
$qlibData = (Split-Path $shared -Parent) + "\cn_data"

$reqFile    = Join-Path $shared "predict_request.json"
$statusFile = Join-Path $shared "predict_status.json"

$env:QLIB_DATA_PATH      = $qlibData
$env:PARQUET_DIR         = Join-Path $shared "tushare_daily"
$env:PREDICT_DATA_DIR    = $shared
$env:STOCK_META_DB       = Join-Path $proj "data\stock_meta.db"
$env:QLIB_KERNELS        = "8"
$env:PREDICT_TRAIN_START = "2020-01-01"

function Write-Status($state, $msg) {
  $obj = @{ state = $state; msg = $msg; updated_at = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") }
  ($obj | ConvertTo-Json -Compress) | Out-File -FilePath $statusFile -Encoding utf8
}

if (-not (Test-Path $shared)) {
  Write-Host "[watch] shared dir not reachable: $shared" -ForegroundColor Red
  Write-Host "        check the NAS is online and the UNC path is correct (or set `$env:SHARED_DIR)."
  exit 1
}
if (-not (Test-Path $env:STOCK_META_DB)) {
  Write-Host "[watch] missing stock_meta.db at $($env:STOCK_META_DB) (build it, see run_predict_pc.ps1)" -ForegroundColor Yellow
  exit 1
}

Write-Host "[watch] watching $reqFile  (every 15s, Ctrl+C to stop)" -ForegroundColor Cyan
Write-Status "idle" "waiting"
Set-Location $proj

while ($true) {
  if (Test-Path $reqFile) {
    $retrain = $false; $update = $false
    try { $r = (Get-Content $reqFile -Raw | ConvertFrom-Json); $retrain = [bool]$r.retrain; $update = [bool]$r.update } catch {}
    $pargs = @()
    if ($update) {
      if (-not $env:TUSHARE_TOKEN -and (Test-Path "$proj\data\.tushare_token")) {
        $env:TUSHARE_TOKEN = (Get-Content "$proj\data\.tushare_token" -Raw).Trim()
      }
      if ($env:TUSHARE_TOKEN) { $pargs += "--update" }
      else { Write-Host "[watch] update requested but no TUSHARE_TOKEN (data\.tushare_token), skipping update" -ForegroundColor Yellow }
    }
    if ($retrain) { $pargs += "--train" }
    Write-Host "[watch] request (update=$update retrain=$retrain), running: predict_qlib.py $pargs" -ForegroundColor Yellow
    Write-Status "running" "predicting"
    try {
      python scripts\predict_qlib.py @pargs
      if ($LASTEXITCODE -eq 0) {
        Write-Status "done" "done"; Write-Host "[watch] done" -ForegroundColor Green
      } else {
        Write-Status "error" "exit $LASTEXITCODE"; Write-Host "[watch] failed (exit $LASTEXITCODE)" -ForegroundColor Red
      }
    } catch {
      Write-Status "error" $_.Exception.Message
    }
    Remove-Item $reqFile -Force -ErrorAction SilentlyContinue
  }
  Start-Sleep -Seconds 15
}
