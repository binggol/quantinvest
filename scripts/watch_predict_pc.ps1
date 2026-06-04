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
$rdReqFile    = Join-Path $shared "rdagent_request.json"
$rdStatusFile = Join-Path $shared "rdagent_status.json"

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
function Write-RdStatus($state, $msg) {
  $obj = @{ state = $state; msg = $msg; updated_at = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") }
  ($obj | ConvertTo-Json -Compress) | Out-File -FilePath $rdStatusFile -Encoding utf8
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

  if (Test-Path $rdReqFile) {
    # request flags: retrain ($true=full retrain, $false=reuse cached); batch=因子批次标签(空=默认)
    $rdRetrain = $false   # web 默认快速预测(复用缓存); 缓存不存在时 predict_next_day 自动回退重训
    $rdBatch = ""
    try {
      $rr = (Get-Content $rdReqFile -Raw | ConvertFrom-Json)
      if ($null -ne $rr.retrain) { $rdRetrain = [bool]$rr.retrain }
      if ($null -ne $rr.batch)   { $rdBatch = [string]$rr.batch }
    } catch {}
    $rdMode = if ($rdRetrain) { "1" } else { "0" }
    Write-Host "[watch] RD-Agent request (retrain=$rdRetrain batch='$rdBatch'): sync data + predict..." -ForegroundColor Yellow
    Write-RdStatus "running" "sync data (robocopy Z->C)"
    # 1) Windows robocopy 同步 Z->C (快; WSL rsync 走 /mnt/z 网络盘太慢). 源用 UNC, 不依赖盘符。
    robocopy "$qlibData" "C:\qlib_data\cn_data" /MIR /MT:8 /R:1 /W:2 /XF csi300.txt csi300.txt.bak /NFL /NDL /NJH /NP | Out-Null
    if ($LASTEXITCODE -ge 8) {
      Write-RdStatus "error" "robocopy failed $LASTEXITCODE"
      Write-Host "[watch] RD-Agent robocopy failed $LASTEXITCODE" -ForegroundColor Red
    } else {
      if (-not $env:TUSHARE_TOKEN -and (Test-Path "$proj\data\.tushare_token")) {
        $env:TUSHARE_TOKEN = (Get-Content "$proj\data\.tushare_token" -Raw).Trim()
      }
      # 1.5) 重建 csi300 universe (Windows python). 否则成分股 end_date 过时 -> 最新交易日股池缩水
      Write-RdStatus "running" "rebuild csi300 universe"
      Push-Location "C:\rdagent"; python build_csi300.py; Pop-Location
      # 2) predict_next_day 在 WSL(miniconda rdagent env, 有 qlib) 跑, 用 /mnt 路径
      #    RDAGENT_RETRAIN=1 全量重训(~15min); =0 复用缓存模型只预测(快)
      $stepMsg = if ($rdRetrain) { "predict (WSL full retrain)" } else { "predict (WSL no-retrain, cached model)" }
      if ($rdBatch) { $stepMsg += " [batch=$rdBatch]" }
      Write-RdStatus "running" $stepMsg
      wsl -e bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent && cd /mnt/c/rdagent && RDAGENT_RETRAIN=$rdMode RDAGENT_FACTOR_BATCH='$rdBatch' python predict_next_day.py"
      if ($LASTEXITCODE -ne 0) {
        Write-RdStatus "error" "predict_next_day exit $LASTEXITCODE"
        Write-Host "[watch] RD-Agent predict failed $LASTEXITCODE" -ForegroundColor Red
      } else {
        # 3) post_process + export 在 Windows python 跑 (用 C:/ 路径 + tushare)
        Write-RdStatus "running" "post-process + export (Windows)"
        if (-not $env:TUSHARE_TOKEN -and (Test-Path "$proj\data\.tushare_token")) {
          $env:TUSHARE_TOKEN = (Get-Content "$proj\data\.tushare_token" -Raw).Trim()
        }
        Push-Location "C:\rdagent"
        python post_process.py
        $pp = $LASTEXITCODE
        python export_rdagent.py
        Pop-Location
        if ($pp -eq 0) { Write-RdStatus "done" "done"; Write-Host "[watch] RD-Agent done" -ForegroundColor Green }
        else { Write-RdStatus "error" "post_process exit $pp"; Write-Host "[watch] RD-Agent post_process failed $pp" -ForegroundColor Red }
      }
    }
    Remove-Item $rdReqFile -Force -ErrorAction SilentlyContinue
  }
  Start-Sleep -Seconds 15
}
