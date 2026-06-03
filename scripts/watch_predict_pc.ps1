# PC 常驻监听 (方案B + 网页按钮)。
# 发现共享目录里的 predict_request.json (由 NAS 网页按钮写入) 就跑预测,
# 跑完写 predict_status.json, 并删掉请求文件。NAS 网页轮询状态。
#
# 启动 (开机后跑一次, 一直挂着):
#   powershell -ExecutionPolicy Bypass -File scripts\watch_predict_pc.ps1
# Ctrl+C 退出。

$ErrorActionPreference = "Continue"
$proj = Split-Path -Parent $PSScriptRoot
$shared = "Z:\claude\qlib\data\csv_tmp"
$reqFile = Join-Path $shared "predict_request.json"
$statusFile = Join-Path $shared "predict_status.json"

$env:QLIB_DATA_PATH      = "Z:\claude\qlib\data\cn_data"
$env:PARQUET_DIR         = "Z:\claude\qlib\data\csv_tmp\tushare_daily"
$env:PREDICT_DATA_DIR    = $shared
$env:STOCK_META_DB       = Join-Path $proj "data\stock_meta.db"
$env:QLIB_KERNELS        = "8"
$env:PREDICT_TRAIN_START = "2020-01-01"

function Write-Status($state, $msg) {
  $obj = @{ state = $state; msg = $msg; updated_at = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") }
  ($obj | ConvertTo-Json -Compress) | Out-File -FilePath $statusFile -Encoding utf8
}

if (-not (Test-Path $env:STOCK_META_DB)) {
  Write-Host "缺少 stock_meta.db, 先构建 (见 run_predict_pc.ps1 提示)" -ForegroundColor Yellow
  exit 1
}

Write-Host "[watch] 监听 $reqFile  (每 15s 检查一次, Ctrl+C 退出)" -ForegroundColor Cyan
Write-Status "idle" "等待请求"
Set-Location $proj

while ($true) {
  if (Test-Path $reqFile) {
    $retrain = $false
    try { $retrain = [bool]((Get-Content $reqFile -Raw | ConvertFrom-Json).retrain) } catch {}
    $tag = if ($retrain) { "(含重训)" } else { "" }
    Write-Host "[watch] 收到请求 retrain=$retrain, 开始预测 $tag" -ForegroundColor Yellow
    Write-Status "running" ("PC 预测中 $tag")
    try {
      if ($retrain) { python scripts\predict_qlib.py --train } else { python scripts\predict_qlib.py }
      if ($LASTEXITCODE -eq 0) {
        Write-Status "done" "完成"
        Write-Host "[watch] 完成" -ForegroundColor Green
      } else {
        Write-Status "error" "预测脚本退出码 $LASTEXITCODE"
        Write-Host "[watch] 失败 退出码 $LASTEXITCODE" -ForegroundColor Red
      }
    } catch {
      Write-Status "error" $_.Exception.Message
    }
    Remove-Item $reqFile -Force -ErrorAction SilentlyContinue
  }
  Start-Sleep -Seconds 15
}
