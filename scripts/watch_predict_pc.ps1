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
    # request flags: mine(因子挖掘) / retrain / batch(因子批次标签) / loop_n(挖掘轮数)
    $rdRetrain = $false   # web 默认快速预测(复用缓存); 缓存不存在时 predict_next_day 自动回退重训
    $rdBatch = ""; $rdMine = $false; $rdLoopN = 5; $rdModelEval = $false; $rdModel = "lgb"; $rdRunAll = $false
    try {
      $rr = (Get-Content $rdReqFile -Raw | ConvertFrom-Json)
      if ($null -ne $rr.retrain)    { $rdRetrain = [bool]$rr.retrain }
      if ($null -ne $rr.batch)      { $rdBatch = [string]$rr.batch }
      if ($null -ne $rr.mine)       { $rdMine = [bool]$rr.mine }
      if ($null -ne $rr.loop_n)     { $rdLoopN = [int]$rr.loop_n }
      if ($null -ne $rr.model_eval) { $rdModelEval = [bool]$rr.model_eval }
      if ($null -ne $rr.model)      { $rdModel = [string]$rr.model }
      if ($null -ne $rr.run_all)    { $rdRunAll = [bool]$rr.run_all }
    } catch {}

    # ===== 一键全跑: 所有模型 训练+回测 + 各出买入清单 (供对比). 同步一次数据后循环。 =====
    if ($rdRunAll) {
      $models = @("lgb","xgb","catboost","ols","ridge","lasso")
      Write-Host "[watch] RUN ALL on batch '$rdBatch'..." -ForegroundColor Cyan
      Write-RdStatus "running" "一键全跑: 同步数据 (robocopy Z->C)"
      robocopy "$qlibData" "C:\qlib_data\cn_data" /MIR /MT:8 /R:1 /W:2 /XF csi300.txt csi300.txt.bak /NFL /NDL /NJH /NP | Out-Null
      if (-not $env:TUSHARE_TOKEN -and (Test-Path "$proj\data\.tushare_token")) { $env:TUSHARE_TOKEN = (Get-Content "$proj\data\.tushare_token" -Raw).Trim() }
      Write-RdStatus "running" "一键全跑: 重建 csi300 universe"
      Push-Location "C:\rdagent"; python build_csi300.py; Pop-Location
      $n = $models.Count; $i = 0
      foreach ($m in $models) {
        $i++
        Write-RdStatus "running" "一键全跑 ($i/$n): $m 训练+回测"
        wsl -e bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent && cd /mnt/c/rdagent && RDAGENT_MODEL='$m' RDAGENT_FACTOR_BATCH='$rdBatch' python run_model.py"
        if (Test-Path "C:\rdagent\model_results.json") { Copy-Item "C:\rdagent\model_results.json" (Join-Path $shared "model_results.json") -Force }
      if (Test-Path "C:\rdagent\model_runs_history.json") { Copy-Item "C:\rdagent\model_runs_history.json" (Join-Path $shared "model_runs_history.json") -Force }
        Write-RdStatus "running" "一键全跑 ($i/$n): $m 预测买入清单"
        wsl -e bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent && cd /mnt/c/rdagent && RDAGENT_RETRAIN=1 RDAGENT_MODEL='$m' RDAGENT_FACTOR_BATCH='$rdBatch' python predict_next_day.py"
        if ($LASTEXITCODE -eq 0) {
          Push-Location "C:\rdagent"
          python post_process.py
          $env:RDAGENT_TAG_BUYLIST = "1"; $env:RDAGENT_MODEL = $m; $env:RDAGENT_FACTOR_BATCH = $rdBatch
          python export_rdagent.py
          Remove-Item Env:\RDAGENT_TAG_BUYLIST -ErrorAction SilentlyContinue
          Pop-Location
        }
      }
      Write-RdStatus "done" "一键全跑完成: $n 个模型已回测+出清单, 点 📊 对比"
      Write-Host "[watch] run all done" -ForegroundColor Green
      Remove-Item $rdReqFile -Force -ErrorAction SilentlyContinue
      continue
    }

    # ===== 模型实验室: 训练指定模型 + 回测, 结果写 model_results.json (供网页对比) =====
    if ($rdModelEval) {
      Write-Host "[watch] model eval: $rdModel on batch '$rdBatch'..." -ForegroundColor Cyan
      Write-RdStatus "running" "model eval: $rdModel [batch=$rdBatch] 训练+回测中 (~几分钟)"
      wsl -e bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent && cd /mnt/c/rdagent && RDAGENT_MODEL='$rdModel' RDAGENT_FACTOR_BATCH='$rdBatch' python run_model.py"
      $meExit = $LASTEXITCODE
      if (Test-Path "C:\rdagent\model_results.json") { Copy-Item "C:\rdagent\model_results.json" (Join-Path $shared "model_results.json") -Force }
      if (Test-Path "C:\rdagent\model_runs_history.json") { Copy-Item "C:\rdagent\model_runs_history.json" (Join-Path $shared "model_runs_history.json") -Force }
      if ($meExit -eq 0) { Write-RdStatus "done" "model eval 完成: $rdModel [batch=$rdBatch]"; Write-Host "[watch] model eval done" -ForegroundColor Green }
      else { Write-RdStatus "error" "model eval $rdModel 失败 exit $meExit" }
      Remove-Item $rdReqFile -Force -ErrorAction SilentlyContinue
      continue
    }

    # ===== 因子挖掘 (RD-Agent fin_factor 演化循环, 几小时, 烧 LLM). 产出新批次但不动全局 SOTA 指针 =====
    if ($rdMine) {
      Write-Host "[watch] RD-Agent MINE (loop_n=$rdLoopN): 因子发现 (~几小时)..." -ForegroundColor Magenta
      docker ps 2>&1 | Out-Null
      if ($LASTEXITCODE -ne 0) {
        Write-RdStatus "error" "Docker 未运行, 无法挖掘 (请先启动 Docker Desktop 再试)"
        Write-Host "[watch] mine aborted: Docker 未运行" -ForegroundColor Red
        Remove-Item $rdReqFile -Force -ErrorAction SilentlyContinue
        continue
      }
      Write-RdStatus "running" "mine: 同步数据 (robocopy Z->C)"
      robocopy "$qlibData" "C:\qlib_data\cn_data" /MIR /MT:8 /R:1 /W:2 /XF csi300.txt csi300.txt.bak /NFL /NDL /NJH /NP | Out-Null
      if (-not $env:TUSHARE_TOKEN -and (Test-Path "$proj\data\.tushare_token")) { $env:TUSHARE_TOKEN = (Get-Content "$proj\data\.tushare_token" -Raw).Trim() }
      Write-RdStatus "running" "mine: 重建 csi300 universe"
      Push-Location "C:\rdagent"; python build_csi300.py; Pop-Location
      # fin_factor (Windows anaconda); 日志写到已知目录以便解析 SOTA
      $logPath = "C:\rdagent\log\mine_$(Get-Date -Format yyyyMMdd_HHmmss)"
      $env:LOG_TRACE_PATH = $logPath
      $mineLog = "C:\rdagent\daily_logs\mine_$(Get-Date -Format yyyyMMdd_HHmmss).log"
      if (-not (Test-Path "C:\rdagent\daily_logs")) { New-Item -ItemType Directory -Force "C:\rdagent\daily_logs" | Out-Null }
      Write-RdStatus "running" "mine: rdagent fin_factor loop_n=$rdLoopN (~几小时)"
      Push-Location "C:\rdagent"
      $env:CONDA_DEFAULT_ENV = "base"   # RD-Agent 因子代码在本地 conda 环境跑, 读这个变量 (base 有 qlib)
      & "D:\anaconda3\Scripts\rdagent.exe" fin_factor --loop-n $rdLoopN 2>&1 | Out-File -FilePath $mineLog -Encoding utf8
      $mineExit = $LASTEXITCODE
      Pop-Location
      Remove-Item Env:\LOG_TRACE_PATH -ErrorAction SilentlyContinue
      # fin_factor 即使非零退出(常见: LLM 限流耗尽重试), 已完成的 loop 仍有成果在 trace 里,
      # 所以不直接放弃, 尝试从 session 抢救最优 SOTA。
      if ($mineExit -ne 0) {
        Write-Host "[watch] mine: fin_factor exit $mineExit — 尝试从已完成的 loop 抢救 SOTA" -ForegroundColor Yellow
        Write-RdStatus "running" "mine: fin_factor exit $mineExit, 尝试抢救已完成 loop 的成果"
      }
      Write-RdStatus "running" "mine: 解析新 SOTA workspace"
      $newWs = (& python "C:\rdagent\resolve_sota_ws.py" $logPath | Select-Object -Last 1)
      if (-not $newWs) {
        Write-RdStatus "error" "无法解析 SOTA (fin_factor exit $mineExit, 日志 $mineLog)"
        Remove-Item $rdReqFile -Force -ErrorAction SilentlyContinue
        continue
      }
      Write-Host "[watch] mine: 新 SOTA workspace = $newWs" -ForegroundColor Green
      # 在新 workspace 上评估因子 -> 归档成新批次 (RDAGENT_SOTA_WS_OVERRIDE 不改全局指针/canonical)
      Write-RdStatus "running" "mine: factor_analysis on 新 workspace"
      wsl -e bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent && cd /mnt/c/rdagent && RDAGENT_SOTA_WS_OVERRIDE='$newWs' python factor_analysis.py"
      $faExit = $LASTEXITCODE
      Push-Location "C:\rdagent"; python export_rdagent.py; Pop-Location   # 刷新批次索引给网页下拉
      if ($faExit -eq 0) {
        $doneMsg = if ($mineExit -eq 0) { "mine 完成: 新批次已生成, 去网页下拉选它预测对比" } `
                   else { "mine 部分完成(fin_factor exit $mineExit, 已从完成的 loop 抢救): 新批次已生成" }
        Write-RdStatus "done" $doneMsg
        Write-Host "[watch] mine done (exit=$mineExit)" -ForegroundColor Green
      } else {
        Write-RdStatus "error" "factor_analysis exit $faExit"
      }
      Remove-Item $rdReqFile -Force -ErrorAction SilentlyContinue
      continue
    }

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
      if ($rdModel -and $rdModel -ne "lgb") { $stepMsg += " [model=$rdModel]" }
      Write-RdStatus "running" $stepMsg
      wsl -e bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent && cd /mnt/c/rdagent && RDAGENT_RETRAIN=$rdMode RDAGENT_FACTOR_BATCH='$rdBatch' RDAGENT_MODEL='$rdModel' python predict_next_day.py"
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
        # 给本次预测的买入清单打上 模型+批次 标签 (供网页对比)
        $env:RDAGENT_TAG_BUYLIST = "1"; $env:RDAGENT_MODEL = $rdModel; $env:RDAGENT_FACTOR_BATCH = $rdBatch
        python export_rdagent.py
        Remove-Item Env:\RDAGENT_TAG_BUYLIST -ErrorAction SilentlyContinue
        Pop-Location
        if ($pp -eq 0) { Write-RdStatus "done" "done"; Write-Host "[watch] RD-Agent done" -ForegroundColor Green }
        else { Write-RdStatus "error" "post_process exit $pp"; Write-Host "[watch] RD-Agent post_process failed $pp" -ForegroundColor Red }
      }
    }
    Remove-Item $rdReqFile -Force -ErrorAction SilentlyContinue
  }
  Start-Sleep -Seconds 15
}
