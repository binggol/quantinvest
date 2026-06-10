# 直接重跑某批次的全部模型 (train+backtest), 绕过 watcher, 每个模型完成后把结果拷到共享目录。
# 用途: watcher 死掉导致 run-all 没产出时, 手动补跑, 网页(模型试验室/回测对比)读共享目录即可看到。
param(
  [string]$Batch = "20260608_2321",
  [string[]]$Models = @("lgb","xgb","catboost","ols","ridge","lasso")
)
$ErrorActionPreference = "Continue"
$shared = $env:SHARED_DIR
if (-not $shared) { $shared = "\\192.168.0.106\docker\obsidian\vaults\claude\qlib\data\csv_tmp" }
$log = "C:\rdagent\daily_logs\run_batch_$(Get-Date -Format yyyyMMdd_HHmmss).log"
if (-not (Test-Path "C:\rdagent\daily_logs")) { New-Item -ItemType Directory -Force "C:\rdagent\daily_logs" | Out-Null }
function Log($m){ $line = "[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $m; Write-Host $line; Add-Content -Path $log -Value $line -Encoding utf8 }

function CopyResults {
  foreach ($f in 'model_results.json','model_curves.json','model_runs_history.json') {
    if (Test-Path "C:\rdagent\$f") { Copy-Item "C:\rdagent\$f" (Join-Path $shared $f) -Force }
  }
}

$n = $Models.Count; $i = 0
Log "==== 补跑 batch=$Batch, 共 $n 个模型 ===="
foreach ($m in $Models) {
  $i++
  Log "($i/$n) $m 训练+回测 开始"
  wsl -e bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent && cd /mnt/c/rdagent && RDAGENT_MODEL='$m' RDAGENT_FACTOR_BATCH='$Batch' python run_model.py" 2>&1 | Add-Content -Path $log -Encoding utf8
  $ex = $LASTEXITCODE
  if ($ex -eq 0) { Log "($i/$n) $m 完成, 拷结果到共享目录"; CopyResults }
  else { Log "($i/$n) $m 失败 exit=$ex (跳过, 继续下一个)" }
}
Log "==== 全部完成 ===="
