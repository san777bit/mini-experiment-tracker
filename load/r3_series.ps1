# Runs both noisy-neighbour attacks 3 times each. Usage: .\load\r3_series.ps1 <suffix>
param([string]$Suffix)
$root = Split-Path -Parent $PSScriptRoot
foreach ($mode in @(@{n="stress_hot_run_r3"; a=@()}, @{n="stress_bigbody"; a=@("--pad-kb","1900")})) {
  $f = Join-Path $root "results\$($mode.n)_$Suffix.txt"
  Remove-Item $f -ErrorAction SilentlyContinue
  foreach ($i in 1..3) {
    "=== run $i" | Out-File -Append -Encoding utf8 $f
    & (Join-Path $PSScriptRoot run_in_network.ps1) stress_hot_run.py --writers 200 --seconds 15 @($mode.a) | Out-File -Append -Encoding utf8 $f
  }
}
