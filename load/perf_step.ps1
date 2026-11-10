# Two 40 s load runs with server timings; prints a one-line summary per run. Usage: .\load\perf_step.ps1 <name>
param([string]$Name, [string]$Extra = "")
$root = Split-Path -Parent $PSScriptRoot
$f = Join-Path $root "results\perf_$Name.txt"
Remove-Item $f -ErrorAction SilentlyContinue
foreach ($i in 1..2) {
  $o = & (Join-Path $PSScriptRoot run_in_network.ps1) loadtest.py --runs 50 --rate 10 --duration 40 --timings @($Extra -split " " | Where-Object { $_ })
  "=== run $i" | Out-File -Append -Encoding utf8 $f; $o | Out-File -Append -Encoding utf8 $f
  $d = ($o -join "`n") | ConvertFrom-Json; $s = $d.server_ms
  "{0} r{1}: {2} ev/s ingest p50/p99 {3}/{4} e2e p50/p99 {5}/{6} lost {7} errors {17} client_lag p50/p99 {18}/{19} srv_req p50/p99 {20}/{21} | conn_wait p95/p99 {8}/{9} acq/event {10:N1} fn p99 {11} lag p99 {12} | tail_fetch p50/p99 {13}/{14} txn_to_tail p50/p99 {15}/{16} hub_wait p99 {22} txn_to_sse p50/p99 {23}/{24} || send>txn {25} txn>ack {26} txn>viewer {27} ack>viewer {28} sse_send p50/p99 {29}/{30}" -f $Name, $i, $d.achieved_events_per_s, $d.ingest_ms.p50, $d.ingest_ms.p99, $d.e2e_ms.p50, $d.e2e_ms.p99, $d.viewer_lost_events, $s.db_conn_wait_main.p95, $s.db_conn_wait_main.p99, (($s.db_conn_wait_main.n + $s.db_conn_wait_hub.n) / [math]::Max(1,$s.ingest_fn.n)), $s.ingest_fn.p99, $s.loop_lag.p99, $s.tail_fetch.p50, $s.tail_fetch.p99, $s.txn_start_to_tail.p50, $s.txn_start_to_tail.p99, ($d.request_errors | ConvertTo-Json -Compress), $d.client_loop_lag_ms.p50, $d.client_loop_lag_ms.p99, $s.request_ingest_200.p50, $s.request_ingest_200.p99, $s.db_conn_wait_hub.p99, $s.txn_start_to_sse.p50, $s.txn_start_to_sse.p99, ("{0}/{1}" -f $d.e2e_segments_ms.send_to_txn.p50, $d.e2e_segments_ms.send_to_txn.p99), ("{0}/{1}" -f $d.e2e_segments_ms.txn_to_ack.p50, $d.e2e_segments_ms.txn_to_ack.p99), ("{0}/{1}" -f $d.e2e_segments_ms.txn_to_viewer.p50, $d.e2e_segments_ms.txn_to_viewer.p99), ("{0}/{1}" -f $d.e2e_segments_ms.ack_to_viewer.p50, $d.e2e_segments_ms.ack_to_viewer.p99), $s.sse_send.p50, $s.sse_send.p99
}
