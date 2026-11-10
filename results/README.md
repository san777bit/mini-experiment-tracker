# Raw outputs

Terminal output as it came out of each run, not edited. Failed runs are kept: start with
[00_red_runs.md](00_red_runs.md). Where a file has `before` / `after` in its name, `before` is the code before the
fix it was measured for. The docs that interpret these files: [NOTE.md](../NOTE.md), [STRESS.md](../STRESS.md),
[PERF.md](../PERF.md).

## Tests on the final code
| File | What |
|---|---|
| `round5_test_*.txt` | each test file run with `-s -v`, including the printed evidence (`db=750 page=750 ...`) |
| `full_suite.txt` | whole suite, `-q` |
| `full_suite_r3_first.txt` | first full run after round 3 |
| `mutations.txt` | mutation check: each planted bug and whether a test caught it |
| `onekey_150k_before.txt` / `_after.txt` | one metric with 150k steps: chart crash, then the fix |
| `silent_failures.txt` | round-1 run of the silent-failure tests |

## Load (task point 5, 50 runs x 10 events/s)
| File | What |
|---|---|
| `load_50x10*.txt` | 50x10 over the rounds; `_round3_ab` is an A/B, `_httpx_INVALID` is a generator artefact kept as a warning |
| `load_100x10.txt`, `load_200x10_batch10.txt` | beyond the target |
| `perf_baseline_*.txt` | round-4 starting point |
| `perf_o*_*.txt` | one file per optimisation tried in round 4, kept or reverted (see PERF.md) |
| `perf_diag_sync_commit_off.txt` | diagnostic only: WAL fsync cost |
| `perf_final_*.txt`, `perf_round5_final.txt` | final latency and capacity |

## Stress and isolation (STRESS.md)
| File | What |
|---|---|
| `stress_edges_*`, `stress_heavy_before.txt` | round-2 edge cases and infrastructure failures |
| `stress_bigbody_*` | 1.9 MB bodies from a neighbour; `_sameloop` files are the invalid same-event-loop measurement |
| `stress_hot_run_*` | one hot run under attack; `_r3_` = round 3 |
| `stress_ignore429_600w_*` | 600 writers that ignore 429 |
| `stress_fanout_*` | 1000 viewers |
| `stress_partition_*` | API cut off from Postgres |
| `timing_*.txt` | timing side channel: foreign vs missing run IDs |
