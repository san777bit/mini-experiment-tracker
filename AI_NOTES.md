# AI tools: where and how

Tool: Claude Code (Claude Opus 5.5) in the Claude desktop app. It wrote most of the code and tests from a plan we agreed
on; I chose the stack and directed the work. Every claim in NOTE.md comes from a test or script run whose raw output is
in `results/`, not from the assistant's description.

## Accepted
- Overall design (per-run seq under a row lock, SSE that reads from the DB, NOTIFY as a hint, RLS as a second wall).
- Test structure: Playwright + toxiproxy e2e, isolation matrix, one test per silent failure, mutation check.

## Where the AI was wrong, and how it surfaced
- **Load-test client.** It wrote the first load generator with httpx and read the result (53 ev/s) as a server limit.
  Comparing three clients showed httpx 0.28.1 was the bottleneck (43 vs 1099 req/s). Rewritten with aiohttp.
- **NOTIFY inside the transaction.** Its original design. Correct, but it serialized all commits. Found from
  `pg_stat_activity` wait events, not by reasoning.
- **"One process on purpose" comment** in the Dockerfile: wrong, since each process has its own LISTEN. Corrected.
- **Test bugs** (results/00_red_runs.md): an impossible client-side accounting assertion, reused keys in the crash test,
  a broken expected string in the mutation script. Each was a red run that I/it investigated before changing anything.
- **Missed validation**: NUL bytes in metric keys (500, poison batch). Caught by its own test, then fixed.

- **Round 2 predictions.** Before each attack the assistant wrote down its expectation (STRESS.md). 3 of 14 were
  wrong: it expected a Postgres restart to cause errors, a 100k-point page to lag, and the API to stay dead after a
  partition healed. The measurements decided, not the predictions.
- **Self-inflicted regression.** Its per-project DB limit cut a single big project's throughput by 6% until the load
  test caught it and the limit was retuned.

## What I verified or redid myself
- TODO (fill in before sending): which files I read line by line, what I reran by hand, what I changed.
