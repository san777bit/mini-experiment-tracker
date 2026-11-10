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

- **Round 3 benchmark artifact.** Its noisy-neighbour script ran victim and attackers in one event loop; after the
  fix the victim looked no better. Splitting attackers into separate processes showed part of round 2's "185 ms" was
  the generator itself. Same class of mistake as the httpx one; all round-3 numbers were re-taken.
- **Round 3 prediction.** It expected a request-count budget to cover big bodies; it did not (byte budget added).

- **Round 4.** It set the per-process request budget too low in round 3; a legitimate load got 429s and the
  generator hid it (latency printed, errors not). One of its timers measured the wrong row of a batch. Three of its
  optimizations (commit_delay, pool split, access log off) did nothing measurable and were reverted.

- **Round 5.** Asked to check the work against the task, it found its own round-4 change had removed the second
  isolation wall from the live path without saying so, and that the design had never fit on one page. It also
  claimed the note was ~3 pages before measuring; measured, it was 2.

## What I verified or redid myself
- **Read closely:** the frontend (`api/app/static/`: the runs list, the live run page, the SSE client and its
  seq/gap handling) and the overall architecture (ingest path, per-run seq, hub/NOTIFY fan-out, RLS), against
  DESIGN.md and CHANGES.md. I did not read the load scripts or every test line by line.
- **Ran by hand:** `docker compose up` from scratch, created a project and runs in the browser, drove a run with
  `scripts/demo_producer.py --chaos --lose-every` and watched the page (conflicts, "reported, not stored" tile),
  rotated the key with a page open, and cut the network in DevTools to watch the reconnect and catch-up.
- **Found myself:** the "New run" form was visible before any key was entered (a `display:flex` rule overrode
  `hidden`); reported it, the fix is the `[hidden]` rule in `style.css`. I also directed the scope of rounds 3-5
  and the UI redesign, and rejected results I did not find convincing (e.g. latency after round 3).
