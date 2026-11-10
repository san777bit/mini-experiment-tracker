"""Do the tests catch the bugs they claim to catch?

For each mutation: patch the API source (and/or the ingest_batch SQL function), rebuild, run the
targeted tests, restore. A mutation whose anchor text is missing aborts the script (no silent
no-op mutations).

    .venv/Scripts/python scripts/mutation_check.py > results/mutations.txt
"""
from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "api" / "app"
MAIN = APP / "main.py"
INIT_SQL = ROOT / "db" / "init.sql"
PY = str(ROOT / ".venv" / "Scripts" / "python.exe") if sys.platform == "win32" else sys.executable


@dataclass
class Mutation:
    name: str
    expect: str                        # "red" or "green" (and why)
    edits: list[tuple]                 # (original, replacement) in main.py, or (file, original, replacement)
    tests: list[str]
    sql_before: list[str] = field(default_factory=list)
    sql_after: list[str] = field(default_factory=list)
    fn_edits: list[tuple[str, str]] = field(default_factory=list)   # applied to ingest_batch


def ingest_fn() -> str:
    """The ingest_batch definition from init.sql, as a CREATE OR REPLACE statement."""
    sql = INIT_SQL.read_text(encoding="utf-8")
    start = sql.index("CREATE FUNCTION ingest_batch(")
    end = sql.index("$$;", start) + 3
    return "CREATE OR REPLACE" + sql[start:end][len("CREATE"):]


MUTATIONS = [
    Mutation(
        "M1 seq from a global sequence, no run row lock (BIGSERIAL-style)",
        "red: reader sees holes / out-of-order commits",
        [],
        ["tests/test_silent_failures.py::test_events_lost_on_reconnect_with_concurrent_writers"],
        sql_before=["CREATE SEQUENCE IF NOT EXISTS m1_seq", "GRANT USAGE ON SEQUENCE m1_seq TO app"],
        fn_edits=[
            ("AND r.project_id = p_project FOR UPDATE;", "AND r.project_id = p_project;"),
            ("v_last + row_number() OVER (ORDER BY ord)", "nextval('m1_seq')"),
            ("o_last_seq := v_last + o_accepted;",
             "o_last_seq := (SELECT max(seq) FROM metrics WHERE run_id = p_run);"),
            ("SET last_seq = o_last_seq,", "SET last_seq = greatest(runs.last_seq, o_last_seq),"),
        ],
    ),
    Mutation(
        "M2 stream resumes from Last-Event-ID + 1 (off by one)",
        "red: one event lost per reconnect; the page counts gaps",
        [("cursor = int(last_event_id) if last_event_id is not None",
          "cursor = int(last_event_id) + 1 if last_event_id is not None")],
        ["tests/test_silent_failures.py::test_events_lost_on_reconnect_with_concurrent_writers",
         "tests/test_consistency_e2e.py::test_page_matches_db_under_chaos"],
    ),
    Mutation(
        "M3a project filter removed from GET /api/runs/{id} (RLS still on)",
        "green: RLS is the second wall",
        [('f"SELECT {RUN_COLS} FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id',
          'f"SELECT {RUN_COLS} FROM runs WHERE id = $1", rid')],
        ["tests/test_isolation.py::test_foreign_run_is_indistinguishable_from_missing"],
    ),
    Mutation(
        "M3b project filter removed AND RLS disabled on runs",
        "red: A reads B's run",
        [('f"SELECT {RUN_COLS} FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id',
          'f"SELECT {RUN_COLS} FROM runs WHERE id = $1", rid')],
        ["tests/test_isolation.py::test_foreign_run_is_indistinguishable_from_missing"],
        sql_before=["ALTER TABLE runs DISABLE ROW LEVEL SECURITY"],
        sql_after=["ALTER TABLE runs ENABLE ROW LEVEL SECURITY"],
    ),
    Mutation(
        "M4 rotation issues a new key but does not revoke the old one",
        "red: old key keeps working",
        [('"UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL RETURNING id"',
          '"SELECT id FROM api_keys WHERE id = $1"  -- MUTATION: no revoke')],
        ["tests/test_silent_failures.py::test_old_key_stops_working_after_rotation"],
    ),
    Mutation(
        "M5 no push, no NOTIFY, no local wake-up at all",
        "green: slower, but pushes/NOTIFY are only hints and the 2 s poll still delivers everything",
        [("        HUB.publish(rid, auth.project_id, rows, last_seq)", "        pass  # MUTATION")],
        ["tests/test_silent_failures.py::test_events_lost_on_reconnect_with_concurrent_writers",
         "tests/test_silent_failures.py::test_lost_notify_does_not_lose_events"],
    ),
    # --- round 2/3 fixes --------------------------------------------------------------------
    Mutation(
        "M6 key re-check removed from ingest_batch",
        "red: a write authenticated before rotation commits after it",
        [],
        ["tests/test_stress_edges.py::test_write_authenticated_before_rotation_cannot_commit_after_it"],
        fn_edits=[("IF NOT FOUND THEN o_status := 'revoked'; RETURN; END IF;", "-- MUTATION: no re-check")],
    ),
    Mutation(
        "M7 rejected-key cache disabled",
        "red: every request with a revoked key is a DB lookup again",
        [(APP / "auth.py", "        if digest in self.bad:\n", "        if False:  # MUTATION\n")],
        ["tests/test_round3.py::test_requests_with_a_rejected_key_do_not_touch_the_db"],
    ),
    Mutation(
        "M8 fair gate degraded to FIFO",
        "red: a quiet tenant waits behind a hot tenant's whole queue",
        [(APP / "limits.py", "key=lambda t: (self.held.get(t, 0), self.waiting[t][0][0])",
          "key=lambda t: self.waiting[t][0][0]")],
        ["tests/test_round3.py::test_gate_serves_a_quiet_tenant_before_a_hot_tenants_queue"],
    ),
    # --- round 4 ---------------------------------------------------------------------------
    Mutation(
        "M9 pushed rows appended without the contiguity check",
        "red: out-of-order NOTIFYs from two processes put a hole/disorder in the live tail",
        [(APP / "hub.py", "            if r.seq != self.last_seq + 1:\n                return False\n",
          "            pass  # MUTATION\n")],
        ["tests/test_silent_failures.py::test_events_lost_on_reconnect_with_concurrent_writers",
         "tests/test_round4.py::test_push_with_a_hole_is_refused_and_falls_back_to_the_db"],
    ),
    Mutation(
        "M10 project check removed from pushed rows",
        "red: rows claiming another project reach the tail (no RLS on the push path)",
        [(APP / "hub.py", "        if project_id != self.project_id:\n", "        if False:  # MUTATION\n")],
        ["tests/test_round4.py::test_pushed_rows_claiming_another_project_never_reach_the_tail"],
    ),
    Mutation(
        "M11 revocation NOTIFY ignored by other processes",
        "red: a stream opened with a rotated key keeps streaming in other processes",
        [(APP / "hub.py", "                self.key_revoked(UUID(msg[\"k\"]))", "                pass  # MUTATION")],
        ["tests/test_round4.py::test_revocation_from_another_process_closes_streams_and_drops_cached_auth"],
    ),
    Mutation(
        "M12 server ignores client_points",
        "red: points that never arrived are invisible again",
        [],
        ["tests/test_round4.py::test_points_the_server_never_received_are_visible_and_alertable"],
        fn_edits=[("client_points = greatest(runs.client_points, p_client_points)",
                   "client_points = runs.client_points")],
    ),
]


def sh(*cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")


def rebuild() -> None:
    r = sh("docker", "compose", "up", "-d", "--build", "--wait", "api")
    if r.returncode != 0:
        raise SystemExit(f"rebuild failed:\n{r.stdout}\n{r.stderr}")


def psql(stmt: str) -> None:
    r = sh("docker", "compose", "exec", "-T", "db", "psql", "-v", "ON_ERROR_STOP=1",
           "-U", "owner", "-d", "tracker", "-c", stmt)
    if r.returncode != 0:
        raise SystemExit(f"psql failed: {r.stderr}")


def main() -> None:
    files = [MAIN, APP / "auth.py", APP / "limits.py", APP / "hub.py"]
    originals = {f: f.read_text(encoding="utf-8") for f in files}
    fn_original = ingest_fn()
    summary = []
    try:
        for m in MUTATIONS:
            srcs = dict(originals)
            for edit in m.edits:
                path, old, new = edit if len(edit) == 3 else (MAIN, *edit)
                if old not in srcs[path]:
                    raise SystemExit(f"[{m.name}] anchor not found, mutation would be a no-op:\n{old}")
                srcs[path] = srcs[path].replace(old, new, 1)
            fn = fn_original
            for old, new in m.fn_edits:
                if old not in fn:
                    raise SystemExit(f"[{m.name}] SQL anchor not found, mutation would be a no-op:\n{old}")
                fn = fn.replace(old, new, 1)
            for f in files:
                f.write_text(srcs[f], encoding="utf-8", newline="\n")
            rebuild()
            for s in m.sql_before:
                psql(s)
            if m.fn_edits:
                psql(fn)
            r = sh(PY, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *m.tests)
            if m.fn_edits:
                psql(fn_original)
            for s in m.sql_after:
                psql(s)
            outcome = "green" if r.returncode == 0 else "red"
            ok = outcome == m.expect.split(":")[0]
            tail = "\n".join((r.stdout + r.stderr).strip().splitlines()[-12:])
            print(f"=== {m.name}\nexpected: {m.expect}\nactual:   {outcome}  -> {'AS EXPECTED' if ok else 'SURPRISE'}\n{tail}\n",
                  flush=True)
            summary.append(f"{'OK      ' if ok else 'SURPRISE'} {outcome:5s} {m.name}")
    finally:
        for f, text in originals.items():
            f.write_text(text, encoding="utf-8", newline="\n")
        psql(fn_original)
        rebuild()
    print("=== summary")
    print("\n".join(summary))


if __name__ == "__main__":
    main()
