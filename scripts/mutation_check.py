"""Do the tests catch the bugs they claim to catch?

For each mutation: patch the API source, rebuild, run the targeted tests, restore.
A mutation whose anchor text is missing aborts the script (no silent no-op mutations).

    .venv/Scripts/python scripts/mutation_check.py > results/mutations.txt
"""
from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "api" / "app" / "main.py"
PY = str(ROOT / ".venv" / "Scripts" / "python.exe") if sys.platform == "win32" else sys.executable


@dataclass
class Mutation:
    name: str
    expect: str                        # "red" or "green" (and why)
    edits: list[tuple[str, str]]       # (original, replacement) in main.py
    tests: list[str]
    sql_before: list[str] = field(default_factory=list)
    sql_after: list[str] = field(default_factory=list)


LOCK_BLOCK = '''        last_seq = await conn.fetchval(
            "SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2 FOR UPDATE",
            rid, auth.project_id,
        )
        if last_seq is None:
            raise NOT_FOUND'''

M1_BLOCK = '''        if await conn.fetchval("SELECT 1 FROM runs WHERE id = $1 AND project_id = $2", rid, auth.project_id) is None:
            raise NOT_FOUND
        # MUTATION: seq range reserved in a separate, already committed statement (BIGSERIAL-like)
        async with db.project_tx(auth.project_id) as c2:
            last_seq = await c2.fetchval(
                "UPDATE runs SET last_seq = last_seq + $2 WHERE id = $1 RETURNING last_seq - $2",
                rid, len(body.events))'''

MUTATIONS = [
    Mutation(
        "M1 seq allocated outside the ingest transaction (no per-run serialization)",
        "red: reader sees holes / out-of-order commits",
        [(LOCK_BLOCK, M1_BLOCK),
         ("UPDATE runs SET last_seq = $2, conflicts", "UPDATE runs SET last_seq = greatest(last_seq, $2), conflicts")],
        ["tests/test_silent_failures.py::test_events_lost_on_reconnect_with_concurrent_writers"],
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
        "M5 no NOTIFY and no local wake-up at all",
        "green: slower, but NOTIFY is only a hint and the 2 s poll still delivers everything",
        [("        HUB.wake(rid)\n", "        pass  # MUTATION\n"),
         ("""await db.execute("SELECT pg_notify('run_events', $1)", f"{rid}:{last_seq}")""",
          "pass  # MUTATION")],
        ["tests/test_silent_failures.py::test_events_lost_on_reconnect_with_concurrent_writers",
         "tests/test_silent_failures.py::test_lost_notify_does_not_lose_events"],
    ),
]


def sh(*cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")


def rebuild() -> None:
    r = sh("docker", "compose", "up", "-d", "--build", "--wait", "api")
    if r.returncode != 0:
        raise SystemExit(f"rebuild failed:\n{r.stdout}\n{r.stderr}")


def psql(stmt: str) -> None:
    r = sh("docker", "compose", "exec", "-T", "db", "psql", "-U", "owner", "-d", "tracker", "-c", stmt)
    if r.returncode != 0:
        raise SystemExit(f"psql failed: {r.stderr}")


def main() -> None:
    original = MAIN.read_text(encoding="utf-8")
    summary = []
    try:
        for m in MUTATIONS:
            src = original
            for old, new in m.edits:
                if old not in src:
                    raise SystemExit(f"[{m.name}] anchor not found, mutation would be a no-op:\n{old}")
                src = src.replace(old, new, 1)
            MAIN.write_text(src, encoding="utf-8")
            rebuild()
            for s in m.sql_before:
                psql(s)
            r = sh(PY, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *m.tests)
            for s in m.sql_after:
                psql(s)
            outcome = "green" if r.returncode == 0 else "red"
            ok = outcome == m.expect.split(":")[0]
            tail = "\n".join((r.stdout + r.stderr).strip().splitlines()[-12:])
            print(f"=== {m.name}\nexpected: {m.expect}\nactual:   {outcome}  -> {'AS EXPECTED' if ok else 'SURPRISE'}\n{tail}\n",
                  flush=True)
            summary.append(f"{'OK      ' if ok else 'SURPRISE'} {outcome:5s} {m.name}")
    finally:
        MAIN.write_text(original, encoding="utf-8")
        rebuild()
    print("=== summary")
    print("\n".join(summary))


if __name__ == "__main__":
    main()
