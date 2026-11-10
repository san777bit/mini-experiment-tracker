"""Round 4: rows pushed to live tails (no DB read on the hot path) must never create or hide a hole.

Unit tests on RunTail.push / Hub payload handling, in-process (no server, no DB access).
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))
from app import hub  # noqa: E402
from app.hub import Hub, Row, RunTail  # noqa: E402


def rows(*seqs: int) -> list[Row]:
    return [Row(s, "loss", s, s / 10, 0.0) for s in seqs]


def tail_at(last_seq: int) -> RunTail:
    return RunTail(uuid.uuid4(), uuid.uuid4(), last_seq)


async def test_push_appends_contiguous_rows_and_publishes():
    t = tail_at(10)
    ev = t.changed()
    t.push(rows(11, 12), 12)
    assert [r.seq for r in t.rows] == [11, 12] and t.last_seq == 12
    assert ev.is_set() and t.status[0] == 12
    assert not t.wake.is_set()          # no DB read needed


async def test_push_with_a_hole_is_refused_and_falls_back_to_the_db():
    """Two processes' NOTIFYs for one run can arrive out of order (13 before 11..12)."""
    t = tail_at(10)
    t.push(rows(13), 13)
    assert t.rows == [] and t.last_seq == 10   # nothing appended past the hole
    assert t.wake.is_set()                     # the DB read will fill 11..13 in order


async def test_push_ignores_rows_it_already_has():
    t = tail_at(10)
    t.push(rows(11, 12), 12)
    t.push(rows(11, 12, 13), 13)               # e.g. a DB read raced the push
    assert [r.seq for r in t.rows] == [11, 12, 13]


async def test_push_none_means_read_the_db():
    t = tail_at(10)
    t.push(None, 11)
    assert t.rows == [] and t.wake.is_set()


async def test_notify_payload_round_trip_keeps_values_exact():
    """Rows travel as JSON between processes: floats and unicode keys must survive bit-exact."""
    h = Hub()
    run = uuid.uuid4()
    t = RunTail(run, uuid.uuid4(), 0)
    h._tails[run] = [t, 1]
    vals = [-0.0, 5e-324, 1.7976931348623157e308, 0.1 + 0.2]
    sent = [Row(i + 1, "précision/лосс", i, v, 0.0) for i, v in enumerate(vals)]
    payload = '{"p":%d,"i":[%s]}' % (hub.PID + 1, Hub._item(run, len(vals), sent))
    h._on_notify(None, 0, hub.CHANNEL, payload)
    got = [r.value for r in t.rows]
    assert [repr(x) for x in got] == [repr(x) for x in vals]
    assert t.rows[0].key == "précision/лосс"


async def test_own_notify_is_ignored_and_bad_payload_wakes_everyone():
    h = Hub()
    run = uuid.uuid4()
    t = RunTail(run, uuid.uuid4(), 0)
    h._tails[run] = [t, 1]
    h._on_notify(None, 0, hub.CHANNEL, '{"p":%d,"i":[%s]}' % (hub.PID, Hub._item(run, 1, rows(1))))
    assert t.rows == []                        # own process already pushed locally
    h._on_notify(None, 0, hub.CHANNEL, "not json")
    assert t.wake.is_set()


async def test_too_big_batch_is_sent_as_a_hint():
    big = [Row(i + 1, "k" * 100, i, 1.0, 0.0) for i in range(200)]
    item = json.loads(Hub._item(uuid.uuid4(), 200, big))
    assert item["rows"] is None
