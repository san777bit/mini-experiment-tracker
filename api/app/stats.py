from __future__ import annotations

from collections import Counter


class Stats:
    """Process-local counters, exported at /api/admin/stats (admin token only:
    global counters would leak other projects' activity)."""

    def __init__(self) -> None:
        self.c: Counter[str] = Counter()
        self.gauges: dict[str, int] = {}

    def inc(self, name: str, n: int = 1) -> None:
        self.c[name] += n

    def gauge(self, name: str, delta: int) -> None:
        self.gauges[name] = self.gauges.get(name, 0) + delta

    def snapshot(self) -> dict[str, int]:
        return {**self.c, **self.gauges}


STATS = Stats()
