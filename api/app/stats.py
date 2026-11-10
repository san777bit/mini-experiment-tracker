from __future__ import annotations

import asyncio
import os
import random
import time
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


class Timers:
    """Per-phase latency samples (reservoir of 4000 per phase) for the admin-only diagnostics."""

    SIZE = 4000

    def __init__(self) -> None:
        self.n: Counter[str] = Counter()
        self.samples: dict[str, list[float]] = {}

    def add(self, name: str, ms: float) -> None:
        self.n[name] += 1
        xs = self.samples.setdefault(name, [])
        if len(xs) < self.SIZE:
            xs.append(ms)
        else:
            i = random.randrange(self.n[name])
            if i < self.SIZE:
                xs[i] = ms

    def reset(self) -> None:
        self.n.clear()
        self.samples.clear()

    def snapshot(self) -> dict[str, object]:
        return {"pid": os.getpid(), "n": dict(self.n), "samples": self.samples}


TIMERS = Timers()


async def loop_lag_monitor(period: float = 0.05) -> None:
    """How late does a 50 ms sleep wake up? Lateness = time this process's event loop was busy
    (or the process was not scheduled): the CPU-saturation signal behind tail latency."""
    while True:
        t0 = time.perf_counter()
        await asyncio.sleep(period)
        TIMERS.add("loop_lag", (time.perf_counter() - t0 - period) * 1000)


STATS = Stats()
