"""Send metrics to a run so you can watch the run page update live.

    .venv/Scripts/python scripts/demo_producer.py --key sk_... --run <run id> [--rate 5] [--chaos] [--lose-every 0]

--chaos       shuffles each batch, resends ~30% of batches, sends one conflicting value now and then
              (the page must still show each point once, ordered by step; conflicts tile goes up)
--lose-every  N>0: every N-th batch is "lost" (never sent) but counted in client_points, so the
              page's "reported by client, not stored" tile shows the gap
Ctrl+C to stop.
"""
from __future__ import annotations

import argparse
import math
import random
import time
import uuid

import httpx

API = "http://localhost:8000"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True)
    ap.add_argument("--run", required=True)
    ap.add_argument("--rate", type=float, default=5, help="batches per second")
    ap.add_argument("--batch", type=int, default=3, help="steps per batch (each step: loss + acc)")
    ap.add_argument("--chaos", action="store_true")
    ap.add_argument("--lose-every", type=int, default=0)
    a = ap.parse_args()

    h = {"authorization": f"Bearer {a.key}"}
    step = produced = n = 0
    with httpx.Client(base_url=API, timeout=10) as c:
        while True:
            events = []
            for _ in range(a.batch):
                events.append({"key": "loss", "step": step, "value": round(2.0 * math.exp(-step / 200) + random.uniform(0, .05), 4)})
                events.append({"key": "acc", "step": step, "value": round(1 - math.exp(-step / 150), 4)})
                step += 1
            produced += len(events)
            n += 1
            if a.chaos:
                random.shuffle(events)
            body = {"batch_id": str(uuid.uuid4()), "events": events, "client_points": produced}
            if a.lose_every and n % a.lose_every == 0:
                print(f"batch {n}: LOST on purpose ({len(events)} points)")
            else:
                sends = 2 if a.chaos and random.random() < 0.3 else 1
                for _ in range(sends):
                    r = c.post(f"/api/runs/{a.run}/metrics", json=body, headers=h)
                    print(f"batch {n}: {r.status_code} {r.text}")
                if a.chaos and random.random() < 0.1:      # same point, different value: first write wins
                    bad = {"batch_id": str(uuid.uuid4()), "events": [{**events[0], "value": -1.0}],
                           "client_points": produced}
                    print("  conflict:", c.post(f"/api/runs/{a.run}/metrics", json=bad, headers=h).text)
            time.sleep(1 / a.rate)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
