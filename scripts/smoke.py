"""Quick manual smoke check against a running stack: python scripts/smoke.py"""
import uuid

import httpx

B = "http://localhost:8000"
ADMIN = {"x-admin-token": "dev-admin-token"}
c = httpx.Client(base_url=B, timeout=10)
p = c.post("/api/admin/projects", json={"name": "A"}, headers=ADMIN).json()
q = c.post("/api/admin/projects", json={"name": "B"}, headers=ADMIN).json()
ha = {"authorization": "Bearer " + p["api_key"]}
hb = {"authorization": "Bearer " + q["api_key"]}
run = c.post("/api/runs", json={"name": "r1"}, headers=ha).json()
print("run", run)
batch = {"batch_id": str(uuid.uuid4()), "events": [{"key": "loss", "step": s, "value": 1 / (s + 1)} for s in range(5)]}
print("ingest", c.post(f"/api/runs/{run['id']}/metrics", json=batch, headers=ha).json())
print("repeat", c.post(f"/api/runs/{run['id']}/metrics", json=batch, headers=ha).json())
batch2 = {"batch_id": str(uuid.uuid4()), "events": [{"key": "loss", "step": 0, "value": 9.0}, {"key": "acc", "step": 0, "value": 0.1}]}
print("conflict", c.post(f"/api/runs/{run['id']}/metrics", json=batch2, headers=ha).json())
print("read", c.get(f"/api/runs/{run['id']}/metrics", headers=ha).json())
r = c.get(f"/api/runs/{run['id']}", headers=hb)
print("B reads A run", r.status_code, r.text)
r = c.get(f"/api/runs/{uuid.uuid4()}", headers=hb)
print("B reads missing", r.status_code, r.text)
r = c.get("/api/runs/not-a-uuid", headers=hb)
print("malformed", r.status_code, r.text)
r = c.post(f"/api/runs/{run['id']}/metrics", json={"batch_id": str(uuid.uuid4()), "events": [{"key": "x", "step": 1, "value": "NaN"}]}, headers=ha)
print("nan", r.status_code, r.text)
with c.stream("GET", f"/api/runs/{run['id']}/stream", headers={**ha, "last-event-id": "3"}) as s:
    lines = []
    for line in s.iter_lines():
        lines.append(line)
        if line.startswith("event: status"):
            break
    print("stream", lines)
print("headers", dict(c.get("/api/runs", headers=ha).headers))
print("run page key", p["api_key"], "url", f"{B}/runs/{run['id']}")
