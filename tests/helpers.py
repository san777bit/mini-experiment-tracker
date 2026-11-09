"""Shared test plumbing. Tests talk to a running `docker compose up` stack."""
from __future__ import annotations

import asyncio
import json
import os
import random
import uuid
from dataclasses import dataclass, field

import asyncpg
import httpx

API = os.environ.get("API_URL", "http://localhost:8000")
API_PROXY = os.environ.get("API_PROXY_URL", "http://localhost:18000")       # browser / readers
INGEST_PROXY = os.environ.get("INGEST_PROXY_URL", "http://localhost:18001")  # producers
TOXI = os.environ.get("TOXIPROXY_URL", "http://localhost:8474")
# Owner role bypasses RLS: this is the ground truth the page is compared to.
DB_OWNER = os.environ.get("DB_OWNER_URL", "postgresql://owner:owner@localhost:55432/tracker")
DB_APP = os.environ.get("DB_APP_URL", "postgresql://app:app@localhost:55432/tracker")
ADMIN = {"x-admin-token": os.environ.get("ADMIN_TOKEN", "dev-admin-token")}


@dataclass
class Project:
    id: str
    key: str

    @property
    def h(self) -> dict[str, str]:
        return {"authorization": f"Bearer {self.key}"}


async def new_project(name: str = "p") -> Project:
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post("/api/admin/projects", json={"name": name}, headers=ADMIN)
        r.raise_for_status()
        d = r.json()
        return Project(d["project_id"], d["api_key"])


async def new_run(p: Project, name: str = "run") -> str:
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        r = await c.post("/api/runs", json={"name": name}, headers=p.h)
        r.raise_for_status()
        return r.json()["id"]


def batch(events: list[dict], batch_id: str | None = None) -> dict:
    return {"batch_id": batch_id or str(uuid.uuid4()), "events": events}


# --- toxiproxy ---------------------------------------------------------------


class Toxi:
    def __init__(self) -> None:
        self.c = httpx.AsyncClient(base_url=TOXI, timeout=10)

    async def reset(self) -> None:
        (await self.c.post("/reset")).raise_for_status()

    async def enable(self, proxy: str, on: bool) -> None:
        (await self.c.post(f"/proxies/{proxy}", json={"enabled": on})).raise_for_status()

    async def add(self, proxy: str, name: str, type_: str, stream: str, attrs: dict, toxicity: float = 1.0) -> None:
        r = await self.c.post(
            f"/proxies/{proxy}/toxics",
            json={"name": name, "type": type_, "stream": stream, "toxicity": toxicity, "attributes": attrs},
        )
        r.raise_for_status()

    async def remove(self, proxy: str, name: str) -> None:
        await self.c.delete(f"/proxies/{proxy}/toxics/{name}")

    async def close(self) -> None:
        await self.c.aclose()


# --- unreliable producer -------------------------------------------------------


@dataclass
class ProducerStats:
    sent_requests: int = 0
    failed_attempts: int = 0
    acked_batches: int = 0
    accepted: int = 0
    duplicates: int = 0
    conflicts: int = 0
    errors: list[str] = field(default_factory=list)


async def send_until_acked(
    client: httpx.AsyncClient, run_id: str, p: Project, body: dict, st: ProducerStats, max_attempts: int = 60
) -> dict:
    """Client contract on an unreliable network: on ANY failure resend the exact same
    batch (same batch_id, same events). The server's idempotency makes this safe."""
    delay = 0.05
    for _ in range(max_attempts):
        st.sent_requests += 1
        try:
            r = await client.post(f"/api/runs/{run_id}/metrics", json=body, headers=p.h)
            if r.status_code == 200:
                d = r.json()
                st.acked_batches += 1
                st.accepted += d["accepted"]
                st.duplicates += d["duplicates"]
                st.conflicts += d["conflicts"]
                return d
            if r.status_code < 500:
                st.errors.append(f"HTTP {r.status_code} {r.text[:200]}")
                raise AssertionError(f"non-retryable {r.status_code}: {r.text[:200]}")
            st.failed_attempts += 1
        except (httpx.TransportError, json.JSONDecodeError, KeyError):
            st.failed_attempts += 1
        await asyncio.sleep(delay + random.random() * delay)
        delay = min(delay * 2, 1.0)
    raise AssertionError("batch never acknowledged")


def fresh_conn_client(base: str, timeout: float = 5.0) -> httpx.AsyncClient:
    # No keep-alive: every request is a new TCP connection, so per-connection
    # toxics (probabilistic resets) apply per request.
    return httpx.AsyncClient(
        base_url=base, timeout=timeout,
        limits=httpx.Limits(max_keepalive_connections=0, max_connections=50),
    )


# --- ground truth from the DB -------------------------------------------------------


async def db_points(run_id: str) -> dict[tuple[str, int], float]:
    conn = await asyncpg.connect(DB_OWNER)
    try:
        rows = await conn.fetch(
            "SELECT metric_key, step, value FROM metrics WHERE run_id = $1", uuid.UUID(run_id)
        )
    finally:
        await conn.close()
    return {(r["metric_key"], r["step"]): r["value"] for r in rows}


async def db_run(run_id: str) -> dict:
    conn = await asyncpg.connect(DB_OWNER)
    try:
        row = await conn.fetchrow(
            """SELECT r.last_seq, r.conflicts,
                      (SELECT count(*) FROM metrics m WHERE m.run_id = r.id) AS n,
                      (SELECT coalesce(max(seq), 0) FROM metrics m WHERE m.run_id = r.id) AS max_seq
                 FROM runs r WHERE r.id = $1""",
            uuid.UUID(run_id),
        )
    finally:
        await conn.close()
    return dict(row)


# --- raw SSE reader (no browser) --------------------------------------------------------


async def read_sse(
    client: httpx.AsyncClient, run_id: str, p: Project, after: int, stop_at_seq: int | None = None,
    max_seconds: float = 30.0,
) -> list[dict]:
    """Reads metric events after `after` until stop_at_seq is reached or time runs out."""
    out: list[dict] = []
    headers = {**p.h, "last-event-id": str(after)}

    async def go() -> None:
        async with client.stream("GET", f"/api/runs/{run_id}/stream", headers=headers) as s:
            s.raise_for_status()
            ev_type = None
            async for line in s.aiter_lines():
                if line.startswith("event: "):
                    ev_type = line[7:]
                elif line.startswith("data: ") and ev_type == "metric":
                    out.append(json.loads(line[6:]))
                    if stop_at_seq is not None and out[-1]["seq"] >= stop_at_seq:
                        return

    try:
        await asyncio.wait_for(go(), timeout=max_seconds)
    except asyncio.TimeoutError:
        pass
    return out
