from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass
from uuid import UUID

from fastapi import Request

from . import db
from .errors import UNAUTHORIZED, ApiError
from .stats import STATS, TIMERS

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")


@dataclass(frozen=True)
class Auth:
    project_id: UUID
    key_id: UUID


def hash_key(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest()


def new_key() -> tuple[str, str, bytes]:
    """Returns (plain key shown once, short prefix safe to log, sha256 stored in DB)."""
    plain = "sk_" + secrets.token_urlsafe(32)
    return plain, plain[:7], hash_key(plain)


def bearer_token(header: str) -> str | None:
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


class KeyCache:
    """What this process has learned about key hashes.

    - known_bad: hashes the DB said are not an active key (unknown or revoked). A random 256-bit
      key cannot become valid later and a revoked key is never un-revoked, so a hit is a safe 401
      without a DB round trip. Bounded LRU: a flood of distinct junk keys still costs DB lookups,
      but those run as the SYSTEM tenant and get only a fair share of the pool.
    - owner: hash -> (project, key id) for keys that were valid recently. Used to charge a request
      to its project's budget before the body is read, and (require_project_for_write) to skip the
      separate auth lookup on write paths that re-check the key inside their own transaction
      (ingest_batch, recheck_key_in_tx). A stale entry for a just-revoked key therefore still ends
      in 401 at the in-transaction check; reads always go to the DB.
    """

    def __init__(self, max_bad: int = 50_000, max_owner: int = 50_000, owner_ttl: float = 60.0) -> None:
        self.max_bad, self.max_owner, self.owner_ttl = max_bad, max_owner, owner_ttl
        self.bad: OrderedDict[bytes, None] = OrderedDict()
        self.owner: dict[bytes, tuple[UUID, UUID, float]] = {}

    def is_bad(self, digest: bytes) -> bool:
        if digest in self.bad:
            self.bad.move_to_end(digest)
            return True
        return False

    def mark_bad(self, digest: bytes) -> None:
        self.owner.pop(digest, None)
        self.bad[digest] = None
        self.bad.move_to_end(digest)
        while len(self.bad) > self.max_bad:
            self.bad.popitem(last=False)

    def known(self, digest: bytes) -> Auth | None:
        hit = self.owner.get(digest)
        if hit is None or hit[2] < time.monotonic():
            return None
        return Auth(project_id=hit[0], key_id=hit[1])

    def project_of(self, digest: bytes) -> UUID | None:
        hit = self.known(digest)
        return hit.project_id if hit else None

    def mark_good(self, digest: bytes, project_id: UUID, key_id: UUID) -> None:
        if len(self.owner) >= self.max_owner:
            self.owner.clear()
        self.owner[digest] = (project_id, key_id, time.monotonic() + self.owner_ttl)

    def revoked(self, key_id: UUID) -> None:
        """An in-transaction check found this key revoked: stop trusting its cache entries."""
        for digest in [d for d, v in self.owner.items() if v[1] == key_id]:
            self.mark_bad(digest)


KEYS = KeyCache()


async def require_project(request: Request) -> Auth:
    token = bearer_token(request.headers.get("authorization", ""))
    if token is None:
        STATS.inc("auth_failures")
        raise UNAUTHORIZED
    digest = hash_key(token)
    if KEYS.is_bad(digest):   # normally answered earlier by RateLimitMiddleware
        STATS.inc("auth_failures_cached")
        raise UNAUTHORIZED
    # Lookup by hash: the DB never sees the plain key, and an index lookup on a
    # hash does not leak a prefix-match timing signal. Charged to the project we believe owns the
    # key, so a junk-key flood (SYSTEM tenant) cannot crowd out a real project's lookups.
    STATS.inc("auth_db_lookups")
    t0 = time.perf_counter()
    row = await db.fetchrow(
        "SELECT id, project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        digest, tenant=KEYS.project_of(digest) or db.SYSTEM,
    )
    TIMERS.add("auth_db", (time.perf_counter() - t0) * 1000)
    if row is None:
        KEYS.mark_bad(digest)
        STATS.inc("auth_failures")
        raise UNAUTHORIZED
    KEYS.mark_good(digest, row["project_id"], row["id"])
    return Auth(project_id=row["project_id"], key_id=row["id"])


async def require_project_for_write(request: Request) -> Auth:
    """Auth for endpoints whose transaction re-checks the key itself (FOR SHARE).

    Round-4 breakdown: ~4.4 pooled connections per ingested event, one of them this lookup. The
    in-transaction re-check is what makes rotation safe anyway (a write that raced a rotation is
    rejected there), so a recently verified key skips the separate lookup here.
    """
    token = bearer_token(request.headers.get("authorization", ""))
    if token is not None:
        hit = KEYS.known(hash_key(token))
        if hit is not None:
            STATS.inc("auth_cached_write")
            return hit
    return await require_project(request)


async def recheck_key_in_tx(conn: object, key_id: UUID) -> None:
    """Re-validate the key inside a write transaction and hold a share lock on it until commit.

    Found by test_write_authenticated_before_rotation_cannot_commit_after_it: auth runs at request
    start, so a write that waited (e.g. on the run lock) committed after its key was revoked.
    With FOR SHARE, a rotation either commits first (we see revoked_at and abort) or waits for
    this write to commit (the write really happened before the revocation).
    """
    ok = await conn.fetchval(  # type: ignore[attr-defined]
        "SELECT 1 FROM api_keys WHERE id = $1 AND revoked_at IS NULL FOR SHARE", key_id
    )
    if ok is None:
        KEYS.revoked(key_id)
        STATS.inc("writes_rejected_revoked_in_flight")
        raise UNAUTHORIZED


async def key_is_active(key_id: UUID, project_id: UUID) -> bool:
    return bool(
        await db.fetchval(
            "SELECT revoked_at IS NULL FROM api_keys WHERE id = $1", key_id, tenant=project_id
        )
    )


def require_admin(request: Request) -> None:
    given = request.headers.get("x-admin-token", "")
    if not ADMIN_TOKEN or not hmac.compare_digest(given.encode(), ADMIN_TOKEN.encode()):
        raise ApiError(401, "unauthorized")
