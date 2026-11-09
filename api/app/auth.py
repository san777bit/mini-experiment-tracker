from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from uuid import UUID

from fastapi import Request

from . import db
from .errors import UNAUTHORIZED, ApiError
from .stats import STATS

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


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


async def require_project(request: Request) -> Auth:
    token = _bearer(request)
    if token is None:
        STATS.inc("auth_failures")
        raise UNAUTHORIZED
    # Lookup by hash: the DB never sees the plain key, and an index lookup on a
    # hash does not leak a prefix-match timing signal.
    row = await db.pool().fetchrow(
        "SELECT id, project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        hash_key(token),
    )
    if row is None:
        STATS.inc("auth_failures")
        raise UNAUTHORIZED
    return Auth(project_id=row["project_id"], key_id=row["id"])


async def key_is_active(key_id: UUID) -> bool:
    return bool(
        await db.pool().fetchval(
            "SELECT revoked_at IS NULL FROM api_keys WHERE id = $1", key_id
        )
    )


def require_admin(request: Request) -> None:
    given = request.headers.get("x-admin-token", "")
    if not ADMIN_TOKEN or not hmac.compare_digest(given.encode(), ADMIN_TOKEN.encode()):
        raise ApiError(401, "unauthorized")
