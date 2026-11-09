from __future__ import annotations


class ApiError(Exception):
    """Error with a fixed, non-revealing body: {"error": code}."""

    def __init__(self, status: int, code: str) -> None:
        self.status = status
        self.code = code


NOT_FOUND = ApiError(404, "not_found")
UNAUTHORIZED = ApiError(401, "unauthorized")
