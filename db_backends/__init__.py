"""Database backends for db_bench."""

from __future__ import annotations

import urllib.parse
from typing import Tuple

from db_backends.base import Backend, workload_select_columns
from db_backends.mysql import mysql_backend
from db_backends.postgresql import postgresql_backend

__all__ = (
    "Backend",
    "get_backend",
    "mysql_backend",
    "normalize_scheme",
    "parse_db_url",
    "postgresql_backend",
    "workload_select_columns",
)


def normalize_scheme(scheme: str) -> str:
    s = (scheme or "").lower().replace("+asyncpg", "").replace("+psycopg2", "")
    if s == "postgres":
        return "postgresql"
    return s


def parse_db_url(url: str) -> Tuple[str, dict]:
    """Return (normalized_scheme, connection kwargs)."""
    parsed = urllib.parse.urlparse(url)
    scheme = normalize_scheme(parsed.scheme or "")
    if scheme not in ("postgresql", "mysql"):
        raise SystemExit(
            f"Unsupported URL scheme {parsed.scheme!r}. Use postgresql:// or mysql://"
        )
    user = urllib.parse.unquote(parsed.username or "")
    password = urllib.parse.unquote(parsed.password or "") if parsed.password else ""
    host = parsed.hostname or "localhost"
    port = parsed.port
    db = (parsed.path or "/").lstrip("/") or "postgres"
    return scheme, {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "database": db,
    }


def get_backend(scheme: str) -> Backend:
    s = normalize_scheme(scheme)
    if s == "postgresql":
        return postgresql_backend
    if s == "mysql":
        return mysql_backend
    raise SystemExit(f"Unsupported database scheme {scheme!r}")
