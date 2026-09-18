"""Application configuration read from environment variables."""

from __future__ import annotations

import os
from pathlib import Path


def database_path() -> str:
    """Return the SQLite database path (env var DATABASE_PATH overrides default)."""
    return os.environ.get("DATABASE_PATH", str(Path(__file__).resolve().parent.parent / "data" / "events.db"))


def server_port() -> int:
    try:
        return int(os.environ.get("APP_PORT", "8000"))
    except ValueError:
        return 8000


def lease_duration_seconds() -> int:
    """Lifetime of a claim lease (env var LEASE_DURATION_SECONDS overrides default)."""
    try:
        value = int(os.environ.get("LEASE_DURATION_SECONDS", "30"))
    except ValueError:
        return 30
    return value if value > 0 else 30


def consumer_max_attempts() -> int:
    """Consecutive nack failures after which a head event is dead-lettered.

    Env var CONSUMER_MAX_ATTEMPTS overrides the default of 3. Non-integer or
    non-positive values fall back to the default.
    """
    try:
        value = int(os.environ.get("CONSUMER_MAX_ATTEMPTS", "3"))
    except ValueError:
        return 3
    return value if value >= 1 else 3


# Fallback HMAC key for snapshot export cursors, used only when
# SNAPSHOT_CURSOR_SECRET is not set. Fine for local development; real
# deployments must set the env var (docker-compose.yml ships a usable default).
DEFAULT_SNAPSHOT_CURSOR_SECRET = "dev-snapshot-cursor-secret"


def snapshot_cursor_secret() -> str:
    """HMAC-SHA256 key signing snapshot export cursors (env SNAPSHOT_CURSOR_SECRET)."""
    return os.environ.get("SNAPSHOT_CURSOR_SECRET", DEFAULT_SNAPSHOT_CURSOR_SECRET)
