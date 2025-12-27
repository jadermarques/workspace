"""Database connection helpers."""

import sqlite3
from contextlib import contextmanager

from .db_init import DB_PATH, ensure_db


@contextmanager
def get_conn():
    """Context manager to open SQLite connections and ensure schema."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


__all__ = ["get_conn"]
