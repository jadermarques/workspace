import sqlite3
from contextlib import contextmanager

from .db_init import DB_PATH, ensure_db


@contextmanager
def get_conn():
    """
    Context manager para abrir conexões SQLite garantindo o schema.
    """
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


__all__ = ["get_conn"]
