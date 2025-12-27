"""Formatting helpers for timestamps and other display values."""

from datetime import datetime


def format_ts(ts: str) -> str:
    """Convert an ISO timestamp to a readable string, fallback to input."""
    if not ts:
        return ""
    try:
        return datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts


__all__ = ["format_ts"]
