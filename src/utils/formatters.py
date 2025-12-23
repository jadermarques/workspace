from datetime import datetime


def format_ts(ts: str) -> str:
    """
    Converte timestamps ISO em string legível; retorna entrada se falhar.
    """
    if not ts:
        return ""
    try:
        return datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts


__all__ = ["format_ts"]
