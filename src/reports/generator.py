"""Report data generators from stored logs."""

import pandas as pd

from src.bot.engine import load_logs


def load_logs_df(limit: int = 200) -> pd.DataFrame:
    """Convert persisted logs into a DataFrame for analysis."""
    rows = load_logs(limit=limit)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


__all__ = ["load_logs_df"]
