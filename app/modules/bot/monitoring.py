"""Monitoring UI for conversation logs."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bot.engine import load_logs
from src.utils.formatters import format_ts


def render_logs(limit: int = 200):
    """Render conversation logs with optional record limit."""
    st.header("Logs de Conversa")
    rows = load_logs(limit=limit)
    if not rows:
        st.info("Nenhum log salvo ainda.")
        return
    df = pd.DataFrame(rows)
    if "created_at" in df.columns:
        df["created_at"] = df["created_at"].apply(format_ts)
    st.dataframe(df, use_container_width=True)


__all__ = ["render_logs"]
