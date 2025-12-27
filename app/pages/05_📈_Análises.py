"""Streamlit page for analytics modules."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.sidebar import render_sidebar
from app.modules.analytics.conversations import (
    render_conversations_analysis_tab,
    render_conversations_insights_tab,
    render_conversations_tab,
)
from app.modules.analytics.messages import render_messages_tab
from app.modules.bot.monitoring import render_logs


def main():
    """Render the analytics page with conversations and messages tabs."""
    st.set_page_config(page_title="Análises", page_icon="📈", layout="wide")
    render_sidebar(show_selector=False)
    tab_conversas, tab_analise_conversas, tab_insights, tab_mensagens = st.tabs(
        ["Conversas", "Análise de Conversas", "Insights", "Mensagens"]
    )
    with tab_conversas:
        render_conversations_tab()
    with tab_analise_conversas:
        render_conversations_analysis_tab()
    with tab_insights:
        render_conversations_insights_tab()
    with tab_mensagens:
        render_messages_tab()


if __name__ == "__main__":
    main()
