"""Streamlit page for reports."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.sidebar import render_sidebar
from app.modules.bot.report import render_atendimentos_dashboard


def main():
    """Render the reports page and attendance dashboard."""
    st.set_page_config(page_title="Relatórios", page_icon="📊", layout="wide")
    render_sidebar(show_selector=False)
    render_atendimentos_dashboard()


if __name__ == "__main__":
    main()
