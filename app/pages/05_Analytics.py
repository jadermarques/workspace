import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.sidebar import render_sidebar
from app.modules.analytics.conversations import render_conversation_metrics
from app.modules.bot.monitoring import render_logs


def main():
    st.set_page_config(page_title="Analytics", page_icon="📈", layout="wide")
    render_sidebar(show_selector=False)
    render_conversation_metrics()
    st.markdown("---")
    render_logs(limit=200)


if __name__ == "__main__":
    main()
