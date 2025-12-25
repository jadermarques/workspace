"""Streamlit page for bot studio configuration."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.sidebar import render_sidebar
from app.modules.bot.studio import render_bot_studio_module


def main():
    """Render the bot studio page."""
    st.set_page_config(page_title="Bot Studio", page_icon="🤖", layout="wide")
    render_sidebar(show_selector=False)
    render_bot_studio_module()


if __name__ == "__main__":
    main()
