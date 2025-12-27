"""Sidebar navigation helpers and defaults for the Streamlit app."""

import subprocess
from typing import Dict, List

import streamlit as st

from src.bot.engine import set_bot_enabled


DEFAULT_MODULES = ["Principal", "Bot Studio", "Configurações", "Dashboards", "Gestão", "Análises", "Ajuda"]
_BOOTSTRAPPED = False


def _bootstrap_bot_state():
    """Ensure the bot starts disabled when the Streamlit process boots."""
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    set_bot_enabled(False)
    try:
        subprocess.run(
            ["pkill", "-f", "uvicorn app.modules.bot.bot_start:app"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except FileNotFoundError:
        pass
    _BOOTSTRAPPED = True


def render_sidebar(modules: List[str] = None, default: str = None, show_selector: bool = True) -> str:
    """Render the sidebar navigation and return the selected module."""
    _bootstrap_bot_state()
    modules = modules or DEFAULT_MODULES
    if default is None:
        default = modules[0]

    icons: Dict[str, str] = {
        "Principal": "🏠",
        "Bot Studio": "🤖",
        "Configurações": "⚙️",
        "Dashboards": "📊",
        "Gestão": "🗂️",
        "Análises": "📈",
        "Ajuda": "❓",
    }
    if show_selector:
        selection = st.sidebar.selectbox(
            label="Navegação",
            options=modules,
            index=modules.index(default) if default in modules else 0,
            format_func=lambda name: f"{icons.get(name, '')} {name}".strip(),
            label_visibility="collapsed",
        )
        return selection
    return default


__all__ = ["render_sidebar", "DEFAULT_MODULES"]
