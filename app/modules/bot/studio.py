import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.modules.bot.profiles import render_profiles_tab
from app.modules.bot.monitoring import render_logs
from src.bot.engine import load_settings, set_bot_enabled


def _render_activation():
    current = load_settings() or {}
    enabled = st.checkbox("Bot ligado", value=current.get("bot_enabled", True))
    if st.button("Salvar ativação", type="primary"):
        set_bot_enabled(bool(enabled))
        st.success(f"Bot {'ligado' if enabled else 'desligado'}.")


def render_bot_studio_module():
    tabs = st.tabs(["Perfil Bot", "Logs", "Monitoramento", "Ativação BOT"])
    with tabs[0]:
        render_profiles_tab()
    with tabs[1]:
        render_logs(limit=200)
    with tabs[2]:
        st.info("Monitoramento em construção.")
    with tabs[3]:
        _render_activation()


__all__ = ["render_bot_studio_module"]
