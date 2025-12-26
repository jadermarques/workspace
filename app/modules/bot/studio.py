"""UI composition for the Bot Studio module."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.modules.bot.monitoring import render_logs
from app.modules.bot.profiles import render_profiles_tab
from src.bot.engine import load_settings, set_bot_enabled

UVICORN_BIN = shutil.which("uvicorn")
if UVICORN_BIN:
    WEBHOOK_COMMAND = [
        UVICORN_BIN,
        "app.modules.bot.bot_start:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
    ]
else:
    WEBHOOK_COMMAND = [
        sys.executable,
        "-m",
        "uvicorn",
        "app.modules.bot.bot_start:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
    ]


def _is_webhook_running() -> bool:
    try:
        result = subprocess.run(
            ["pgrep", "-f", "uvicorn app.modules.bot.bot_start:app"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def _start_webhook() -> bool:
    if _is_webhook_running():
        return False
    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = f"{WORKSPACE_ROOT}{os.pathsep}{env.get('PYTHONPATH', '')}".rstrip(os.pathsep)
        subprocess.Popen(
            WEBHOOK_COMMAND,
            cwd=str(WORKSPACE_ROOT),
            env=env,
            stdout=None,
            stderr=None,
            start_new_session=True,
        )
        return True
    except FileNotFoundError:
        return False


def _stop_webhook() -> bool:
    if not _is_webhook_running():
        return False
    try:
        result = subprocess.run(
            ["pkill", "-f", "uvicorn app.modules.bot.bot_start:app"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def _render_activation():
    """Render bot activation toggle and persist changes."""
    current = load_settings() or {}
    enabled = bool(current.get("bot_enabled", True))
    status_text = "Bot ligado" if enabled else "Bot desligado"
    st.write(status_text)

    col_on, col_off = st.columns(2)
    if col_on.button("Ativar bot", type="primary", disabled=enabled):
        set_bot_enabled(True)
        started = _start_webhook()
        if started:
            st.success("Bot ativado e webhook iniciado.")
        else:
            if _is_webhook_running():
                st.info("Bot ativado. Webhook já está em execução.")
            else:
                st.error("Bot ativado, mas não foi possível iniciar o webhook.")
    if col_off.button("Desativar bot", disabled=not enabled):
        set_bot_enabled(False)
        stopped = _stop_webhook()
        if stopped:
            st.success("Bot desativado e webhook interrompido.")
        else:
            if _is_webhook_running():
                st.warning("Bot desativado, mas não foi possível interromper o webhook.")
            else:
                st.info("Bot desativado. Webhook já estava parado.")


def render_bot_studio_module():
    """Render the Bot Studio tabs (profiles, logs, monitoring, activation)."""
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
