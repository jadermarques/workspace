"""Streamlit page for management modules."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.sidebar import render_sidebar
from app.modules.management.insights_prompts import render_insights_prompts_tab


def main():
    """Render the management page placeholder content."""
    st.set_page_config(page_title="Gestão", page_icon="🗂️", layout="wide")
    render_sidebar(show_selector=False)
    st.header("Gestão")
    tab_users, tab_permissions, tab_insights = st.tabs(
        ["Cadastro de Usuários", "Permissões", "Prompts de Insights"]
    )

    with tab_users:
        st.subheader("Cadastro de Usuários")
        st.info("Em breve.")

    with tab_permissions:
        st.subheader("Permissões")
        st.info("Em breve.")

    with tab_insights:
        render_insights_prompts_tab()


if __name__ == "__main__":
    main()
