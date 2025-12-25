"""Settings UI for system-wide configuration."""

import streamlit as st


def render_system_info():
    """Render placeholder system settings content."""
    st.header("Sistema")
    st.write("Ajuste configurações gerais do app aqui.")


__all__ = ["render_system_info"]
