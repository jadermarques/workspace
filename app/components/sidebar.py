from typing import Dict, List

import streamlit as st


DEFAULT_MODULES = ["Principal", "Bot Studio", "Configurações", "Relatórios", "Gestão", "Analytics"]


def render_sidebar(modules: List[str] = None, default: str = None, show_selector: bool = True) -> str:
    """
    Menu lateral usando selectbox (estilo lista simples) com ícones.
    Quando show_selector=False, apenas retorna o default (para páginas isoladas).
    """
    modules = modules or DEFAULT_MODULES
    if default is None:
        default = modules[0]

    icons: Dict[str, str] = {
        "Principal": "🏠",
        "Bot Studio": "🤖",
        "Configurações": "⚙️",
        "Relatórios": "📊",
        "Gestão": "🗂️",
        "Analytics": "📈",
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
