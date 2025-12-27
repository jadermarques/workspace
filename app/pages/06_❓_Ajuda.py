"""Streamlit page for workspace help."""

import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.sidebar import render_sidebar


def main():
    """Render the help page with a brief workspace overview."""
    st.set_page_config(page_title="Ajuda", page_icon="❓", layout="wide")
    render_sidebar(show_selector=False)

    st.title("Ajuda")
    st.caption("Guia rápido das principais funcionalidades do workspace.")

    st.markdown("### O que você encontra em cada área")

    style_html = textwrap.dedent(
        """\
        <style>
        .help-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 16px;
            margin-top: 12px;
        }
        .help-card {
            background: #f8fafc;
            border: 1px solid rgba(0,0,0,0.06);
            padding: 16px;
            border-radius: 12px;
            box-sizing: border-box;
            height: 100%;
        }
        .help-card h4 {
            margin: 0 0 6px 0;
            font-size: 16px;
        }
        .help-card p {
            margin: 0;
            font-size: 13px;
            color: #334155;
        }
        @media (max-width: 1100px) {
            .help-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
        }
        @media (max-width: 700px) {
            .help-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """
    ).strip()
    st.markdown(style_html, unsafe_allow_html=True)

    cards = [
        ("Principal", "Visão geral do sistema e status das integrações."),
        ("Bot Studio", "Criação e ajuste do comportamento do bot e fluxos de atendimento."),
        ("Configurações", "Credenciais, integrações e preferências do workspace."),
        ("Dashboards", "Indicadores operacionais e exportação de dados."),
        ("Gestão", "Organização de recursos internos, cadastros e controle operacional."),
        ("Análises", "Leitura aprofundada de conversas, métricas e mensagens."),
        ("Ajuda", "Resumo rápido das funcionalidades do workspace."),
    ]

    cards_html = "\n".join(
        [f"<div class=\"help-card\"><h4>{title}</h4><p>{text}</p></div>" for title, text in cards]
    )
    grid_html = f"<div class=\"help-grid\">{cards_html}</div>"
    st.markdown(grid_html, unsafe_allow_html=True)

    st.info("Use a barra lateral para acessar cada módulo e acompanhar a evolução do seu workspace.")


if __name__ == "__main__":
    main()
