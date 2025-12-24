import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.sidebar import render_sidebar


def main():
    st.set_page_config(page_title="Gestão", page_icon="🗂️", layout="wide")
    render_sidebar(show_selector=False)
    st.header("Gestão")
    st.info("Espaço reservado para controle de usuários/grupos.")

    st.subheader("Cadastros")
    st.write("Inclua aqui cadastros de usuários, grupos e permissões.")


if __name__ == "__main__":
    main()
