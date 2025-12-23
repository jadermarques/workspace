import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bot.engine import load_logs


def render_conversation_metrics():
    st.header("Métricas de Conversas")
    rows = load_logs(limit=500)
    if not rows:
        st.info("Nenhum log disponível.")
        return
    df = pd.DataFrame(rows)
    df["direction"] = df["direction"].fillna("desconhecido")
    df["profile_name"] = df["profile_name"].fillna("N/A")
    total = len(df)
    received = int((df["direction"] == "cliente").sum())
    sent = int((df["direction"] == "assistant").sum()) + int((df["direction"] == "bot").sum())

    col1, col2, col3 = st.columns(3)
    col1.metric("Mensagens totais", total)
    col2.metric("Recebidas (cliente)", received)
    col3.metric("Enviadas (bot)", sent)

    st.markdown("### Top perfis usados")
    profile_counts = df["profile_name"].value_counts().reset_index()
    profile_counts.columns = ["profile_name", "mensagens"]
    st.dataframe(profile_counts, use_container_width=True)

    st.markdown("### Registros recentes")
    st.dataframe(df.head(200), use_container_width=True)


__all__ = ["render_conversation_metrics"]
