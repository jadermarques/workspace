import streamlit as st


def metric_card(label: str, value, help_text: str = ""):
    col = st.container(border=True)
    with col:
        st.metric(label, value)
        if help_text:
            st.caption(help_text)


__all__ = ["metric_card"]
