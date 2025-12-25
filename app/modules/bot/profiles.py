"""UI for managing bot prompt profiles."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bot.engine import (
    delete_prompt_profile,
    get_prompt_profile,
    load_prompt_profiles,
    save_prompt_profile,
)


def render_profiles_tab():
    """Render the prompt profiles management tab."""
    st.header("Perfis do BOT")
    profiles_list = load_prompt_profiles()
    profile_choices = {"Novo perfil": None}
    for p in profiles_list:
        profile_choices[p["name"] or f"Perfil #{p['id']}"] = p["id"]
    selected_profile_label = st.selectbox("Perfis", options=list(profile_choices.keys()))
    selected_profile_id = profile_choices.get(selected_profile_label)
    existing_profile = get_prompt_profile(selected_profile_id) if selected_profile_id else {"name": "", "details": "", "prompt_text": ""}

    st.markdown("Perfis cadastrados")
    if profiles_list:
        for p in profiles_list:
            col_n, col_d, col_prev, col_edit, col_del = st.columns([2, 3, 4, 1, 1])
            with col_n:
                st.markdown(f"**{p['name'] or 'Sem nome'}**")
            with col_d:
                st.caption(p.get("details") or "-")
            with col_prev:
                preview = (p.get("prompt_text") or "").strip()
                if len(preview) > 160:
                    preview = preview[:160] + "..."
                st.code(preview or "(vazio)", language="text")
            with col_edit:
                if st.button("Editar", key=f"edit_profile_{p['id']}"):
                    st.session_state["selected_profile_id"] = p["id"]
                    st.rerun()
            with col_del:
                if st.button("Excluir", key=f"delete_profile_{p['id']}"):
                    delete_prompt_profile(p["id"])
                    if st.session_state.get("selected_profile_id") == p["id"]:
                        st.session_state["selected_profile_id"] = None
                    st.rerun()
    else:
        st.info("Nenhum perfil cadastrado ainda.")

    with st.form("profile_form"):
        name_input = st.text_input("Nome do perfil", value=existing_profile.get("name", ""))
        details_input = st.text_area("Detalhes do perfil", value=existing_profile.get("details", ""), height=80)
        prompt_text_input = st.text_area("Prompt completo", value=existing_profile.get("prompt_text", ""), height=260)
        col_save, col_delete = st.columns(2)
        save_profile_btn = col_save.form_submit_button("Salvar perfil", type="primary")
        delete_profile_btn = col_delete.form_submit_button("Excluir perfil", disabled=selected_profile_id is None)

        if save_profile_btn:
            if not name_input.strip():
                st.error("Informe um nome para o perfil.")
            elif not prompt_text_input.strip():
                st.error("Informe o conteúdo do prompt.")
            else:
                saved_id = save_prompt_profile(
                    name_input.strip(),
                    details_input.strip(),
                    prompt_text_input.strip(),
                    profile_id=selected_profile_id,
                )
                st.session_state["selected_profile_id"] = saved_id
                st.success("Perfil salvo.")
                st.rerun()
        if delete_profile_btn and selected_profile_id:
            delete_prompt_profile(selected_profile_id)
            if st.session_state.get("selected_profile_id") == selected_profile_id:
                st.session_state["selected_profile_id"] = None
            st.success("Perfil excluído.")
            st.rerun()


__all__ = ["render_profiles_tab"]
