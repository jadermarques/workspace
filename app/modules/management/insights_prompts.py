"""UI for managing insights prompts."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.database import get_conn


def _load_insight_prompts():
    with get_conn() as conn:
        conn.row_factory = None
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, description, created_at, prompt_text
            FROM insight_prompts
            ORDER BY datetime(created_at) DESC, id DESC
            """
        )
        rows = cur.fetchall()
    prompts = []
    for row in rows:
        prompt_id, name, description, created_at, prompt_text = row
        prompts.append(
            {
                "id": prompt_id,
                "name": name or "",
                "description": description or "",
                "created_at": created_at or "",
                "prompt_text": prompt_text or "",
            }
        )
    return prompts


def _get_insight_prompt(prompt_id: int):
    if prompt_id is None:
        return None
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, description, created_at, prompt_text
            FROM insight_prompts
            WHERE id = ?
            """,
            (prompt_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    prompt_id, name, description, created_at, prompt_text = row
    return {
        "id": prompt_id,
        "name": name or "",
        "description": description or "",
        "created_at": created_at or "",
        "prompt_text": prompt_text or "",
    }


def _save_insight_prompt(name: str, description: str, prompt_text: str, prompt_id: int = None) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        if prompt_id is None:
            cur.execute(
                """
                INSERT INTO insight_prompts (name, description, prompt_text)
                VALUES (?, ?, ?)
                """,
                (name, description, prompt_text),
            )
            conn.commit()
            return cur.lastrowid
        cur.execute(
            """
            UPDATE insight_prompts
            SET name = ?, description = ?, prompt_text = ?
            WHERE id = ?
            """,
            (name, description, prompt_text, prompt_id),
        )
        conn.commit()
    return prompt_id


def _delete_insight_prompt(prompt_id: int) -> None:
    if prompt_id is None:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM insight_prompts WHERE id = ?", (prompt_id,))
        conn.commit()


def render_insights_prompts_tab():
    """Render the insights prompts management tab."""
    st.subheader("Prompts de Insights")
    prompts = _load_insight_prompts()
    prompt_by_id = {prompt["id"]: prompt for prompt in prompts}
    selected_id = st.session_state.get("insight_prompt_selected_id")
    if selected_id not in prompt_by_id:
        selected_id = None
        st.session_state["insight_prompt_selected_id"] = None
    existing_prompt = prompt_by_id.get(selected_id) if selected_id else {"name": "", "description": "", "prompt_text": "", "created_at": ""}

    col_title, col_new = st.columns([3, 1])
    with col_title:
        st.markdown("Prompts cadastrados")
    with col_new:
        if st.button("Novo prompt", key="insight_prompt_new"):
            st.session_state["insight_prompt_selected_id"] = None
            st.rerun()

    if prompts:
        header_id, header_name, header_desc, header_date, header_edit, header_del = st.columns([1, 2, 3, 2, 1, 1])
        header_id.caption("ID")
        header_name.caption("Nome")
        header_desc.caption("Descrição")
        header_date.caption("Criado em")
        header_edit.caption("Editar")
        header_del.caption("Excluir")
        for prompt in prompts:
            col_id, col_name, col_desc, col_date, col_edit, col_del = st.columns([1, 2, 3, 2, 1, 1])
            with col_id:
                st.write(prompt["id"])
            with col_name:
                st.write(prompt["name"] or "Sem nome")
            with col_desc:
                st.write(prompt.get("description") or "-")
            with col_date:
                st.write(prompt.get("created_at") or "-")
            with col_edit:
                if st.button("Editar", key=f"edit_insight_prompt_{prompt['id']}"):
                    st.session_state["insight_prompt_selected_id"] = prompt["id"]
                    st.rerun()
            with col_del:
                if st.button("Excluir", key=f"delete_insight_prompt_{prompt['id']}"):
                    _delete_insight_prompt(prompt["id"])
                    if selected_id == prompt["id"]:
                        st.session_state["insight_prompt_selected_id"] = None
                    st.rerun()
    else:
        st.info("Nenhum prompt cadastrado ainda.")

    form_state_key = "insight_prompt_form_id"
    name_key = "insight_prompt_name"
    description_key = "insight_prompt_description"
    prompt_key = "insight_prompt_text"
    if st.session_state.get(form_state_key) != selected_id:
        st.session_state[form_state_key] = selected_id
        st.session_state[name_key] = existing_prompt.get("name", "")
        st.session_state[description_key] = existing_prompt.get("description", "")
        st.session_state[prompt_key] = existing_prompt.get("prompt_text", "")

    with st.form("insight_prompt_form"):
        name_input = st.text_input("Nome", key=name_key)
        description_input = st.text_area("Descrição", key=description_key, height=80)
        if existing_prompt.get("created_at"):
            st.text_input("Data de criação", value=existing_prompt.get("created_at", ""), disabled=True)
        prompt_text_input = st.text_area("Prompt", key=prompt_key, height=240)
        col_save, col_delete = st.columns(2)
        save_btn = col_save.form_submit_button("Salvar", type="primary")
        delete_btn = col_delete.form_submit_button("Excluir", disabled=selected_id is None)

        if save_btn:
            if not name_input.strip():
                st.error("Informe um nome para o prompt.")
            elif not prompt_text_input.strip():
                st.error("Informe o conteúdo do prompt.")
            else:
                saved_id = _save_insight_prompt(
                    name_input.strip(),
                    description_input.strip(),
                    prompt_text_input.strip(),
                    prompt_id=selected_id,
                )
                st.session_state["insight_prompt_selected_id"] = saved_id
                st.success("Prompt salvo.")
                st.rerun()
        if delete_btn and selected_id:
            _delete_insight_prompt(selected_id)
            st.session_state["insight_prompt_selected_id"] = None
            st.success("Prompt excluído.")
            st.rerun()


__all__ = ["render_insights_prompts_tab"]
