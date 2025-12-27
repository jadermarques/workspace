from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import app.components.sidebar as sidebar
from src.utils import database, db_init


def _get_tab(at: AppTest, label: str):
    for tab in at.tabs:
        if tab.label == label:
            return tab
    raise AssertionError(f"Tab '{label}' nao encontrada.")


def _click_button_by_label(container, label: str):
    for btn in container.button:
        if btn.label == label:
            btn.click()
            return
    raise AssertionError(f"Botao '{label}' nao encontrado.")


def _fetch_prompt_row(db_path: Path):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, description, prompt_text FROM insight_prompts ORDER BY id DESC")
        return cur.fetchone()


def _count_prompts(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM insight_prompts")
        return int(cur.fetchone()[0])


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    db_path = tmp_path / "bot_config.db"
    monkeypatch.setattr(db_init, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db_init, "DB_PATH", db_path)
    monkeypatch.setattr(database, "DB_PATH", db_path)
    db_init.ensure_db()
    return db_path


def test_insights_prompts_crud(isolated_db, monkeypatch):
    monkeypatch.setattr(sidebar, "_bootstrap_bot_state", lambda: None)

    at = AppTest.from_file("app/pages/04_🗂️_Gestão.py").run()
    tab = _get_tab(at, "Prompts de Insights")

    tab.text_input(key="insight_prompt_name").set_value("Prompt teste")
    tab.text_area(key="insight_prompt_description").set_value("Descricao de teste")
    tab.text_area(key="insight_prompt_text").set_value("Texto do prompt de teste.")
    _click_button_by_label(tab, "Salvar")
    at = at.run()

    row = _fetch_prompt_row(isolated_db)
    assert row is not None
    prompt_id, name, description, prompt_text = row
    assert name == "Prompt teste"
    assert description == "Descricao de teste"
    assert prompt_text == "Texto do prompt de teste."

    tab = _get_tab(at, "Prompts de Insights")
    tab.button(key=f"edit_insight_prompt_{prompt_id}").click()
    at = at.run()

    tab = _get_tab(at, "Prompts de Insights")
    tab.text_input(key="insight_prompt_name").set_value("Prompt atualizado")
    tab.text_area(key="insight_prompt_description").set_value("Descricao atualizada")
    tab.text_area(key="insight_prompt_text").set_value("Texto do prompt atualizado.")
    _click_button_by_label(tab, "Salvar")
    at = at.run()

    row = _fetch_prompt_row(isolated_db)
    assert row is not None
    _, name, description, prompt_text = row
    assert name == "Prompt atualizado"
    assert description == "Descricao atualizada"
    assert prompt_text == "Texto do prompt atualizado."

    tab = _get_tab(at, "Prompts de Insights")
    tab.button(key=f"delete_insight_prompt_{prompt_id}").click()
    at = at.run()

    assert _count_prompts(isolated_db) == 0
