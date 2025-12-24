import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # raiz do projeto
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os
from datetime import datetime, timezone

import requests
import streamlit as st
from openai import OpenAI

from app.components.sidebar import DEFAULT_MODULES, render_sidebar
from src.bot.engine import load_env_once, load_settings
from src.utils.db_init import DB_PATH, ensure_db
from src.utils.timezone import TZ


def main():
    st.set_page_config(page_title="Principal", page_icon="🧰", layout="wide")
    render_sidebar(modules=DEFAULT_MODULES, default="Principal", show_selector=False)

    load_env_once()
    st.title("Principal")
    st.caption("Visão geral do sistema")

    def status_chip(label: str, status: str, detail: str = ""):
        colors = {
            "ok": ("#16a34a", "#ecfdf3"),
            "warn": ("#b45309", "#fffbeb"),
            "error": ("#b91c1c", "#fef2f2"),
        }
        fg, bg = colors.get(status, ("#1f2937", "#f3f4f6"))
        dot = "🟢" if status == "ok" else "🟠" if status == "warn" else "🔴"
        st.markdown(
            f"""
            <div style="background:{bg};color:{fg};padding:12px 14px;border-radius:10px;border:1px solid rgba(0,0,0,0.05);">
                <strong>{dot} {label}</strong><br/>
                <span style="font-size:13px;">{detail}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Status DB
    try:
        ensure_db()
        db_exists = DB_PATH.exists()
        db_status = ("ok", f"DB OK em {DB_PATH}" if db_exists else "DB criado em memória")
    except Exception as e:
        db_status = ("error", f"Erro: {e}")

    # Status Chatwoot
    settings = load_settings() or {}
    cw_url = (settings.get("chatwoot_url") or "").rstrip("/")
    cw_token = settings.get("chatwoot_api_token") or ""
    cw_account = settings.get("chatwoot_account_id") or ""
    if cw_url and cw_token and cw_account:
        try:
            resp = requests.get(
                f"{cw_url}/api/v1/accounts/{cw_account}",
                headers={"api_access_token": cw_token},
                timeout=5,
            )
            if resp.status_code < 400:
                online_msg = ""
                try:
                    online_count = 0
                    # 1) Tenta endpoint de usuários
                    users_resp = requests.get(
                        f"{cw_url}/api/v1/accounts/{cw_account}/users",
                        headers={"api_access_token": cw_token},
                        params={"page": 1, "per_page": 200},
                        timeout=5,
                    )
                    users_data = users_resp.json() if users_resp.status_code < 400 else {}
                    users_list = []
                    if isinstance(users_data, dict):
                        users_list = users_data.get("data") or users_data.get("payload") or users_data.get("users") or []
                    elif isinstance(users_data, list):
                        users_list = users_data
                    # 2) Se vazio, tenta endpoint de agentes
                    if not users_list:
                        agents_resp = requests.get(
                            f"{cw_url}/api/v1/accounts/{cw_account}/agents",
                            headers={"api_access_token": cw_token},
                            params={"page": 1, "per_page": 200},
                            timeout=5,
                        )
                        agents_data = agents_resp.json() if agents_resp.status_code < 400 else {}
                        if isinstance(agents_data, dict):
                            users_list = agents_data.get("data") or agents_data.get("payload") or agents_data.get("agents") or []
                        elif isinstance(agents_data, list):
                            users_list = agents_data

                    for u in users_list:
                        status = (
                            (u.get("availability_status") or u.get("status") or u.get("availability") or "")
                            if isinstance(u, dict)
                            else ""
                        )
                        status_l = str(status).lower()
                        if status_l in ("online", "busy", "available"):
                            online_count += 1
                    online_msg = f"<br/><span style='font-size:13px;'>Usuários online: {online_count}</span>"
                except Exception:
                    online_msg = ""
                cw_status = ("ok", f"Chatwoot conectado{online_msg}")
            else:
                cw_status = ("warn", f"Chatwoot respondeu {resp.status_code}")
        except Exception as e:
            cw_status = ("warn", f"Chatwoot erro: {e}")
    else:
        cw_status = ("warn", "Credenciais Chatwoot ausentes")

    # Versão e hora do Chatwoot
    if cw_url and cw_token and cw_account:
        try:
            resp = requests.get(
                f"{cw_url}/api",
                headers={"api_access_token": cw_token},
                timeout=5,
            )
            if resp.status_code < 400:
                data = resp.json() or {}
                version = data.get("version") or data.get("chatwoot_version")
                if version:
                    cw_version_status = ("ok", f"Versão {version} • host: {cw_url}")
                else:
                    cw_version_status = ("warn", "Versão não disponível no retorno da API")
                timestamp = data.get("timestamp")
                cw_time_status = ("warn", "Hora não disponível no retorno da API")
                if timestamp:
                    try:
                        if isinstance(timestamp, (int, float)):
                            cw_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        elif isinstance(timestamp, str):
                            try:
                                cw_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                                if cw_dt.tzinfo is None:
                                    cw_dt = cw_dt.replace(tzinfo=timezone.utc)
                            except ValueError:
                                cw_dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
                        else:
                            cw_dt = None
                    except Exception:
                        cw_dt = None
                    if cw_dt:
                        cw_dt_local = cw_dt.astimezone(TZ)
                        cw_time_status = (
                            "ok",
                            "Hora: "
                            f"{cw_dt_local.strftime('%H:%M:%S')} • Time zone: {TZ.key}"
                            f"<br/><span style='font-size:13px;'>UTC: {cw_dt.strftime('%H:%M:%S')}</span>",
                        )
            else:
                cw_version_status = ("warn", f"Chatwoot respondeu {resp.status_code} ao buscar versão")
                cw_time_status = ("warn", f"Chatwoot respondeu {resp.status_code} ao buscar hora")
        except Exception as e:
            cw_version_status = ("warn", f"Erro ao buscar versão: {e}")
            cw_time_status = ("warn", f"Erro ao buscar hora: {e}")
    else:
        cw_version_status = ("warn", "Credenciais Chatwoot ausentes")
        cw_time_status = ("warn", "Credenciais Chatwoot ausentes")

    # Status OpenAI
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        masked = api_key[:4] + "..." + api_key[-4:] if len(api_key) > 8 else "***"
        model_to_test = settings.get("model") or "gpt-4.1-mini"
        try:
            client = OpenAI(api_key=api_key)
            resp = client.responses.create(
                model=model_to_test,
                input="Teste rápido: responda 'ok'.",
                max_output_tokens=5,
            )
            oa_status = ("ok", f"OPENAI_API_KEY carregada ({masked}) — teste OK no modelo {model_to_test}")
        except Exception as e:
            msg = str(e)
            if "proxies" in msg.lower():
                oa_status = ("ok", f"OPENAI_API_KEY carregada ({masked}); teste de modelo ignorado por incompatibilidade de proxy/cliente.")
            else:
                oa_status = ("warn", f"Chave carregada ({masked}), falha ao testar modelo {model_to_test}: {msg}")
    else:
        oa_status = ("error", "OPENAI_API_KEY não encontrada (.env)")

    st.markdown("### Status do sistema")
    col1, col2, col3, col4 = st.columns(4)
    # Monta detalhes do DB (workspace + Chatwoot)
    cw_connected = cw_status[0] == "ok"
    db_lines = []
    db_lines.append(f"Workspace DB: SQLite (bot_config.db) • host: local • status: {'ativo' if db_status[0]=='ok' else db_status[1]}")
    db_lines.append(f"Chatwoot DB: host: {cw_url or 'n/d'} • status: {'ativo' if cw_connected else 'indisponível'} (via API)")
    db_detail = "<br/>".join(db_lines)
    db_overall_status = db_status[0] if cw_connected else ("warn" if db_status[0] == "ok" else db_status[0])

    with col1:
        status_chip("Banco de dados", db_overall_status, db_detail)
    with col2:
        status_chip("Chatwoot", cw_status[0], cw_status[1])
    with col3:
        status_chip("OpenAI", oa_status[0], oa_status[1])
    with col4:
        status_chip("Versão do Chatwoot", cw_version_status[0], cw_version_status[1])

    workspace_now = datetime.now(TZ)
    workspace_time_status = ("ok", f"Hora: {workspace_now.strftime('%H:%M:%S')} • Time zone: {TZ.key}")

    col_time1, col_time2 = st.columns(2)
    with col_time1:
        status_chip("Hora do Chatwoot", cw_time_status[0], cw_time_status[1])
    with col_time2:
        status_chip("Hora do Workspace", workspace_time_status[0], workspace_time_status[1])

    st.markdown("### Como navegar")
    st.info(
        "Use a lista de páginas na lateral (Principal, Bot Studio, Configurações, Relatórios, Gestão, Analytics). "
        "Cada página carrega a aplicação específica na área central."
    )


if __name__ == "__main__":
    main()
