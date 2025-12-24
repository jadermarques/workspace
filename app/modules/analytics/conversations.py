import sys
import json
import re
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bot.engine import load_settings

TZ = timezone(timedelta(hours=-3))


def _cw_headers(token: str) -> Dict[str, str]:
    return {"api_access_token": token, "Content-Type": "application/json"}


def _parse_ts(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except Exception:
        return None
    return None


def _format_datetime_value(value) -> str:
    dt = _parse_ts(value)
    if not dt:
        return value
    return dt.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")


def _match_partial(text: str, pattern: str) -> bool:
    if not pattern:
        return True
    text = text or ""
    pattern = pattern.strip()
    if not pattern:
        return True
    if "*" in pattern:
        regex = re.escape(pattern).replace("\\*", ".*")
        return re.search(regex, text, re.IGNORECASE) is not None
    return pattern.lower() in text.lower()


def _request_with_retry(url: str, params: Dict, headers: Dict, timeout: int, retries: int = 1):
    last_exc = None
    current_timeout = timeout
    for attempt in range(retries + 1):
        try:
            return requests.get(url, params=params, headers=headers, timeout=current_timeout)
        except requests.exceptions.ReadTimeout as exc:
            last_exc = exc
            if attempt < retries:
                time_module.sleep(1 + attempt)
                current_timeout = min(current_timeout + 10, 60)
                continue
            raise
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt < retries:
                time_module.sleep(1 + attempt)
                continue
            raise
    raise last_exc


def _fetch_inboxes(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 100) -> List[Dict]:
    inboxes = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/inboxes"
        resp = requests.get(
            url,
            params={"page": page, "per_page": per_page},
            headers=_cw_headers(token),
            timeout=15,
        )
        if resp.status_code >= 400:
            break
        data = resp.json() or {}
        payload = []
        if isinstance(data, dict):
            payload = data.get("data") or data.get("payload") or data.get("inboxes") or []
        elif isinstance(data, list):
            payload = data
        if not payload:
            break
        for inbox in payload:
            if isinstance(inbox, dict):
                inboxes.append(
                    {
                        "id": inbox.get("id"),
                        "name": inbox.get("name") or inbox.get("channel_type") or str(inbox.get("id")),
                    }
                )
        if len(payload) < per_page:
            break
        page += 1
    return inboxes


def _fetch_agents(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 100) -> List[Dict]:
    agents = []
    endpoints = ["/users", "/agents"]
    for endpoint in endpoints:
        page = 1
        while page <= max_pages:
            url = f"{base_url}/api/v1/accounts/{account_id}{endpoint}"
            resp = requests.get(
                url,
                params={"page": page, "per_page": per_page},
                headers=_cw_headers(token),
                timeout=15,
            )
            if resp.status_code >= 400:
                break
            data = resp.json() or {}
            payload = []
            if isinstance(data, dict):
                payload = data.get("data") or data.get("payload") or data.get("users") or data.get("agents") or []
            elif isinstance(data, list):
                payload = data
            if not payload:
                break
            for user in payload:
                if isinstance(user, dict):
                    uid = user.get("id")
                    name = user.get("name") or user.get("email") or str(uid)
                    if uid:
                        agents.append({"id": uid, "name": name})
            if len(payload) < per_page:
                break
            page += 1
        if agents:
            break
    return agents


def _fetch_conversations(base_url: str, account_id: str, token: str, start_dt: datetime, status: Optional[str] = None, max_pages: int = 30, per_page: int = 50) -> List[Dict]:
    conversations = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations"
        params = {"page": page, "per_page": per_page, "sort": "last_activity_at"}
        if status and status != "Todos":
            params["status"] = status
        try:
            resp = _request_with_retry(url, params=params, headers=_cw_headers(token), timeout=25, retries=2)
        except requests.exceptions.ReadTimeout as exc:
            raise RuntimeError(
                "Tempo limite ao buscar conversas no Chatwoot. Tente reduzir o período ou aplicar mais filtros."
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"Erro ao buscar conversas no Chatwoot: {exc}") from exc
        if resp.status_code >= 400:
            raise RuntimeError(f"Chatwoot respondeu {resp.status_code} ao listar conversas: {resp.text[:200]}")
        data = resp.json() or {}
        payload = data.get("data", {}).get("payload") or data.get("payload") or []
        if not payload:
            break
        conversations.extend(payload)
        last = payload[-1]
        last_ts_raw = last.get("last_activity_at") or last.get("updated_at") or last.get("created_at")
        last_dt = _parse_ts(last_ts_raw)
        if last_dt and last_dt.astimezone(TZ) < start_dt:
            break
        page += 1
    return conversations


def _normalize_conversation(conv: Dict) -> Dict:
    clean = {}
    for k, v in conv.items():
        if isinstance(v, (dict, list)):
            clean[k] = json.dumps(v, ensure_ascii=False)
        else:
            clean[k] = v
    return clean


def render_conversations_tab():
    st.subheader("Conversas")
    settings = load_settings() or {}
    cw_url = (settings.get("chatwoot_url") or "").rstrip("/")
    cw_token = settings.get("chatwoot_api_token") or ""
    cw_account = settings.get("chatwoot_account_id") or ""

    if not all([cw_url, cw_token, cw_account]):
        st.error("Configure CHATWOOT_URL, CHATWOOT_API_TOKEN e CHATWOOT_ACCOUNT_ID para usar este painel.")
        return

    default_start = date.today() - timedelta(days=7)
    default_end = date.today()

    inbox_cache = st.session_state.get("cw_conv_inboxes")
    if inbox_cache is None:
        with st.spinner("Carregando caixas de entrada..."):
            try:
                inbox_cache = _fetch_inboxes(cw_url, cw_account, cw_token)
            except Exception as e:
                inbox_cache = []
                st.warning(f"Não foi possível carregar caixas de entrada: {e}")
        st.session_state["cw_conv_inboxes"] = inbox_cache
    inbox_options = {i["name"]: i["id"] for i in inbox_cache if i.get("id")}
    inbox_id_to_name = {i["id"]: i["name"] for i in inbox_cache if i.get("id")}

    agents_cache = st.session_state.get("cw_conv_agents")
    if agents_cache is None:
        with st.spinner("Carregando agentes/usuários..."):
            try:
                agents_cache = _fetch_agents(cw_url, cw_account, cw_token)
            except Exception as e:
                agents_cache = []
                st.warning(f"Não foi possível carregar agentes/usuários: {e}")
        st.session_state["cw_conv_agents"] = agents_cache
    agent_options = ["Todos"] + [f"{a['id']} - {a['name']}" for a in agents_cache]

    defaults = {
        "conv_start_date": default_start,
        "conv_end_date": default_end,
        "conv_contact_name": "",
        "conv_agent": "Todos",
        "conv_contact_number": "",
        "conv_conversation_id": "",
        "conv_status": "Todos",
        "conv_assigned": "Todos",
        "conv_inboxes": list(inbox_options.keys()),
    }
    if st.session_state.get("conv_clear_filters"):
        for key, default_value in defaults.items():
            st.session_state[key] = default_value
        st.session_state.pop("conv_results", None)
        st.session_state["conv_clear_filters"] = False

    for key, default_value in defaults.items():
        st.session_state.setdefault(key, default_value)
    if st.session_state.get("conv_inboxes"):
        valid_inboxes = [name for name in st.session_state["conv_inboxes"] if name in inbox_options]
        if not valid_inboxes:
            valid_inboxes = list(inbox_options.keys())
        st.session_state["conv_inboxes"] = valid_inboxes

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Data inicial", key="conv_start_date")
    with col2:
        end_date = st.date_input("Data final", key="conv_end_date")

    col3, col4 = st.columns(2)
    with col3:
        contact_name = st.text_input("Nome do contato (parcial)", key="conv_contact_name")
    with col4:
        agent_label = st.selectbox("Agente", options=agent_options, key="conv_agent")

    col5, col6 = st.columns(2)
    with col5:
        contact_number = st.text_input("Número do contato (parcial)", key="conv_contact_number")
    with col6:
        conversation_id_filter = st.text_input("ID da conversa", key="conv_conversation_id")

    col7, col8 = st.columns(2)
    with col7:
        status_options = ["Todos", "open", "resolved", "pending", "snoozed"]
        status_filter = st.selectbox("Status da conversa", options=status_options, key="conv_status")
    with col8:
        assigned_filter = st.selectbox("Conversa atribuída", options=["Todos", "Sim", "Não"], key="conv_assigned")

    selected_inboxes = st.multiselect(
        "Caixa de entrada",
        options=list(inbox_options.keys()),
        key="conv_inboxes",
    )
    selected_inbox_ids = {inbox_options[name] for name in selected_inboxes} if selected_inboxes else set()

    col_btn1, col_btn2 = st.columns(2)
    gerar = col_btn1.button("Gerar", type="primary", key="conv_generate")
    limpar = col_btn2.button("Limpar", type="secondary", key="conv_clear")

    if limpar:
        st.session_state["conv_clear_filters"] = True
        st.rerun()

    selected_agent_id = None
    if agent_label != "Todos":
        selected_agent_id = str(agent_label.split(" - ")[0]).strip()

    if gerar:
        if start_date > end_date:
            st.error("Período inválido: data inicial maior que a final.")
            return
        start_dt = datetime.combine(start_date, time.min, tzinfo=TZ)
        end_dt = datetime.combine(end_date, time.max, tzinfo=TZ)

        with st.spinner("Buscando conversas no Chatwoot..."):
            try:
                conversations = _fetch_conversations(
                    cw_url,
                    cw_account,
                    cw_token,
                    start_dt,
                    status=status_filter if status_filter != "Todos" else None,
                )
            except Exception as e:
                st.error(f"Falha ao buscar conversas: {e}")
                return

        rows = []
        for conv in conversations:
            conv_id = conv.get("id") or conv.get("display_id")
            if conv_id is None:
                continue
            if conversation_id_filter and str(conv_id) != conversation_id_filter.strip():
                continue

            inbox_id = conv.get("inbox_id")
            if selected_inbox_ids and inbox_id not in selected_inbox_ids:
                continue
            inbox_name = inbox_id_to_name.get(inbox_id, inbox_id)

            meta = conv.get("meta", {}) or {}
            sender = meta.get("sender") or conv.get("contact") or {}
            contact_name_val = sender.get("name") or sender.get("identifier") or ""
            contact_phone_val = sender.get("phone_number") or sender.get("phone") or sender.get("identifier") or ""

            if contact_name and not _match_partial(contact_name_val, contact_name):
                continue
            if contact_number and not _match_partial(contact_phone_val, contact_number):
                continue

            assignee = meta.get("assignee") or conv.get("assignee") or {}
            assignee_id = assignee.get("id") if isinstance(assignee, dict) else None
            assignee_name = assignee.get("name") or assignee.get("email") if isinstance(assignee, dict) else ""

            if selected_agent_id and str(assignee_id) != str(selected_agent_id):
                continue
            if assigned_filter == "Sim" and not assignee_id:
                continue
            if assigned_filter == "Não" and assignee_id:
                continue

            status_val = conv.get("status") or meta.get("status")
            if status_filter != "Todos" and str(status_val) != status_filter:
                continue

            created_raw = conv.get("created_at") or conv.get("last_activity_at") or conv.get("updated_at")
            created_dt = _parse_ts(created_raw)
            if created_dt:
                created_local = created_dt.astimezone(TZ)
                if created_local < start_dt or created_local > end_dt:
                    continue

            row = _normalize_conversation(conv)
            row.update(
                {
                    "conversation_id": conv_id,
                    "contact_name": contact_name_val,
                    "contact_phone": contact_phone_val,
                    "assignee_id": assignee_id,
                    "assignee_name": assignee_name,
                    "inbox_name": inbox_name,
                }
            )
            rows.append(row)

        st.session_state["conv_results"] = rows

    results = st.session_state.get("conv_results") or []
    if results:
        df = pd.DataFrame(results)
        for col in [
            "created_at",
            "updated_at",
            "timestamp",
            "last_activity_at",
            "waiting_since",
            "agent_last_seen_at",
            "first_reply_created_at",
            "assignee_last_seen_at",
        ]:
            if col in df.columns:
                df[col] = df[col].apply(_format_datetime_value)
        columns_selected = st.multiselect(
            "Colunas para visualizar",
            options=list(df.columns),
            default=list(df.columns),
            help="Selecione quais colunas deseja ver na tabela abaixo.",
        )
        if not columns_selected:
            st.info("Selecione ao menos uma coluna para visualizar os dados.")
            return
        df_display = df.reindex(columns=columns_selected)
        st.dataframe(df_display, use_container_width=True)
        csv_data = df_display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Exportar CSV",
            data=csv_data,
            file_name="conversas_chatwoot.csv",
            mime="text/csv",
        )
    elif gerar:
        st.info("Nenhuma conversa encontrada para os filtros.")


__all__ = ["render_conversations_tab"]
