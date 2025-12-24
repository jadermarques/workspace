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
from src.utils.timezone import TZ


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


def _format_duration(start_dt: Optional[datetime], end_dt: Optional[datetime]) -> str:
    if not start_dt or not end_dt:
        return ""
    delta = end_dt - start_dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return ""
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


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


def _fetch_conversations(base_url: str, account_id: str, token: str, start_dt: datetime, status: str = "all", max_pages: int = 30, per_page: int = 50) -> List[Dict]:
    conversations = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations"
        params = {"page": page, "per_page": per_page, "sort": "last_activity_at", "status": status}
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


def _fetch_messages(
    base_url: str,
    account_id: str,
    token: str,
    conversation_id: str,
    start_dt: Optional[datetime] = None,
    max_batches: int = 200,
) -> List[Dict]:
    messages = []
    before_id = None
    batches = 0
    while batches < max_batches:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
        params = {}
        if before_id:
            params["before"] = before_id
        try:
            resp = _request_with_retry(
                url,
                params=params,
                headers=_cw_headers(token),
                timeout=25,
                retries=2,
            )
        except requests.exceptions.ReadTimeout as exc:
            raise RuntimeError(
                "Tempo limite ao buscar mensagens no Chatwoot. Tente reduzir o período ou aplicar mais filtros."
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"Erro ao buscar mensagens no Chatwoot: {exc}") from exc
        if resp.status_code >= 400:
            raise RuntimeError(f"Chatwoot respondeu {resp.status_code} ao buscar mensagens: {resp.text[:200]}")
        data = resp.json() or {}
        payload = data.get("data", []) or data.get("payload") or []
        if not payload:
            break
        messages.extend(payload)

        oldest = payload[0]
        next_before = oldest.get("id")
        if not next_before or next_before == before_id:
            break
        before_id = next_before

        if start_dt:
            oldest_dt = _parse_ts(oldest.get("created_at") or oldest.get("timestamp"))
            if oldest_dt and oldest_dt.astimezone(TZ) < start_dt:
                break
        if len(payload) < 20:
            break
        batches += 1
    return messages


def _normalize_conversation(conv: Dict) -> Dict:
    clean = {}
    for k, v in conv.items():
        if isinstance(v, (dict, list)):
            clean[k] = json.dumps(v, ensure_ascii=False)
        else:
            clean[k] = v
    return clean


def _build_state_key(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def _render_conversation_filters(prefix: str, inbox_options: Dict[str, int], agent_options: List[str], default_start: date, default_end: date, result_keys: List[str]) -> Dict:
    defaults = {
        _build_state_key(prefix, "start_date"): default_start,
        _build_state_key(prefix, "end_date"): default_end,
        _build_state_key(prefix, "contact_name"): "",
        _build_state_key(prefix, "agent"): "Todos",
        _build_state_key(prefix, "contact_number"): "",
        _build_state_key(prefix, "conversation_id"): "",
        _build_state_key(prefix, "status"): "Todos",
        _build_state_key(prefix, "assigned"): "Todos",
        _build_state_key(prefix, "inboxes"): list(inbox_options.keys()),
    }
    clear_key = _build_state_key(prefix, "clear_filters")
    if st.session_state.get(clear_key):
        for key, default_value in defaults.items():
            st.session_state[key] = default_value
        for key in result_keys:
            st.session_state.pop(key, None)
        st.session_state[clear_key] = False

    for key, default_value in defaults.items():
        st.session_state.setdefault(key, default_value)
    inbox_key = _build_state_key(prefix, "inboxes")
    if st.session_state.get(inbox_key):
        valid_inboxes = [name for name in st.session_state[inbox_key] if name in inbox_options]
        if not valid_inboxes:
            valid_inboxes = list(inbox_options.keys())
        st.session_state[inbox_key] = valid_inboxes

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Data inicial", key=_build_state_key(prefix, "start_date"))
    with col2:
        end_date = st.date_input("Data final", key=_build_state_key(prefix, "end_date"))

    col3, col4 = st.columns(2)
    with col3:
        contact_name = st.text_input("Nome do contato (parcial)", key=_build_state_key(prefix, "contact_name"))
    with col4:
        agent_label = st.selectbox("Agente", options=agent_options, key=_build_state_key(prefix, "agent"))

    col5, col6 = st.columns(2)
    with col5:
        contact_number = st.text_input("Número do contato (parcial)", key=_build_state_key(prefix, "contact_number"))
    with col6:
        conversation_id_filter = st.text_input("ID da conversa", key=_build_state_key(prefix, "conversation_id"))

    col7, col8 = st.columns(2)
    with col7:
        status_options = ["Todos", "open", "resolved", "pending", "snoozed"]
        status_filter = st.selectbox("Status da conversa", options=status_options, key=_build_state_key(prefix, "status"))
    with col8:
        assigned_filter = st.selectbox("Conversa atribuída", options=["Todos", "Sim", "Não"], key=_build_state_key(prefix, "assigned"))

    selected_inboxes = st.multiselect(
        "Caixa de entrada",
        options=list(inbox_options.keys()),
        key=_build_state_key(prefix, "inboxes"),
    )
    selected_inbox_ids = {inbox_options[name] for name in selected_inboxes} if selected_inboxes else set()

    col_btn1, col_btn2 = st.columns(2)
    gerar = col_btn1.button("Gerar", type="primary", key=_build_state_key(prefix, "generate"))
    limpar = col_btn2.button("Limpar", type="secondary", key=_build_state_key(prefix, "clear"))

    if limpar:
        st.session_state[clear_key] = True
        st.rerun()

    selected_agent_id = None
    if agent_label != "Todos":
        selected_agent_id = str(agent_label.split(" - ")[0]).strip()

    return {
        "start_date": start_date,
        "end_date": end_date,
        "contact_name": contact_name,
        "contact_number": contact_number,
        "conversation_id_filter": conversation_id_filter,
        "status_filter": status_filter,
        "assigned_filter": assigned_filter,
        "selected_inbox_ids": selected_inbox_ids,
        "selected_agent_id": selected_agent_id,
        "gerar": gerar,
    }


def _collect_conversation_rows(conversations: List[Dict], filters: Dict, inbox_id_to_name: Dict[int, str], start_dt: datetime, end_dt: datetime, enforce_created_range: bool = True):
    rows = []
    conversation_api_ids = []
    for conv in conversations:
        api_id = conv.get("id")
        conv_id = api_id or conv.get("display_id")
        if conv_id is None:
            continue
        if filters["conversation_id_filter"] and str(conv_id) != filters["conversation_id_filter"].strip():
            continue

        inbox_id = conv.get("inbox_id")
        if filters["selected_inbox_ids"] and inbox_id not in filters["selected_inbox_ids"]:
            continue
        inbox_name = inbox_id_to_name.get(inbox_id, inbox_id)

        meta = conv.get("meta", {}) or {}
        sender = meta.get("sender") or conv.get("contact") or {}
        contact_name_val = sender.get("name") or sender.get("identifier") or ""
        contact_phone_val = sender.get("phone_number") or sender.get("phone") or sender.get("identifier") or ""

        if filters["contact_name"] and not _match_partial(contact_name_val, filters["contact_name"]):
            continue
        if filters["contact_number"] and not _match_partial(contact_phone_val, filters["contact_number"]):
            continue

        assignee = meta.get("assignee") or conv.get("assignee") or {}
        assignee_id = assignee.get("id") if isinstance(assignee, dict) else None
        assignee_name = assignee.get("name") or assignee.get("email") if isinstance(assignee, dict) else ""

        if filters["selected_agent_id"] and str(assignee_id) != str(filters["selected_agent_id"]):
            continue
        if filters["assigned_filter"] == "Sim" and not assignee_id:
            continue
        if filters["assigned_filter"] == "Não" and assignee_id:
            continue

        status_val = conv.get("status") or meta.get("status")
        if filters["status_filter"] != "Todos" and str(status_val) != filters["status_filter"]:
            continue

        if enforce_created_range:
            created_raw = conv.get("created_at")
            created_dt = _parse_ts(created_raw)
            if not created_dt:
                continue
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
        if api_id is not None:
            conversation_api_ids.append(api_id)
    return rows, conversation_api_ids


def _message_direction(msg: Dict) -> Optional[str]:
    msg_type = msg.get("message_type")
    if isinstance(msg_type, int):
        if msg_type == 0:
            return "incoming"
        if msg_type == 1:
            return "outgoing"
        return None
    msg_type_str = str(msg_type).lower()
    if msg_type_str.isdigit():
        try:
            msg_type_int = int(msg_type_str)
        except ValueError:
            msg_type_int = None
        if msg_type_int == 0:
            return "incoming"
        if msg_type_int == 1:
            return "outgoing"
        return None
    if msg_type_str in ("incoming", "outgoing"):
        return msg_type_str
    return None


def _count_message_directions(messages: List[Dict], start_dt: datetime, end_dt: datetime):
    received = 0
    sent = 0
    private_total = 0
    for msg in messages:
        msg_ts_raw = msg.get("created_at") or msg.get("timestamp")
        msg_dt = _parse_ts(msg_ts_raw)
        if msg_dt:
            msg_local = msg_dt.astimezone(TZ)
            if msg_local < start_dt or msg_local > end_dt:
                continue
        is_private = msg.get("private")
        if isinstance(is_private, str):
            is_private = is_private.strip().lower() in ("true", "1", "yes", "sim")
        if is_private:
            private_total += 1
        direction = _message_direction(msg)
        if direction == "incoming":
            received += 1
        elif direction == "outgoing":
            sent += 1
    return received, sent, private_total


def render_conversations_tab():
    st.subheader("Conversas")
    settings = load_settings() or {}
    cw_url = (settings.get("chatwoot_url") or "").rstrip("/")
    cw_token = settings.get("chatwoot_api_token") or ""
    cw_account = settings.get("chatwoot_account_id") or ""

    if not all([cw_url, cw_token, cw_account]):
        st.error("Configure CHATWOOT_URL, CHATWOOT_API_TOKEN e CHATWOOT_ACCOUNT_ID para usar este painel.")
        return

    today_local = datetime.now(TZ).date()
    default_start = today_local - timedelta(days=7)
    default_end = today_local

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

    filters = _render_conversation_filters(
        "conv",
        inbox_options,
        agent_options,
        default_start,
        default_end,
        result_keys=["conv_results"],
    )

    if filters["gerar"]:
        if filters["start_date"] > filters["end_date"]:
            st.error("Período inválido: data inicial maior que a final.")
            return
        start_dt = datetime.combine(filters["start_date"], time.min, tzinfo=TZ)
        end_dt = datetime.combine(filters["end_date"], time.max, tzinfo=TZ)

        with st.spinner("Buscando conversas no Chatwoot..."):
            try:
                conversations = _fetch_conversations(
                    cw_url,
                    cw_account,
                    cw_token,
                    start_dt,
                    status=filters["status_filter"] if filters["status_filter"] != "Todos" else "all",
                )
            except Exception as e:
                st.error(f"Falha ao buscar conversas: {e}")
                return

        rows, _ = _collect_conversation_rows(conversations, filters, inbox_id_to_name, start_dt, end_dt, enforce_created_range=True)
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
    elif filters["gerar"]:
        st.info("Nenhuma conversa encontrada para os filtros.")


def render_conversations_analysis_tab():
    st.subheader("Análise de Conversas")
    settings = load_settings() or {}
    cw_url = (settings.get("chatwoot_url") or "").rstrip("/")
    cw_token = settings.get("chatwoot_api_token") or ""
    cw_account = settings.get("chatwoot_account_id") or ""

    if not all([cw_url, cw_token, cw_account]):
        st.error("Configure CHATWOOT_URL, CHATWOOT_API_TOKEN e CHATWOOT_ACCOUNT_ID para usar este painel.")
        return

    today_local = datetime.now(TZ).date()
    default_start = today_local - timedelta(days=7)
    default_end = today_local

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

    filters = _render_conversation_filters(
        "conv_analysis",
        inbox_options,
        agent_options,
        default_start,
        default_end,
        result_keys=["conv_analysis_stats", "conv_analysis_messages"],
    )

    if filters["gerar"]:
        if filters["start_date"] > filters["end_date"]:
            st.error("Período inválido: data inicial maior que a final.")
            return
        start_dt = datetime.combine(filters["start_date"], time.min, tzinfo=TZ)
        end_dt = datetime.combine(filters["end_date"], time.max, tzinfo=TZ)

        with st.spinner("Buscando conversas no Chatwoot..."):
            try:
                conversations = _fetch_conversations(
                    cw_url,
                    cw_account,
                    cw_token,
                    start_dt,
                    status=filters["status_filter"] if filters["status_filter"] != "Todos" else "all",
                )
            except Exception as e:
                st.error(f"Falha ao buscar conversas: {e}")
                return

        rows, conv_api_ids = _collect_conversation_rows(conversations, filters, inbox_id_to_name, start_dt, end_dt, enforce_created_range=True)
        message_scope_rows, message_conv_ids = _collect_conversation_rows(
            conversations,
            filters,
            inbox_id_to_name,
            start_dt,
            end_dt,
            enforce_created_range=False,
        )
        if not rows:
            st.session_state["conv_analysis_stats"] = None
            st.session_state["conv_analysis_messages"] = []
        else:
            total_recebidas = 0
            total_enviadas = 0
            total_privadas = 0
            conversas_privadas = set()
            message_rows = []
            message_conv_set = set(message_conv_ids)
            table_conv_set = set(conv_api_ids)
            conv_meta = {}
            for conv in conversations:
                conv_id = conv.get("id") or conv.get("display_id")
                if conv_id is None or conv_id not in message_conv_set:
                    continue
                created_raw = conv.get("created_at")
                created_dt = _parse_ts(created_raw)
                created_local = created_dt.astimezone(TZ) if created_dt else None
                first_reply_raw = conv.get("first_reply_created_at")
                first_reply_dt = None
                if first_reply_raw not in (None, "", 0, "0", 0.0, "0.0"):
                    first_reply_dt = _parse_ts(first_reply_raw)
                    if first_reply_dt:
                        first_reply_dt = first_reply_dt.astimezone(TZ)
                meta = conv.get("meta", {}) or {}
                sender = meta.get("sender") or conv.get("contact") or {}
                contact_name_val = sender.get("name") or sender.get("identifier") or ""
                contact_phone_val = sender.get("phone_number") or sender.get("phone") or sender.get("identifier") or ""
                conv_meta[conv_id] = {
                    "created_dt": created_local,
                    "created_str": created_local.strftime("%d/%m/%Y %H:%M:%S") if created_local else "",
                    "inbox_name": inbox_id_to_name.get(conv.get("inbox_id"), conv.get("inbox_id")),
                    "first_reply_delta": _format_duration(created_local, first_reply_dt),
                    "contact_name": contact_name_val,
                    "contact_phone": contact_phone_val,
                }
            with st.spinner("Contando mensagens das conversas..."):
                for conv_id in message_conv_ids:
                    conv_info = conv_meta.get(conv_id) or {}
                    try:
                        msgs = _fetch_messages(cw_url, cw_account, cw_token, conv_id, start_dt=start_dt)
                    except Exception as e:
                        st.warning(f"Falha ao buscar mensagens da conversa {conv_id}: {e}")
                        continue
                    recv, sent, priv = _count_message_directions(msgs, start_dt, end_dt)
                    total_recebidas += recv
                    total_enviadas += sent
                    total_privadas += priv
                    if priv:
                        conversas_privadas.add(conv_id)
                    should_add_rows = conv_id in table_conv_set
                    for msg in msgs:
                        msg_dt_raw = msg.get("created_at") or msg.get("timestamp")
                        msg_dt = _parse_ts(msg_dt_raw)
                        msg_local = None
                        if msg_dt:
                            msg_local = msg_dt.astimezone(TZ)
                            if msg_local < start_dt or msg_local > end_dt:
                                continue
                        if not should_add_rows:
                            continue
                        content = msg.get("content")
                        if content is None:
                            content = msg.get("processed_message_content") or ""
                        message_rows.append(
                            {
                                "id_conversa": conv_id,
                                "nome do contato": conv_info.get("contact_name", ""),
                                "numero do contato": conv_info.get("contact_phone", ""),
                                "data hora de início da conversa": conv_info.get("created_str", ""),
                                "caixa de entrada": conv_info.get("inbox_name", ""),
                                "tempo para a primeira resposta": conv_info.get("first_reply_delta", ""),
                                "mensagem": content,
                                "_sort_conv_dt": conv_info.get("created_dt"),
                                "_sort_msg_dt": msg_local if msg_dt else None,
                            }
                        )
            message_rows.sort(
                key=lambda row: (
                    row.get("_sort_conv_dt") or datetime.min.replace(tzinfo=TZ),
                    str(row.get("id_conversa")),
                    row.get("_sort_msg_dt") or datetime.min.replace(tzinfo=TZ),
                )
            )
            for row in message_rows:
                row.pop("_sort_conv_dt", None)
                row.pop("_sort_msg_dt", None)
            st.session_state["conv_analysis_stats"] = {
                "total_conversas": len(rows),
                "total_conversas_privadas": len(conversas_privadas),
                "total_recebidas": total_recebidas,
                "total_enviadas": total_enviadas,
                "total_privadas": total_privadas,
            }
            st.session_state["conv_analysis_messages"] = message_rows

    stats = st.session_state.get("conv_analysis_stats")
    if stats:
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total de conversas", stats["total_conversas"])
        col2.metric("Total de conversas privadas", stats["total_conversas_privadas"])
        col3.metric("Total de mensagens recebidas", stats["total_recebidas"])
        col4.metric("Total de mensagens enviadas", stats["total_enviadas"])
        col5.metric("Total de mensagens privadas", stats["total_privadas"])
        messages_table = st.session_state.get("conv_analysis_messages") or []
        if messages_table:
            df_messages = pd.DataFrame(messages_table)
            display_cols = [
                "id_conversa",
                "nome do contato",
                "numero do contato",
                "data hora de início da conversa",
                "caixa de entrada",
                "tempo para a primeira resposta",
                "mensagem",
            ]
            df_messages = df_messages.reindex(columns=display_cols)
            st.dataframe(df_messages, use_container_width=True)
            csv_data = df_messages.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Exportar CSV",
                data=csv_data,
                file_name="analise_conversas_mensagens.csv",
                mime="text/csv",
                key="conv_analysis_messages_csv",
            )
        elif filters["gerar"]:
            st.info("Nenhuma mensagem encontrada para o período selecionado.")
    elif filters["gerar"]:
        st.info("Nenhuma conversa encontrada para os filtros.")


__all__ = ["render_conversations_tab", "render_conversations_analysis_tab"]
