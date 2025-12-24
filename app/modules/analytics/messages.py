import sys
import json
import re
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


def _format_datetime_value(value, with_ms: bool = False) -> str:
    dt = _parse_ts(value)
    if not dt:
        return value
    dt_local = dt.astimezone(TZ)
    if with_ms:
        ms = int(dt_local.microsecond / 1000)
        return f"{dt_local:%d/%m/%Y %H:%M:%S}.{ms:03d}"
    return dt_local.strftime("%d/%m/%Y %H:%M:%S")


def _match_pattern(text: str, pattern: str) -> bool:
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


def _fetch_conversations(base_url: str, account_id: str, token: str, start_dt: datetime, max_pages: int = 50, per_page: int = 50) -> List[Dict]:
    conversations = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations"
        resp = requests.get(
            url,
            params={"page": page, "per_page": per_page, "sort": "last_activity_at"},
            headers=_cw_headers(token),
            timeout=20,
        )
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


def _fetch_messages(base_url: str, account_id: str, token: str, conversation_id: str, max_pages: int = 20, per_page: int = 50) -> List[Dict]:
    messages = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
        resp = requests.get(
            url,
            params={"page": page, "per_page": per_page},
            headers=_cw_headers(token),
            timeout=20,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Chatwoot respondeu {resp.status_code} ao buscar mensagens: {resp.text[:200]}")
        data = resp.json() or {}
        payload = data.get("data", []) or data.get("payload") or []
        if not payload:
            break
        messages.extend(payload)
        if len(payload) < per_page:
            break
        page += 1
    return messages


def _normalize_message(msg: Dict) -> Dict:
    clean = {}
    for k, v in msg.items():
        if isinstance(v, (dict, list)):
            clean[k] = json.dumps(v, ensure_ascii=False)
        else:
            clean[k] = v
    return clean


def _extract_transcription(payload) -> Optional[str]:
    # Apenas lê transcrições já presentes no payload da API do Chatwoot (sem chamadas externas).
    keys = {
        "transcription",
        "transcript",
        "transcribed_text",
        "transcription_text",
        "speech_to_text",
    }

    def _search(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in keys and isinstance(v, str) and v.strip():
                    return v.strip()
                if isinstance(v, (dict, list)):
                    found = _search(v)
                    if found:
                        return found
        elif isinstance(obj, list):
            for item in obj:
                found = _search(item)
                if found:
                    return found
        return None

    return _search(payload)


def render_messages_tab():
    st.subheader("Mensagens")
    settings = load_settings() or {}
    cw_url = (settings.get("chatwoot_url") or "").rstrip("/")
    cw_token = settings.get("chatwoot_api_token") or ""
    cw_account = settings.get("chatwoot_account_id") or ""

    if not all([cw_url, cw_token, cw_account]):
        st.error("Configure CHATWOOT_URL, CHATWOOT_API_TOKEN e CHATWOOT_ACCOUNT_ID para usar este painel.")
        return

    default_start = date.today() - timedelta(days=7)
    default_end = date.today()

    inbox_cache = st.session_state.get("cw_inboxes")
    if inbox_cache is None:
        with st.spinner("Carregando caixas de entrada..."):
            try:
                inbox_cache = _fetch_inboxes(cw_url, cw_account, cw_token)
            except Exception as e:
                inbox_cache = []
                st.warning(f"Não foi possível carregar caixas de entrada: {e}")
        st.session_state["cw_inboxes"] = inbox_cache
    inbox_options = {i["name"]: i["id"] for i in inbox_cache if i.get("id")}
    inbox_id_to_name = {i["id"]: i["name"] for i in inbox_cache if i.get("id")}

    defaults = {
        "msg_start_date": default_start,
        "msg_end_date": default_end,
        "msg_contact_name": "",
        "msg_contact_number": "",
        "msg_conversation_id": "",
        "msg_message_status": "Todos",
        "msg_audio_filter": "Todos",
        "msg_inboxes": list(inbox_options.keys()),
    }
    if st.session_state.get("msg_clear_filters"):
        for key, default_value in defaults.items():
            st.session_state[key] = default_value
        st.session_state.pop("msg_results", None)
        st.session_state["msg_clear_filters"] = False

    for key, default_value in defaults.items():
        st.session_state.setdefault(key, default_value)
    if st.session_state.get("msg_inboxes"):
        valid_inboxes = [name for name in st.session_state["msg_inboxes"] if name in inbox_options]
        if not valid_inboxes:
            valid_inboxes = list(inbox_options.keys())
        st.session_state["msg_inboxes"] = valid_inboxes

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Data inicial", key="msg_start_date")
    with col2:
        end_date = st.date_input("Data final", key="msg_end_date")

    col3, col4 = st.columns(2)
    with col3:
        contact_name = st.text_input("Nome do contato (use * como curinga)", key="msg_contact_name")
    with col4:
        contact_number = st.text_input("Número do contato (parcial)", key="msg_contact_number")

    conversation_id_filter = st.text_input("ID da conversa", key="msg_conversation_id")

    col5, col6 = st.columns(2)
    with col5:
        status_options = ["Todos", "sent", "delivered", "read", "failed", "pending"]
        message_status = st.selectbox(
            "Status da mensagem",
            options=status_options,
            index=0,
            key="msg_message_status",
        )
    with col6:
        audio_filter = st.selectbox(
            "Mensagem Áudio",
            options=["Todos", "Sim", "Não"],
            index=0,
            key="msg_audio_filter",
        )

    selected_inboxes = st.multiselect(
        "Caixa de entrada",
        options=list(inbox_options.keys()),
        key="msg_inboxes",
    )
    selected_inbox_ids = {inbox_options[name] for name in selected_inboxes} if selected_inboxes else set()

    col_btn1, col_btn2 = st.columns(2)
    gerar = col_btn1.button("Gerar", type="primary", key="msg_generate")
    limpar = col_btn2.button("Limpar", type="secondary", key="msg_clear")

    if limpar:
        st.session_state["msg_clear_filters"] = True
        st.rerun()

    if gerar:
        if start_date > end_date:
            st.error("Período inválido: data inicial maior que a final.")
            return
        start_dt = datetime.combine(start_date, time.min, tzinfo=TZ)
        end_dt = datetime.combine(end_date, time.max, tzinfo=TZ)

        with st.spinner("Buscando mensagens no Chatwoot..."):
            try:
                conversations = _fetch_conversations(cw_url, cw_account, cw_token, start_dt)
            except Exception as e:
                st.error(f"Falha ao listar conversas: {e}")
                return

            rows = []
            for conv in conversations:
                conv_id = conv.get("id")
                if conv_id is None:
                    continue
                if conversation_id_filter and str(conv_id) != conversation_id_filter.strip():
                    continue
                inbox_id = conv.get("inbox_id")
                inbox_label = inbox_id_to_name.get(inbox_id, inbox_id)
                if selected_inbox_ids and inbox_id not in selected_inbox_ids:
                    continue

                meta = conv.get("meta", {}) or {}
                sender = meta.get("sender") or conv.get("contact") or {}
                contact_name_val = sender.get("name") or sender.get("identifier") or ""
                contact_phone_val = (
                    sender.get("phone_number")
                    or sender.get("phone")
                    or sender.get("identifier")
                    or ""
                )

                if contact_name and not _match_pattern(contact_name_val, contact_name):
                    continue
                if contact_number and not _match_pattern(contact_phone_val, contact_number):
                    continue

                try:
                    msgs = _fetch_messages(cw_url, cw_account, cw_token, conv_id)
                except Exception as e:
                    st.warning(f"Falha ao buscar mensagens da conversa {conv_id}: {e}")
                    continue

                for msg in msgs:
                    msg_dt_raw = msg.get("created_at") or msg.get("timestamp")
                    msg_dt = _parse_ts(msg_dt_raw)
                    if not msg_dt:
                        continue
                    msg_dt_local = msg_dt.astimezone(TZ)
                    if msg_dt_local < start_dt or msg_dt_local > end_dt:
                        continue

                    status_val = msg.get("status") or msg.get("delivery_status") or msg.get("message_status") or msg.get("state")
                    if message_status != "Todos" and str(status_val) != message_status:
                        continue

                    attachments = msg.get("attachments") or []
                    has_audio = any(
                        isinstance(att, dict) and att.get("file_type") == "audio" for att in attachments
                    )
                    if audio_filter == "Sim" and not has_audio:
                        continue
                    if audio_filter == "Não" and has_audio:
                        continue

                    row = _normalize_message(msg)
                    if has_audio:
                        transcript = (
                            _extract_transcription(attachments)
                            or _extract_transcription(msg.get("content_attributes") or {})
                            or _extract_transcription(msg.get("data") or {})
                        )
                        if transcript:
                            existing_content = row.get("content") or ""
                            prefix = "\n" if existing_content else ""
                            row["content"] = f"{existing_content}{prefix}[transc.]: {transcript}"
                    row.update(
                        {
                            "conversation_id": conv_id,
                            "contact_name": contact_name_val,
                            "contact_phone": contact_phone_val,
                            "inbox_id": inbox_label,
                            "midia": "audio" if has_audio else "",
                        }
                    )
                    rows.append(row)

            st.session_state["msg_results"] = rows

    results = st.session_state.get("msg_results") or []
    if results:
        df = pd.DataFrame(results)
        for col in ["created_at", "updated_at", "timestamp", "waiting_since", "agent_last_seen_at"]:
            if col in df.columns:
                use_ms = col == "created_at"
                df[col] = df[col].apply(lambda v: _format_datetime_value(v, with_ms=use_ms))
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
            file_name="mensagens_chatwoot.csv",
            mime="text/csv",
        )
    elif gerar:
        st.info("Nenhuma mensagem encontrada para os filtros.")


__all__ = ["render_messages_tab"]
