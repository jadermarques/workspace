"""Analytics views and helpers for Chatwoot conversations.

This module fetches conversations/messages from the Chatwoot API, applies filters,
and renders Streamlit tabs for conversation listings and analysis metrics.
"""

import os
import sys
import json
import re
import time as time_module
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from openai import OpenAI

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bot.engine import load_env_once, load_settings
from src.bot.rules import extrair_texto_resposta
from src.utils.database import get_conn
from src.utils.timezone import TZ


def _cw_headers(token: str) -> Dict[str, str]:
    """Build default headers for Chatwoot API requests."""
    return {"api_access_token": token, "Content-Type": "application/json"}


def _parse_ts(value) -> Optional[datetime]:
    """Parse timestamps from numeric or string values into UTC-aware datetimes."""
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
    """Format a timestamp value into local timezone string."""
    dt = _parse_ts(value)
    if not dt:
        return value
    return dt.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")


def _format_duration(start_dt: Optional[datetime], end_dt: Optional[datetime]) -> str:
    """Return a HH:MM:SS duration string for a time delta."""
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
    """Match a partial pattern against text with optional wildcard support."""
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
    """Request a URL with basic retry handling for transient errors."""
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


def _request_with_rate_limit(
    url: str,
    params: Dict,
    headers: Dict,
    timeout: int,
    retries: int = 1,
    rate_limit_retries: int = 3,
    base_delay: float = 1.5,
):
    """Request a URL with retry and basic 429 backoff handling."""
    last_resp = None
    for attempt in range(rate_limit_retries + 1):
        resp = _request_with_retry(url, params=params, headers=headers, timeout=timeout, retries=retries)
        last_resp = resp
        if resp.status_code != 429:
            return resp
        retry_after = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
        delay = None
        if retry_after:
            try:
                delay = float(retry_after)
            except (TypeError, ValueError):
                delay = None
        if delay is None:
            delay = base_delay * (2**attempt)
        time_module.sleep(delay + random.uniform(0, 0.3))
    return last_resp


def _fetch_inboxes(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 100) -> List[Dict]:
    """Fetch inboxes from Chatwoot with pagination."""
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
    """Fetch agents/users from Chatwoot, trying multiple endpoints."""
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


def _fetch_teams(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 100) -> List[Dict]:
    """Fetch teams from Chatwoot with pagination."""
    teams = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/teams"
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
            payload = data.get("data") or data.get("payload") or data.get("teams") or []
        elif isinstance(data, list):
            payload = data
        if not payload:
            break
        for team in payload:
            if isinstance(team, dict):
                team_id = team.get("id")
                name = team.get("name") or team.get("title") or str(team_id)
                if team_id:
                    teams.append({"id": team_id, "name": name})
        if len(payload) < per_page:
            break
        page += 1
    return teams


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_conversations(base_url: str, account_id: str, token: str, start_dt: datetime, status: str = "all", max_pages: int = 30, per_page: int = 50) -> List[Dict]:
    """Fetch conversations from Chatwoot, stopping when past the start date."""
    conversations = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations"
        params = {"page": page, "per_page": per_page, "sort": "last_activity_at", "status": status}
        try:
            resp = _request_with_rate_limit(url, params=params, headers=_cw_headers(token), timeout=25, retries=2)
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


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_messages(
    base_url: str,
    account_id: str,
    token: str,
    conversation_id: str,
    start_dt: Optional[datetime] = None,
    max_batches: int = 200,
) -> List[Dict]:
    """Fetch conversation messages using the `before` cursor for pagination."""
    messages = []
    before_id = None
    batches = 0
    while batches < max_batches:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
        params = {}
        if before_id:
            params["before"] = before_id
        try:
            resp = _request_with_rate_limit(
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
    """Normalize conversation payload by serializing nested values."""
    clean = {}
    for k, v in conv.items():
        if isinstance(v, (dict, list)):
            clean[k] = json.dumps(v, ensure_ascii=False)
        else:
            clean[k] = v
    return clean


def _build_state_key(prefix: str, suffix: str) -> str:
    """Build a namespaced Streamlit session_state key."""
    return f"{prefix}_{suffix}"


def _normalize_message_statuses(key: str, options: List[str]) -> None:
    """Normalize multiselect values for message status filters."""
    if key not in st.session_state:
        return
    current = st.session_state.get(key) or []
    valid = [status for status in current if status in options]
    if not valid:
        st.session_state[key] = ["Todos"]
        return
    if "Todos" in valid and len(valid) > 1:
        st.session_state[key] = [status for status in valid if status != "Todos"]


def _load_insight_prompts() -> List[Dict]:
    """Load insight prompts from the local workspace database."""
    with get_conn() as conn:
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


def _get_insight_prompt(prompt_id: Optional[int]) -> Optional[Dict]:
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


def _run_insights_prompt(prompt_text: str, context_text: str, model: str) -> str:
    """Execute the selected insight prompt via OpenAI and return the output."""
    load_env_once()
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY não definida no ambiente.")
    client = OpenAI(api_key=api_key)
    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": prompt_text},
            {"role": "user", "content": context_text},
        ],
    }
    if not str(model or "").lower().startswith("gpt-5"):
        payload["temperature"] = 0.3
    response = client.responses.create(**payload)
    output = extrair_texto_resposta(response)
    if not output:
        raise RuntimeError("Resposta vazia do modelo.")
    return output


def _build_insights_context(
    conversations: List[Dict],
    filters: Dict,
    inbox_id_to_name: Dict[int, str],
    start_dt: datetime,
    end_dt: datetime,
    cw_url: str,
    cw_account: str,
    cw_token: str,
    max_messages: int = 160,
    max_chars: int = 12000,
):
    """Build a compact context string from filtered conversations/messages."""
    rows, conv_api_ids = _collect_conversation_rows(
        conversations,
        filters,
        inbox_id_to_name,
        start_dt,
        end_dt,
        enforce_created_range=True,
    )
    _, message_conv_ids = _collect_conversation_rows(
        conversations,
        filters,
        inbox_id_to_name,
        start_dt,
        end_dt,
        enforce_created_range=False,
    )

    total_recebidas = 0
    total_enviadas = 0
    total_privadas = 0
    conversas_privadas = set()
    total_mensagens = 0
    message_rows = []
    allowed_conv_ids = set()
    conversation_type = filters.get("conversation_type") or "Todos"
    selected_message_statuses = filters.get("message_statuses") or ["Todos"]
    if "Todos" in selected_message_statuses:
        selected_message_statuses = []

    conv_meta = {}
    for conv in conversations:
        conv_id = conv.get("id") or conv.get("display_id")
        if conv_id is None or conv_id not in message_conv_ids:
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

    max_workers = min(3, max(1, len(message_conv_ids)))
    batch_size = max_workers * 2
    batch_pause = 0.4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch in _chunk_list(message_conv_ids, batch_size):
            future_map = {
                executor.submit(
                    _fetch_messages,
                    cw_url,
                    cw_account,
                    cw_token,
                    conv_id,
                    start_dt=start_dt,
                ): conv_id
                for conv_id in batch
            }
            for future in as_completed(future_map):
                conv_id = future_map[future]
                conv_info = conv_meta.get(conv_id) or {}
                try:
                    msgs = future.result()
                except Exception:
                    continue
                has_bot_outgoing = False
                has_agent_outgoing = False
                conv_received = 0
                conv_sent = 0
                conv_priv = 0
                conv_rows = []
                conv_msg_count = 0
                for msg in msgs:
                    msg_dt_raw = msg.get("created_at") or msg.get("timestamp")
                    msg_dt = _parse_ts(msg_dt_raw)
                    msg_local = None
                    if msg_dt:
                        msg_local = msg_dt.astimezone(TZ)
                        if msg_local < start_dt or msg_local > end_dt:
                            continue
                    status_val = msg.get("status") or msg.get("delivery_status") or msg.get("message_status") or msg.get("state")
                    if selected_message_statuses and str(status_val) not in selected_message_statuses:
                        continue
                    direction = _message_direction(msg)
                    if direction == "outgoing":
                        if _is_bot_sender(msg):
                            has_bot_outgoing = True
                        elif _is_agent_sender(msg):
                            has_agent_outgoing = True
                    if not _include_message_for_type(msg, conversation_type):
                        continue
                    is_private = msg.get("private")
                    if isinstance(is_private, str):
                        is_private = is_private.strip().lower() in ("true", "1", "yes", "sim")
                    if is_private:
                        conv_priv += 1
                    if direction == "incoming":
                        conv_received += 1
                    elif direction == "outgoing":
                        conv_sent += 1
                    content = msg.get("content")
                    if content is None:
                        content = msg.get("processed_message_content") or ""
                    conv_rows.append(
                        {
                            "id_conversa": conv_id,
                            "autor": _message_sender_label(msg),
                            "nome do contato": conv_info.get("contact_name", ""),
                            "numero do contato": conv_info.get("contact_phone", ""),
                            "data hora de início da conversa": conv_info.get("created_str", ""),
                            "caixa de entrada": conv_info.get("inbox_name", ""),
                            "tempo para a primeira resposta": conv_info.get("first_reply_delta", ""),
                            "status da mensagem": status_val,
                            "mensagem": content,
                            "data hora da mensagem": msg_local.strftime("%d/%m/%Y %H:%M:%S") if msg_local else "",
                            "_sort_conv_dt": conv_info.get("created_dt"),
                            "_sort_msg_dt": msg_local if msg_dt else None,
                        }
                    )
                    conv_msg_count += 1
                if conversation_type == "Bot" and not has_bot_outgoing:
                    continue
                if conversation_type == "Agente" and not has_agent_outgoing:
                    continue
                allowed_conv_ids.add(conv_id)
                total_recebidas += conv_received
                total_enviadas += conv_sent
                total_privadas += conv_priv
                total_mensagens += conv_msg_count
                if conv_priv:
                    conversas_privadas.add(conv_id)
                if conv_rows:
                    message_rows.extend(conv_rows)
            if len(message_conv_ids) > batch_size:
                time_module.sleep(batch_pause + random.uniform(0, 0.2))

    if conversation_type != "Todos":
        rows = [row for row in rows if row.get("conversation_id") in allowed_conv_ids]

    unique_clients = set()
    for row in rows:
        contact_key = (row.get("contact_phone") or "").strip()
        if not contact_key:
            contact_key = (row.get("contact_name") or "").strip()
        if contact_key:
            unique_clients.add(contact_key)

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

    stats = {
        "total_conversas": len(rows),
        "total_conversas_privadas": len(conversas_privadas),
        "total_recebidas": total_recebidas,
        "total_enviadas": total_enviadas,
        "total_privadas": total_privadas,
        "total_mensagens": total_mensagens,
        "total_clientes_unicos": len(unique_clients),
    }

    lines = []
    filter_lines = []
    filter_lines.append(f"Período: {start_dt.strftime('%d/%m/%Y')} a {end_dt.strftime('%d/%m/%Y')}")
    if filters.get("contact_name"):
        filter_lines.append(f"Nome do contato (parcial): {filters.get('contact_name')}")
    if filters.get("contact_number"):
        filter_lines.append(f"Número do contato (parcial): {filters.get('contact_number')}")
    if filters.get("conversation_id_filter"):
        filter_lines.append(f"ID da conversa: {filters.get('conversation_id_filter')}")
    filter_lines.append(f"Status da conversa: {filters.get('status_filter', 'Todos')}")
    filter_lines.append(f"Conversa atribuída: {filters.get('assigned_filter', 'Todos')}")
    filter_lines.append(f"Tipo de conversa: {filters.get('conversation_type', 'Todos')}")
    status_list = filters.get("message_statuses") or ["Todos"]
    if "Todos" in status_list:
        filter_lines.append("Status da mensagem: Todos")
    else:
        filter_lines.append(f"Status da mensagem: {', '.join(status_list)}")
    if filters.get("selected_agent_id"):
        filter_lines.append(f"Agente: {filters.get('selected_agent_id')}")
    if filters.get("selected_team_id"):
        filter_lines.append(f"Time: {filters.get('selected_team_id')}")
    selected_inboxes = filters.get("selected_inbox_ids") or set()
    if selected_inboxes:
        inbox_names = [str(inbox_id_to_name.get(i, i)) for i in selected_inboxes]
        inbox_names_sorted = sorted(inbox_names, key=str)
        if len(inbox_names_sorted) > 5:
            preview = ", ".join(inbox_names_sorted[:5])
            filter_lines.append(f"Caixas de entrada: {preview} (+{len(inbox_names_sorted) - 5})")
        else:
            filter_lines.append(f"Caixas de entrada: {', '.join(inbox_names_sorted)}")
    else:
        filter_lines.append("Caixas de entrada: Todas")
    filter_lines.append(f"Limites: {max_messages} mensagens, {max_chars} caracteres")

    lines.append("Filtros aplicados")
    for item in filter_lines:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("Resumo")
    lines.append(f"- Total de conversas: {stats['total_conversas']}")
    lines.append(f"- Total de conversas privadas: {stats['total_conversas_privadas']}")
    lines.append(f"- Total de mensagens recebidas: {stats['total_recebidas']}")
    lines.append(f"- Total de mensagens enviadas: {stats['total_enviadas']}")
    lines.append(f"- Total de mensagens privadas: {stats['total_privadas']}")
    lines.append(f"- Total de mensagens: {stats['total_mensagens']}")
    lines.append(f"- Total de clientes únicos: {stats['total_clientes_unicos']}")
    lines.append("")
    lines.append("Mensagens (amostra):")

    total_chars = 0
    count = 0
    for row in message_rows:
        if count >= max_messages or total_chars >= max_chars:
            break
        content = (row.get("mensagem") or "").replace("\n", " ").strip()
        if len(content) > 240:
            content = content[:240] + "..."
        line = (
            f"- [{row.get('status da mensagem')}] conv {row.get('id_conversa')} | "
            f"{row.get('autor')} | {row.get('data hora da mensagem')} | {content}"
        )
        lines.append(line)
        count += 1
        total_chars += len(line)
    if count < len(message_rows):
        lines.append(f"... ({len(message_rows) - count} mensagens omitidas)")

    return stats, "\n".join(lines), filter_lines

def _chunk_list(values: List, size: int):
    """Yield fixed-size chunks from a list."""
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _insights_filters_signature(filters: Dict) -> tuple:
    """Return a hashable signature for the current insights filters."""
    selected_inboxes = tuple(sorted(filters.get("selected_inbox_ids") or []))
    message_statuses = tuple(sorted(filters.get("message_statuses") or []))
    return (
        filters.get("start_date"),
        filters.get("end_date"),
        filters.get("contact_name") or "",
        filters.get("contact_number") or "",
        filters.get("conversation_id_filter") or "",
        filters.get("status_filter") or "",
        filters.get("assigned_filter") or "",
        selected_inboxes,
        filters.get("selected_agent_id") or "",
        filters.get("selected_team_id") or "",
        filters.get("conversation_type") or "",
        message_statuses,
        filters.get("selected_prompt_id"),
    )


def _render_conversation_filters(
    prefix: str,
    inbox_options: Dict[str, int],
    agent_options: List[str],
    default_start: date,
    default_end: date,
    result_keys: List[str],
    team_options: Optional[List[str]] = None,
    conversation_type_options: Optional[List[str]] = None,
    message_status_options: Optional[List[str]] = None,
    insight_prompt_options: Optional[Dict[str, Optional[int]]] = None,
    require_prompt: bool = False,
    insight_limit_defaults: Optional[Dict[str, int]] = None,
    generate_label: str = "Gerar",
    clear_label: str = "Limpar",
) -> Dict:
    """Render filter controls and return their values in a dict."""
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
    if team_options is not None:
        defaults[_build_state_key(prefix, "team")] = "Todos"
    if conversation_type_options is not None:
        defaults[_build_state_key(prefix, "conversation_type")] = "Todos"
    if message_status_options is not None:
        defaults[_build_state_key(prefix, "message_statuses")] = ["Todos"]
    if insight_prompt_options is not None:
        defaults[_build_state_key(prefix, "insight_prompt")] = next(iter(insight_prompt_options.keys()), "")
    if insight_limit_defaults is not None:
        defaults[_build_state_key(prefix, "insight_max_messages")] = int(
            insight_limit_defaults.get("max_messages", 160)
        )
        defaults[_build_state_key(prefix, "insight_max_chars")] = int(
            insight_limit_defaults.get("max_chars", 12000)
        )
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
    message_status_key = _build_state_key(prefix, "message_statuses")
    if message_status_options is not None:
        _normalize_message_statuses(message_status_key, message_status_options)
    insight_prompt_key = _build_state_key(prefix, "insight_prompt")
    if insight_prompt_options is not None:
        current_prompt = st.session_state.get(insight_prompt_key)
        if current_prompt not in insight_prompt_options:
            st.session_state[insight_prompt_key] = next(iter(insight_prompt_options.keys()), "")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Data inicial", key=_build_state_key(prefix, "start_date"))
    with col2:
        end_date = st.date_input("Data final", key=_build_state_key(prefix, "end_date"))

    if team_options is not None:
        col3, col4, col5 = st.columns(3)
        with col3:
            contact_name = st.text_input("Nome do contato (parcial)", key=_build_state_key(prefix, "contact_name"))
        with col4:
            agent_label = st.selectbox("Agente", options=agent_options, key=_build_state_key(prefix, "agent"))
        with col5:
            team_label = st.selectbox("Time", options=team_options, key=_build_state_key(prefix, "team"))
    else:
        col3, col4 = st.columns(2)
        with col3:
            contact_name = st.text_input("Nome do contato (parcial)", key=_build_state_key(prefix, "contact_name"))
        with col4:
            agent_label = st.selectbox("Agente", options=agent_options, key=_build_state_key(prefix, "agent"))
        team_label = "Todos"

    if conversation_type_options is not None:
        col6, col7, col8 = st.columns(3)
        with col6:
            contact_number = st.text_input("Número do contato (parcial)", key=_build_state_key(prefix, "contact_number"))
        with col7:
            conversation_id_filter = st.text_input("ID da conversa", key=_build_state_key(prefix, "conversation_id"))
        with col8:
            conversation_type = st.selectbox(
                "Tipo de conversa",
                options=conversation_type_options,
                key=_build_state_key(prefix, "conversation_type"),
            )
    else:
        col6, col7 = st.columns(2)
        with col6:
            contact_number = st.text_input("Número do contato (parcial)", key=_build_state_key(prefix, "contact_number"))
        with col7:
            conversation_id_filter = st.text_input("ID da conversa", key=_build_state_key(prefix, "conversation_id"))
        conversation_type = "Todos"

    col9, col10 = st.columns(2)
    with col9:
        status_options = ["Todos", "open", "resolved", "pending", "snoozed"]
        status_filter = st.selectbox("Status da conversa", options=status_options, key=_build_state_key(prefix, "status"))
    with col10:
        assigned_filter = st.selectbox("Conversa atribuída", options=["Todos", "Sim", "Não"], key=_build_state_key(prefix, "assigned"))

    message_statuses = ["Todos"]
    if message_status_options is not None:
        message_statuses = st.multiselect(
            "Status da mensagem",
            options=message_status_options,
            key=_build_state_key(prefix, "message_statuses"),
            help="Selecione um ou mais status. Use 'Todos' para limpar o filtro.",
            on_change=_normalize_message_statuses,
            args=(message_status_key, message_status_options),
        )
        if not message_statuses:
            message_statuses = ["Todos"]

    selected_inboxes = st.multiselect(
        "Caixa de entrada",
        options=list(inbox_options.keys()),
        key=_build_state_key(prefix, "inboxes"),
    )
    selected_inbox_ids = {inbox_options[name] for name in selected_inboxes} if selected_inboxes else set()

    selected_prompt_id = None
    if insight_prompt_options is not None:
        selected_prompt_label = st.selectbox(
            "Modelo de prompt",
            options=list(insight_prompt_options.keys()),
            key=insight_prompt_key,
        )
        selected_prompt_id = insight_prompt_options.get(selected_prompt_label)

    max_messages = None
    max_chars = None
    if insight_limit_defaults is not None:
        col_lim1, col_lim2 = st.columns(2)
        with col_lim1:
            max_messages = st.number_input(
                "Limite de mensagens",
                min_value=10,
                max_value=1000,
                step=10,
                key=_build_state_key(prefix, "insight_max_messages"),
            )
        with col_lim2:
            max_chars = st.number_input(
                "Limite de caracteres do contexto",
                min_value=2000,
                max_value=80000,
                step=1000,
                key=_build_state_key(prefix, "insight_max_chars"),
            )

    col_btn1, col_btn2 = st.columns(2)
    disable_generate = bool(require_prompt and insight_prompt_options is not None and selected_prompt_id is None)
    gerar = col_btn1.button(
        generate_label,
        type="primary",
        key=_build_state_key(prefix, "generate"),
        disabled=disable_generate,
    )
    limpar = col_btn2.button(clear_label, type="secondary", key=_build_state_key(prefix, "clear"))

    if limpar:
        st.session_state[clear_key] = True
        st.rerun()

    selected_agent_id = None
    if agent_label != "Todos":
        selected_agent_id = str(agent_label.split(" - ")[0]).strip()
    selected_team_id = None
    if team_label != "Todos":
        selected_team_id = str(team_label.split(" - ")[0]).strip()

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
        "selected_team_id": selected_team_id,
        "conversation_type": conversation_type,
        "message_statuses": message_statuses,
        "selected_prompt_id": selected_prompt_id,
        "insight_max_messages": int(max_messages) if max_messages is not None else None,
        "insight_max_chars": int(max_chars) if max_chars is not None else None,
        "gerar": gerar,
    }


def _collect_conversation_rows(conversations: List[Dict], filters: Dict, inbox_id_to_name: Dict[int, str], start_dt: datetime, end_dt: datetime, enforce_created_range: bool = True):
    """Filter conversations and build row data plus conversation IDs."""
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

        team_id = conv.get("team_id")
        if not team_id:
            team_data = conv.get("team") or meta.get("team") or {}
            if isinstance(team_data, dict):
                team_id = team_data.get("id")
            else:
                team_id = team_data
        if filters.get("selected_team_id") and str(team_id) != str(filters["selected_team_id"]):
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
    """Infer message direction (incoming/outgoing) from payload."""
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


def _message_sender_type(msg: Dict) -> str:
    """Extract sender type from message payload."""
    sender_type = msg.get("sender_type")
    if not sender_type:
        sender = msg.get("sender") or {}
        if isinstance(sender, dict):
            sender_type = sender.get("type") or sender.get("sender_type")
    if not sender_type:
        sender_info = msg.get("sender_info") or {}
        if isinstance(sender_info, dict):
            sender_type = sender_info.get("type") or sender_info.get("sender_type")
    if not sender_type:
        return ""
    return str(sender_type).strip().lower()


def _parse_env_list(value: str) -> List[str]:
    return [item.strip() for item in re.split(r"[;,]", value or "") if item.strip()]


def _bot_sender_config() -> Dict[str, set]:
    """Load bot sender identifiers from environment."""
    load_env_once()
    names = {name.lower() for name in _parse_env_list(os.getenv("BOT_SENDER_NAMES", ""))}
    ids = {sid for sid in _parse_env_list(os.getenv("BOT_SENDER_IDS", ""))}
    return {"names": names, "ids": ids}


def _sender_identity(msg: Dict) -> Dict[str, str]:
    sender = msg.get("sender") or msg.get("sender_info") or {}
    sender_id = ""
    sender_name = ""
    if isinstance(sender, dict):
        sender_id = str(sender.get("id") or "").strip()
        sender_name = str(sender.get("name") or sender.get("email") or sender.get("identifier") or "").strip()
    return {"id": sender_id, "name": sender_name}


def _is_bot_sender(msg: Dict) -> bool:
    """Return True if the message was sent by a bot."""
    sender_type = _message_sender_type(msg)
    if sender_type in ("agentbot", "bot"):
        return True
    config = _bot_sender_config()
    if not config["names"] and not config["ids"]:
        return False
    identity = _sender_identity(msg)
    if identity["id"] and identity["id"] in config["ids"]:
        return True
    if identity["name"] and identity["name"].lower() in config["names"]:
        return True
    return False


def _is_agent_sender(msg: Dict) -> bool:
    """Return True if the message was sent by a human agent."""
    sender_type = _message_sender_type(msg)
    if _is_bot_sender(msg):
        return False
    return sender_type in ("user", "agent")


def _include_message_for_type(msg: Dict, conversation_type: str) -> bool:
    """Filter messages by conversation type (Bot/Agente/Todos)."""
    if conversation_type == "Todos":
        return True
    direction = _message_direction(msg)
    if direction == "incoming":
        return True
    if direction == "outgoing":
        if conversation_type == "Bot":
            return _is_bot_sender(msg)
        if conversation_type == "Agente":
            return _is_agent_sender(msg) and not _is_bot_sender(msg)
    return False


def _message_sender_label(msg: Dict) -> str:
    """Return a label for who sent the message (agent/bot/client)."""
    direction = _message_direction(msg)
    if direction == "incoming":
        return "Cliente"
    if _is_bot_sender(msg):
        return "Bot"
    if _is_agent_sender(msg):
        identity = _sender_identity(msg)
        if identity["name"]:
            return identity["name"]
        for key in ("sender_name", "sender_email", "agent_name", "user_name"):
            value = msg.get(key)
            if value:
                return str(value)
        return "Agente (tipo não informado)"
    for key in ("sender_name", "sender_email", "agent_name", "user_name"):
        value = msg.get(key)
        if value:
            return str(value)
    sender_type = _message_sender_type(msg)
    if not sender_type:
        return "Agente (tipo não informado)"
    return "Agente"


def render_conversations_tab():
    """Render the Conversations tab with filters, table, and CSV export."""
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
    inbox_id_to_name = {i["id"]: i["name"] for i in inbox_cache if i.get("id")}
    inbox_id_to_name = {i["id"]: i["name"] for i in inbox_cache if i.get("id")}
    inbox_id_to_name = {i["id"]: i["name"] for i in inbox_cache if i.get("id")}
    inbox_id_to_name = {i["id"]: i["name"] for i in inbox_cache if i.get("id")}
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
    """Render the Conversations Analysis tab with metrics and message table."""
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

    teams_cache = st.session_state.get("cw_conv_teams")
    if teams_cache is None:
        with st.spinner("Carregando times..."):
            try:
                teams_cache = _fetch_teams(cw_url, cw_account, cw_token)
            except Exception as e:
                teams_cache = []
                st.warning(f"Não foi possível carregar times: {e}")
        st.session_state["cw_conv_teams"] = teams_cache
    team_options = ["Todos"] + [f"{t['id']} - {t['name']}" for t in teams_cache]

    filters = _render_conversation_filters(
        "conv_analysis",
        inbox_options,
        agent_options,
        default_start,
        default_end,
        result_keys=["conv_analysis_stats", "conv_analysis_messages"],
        team_options=team_options,
        conversation_type_options=["Agente", "Bot", "Todos"],
        message_status_options=["Todos", "sent", "delivered", "read", "failed", "pending"],
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
            allowed_conv_ids = set()
            conversation_type = filters.get("conversation_type") or "Todos"
            selected_message_statuses = filters.get("message_statuses") or ["Todos"]
            if "Todos" in selected_message_statuses:
                selected_message_statuses = []
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
                max_workers = min(3, max(1, len(message_conv_ids)))
                batch_size = max_workers * 2
                batch_pause = 0.4
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for batch in _chunk_list(message_conv_ids, batch_size):
                        future_map = {
                            executor.submit(
                                _fetch_messages,
                                cw_url,
                                cw_account,
                                cw_token,
                                conv_id,
                                start_dt=start_dt,
                            ): conv_id
                            for conv_id in batch
                        }
                        for future in as_completed(future_map):
                            conv_id = future_map[future]
                            conv_info = conv_meta.get(conv_id) or {}
                            try:
                                msgs = future.result()
                            except Exception as e:
                                st.warning(f"Falha ao buscar mensagens da conversa {conv_id}: {e}")
                                continue
                            has_bot_outgoing = False
                            has_agent_outgoing = False
                            conv_received = 0
                            conv_sent = 0
                            conv_priv = 0
                            conv_rows = []
                            should_add_rows = conv_id in table_conv_set
                            for msg in msgs:
                                msg_dt_raw = msg.get("created_at") or msg.get("timestamp")
                                msg_dt = _parse_ts(msg_dt_raw)
                                msg_local = None
                                if msg_dt:
                                    msg_local = msg_dt.astimezone(TZ)
                                    if msg_local < start_dt or msg_local > end_dt:
                                        continue
                                status_val = msg.get("status") or msg.get("delivery_status") or msg.get("message_status") or msg.get("state")
                                if selected_message_statuses and str(status_val) not in selected_message_statuses:
                                    continue
                                direction = _message_direction(msg)
                                if direction == "outgoing":
                                    if _is_bot_sender(msg):
                                        has_bot_outgoing = True
                                    elif _is_agent_sender(msg):
                                        has_agent_outgoing = True
                                if not _include_message_for_type(msg, conversation_type):
                                    continue
                                is_private = msg.get("private")
                                if isinstance(is_private, str):
                                    is_private = is_private.strip().lower() in ("true", "1", "yes", "sim")
                                if is_private:
                                    conv_priv += 1
                                if direction == "incoming":
                                    conv_received += 1
                                elif direction == "outgoing":
                                    conv_sent += 1
                                if not should_add_rows:
                                    continue
                                content = msg.get("content")
                                if content is None:
                                    content = msg.get("processed_message_content") or ""
                                conv_rows.append(
                                    {
                                        "id_conversa": conv_id,
                                        "autor": _message_sender_label(msg),
                                        "nome do contato": conv_info.get("contact_name", ""),
                                        "numero do contato": conv_info.get("contact_phone", ""),
                                        "data hora de início da conversa": conv_info.get("created_str", ""),
                                        "caixa de entrada": conv_info.get("inbox_name", ""),
                                        "tempo para a primeira resposta": conv_info.get("first_reply_delta", ""),
                                        "status da mensagem": status_val,
                                        "mensagem": content,
                                        "_sort_conv_dt": conv_info.get("created_dt"),
                                        "_sort_msg_dt": msg_local if msg_dt else None,
                                    }
                                )
                            if conversation_type == "Bot" and not has_bot_outgoing:
                                continue
                            if conversation_type == "Agente" and not has_agent_outgoing:
                                continue
                            allowed_conv_ids.add(conv_id)
                            total_recebidas += conv_received
                            total_enviadas += conv_sent
                            total_privadas += conv_priv
                            if conv_priv:
                                conversas_privadas.add(conv_id)
                            if conv_rows:
                                message_rows.extend(conv_rows)
                        if len(message_conv_ids) > batch_size:
                            time_module.sleep(batch_pause + random.uniform(0, 0.2))
            if conversation_type != "Todos":
                rows = [row for row in rows if row.get("conversation_id") in allowed_conv_ids]
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
                "autor",
                "nome do contato",
                "numero do contato",
                "data hora de início da conversa",
                "caixa de entrada",
                "tempo para a primeira resposta",
                "status da mensagem",
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


def render_conversations_insights_tab():
    """Render the Insights tab with the same filters as conversation analysis."""
    st.subheader("Insights")
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

    teams_cache = st.session_state.get("cw_conv_teams")
    if teams_cache is None:
        with st.spinner("Carregando times..."):
            try:
                teams_cache = _fetch_teams(cw_url, cw_account, cw_token)
            except Exception as e:
                teams_cache = []
                st.warning(f"Não foi possível carregar times: {e}")
        st.session_state["cw_conv_teams"] = teams_cache
    team_options = ["Todos"] + [f"{t['id']} - {t['name']}" for t in teams_cache]

    prompts = _load_insight_prompts()
    prompt_options = {"Selecione um prompt": None}
    for prompt in prompts:
        label = f"#{prompt['id']} - {prompt['name'] or 'Sem nome'}"
        prompt_options[label] = prompt["id"]

    if len(prompt_options) == 1:
        st.warning("Nenhum prompt de insights cadastrado. Cadastre um prompt antes de gerar insights.")

    filters = _render_conversation_filters(
        "conv_insights",
        inbox_options,
        agent_options,
        default_start,
        default_end,
        result_keys=["conv_insights_output", "conv_insights_prompt_name"],
        team_options=team_options,
        conversation_type_options=["Agente", "Bot", "Todos"],
        message_status_options=["Todos", "sent", "delivered", "read", "failed", "pending"],
        insight_prompt_options=prompt_options,
        require_prompt=True,
        generate_label="Gerar insights",
        clear_label="Limpar",
    )

    clear_key = _build_state_key("conv_insights", "clear_filters")
    current_sig = _insights_filters_signature(filters)
    last_sig = st.session_state.get("conv_insights_last_signature")
    if last_sig is not None and current_sig != last_sig:
        for key in (
            "conv_insights_summary",
            "conv_insights_context",
            "conv_insights_output",
            "conv_insights_prompt_id",
            "conv_insights_prompt_name",
        ):
            st.session_state.pop(key, None)
        st.session_state["conv_insights_pending"] = False
    st.session_state["conv_insights_last_signature"] = current_sig

    if filters["gerar"]:
        if len(prompt_options) == 1:
            st.error("Cadastre um prompt de insights para continuar.")
            return
        selected_prompt_id = filters.get("selected_prompt_id")
        if selected_prompt_id is None:
            st.error("Selecione um modelo de prompt para gerar insights.")
            return
        if filters["start_date"] > filters["end_date"]:
            st.error("Período inválido: data inicial maior que a final.")
            return
        prompt_data = _get_insight_prompt(selected_prompt_id)
        if not prompt_data:
            st.error("Não encontrei o prompt selecionado.")
            return
        start_dt = datetime.combine(filters["start_date"], time.min, tzinfo=TZ)
        end_dt = datetime.combine(filters["end_date"], time.max, tzinfo=TZ)

        with st.spinner("Buscando dados para insights..."):
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

            stats, context_text, filter_lines = _build_insights_context(
                conversations,
                filters,
                inbox_id_to_name,
                start_dt,
                end_dt,
                cw_url,
                cw_account,
                cw_token,
                max_messages=160,
                max_chars=12000,
            )

        st.session_state["conv_insights_summary"] = {"filters": filter_lines, "stats": stats}
        st.session_state["conv_insights_context"] = context_text
        st.session_state["conv_insights_prompt_id"] = selected_prompt_id
        st.session_state["conv_insights_pending"] = True
        st.session_state.pop("conv_insights_output", None)
        st.session_state.pop("conv_insights_prompt_name", None)

    summary = st.session_state.get("conv_insights_summary")
    if summary:
        st.markdown("**Resumo Conversas**")
        filters_text = "; ".join(summary.get("filters") or [])
        if filters_text:
            st.caption(f"Filtros selecionados: {filters_text}")
        else:
            st.caption("Filtros selecionados: nenhum filtro adicional.")
        stats = summary.get("stats") or {}
        st.write(f"Total de conversas: {stats.get('total_conversas', 0)}")
        st.write(f"Total de mensagens: {stats.get('total_mensagens', 0)}")
        st.write(f"Total de clientes únicos: {stats.get('total_clientes_unicos', 0)}")

        if st.session_state.get("conv_insights_pending"):
            st.write("Deseja realizar a análise em busca de insights relevantes aos dados filtrados?")
            col_yes, col_no = st.columns(2)
            if col_yes.button("Sim", key="conv_insights_confirm"):
                prompt_id = st.session_state.get("conv_insights_prompt_id")
                prompt_data = _get_insight_prompt(prompt_id)
                if not prompt_data:
                    st.error("Não encontrei o prompt selecionado.")
                else:
                    model = settings.get("model", "gpt-4.1-mini")
                    provider = settings.get("provider", "openai")
                    if provider != "openai":
                        st.error("Provedor não suportado para insights. Ajuste para openai nas configurações.")
                    else:
                        with st.spinner("Gerando insights..."):
                            try:
                                context_text = st.session_state.get("conv_insights_context") or ""
                                output = _run_insights_prompt(prompt_data.get("prompt_text") or "", context_text, model)
                            except Exception as e:
                                st.error(f"Falha ao gerar insights: {e}")
                            else:
                                st.session_state["conv_insights_output"] = output
                                st.session_state["conv_insights_prompt_name"] = prompt_data.get("name") or f"Prompt #{prompt_data.get('id')}"
                                st.session_state["conv_insights_pending"] = False
                                st.rerun()
            if col_no.button("Não", key="conv_insights_cancel"):
                for key in (
                    "conv_insights_summary",
                    "conv_insights_context",
                    "conv_insights_output",
                    "conv_insights_prompt_id",
                    "conv_insights_prompt_name",
                    "conv_insights_last_signature",
                ):
                    st.session_state.pop(key, None)
                st.session_state["conv_insights_pending"] = False
                st.session_state[clear_key] = True
                st.rerun()

    output = st.session_state.get("conv_insights_output")
    if output:
        st.markdown("### Resultado")
        prompt_name = st.session_state.get("conv_insights_prompt_name") or ""
        if prompt_name:
            st.caption(f"Prompt: {prompt_name}")
        st.markdown(output)


__all__ = ["render_conversations_tab", "render_conversations_analysis_tab", "render_conversations_insights_tab"]
