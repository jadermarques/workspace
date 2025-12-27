"""Metrics helpers for Chatwoot analytics and reports."""

from datetime import datetime, timezone, time
from typing import List, Dict

import pandas as pd
import requests

from src.utils.timezone import TZ


def _parse_ts(value):
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


def _chatwoot_headers(token: str):
    """Build default headers for Chatwoot API requests."""
    return {"api_access_token": token, "Content-Type": "application/json"}


def fetch_chatwoot_conversations(base_url: str, account_id: str, token: str, start_dt, status: str = "all", max_pages: int = 10, per_page: int = 50):
    """Fetch conversations from Chatwoot, stopping when past the start date."""
    conversations = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations"
        resp = requests.get(
            url,
            params={"status": status, "page": page, "per_page": per_page, "sort": "last_activity_at"},
            headers=_chatwoot_headers(token),
            timeout=20,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Chatwoot respondeu {resp.status_code}: {resp.text[:200]}")
        data = resp.json() or {}
        payload = data.get("data", {}).get("payload") or data.get("payload") or []
        if not payload:
            break
        conversations.extend(payload)
        last = payload[-1]
        last_ts_raw = last.get("last_activity_at") or last.get("updated_at") or last.get("timestamp") or last.get("created_at")
        last_dt = _parse_ts(last_ts_raw)
        if last_dt and last_dt.astimezone(TZ) < start_dt:
            break
        page += 1
    return conversations


def fetch_chatwoot_messages(base_url: str, account_id: str, token: str, conversation_id, max_pages: int = 4, per_page: int = 50):
    """Fetch messages for a single conversation."""
    messages = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
        resp = requests.get(url, params={"page": page, "per_page": per_page}, headers=_chatwoot_headers(token), timeout=20)
        if resp.status_code >= 400:
            raise RuntimeError(f"Chatwoot respondeu {resp.status_code} ao buscar mensagens da conversa {conversation_id}: {resp.text[:200]}")
        data = resp.json() or {}
        payload = data.get("data", []) or data.get("payload") or []
        if not payload:
            break
        messages.extend(payload)
        page += 1
    return messages


def fetch_chatwoot_agents(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 50):
    """Fetch and return a sorted list of agent names."""
    agents = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/agents"
        resp = requests.get(
            url,
            params={"page": page, "per_page": per_page},
            headers=_chatwoot_headers(token),
            timeout=15,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Chatwoot respondeu {resp.status_code} ao listar agentes: {resp.text[:200]}")
        data = resp.json() or {}
        payload = data if isinstance(data, list) else data.get("data") or data.get("payload") or []
        if not payload:
            break
        for ag in payload:
            if isinstance(ag, dict):
                name = ag.get("name") or ag.get("email") or ""
            else:
                name = str(ag)
            if name:
                agents.append(name)
        page += 1
    return sorted(set(agents))


def build_hourly_df(df: pd.DataFrame):
    """Build an hourly breakdown dataframe for incoming/outgoing messages."""
    hour_counts_df = df.copy()
    hour_counts_df["hora_num"] = hour_counts_df["created_dt"].dt.hour
    grouped_hours = hour_counts_df.groupby(["hora_num", "direction"]).size().unstack(fill_value=0)
    hour_rows = []
    total_received = 0
    total_sent = 0
    for h in range(24):
        received = int(grouped_hours.loc[h].get("cliente", 0)) if h in grouped_hours.index else 0
        sent = int(grouped_hours.loc[h].get("bot", 0)) if h in grouped_hours.index else 0
        total_received += received
        total_sent += sent
        hour_rows.append(
            {
                "Horário": f"{h:02d}:00",
                "total de mensagens recebidas": received,
                "total de mensagens enviadas": sent,
            }
        )
    hour_rows.append(
        {
            "Horário": "TOTAL",
            "total de mensagens recebidas": total_received,
            "total de mensagens enviadas": total_sent,
        }
    )
    return pd.DataFrame(hour_rows)


__all__ = [
    "fetch_chatwoot_agents",
    "fetch_chatwoot_conversations",
    "fetch_chatwoot_messages",
    "TZ",
    "build_hourly_df",
]
