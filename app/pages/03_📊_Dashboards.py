"""Streamlit page for dashboards."""

import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import altair as alt
import pandas as pd
import streamlit as st
import requests

from app.components.sidebar import render_sidebar
from app.modules.bot.report import render_atendimentos_dashboard
from src.bot.engine import load_prompt_profiles, load_settings
from src.utils.timezone import TZ


def _cw_headers(token: str) -> Dict[str, str]:
    return {"api_access_token": token, "Content-Type": "application/json"}


def _parse_ts(value):
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


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_live_conversation_metrics(base_url: str, account_id: str, token: str) -> Dict:
    url = f"{base_url}/api/v2/accounts/{account_id}/live_reports/conversation_metrics"
    resp = requests.get(url, headers=_cw_headers(token), timeout=15)
    if resp.status_code >= 400:
        raise RuntimeError(f"Chatwoot respondeu {resp.status_code}: {resp.text[:200]}")
    return resp.json() or {}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_grouped_conversation_metrics(base_url: str, account_id: str, token: str, group_by: str) -> List[Dict]:
    url = f"{base_url}/api/v2/accounts/{account_id}/live_reports/grouped_conversation_metrics"
    resp = requests.get(url, params={"group_by": group_by}, headers=_cw_headers(token), timeout=15)
    if resp.status_code >= 400:
        raise RuntimeError(f"Chatwoot respondeu {resp.status_code}: {resp.text[:200]}")
    data = resp.json() or []
    return data if isinstance(data, list) else []


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_conversation_reports(
    base_url: str,
    account_id: str,
    token: str,
    since_ts: int,
    until_ts: int,
    timezone_offset: float,
    report_type: str = "account",
    report_id: int | None = None,
    group_by: str = "day",
) -> List[Dict]:
    url = f"{base_url}/api/v2/accounts/{account_id}/reports"
    params = {
        "metric": "conversations_count",
        "since": since_ts,
        "until": until_ts,
        "type": report_type,
        "group_by": group_by,
        "timezone_offset": timezone_offset,
    }
    if report_id is not None:
        params["id"] = report_id
    resp = requests.get(
        url,
        params=params,
        headers=_cw_headers(token),
        timeout=20,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Chatwoot respondeu {resp.status_code}: {resp.text[:200]}")
    data = resp.json() or []
    return data if isinstance(data, list) else []


def _merge_report_rows(rows_list: List[List[Dict]]) -> List[Dict]:
    merged = {}
    for rows in rows_list:
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = row.get("timestamp")
            if ts is None:
                continue
            merged.setdefault(ts, 0)
            merged[ts] += float(row.get("value") or 0)
    return [{"timestamp": ts, "value": merged[ts]} for ts in sorted(merged.keys())]


def _build_date_range(start_date, end_date):
    current = start_date
    dates = []
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _get_timezone_offset_hours() -> float:
    offset = datetime.now(TZ).utcoffset()
    if offset is None:
        return 0.0
    return offset.total_seconds() / 3600


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_inboxes(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 100) -> List[Dict]:
    inboxes = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/inboxes"
        resp = requests.get(
            url,
            params={"page": page, "per_page": per_page},
            headers=_cw_headers(token),
            timeout=12,
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
                inboxes.append(inbox)
        if len(payload) < per_page:
            break
        page += 1
    return inboxes


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_conversations_for_messages(
    base_url: str,
    account_id: str,
    token: str,
    start_dt,
    inbox_id: int | None = None,
    max_pages: int = 30,
    per_page: int = 25,
) -> List[Dict]:
    conversations = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations"
        params = {"page": page, "per_page": per_page, "sort": "last_activity_at", "status": "all"}
        if inbox_id is not None:
            params["inbox_id"] = inbox_id
        resp = requests.get(
            url,
            params=params,
            headers=_cw_headers(token),
            timeout=15,
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
        if len(payload) < per_page:
            break
        page += 1
    return conversations


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_conversation_messages(
    base_url: str,
    account_id: str,
    token: str,
    conversation_id,
    max_pages: int = 4,
    per_page: int = 50,
) -> List[Dict]:
    messages = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
        resp = requests.get(
            url,
            params={"page": page, "per_page": per_page},
            headers=_cw_headers(token),
            timeout=15,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Chatwoot respondeu {resp.status_code} ao buscar mensagens: {resp.text[:200]}")
        data = resp.json() or {}
        payload = data.get("data", []) or data.get("payload") or []
        if not payload:
            break
        messages.extend(payload)
        page += 1
    return messages


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_chatwoot_users(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 100) -> List[Dict]:
    endpoints = ["/users", "/agents"]
    for endpoint in endpoints:
        users = []
        page = 1
        while page <= max_pages:
            url = f"{base_url}/api/v1/accounts/{account_id}{endpoint}"
            resp = requests.get(
                url,
                params={"page": page, "per_page": per_page},
                headers=_cw_headers(token),
                timeout=12,
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
            for item in payload:
                if isinstance(item, dict):
                    users.append(item)
            if len(payload) < per_page:
                break
            page += 1
        if users:
            return users
    return []


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_teams(base_url: str, account_id: str, token: str, max_pages: int = 5, per_page: int = 100) -> List[Dict]:
    teams = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/api/v1/accounts/{account_id}/teams"
        resp = requests.get(
            url,
            params={"page": page, "per_page": per_page},
            headers=_cw_headers(token),
            timeout=12,
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
                teams.append(team)
        if len(payload) < per_page:
            break
        page += 1
    return teams


def _render_kpi_cards(cards: List[Dict[str, Dict[str, str]]]) -> None:
    st.markdown(
        """
        <style>
        .dash-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            gap: 16px;
            margin: 12px 0 4px;
        }
        .dash-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 14px;
            padding: 16px 18px;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
            position: relative;
            overflow: hidden;
        }
        .dash-card::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
            background: linear-gradient(90deg, #0ea5e9, #38bdf8);
        }
        .dash-card h4 {
            margin: 2px 0 10px;
            font-size: 1.05rem;
            color: #0f172a;
            font-weight: 700;
        }
        .dash-card ul {
            list-style: none;
            padding: 0;
            margin: 0;
        }
        .dash-card li {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            padding: 6px 8px;
            font-size: 0.92rem;
            color: #475569;
            border-radius: 8px;
        }
        .dash-card li:nth-child(odd) {
            background: #f1f5f9;
        }
        .dash-card li span.value {
            font-weight: 600;
            color: #0f172a;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    cards_html = ["<div class='dash-cards'>"]
    for card in cards:
        title = card.get("title", "")
        items = card.get("items", {})
        cards_html.append("<div class='dash-card'>")
        cards_html.append(f"<h4>{title}</h4>")
        cards_html.append("<ul>")
        for label, value in items.items():
            cards_html.append(f"<li><span>{label}</span><span class='value'>{value}</span></li>")
        cards_html.append("</ul></div>")
    cards_html.append("</div>")
    st.markdown("\n".join(cards_html), unsafe_allow_html=True)


def main():
    """Render the dashboards page and attendance dashboard."""
    st.set_page_config(page_title="Dashboards", page_icon="📊", layout="wide")
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrame"] table {
            margin-left: auto;
            margin-right: auto;
        }
        div[data-testid="stDataFrame"] th,
        div[data-testid="stDataFrame"] td {
            text-align: center !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    render_sidebar(show_selector=False)
    tab_dashboard, tab_atendimento = st.tabs(["Dashboard", "Atendimento"])
    with tab_dashboard:
        st.subheader("Dashboard")
        st.caption("🟢 Dados em tempo real")
        settings = load_settings() or {}
        chatwoot_url = (settings.get("chatwoot_url") or "").rstrip("/")
        chatwoot_token = settings.get("chatwoot_api_token") or ""
        chatwoot_account = settings.get("chatwoot_account_id") or ""

        if not all([chatwoot_url, chatwoot_token, chatwoot_account]):
            st.error("Configure CHATWOOT_URL, CHATWOOT_API_TOKEN e CHATWOOT_ACCOUNT_ID para ver os indicadores.")
        else:
            try:
                live_metrics = _fetch_live_conversation_metrics(chatwoot_url, chatwoot_account, chatwoot_token)
            except Exception as exc:
                st.error(f"Falha ao buscar métricas do Chatwoot: {exc}")
                live_metrics = {}

            total_open = int(live_metrics.get("open") or 0)
            total_pending = int(live_metrics.get("pending") or 0)
            total_unassigned = int(live_metrics.get("unassigned") or 0)
            total_assigned = max(0, total_open - total_unassigned)
            total_unattended = int(live_metrics.get("unattended") or 0)

            try:
                agents = _fetch_chatwoot_users(chatwoot_url, chatwoot_account, chatwoot_token)
            except Exception as exc:
                st.warning(f"Falha ao buscar agentes: {exc}")
                agents = []

            total_agents = len(agents)
            available = 0
            busy = 0
            disconnected = 0
            for agent in agents:
                status = (
                    agent.get("availability_status")
                    or agent.get("status")
                    or agent.get("availability")
                    or ""
                )
                status_l = str(status).lower()
                if status_l in ("online", "available"):
                    available += 1
                elif status_l in ("busy", "occupied", "in_progress"):
                    busy += 1
                else:
                    disconnected += 1

            try:
                teams = _fetch_teams(chatwoot_url, chatwoot_account, chatwoot_token)
            except Exception as exc:
                st.warning(f"Falha ao buscar times: {exc}")
                teams = []

            total_teams = len(teams)
            try:
                inboxes = _fetch_inboxes(chatwoot_url, chatwoot_account, chatwoot_token)
            except Exception as exc:
                st.warning(f"Falha ao buscar caixas de entrada: {exc}")
                inboxes = []
            inbox_options = sorted(
                [
                    {"id": inbox.get("id"), "name": inbox.get("name") or f"Inbox {inbox.get('id')}"}
                    for inbox in inboxes
                    if isinstance(inbox, dict) and inbox.get("id") is not None
                ],
                key=lambda item: item["name"].lower(),
            )
            try:
                team_metrics = _fetch_grouped_conversation_metrics(
                    chatwoot_url,
                    chatwoot_account,
                    chatwoot_token,
                    "team_id",
                )
            except Exception as exc:
                st.warning(f"Falha ao buscar métricas por time: {exc}")
                team_metrics = []

            teams_in_service = {
                item.get("team_id")
                for item in team_metrics
                if isinstance(item, dict) and item.get("team_id") is not None
            }
            total_teams_in_service = len(teams_in_service)

            profiles = load_prompt_profiles() or []
            total_bots = len(profiles)
            total_bots_active = 1 if bool(settings.get("bot_enabled", True)) else 0

            cards = [
                {
                    "title": "Conversas",
                    "items": {
                        "Total de conversas abertas": total_open,
                        "Total de conversas não atendidas": total_unattended,
                        "Total de conversas não atribuídas": total_unassigned,
                        "Total de conversas atribuídas": total_assigned,
                        "Total de conversas pendentes": total_pending,
                    },
                },
                {
                    "title": "Agentes",
                    "items": {
                        "Total de licenças de agentes": total_agents,
                        "Total de agentes disponíveis (logados)": available,
                        "Total de agentes ocupados (em atendimento)": busy,
                        "Total de agentes desconectados": disconnected,
                    },
                },
                {
                    "title": "Times",
                    "items": {
                        "Total de Times": total_teams,
                        "Total de Times em atendimento": total_teams_in_service,
                    },
                },
                {
                    "title": "Bots",
                    "items": {
                        "Total de Bots ativos": total_bots_active,
                        "Total de Bots cadastrados": total_bots,
                    },
                },
            ]
            _render_kpi_cards(cards)

            st.markdown(
                "<div style='font-size:1.1rem;font-weight:700;margin-top:8px;'>Conversas</div>",
                unsafe_allow_html=True,
            )
            with st.expander("Conversas", expanded=False):
                today_local = datetime.now(TZ).date()
                default_start_date = today_local - timedelta(days=7)
                st.subheader("Conversas por data")
                col_start, col_end = st.columns(2)
                with col_start:
                    start_date = st.date_input(
                        "Data inicial",
                        value=default_start_date,
                        key="conversas_por_data_inicio",
                    )
                with col_end:
                    end_date = st.date_input(
                        "Data final",
                        value=today_local,
                        key="conversas_por_data_fim",
                    )
                inbox_selected = st.multiselect(
                    "Caixas de entrada",
                    options=inbox_options,
                    default=inbox_options,
                    format_func=lambda item: item.get("name", ""),
                    key="conversas_inbox_select",
                )

                if start_date > end_date:
                    st.error("Período inválido: a data inicial é maior que a final.")
                else:
                    start_dt = datetime.combine(start_date, time.min, tzinfo=TZ)
                    end_dt = datetime.combine(end_date, time(23, 59, 59), tzinfo=TZ)
                    since_ts = int(start_dt.timestamp())
                    until_ts = int(end_dt.timestamp())
                    timezone_offset = _get_timezone_offset_hours()
                    with st.spinner("Carregando conversas por data..."):
                        try:
                            inbox_ids = [item.get("id") for item in (inbox_selected or []) if item.get("id")]
                            if not inbox_ids or len(inbox_ids) == len(inbox_options):
                                report_rows = _fetch_conversation_reports(
                                    chatwoot_url,
                                    chatwoot_account,
                                    chatwoot_token,
                                    since_ts,
                                    until_ts,
                                    timezone_offset,
                                    report_type="account",
                                    report_id=None,
                                    group_by="day",
                                )
                            else:
                                report_rows = _merge_report_rows(
                                    [
                                        _fetch_conversation_reports(
                                            chatwoot_url,
                                            chatwoot_account,
                                            chatwoot_token,
                                            since_ts,
                                            until_ts,
                                            timezone_offset,
                                            report_type="inbox",
                                            report_id=inbox_id,
                                            group_by="day",
                                        )
                                        for inbox_id in inbox_ids
                                    ]
                                )
                        except Exception as exc:
                            st.error(f"Falha ao buscar conversas para o gráfico: {exc}")
                            report_rows = []

                    counts = {}
                    for row in report_rows:
                        if not isinstance(row, dict):
                            continue
                        ts_val = row.get("timestamp")
                        if ts_val is None:
                            continue
                        try:
                            day = datetime.fromtimestamp(float(ts_val), tz=TZ).date()
                        except Exception:
                            continue
                        counts[day] = int(row.get("value") or 0)

                    date_range_list = _build_date_range(start_date, end_date)
                    weekday_names = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
                    labels = [
                        f"{day.strftime('%d/%m/%y')} - {weekday_names[day.weekday()]}"
                        for day in date_range_list
                    ]
                    chart_rows = [
                        {
                            "data_label": label,
                            "data": day.strftime("%d/%m/%y"),
                            "dia_semana": weekday_names[day.weekday()],
                            "conversas": counts.get(day, 0),
                        }
                        for day, label in zip(date_range_list, labels)
                    ]

                    chart_df = pd.DataFrame(chart_rows)
                    chart_base = alt.Chart(chart_df).encode(
                        x=alt.X(
                            "data_label:N",
                            sort=labels,
                            title="Data",
                            axis=alt.Axis(labelAngle=90),
                        ),
                        y=alt.Y("conversas:Q", title="Conversas"),
                    )
                    chart = (
                        chart_base.mark_bar()
                        .encode(
                            color=alt.Color("conversas:Q", scale=alt.Scale(scheme="blues"), legend=None),
                            tooltip=[
                                alt.Tooltip("data:N", title="Data"),
                                alt.Tooltip("dia_semana:N", title="Dia da semana"),
                                alt.Tooltip("conversas:Q", title="Conversas"),
                            ],
                        )
                        + chart_base.mark_text(dy=-8, color="#0f172a", size=13).encode(
                            text=alt.Text("conversas:Q", format="d"),
                        )
                    ).properties(height=280)
                    st.altair_chart(chart, use_container_width=True)
                    if not report_rows:
                        st.info("Nenhuma conversa encontrada no período selecionado.")

                    st.subheader("Conversas por dia")
                    weekday_totals = {name: 0 for name in weekday_names}
                    for day in date_range_list:
                        weekday_totals[weekday_names[day.weekday()]] += counts.get(day, 0)
                    weekday_rows = [
                        {"dia_semana": name, "conversas": weekday_totals[name]}
                        for name in weekday_names
                    ]
                    total_conversas_semana = sum(weekday_totals.values())
                    for row in weekday_rows:
                        if total_conversas_semana > 0:
                            percentual = (row["conversas"] / total_conversas_semana) * 100
                        else:
                            percentual = 0
                        row["percentual"] = percentual
                        row["rotulo"] = f"{row['conversas']} ({percentual:.2f}%)"
                    weekday_df = pd.DataFrame(weekday_rows)
                    weekday_colors = ["#1d4ed8", "#dc2626", "#16a34a", "#f59e0b", "#7c3aed", "#0ea5e9", "#f97316"]
                    weekday_base = alt.Chart(weekday_df).encode(
                        x=alt.X("dia_semana:N", sort=weekday_names, title="Dia da semana"),
                        y=alt.Y("conversas:Q", title="Conversas"),
                    )
                    weekday_chart = (
                        weekday_base.mark_bar()
                        .encode(
                            color=alt.Color(
                                "dia_semana:N",
                                scale=alt.Scale(domain=weekday_names, range=weekday_colors),
                                legend=None,
                            ),
                            tooltip=[
                                alt.Tooltip("dia_semana:N", title="Dia da semana"),
                                alt.Tooltip("conversas:Q", title="Conversas"),
                                alt.Tooltip("percentual:Q", title="Percentual", format=".2f"),
                            ],
                        )
                        + weekday_base.mark_text(dy=-8, color="#0f172a", size=13).encode(
                            text=alt.Text("rotulo:N"),
                        )
                    ).properties(height=260)
                    st.altair_chart(weekday_chart, use_container_width=True)

                    st.subheader("Conversas por Hora")
                    with st.spinner("Carregando conversas por hora..."):
                        try:
                            inbox_ids = [item.get("id") for item in (inbox_selected or []) if item.get("id")]
                            if not inbox_ids or len(inbox_ids) == len(inbox_options):
                                hourly_rows = _fetch_conversation_reports(
                                    chatwoot_url,
                                    chatwoot_account,
                                    chatwoot_token,
                                    since_ts,
                                    until_ts,
                                    timezone_offset,
                                    report_type="account",
                                    report_id=None,
                                    group_by="hour",
                                )
                            else:
                                hourly_rows = _merge_report_rows(
                                    [
                                        _fetch_conversation_reports(
                                            chatwoot_url,
                                            chatwoot_account,
                                            chatwoot_token,
                                            since_ts,
                                            until_ts,
                                            timezone_offset,
                                            report_type="inbox",
                                            report_id=inbox_id,
                                            group_by="hour",
                                        )
                                        for inbox_id in inbox_ids
                                    ]
                                )
                        except Exception as exc:
                            st.error(f"Falha ao buscar conversas por hora: {exc}")
                            hourly_rows = []

                    hourly_counts = {h: 0 for h in range(24)}
                    for row in hourly_rows:
                        if not isinstance(row, dict):
                            continue
                        ts_val = row.get("timestamp")
                        if ts_val is None:
                            continue
                        try:
                            hour = datetime.fromtimestamp(float(ts_val), tz=TZ).hour
                        except Exception:
                            continue
                        hourly_counts[hour] += int(row.get("value") or 0)

                    total_conversas_hora = sum(hourly_counts.values())
                    days_in_range = max(len(date_range_list), 1)
                    media_total = total_conversas_hora / days_in_range

                    hour_labels = [f"{h:02d}" for h in range(24)]
                    hourly_rows_long = []
                    for h in range(24):
                        total = hourly_counts[h]
                        media = total / days_in_range
                        percentual_total = (total / total_conversas_hora * 100) if total_conversas_hora > 0 else 0
                        percentual_media = (media / media_total * 100) if media_total > 0 else 0
                        hourly_rows_long.extend(
                            [
                                {
                                    "hora_label": f"{h:02d}",
                                    "tipo": "Total",
                                    "conversas": total,
                                    "percentual": percentual_total,
                                    "rotulo": f"{total} ({percentual_total:.2f}%)",
                                },
                                {
                                    "hora_label": f"{h:02d}",
                                    "tipo": "Média",
                                    "conversas": media,
                                    "percentual": percentual_media,
                                    "rotulo": f"{media:.2f} ({percentual_media:.2f}%)",
                                },
                            ]
                        )

                    hourly_df = pd.DataFrame(hourly_rows_long)
                    hourly_total_df = hourly_df[hourly_df["tipo"] == "Total"].copy()
                    hourly_avg_df = hourly_df[hourly_df["tipo"] == "Média"].copy()

                    hourly_total_table = hourly_total_df[["hora_label", "conversas", "percentual"]].rename(
                        columns={
                            "hora_label": "Hora",
                            "conversas": "Conversas",
                            "percentual": "Percentual (%)",
                        }
                    )
                    hourly_total_table = pd.concat(
                        [
                            hourly_total_table,
                            pd.DataFrame(
                                [{"Hora": "Total", "Conversas": total_conversas_hora, "Percentual (%)": 100.0}]
                            ),
                        ],
                        ignore_index=True,
                    )
                    hourly_total_table["Conversas"] = hourly_total_table["Conversas"].astype(int)
                    hourly_total_table["Percentual (%)"] = hourly_total_table["Percentual (%)"].map(lambda v: f"{v:.2f}")
                    total_max = hourly_total_df["conversas"].max() if not hourly_total_df.empty else 0
                    total_styler = (
                        hourly_total_table.style
                        .set_properties(**{"text-align": "center"})
                        .set_table_styles(
                            [
                                {"selector": "table", "props": [("margin", "0 auto")]},
                                {"selector": "th", "props": [("text-align", "center")]},
                                {"selector": "tbody tr:nth-child(odd)", "props": [("background-color", "#f8fafc")]},
                            ]
                        )
                    )
                    total_styler = total_styler.apply(
                        lambda row: ["font-weight: 700" if row["Hora"] == "Total" else "" for _ in row],
                        axis=1,
                    )
                    if total_max > 0:
                        total_styler = total_styler.apply(
                            lambda s: [
                                "font-weight: 700; background-color: #e0f2fe" if v == total_max else ""
                                for v in s
                            ],
                            subset=["Conversas"],
                        )
                    col_left, col_mid, col_right = st.columns([1, 2, 1])
                    with col_mid:
                        st.dataframe(total_styler, use_container_width=True, hide_index=True)

                    st.subheader("Conversas médias por hora")
                    hourly_avg_table = hourly_avg_df[["hora_label", "conversas", "percentual"]].rename(
                        columns={
                            "hora_label": "Hora",
                            "conversas": "Média de conversas",
                            "percentual": "Percentual (%)",
                        }
                    )
                    hourly_avg_table = pd.concat(
                        [
                            hourly_avg_table,
                            pd.DataFrame(
                                [
                                    {
                                        "Hora": "Total",
                                        "Média de conversas": media_total,
                                        "Percentual (%)": 100.0,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
                    hourly_avg_table["Média de conversas"] = hourly_avg_table["Média de conversas"].map(lambda v: f"{v:.2f}")
                    hourly_avg_table["Percentual (%)"] = hourly_avg_table["Percentual (%)"].map(lambda v: f"{v:.2f}")
                    avg_max = hourly_avg_df["conversas"].max() if not hourly_avg_df.empty else 0
                    avg_styler = (
                        hourly_avg_table.style
                        .set_properties(**{"text-align": "center"})
                        .set_table_styles(
                            [
                                {"selector": "table", "props": [("margin", "0 auto")]},
                                {"selector": "th", "props": [("text-align", "center")]},
                                {"selector": "tbody tr:nth-child(odd)", "props": [("background-color", "#f8fafc")]},
                            ]
                        )
                    )
                    avg_styler = avg_styler.apply(
                        lambda row: ["font-weight: 700" if row["Hora"] == "Total" else "" for _ in row],
                        axis=1,
                    )
                    if avg_max > 0:
                        avg_styler = avg_styler.apply(
                            lambda s: [
                                "font-weight: 700; background-color: #fef3c7" if v == avg_max else ""
                                for v in s
                            ],
                            subset=["Média de conversas"],
                        )
                    col_left_avg, col_mid_avg, col_right_avg = st.columns([1, 2, 1])
                    with col_mid_avg:
                        st.dataframe(avg_styler, use_container_width=True, hide_index=True)
                    if not hourly_rows:
                        st.info("Nenhuma conversa encontrada por hora no período selecionado.")

            st.markdown(
                "<div style='font-size:1.1rem;font-weight:700;margin-top:8px;'>Mensagens</div>",
                unsafe_allow_html=True,
            )
            with st.expander("Mensagens", expanded=False):
                today_local = datetime.now(TZ).date()
                default_start_date = today_local - timedelta(days=7)
                col_start_msg, col_end_msg = st.columns(2)
                with col_start_msg:
                    start_date_msg = st.date_input(
                        "Data inicial",
                        value=default_start_date,
                        key="mensagens_data_inicio",
                    )
                with col_end_msg:
                    end_date_msg = st.date_input(
                        "Data final",
                        value=today_local,
                        key="mensagens_data_fim",
                    )
                inbox_selected_msg = st.multiselect(
                    "Caixas de entrada",
                    options=inbox_options,
                    default=inbox_options,
                    format_func=lambda item: item.get("name", ""),
                    key="mensagens_inbox_select",
                )
                if start_date_msg > end_date_msg:
                    st.error("Período inválido: a data inicial é maior que a final.")
                else:
                    gerar_msg = st.button("Gerar gráfico", type="primary", key="mensagens_gerar")
                    if not gerar_msg:
                        st.info("Ajuste os filtros e clique em 'Gerar gráfico' para montar o gráfico.")
                    else:
                        start_dt_msg = datetime.combine(start_date_msg, time.min, tzinfo=TZ)
                        end_dt_msg = datetime.combine(end_date_msg, time(23, 59, 59), tzinfo=TZ)
                        with st.spinner("Carregando mensagens por tipo..."):
                            try:
                                inbox_ids_msg = [item.get("id") for item in (inbox_selected_msg or []) if item.get("id")]
                                if not inbox_ids_msg or len(inbox_ids_msg) == len(inbox_options):
                                    convs = _fetch_conversations_for_messages(
                                        chatwoot_url,
                                        chatwoot_account,
                                        chatwoot_token,
                                        start_dt_msg,
                                        inbox_id=None,
                                    )
                                else:
                                    convs = []
                                    seen = set()
                                    for inbox_id in inbox_ids_msg:
                                        for conv in _fetch_conversations_for_messages(
                                            chatwoot_url,
                                            chatwoot_account,
                                            chatwoot_token,
                                            start_dt_msg,
                                            inbox_id=inbox_id,
                                        ):
                                            conv_id = conv.get("id") or conv.get("display_id")
                                            if conv_id in seen:
                                                continue
                                            seen.add(conv_id)
                                            convs.append(conv)
                            except Exception as exc:
                                st.error(f"Falha ao buscar conversas para mensagens: {exc}")
                                convs = []

                        if not convs:
                            st.info("Nenhuma conversa encontrada para o período selecionado.")
                        else:
                            total_text = 0
                            total_audio = 0
                            progress = st.progress(0, text="Contando mensagens de texto e áudio...")
                            total_convs = len(convs)
                            for idx, conv in enumerate(convs, start=1):
                                conv_id = conv.get("id") or conv.get("display_id")
                                if conv_id is None:
                                    continue
                                try:
                                    messages = _fetch_conversation_messages(
                                        chatwoot_url,
                                        chatwoot_account,
                                        chatwoot_token,
                                        conv_id,
                                    )
                                except Exception:
                                    continue
                                for msg in messages:
                                    msg_ts_raw = msg.get("created_at") or msg.get("timestamp")
                                    msg_dt = _parse_ts(msg_ts_raw)
                                    if not msg_dt:
                                        continue
                                    msg_dt_local = msg_dt.astimezone(TZ)
                                    if msg_dt_local < start_dt_msg or msg_dt_local > end_dt_msg:
                                        continue
                                    attachments = msg.get("attachments") or []
                                    has_audio = any(
                                        isinstance(att, dict) and att.get("file_type") == "audio"
                                        for att in attachments
                                    )
                                    if has_audio:
                                        total_audio += 1
                                    else:
                                        total_text += 1
                                progress.progress(min(idx / total_convs, 1.0), text=f"Contando mensagens... ({idx}/{total_convs})")
                            progress.empty()

                            total_messages = total_text + total_audio
                            if total_messages == 0:
                                st.info("Nenhuma mensagem encontrada no período selecionado.")
                            else:
                                rows = [
                                    {
                                        "tipo": "Texto",
                                        "mensagens": total_text,
                                        "percentual": (total_text / total_messages) * 100,
                                    },
                                    {
                                        "tipo": "Áudio",
                                        "mensagens": total_audio,
                                        "percentual": (total_audio / total_messages) * 100,
                                    },
                                ]
                                df_msgs = pd.DataFrame(rows)
                                df_msgs["rotulo"] = df_msgs.apply(
                                    lambda r: f"{int(r['mensagens'])} ({r['percentual']:.2f}%)",
                                    axis=1,
                                )
                                chart_base = alt.Chart(df_msgs).encode(
                                    x=alt.X("tipo:N", title="Tipo"),
                                    y=alt.Y("mensagens:Q", title="Mensagens"),
                                )
                                chart_msgs = (
                                    chart_base.mark_bar()
                                    .encode(
                                        color=alt.Color("tipo:N", scale=alt.Scale(range=["#1d4ed8", "#f59e0b"]), legend=None),
                                        tooltip=[
                                            alt.Tooltip("tipo:N", title="Tipo"),
                                            alt.Tooltip("mensagens:Q", title="Mensagens"),
                                            alt.Tooltip("percentual:Q", title="Percentual", format=".2f"),
                                        ],
                                    )
                                    + chart_base.mark_text(dy=-8, color="#0f172a", size=13).encode(
                                        text=alt.Text("rotulo:N"),
                                    )
                                ).properties(height=240)
                                st.altair_chart(chart_msgs, use_container_width=True)

            st.markdown(
                "<div style='font-size:1.1rem;font-weight:700;margin-top:8px;'>Resoluções</div>",
                unsafe_allow_html=True,
            )
            with st.expander("Resoluções", expanded=False):
                today_local = datetime.now(TZ).date()
                default_start_date = today_local - timedelta(days=7)
                col_start_res, col_end_res = st.columns(2)
                with col_start_res:
                    st.date_input(
                        "Data inicial",
                        value=default_start_date,
                        key="resolucoes_data_inicio",
                    )
                with col_end_res:
                    st.date_input(
                        "Data final",
                        value=today_local,
                        key="resolucoes_data_fim",
                    )
                st.multiselect(
                    "Caixas de entrada",
                    options=inbox_options,
                    default=inbox_options,
                    format_func=lambda item: item.get("name", ""),
                    key="resolucoes_inbox_select",
                )
    with tab_atendimento:
        render_atendimentos_dashboard()


if __name__ == "__main__":
    main()
