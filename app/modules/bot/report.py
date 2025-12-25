"""Streamlit dashboard for Chatwoot attendance reports."""

import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.metrics import (
    TZ,
    build_hourly_df,
    fetch_chatwoot_agents,
    fetch_chatwoot_conversations,
    fetch_chatwoot_messages,
)
from src.bot.engine import load_env_once, load_settings


def render_atendimentos_dashboard():
    """Render the Chatwoot attendance dashboard with filters and exports."""
    st.header("Atendimentos (Chatwoot)")
    load_env_once()

    settings = load_settings() or {}
    chatwoot_url = (settings.get("chatwoot_url") or "").rstrip("/")
    chatwoot_token = settings.get("chatwoot_api_token") or ""
    chatwoot_account = settings.get("chatwoot_account_id") or ""

    if not all([chatwoot_url, chatwoot_token, chatwoot_account]):
        st.error("Configure CHATWOOT_URL, CHATWOOT_API_TOKEN e CHATWOOT_ACCOUNT_ID na página Configurações para usar esta aba.")
        return

    hour_options = [time(h, 0) for h in range(24)]
    today_local = datetime.now(TZ).date()
    default_start_date = today_local - timedelta(days=7)
    default_end_date = today_local

    refresh_agents = st.button("Atualizar lista de agentes do Chatwoot", type="secondary")
    cached_agents = st.session_state.get("cw_agents_cache")
    if refresh_agents or cached_agents is None:
        with st.spinner("Buscando agentes no Chatwoot..."):
            try:
                agents_fetched = fetch_chatwoot_agents(chatwoot_url, chatwoot_account, chatwoot_token)
                st.session_state["cw_agents_cache"] = agents_fetched
                cached_agents = agents_fetched
            except Exception as e:
                st.error(f"Não foi possível carregar a lista de agentes: {e}")
                cached_agents = []
    agent_options = cached_agents or []

    with st.form("filtros_atendimentos"):
        st.subheader("Filtros")
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            start_date = st.date_input("Período inicial (dd/mm/aaaa)", value=default_start_date)
        with col_d2:
            end_date = st.date_input("Período final (dd/mm/aaaa)", value=default_end_date)
        selected_hours = st.multiselect(
            "Hora(s) do dia",
            options=hour_options,
            default=hour_options,
            format_func=lambda t: t.strftime("%H:%M"),
            help="Selecione uma ou mais horas; cada escolha filtra o intervalo HH:00 até HH:59.",
        )

        status_choice = st.selectbox(
            "Status da conversa (Chatwoot)",
            options=["all", "open", "resolved", "pending", "snoozed"],
            index=0,
            help="Filtra na API para reduzir volume.",
        )
        agent_selected = st.multiselect(
            "Agente(s)",
            options=agent_options,
            default=agent_options,
            help="Lista carregada da API do Chatwoot. Use 'Atualizar lista de agentes' se estiver vazia.",
        )
        conv_filter = st.text_input("Filtro de Conversation ID (exato)")

        msg_type = st.radio(
            "Tipo de mensagem",
            options=["Todas", "Apenas clientes", "Apenas bot"],
            horizontal=True,
        )

        st.caption("As colunas disponíveis serão carregadas após buscar dados no Chatwoot.")
        gerar = st.form_submit_button("Gerar relatório", type="primary", disabled=not agent_options)

    if not gerar:
        if not agent_options:
            st.warning("Carregue os agentes do Chatwoot antes de gerar o relatório.")
        else:
            st.info("Ajuste os filtros e clique em 'Gerar relatório' para montar o dashboard.")
        return

    if start_date > end_date:
        st.error("Período inválido: a data inicial é maior que a final.")
        return
    if not selected_hours:
        st.error("Selecione ao menos uma hora para filtrar os atendimentos.")
        return

    selected_hours_sorted = sorted(selected_hours, key=lambda t: t.hour)
    hours_selected = {h.hour for h in selected_hours_sorted}
    start_dt = datetime.combine(start_date, time.min, tzinfo=TZ)
    end_dt = datetime.combine(end_date, time(23, 59, 59), tzinfo=TZ)

    with st.spinner("Buscando conversas no Chatwoot..."):
        try:
            conversations = fetch_chatwoot_conversations(chatwoot_url, chatwoot_account, chatwoot_token, start_dt, status=status_choice, max_pages=10)
        except Exception as e:
            st.error(f"Falha ao buscar conversas no Chatwoot: {e}")
            return

    if not conversations:
        st.warning("Nenhuma conversa encontrada no período/status informado.")
        return

    conv_meta = {}
    agent_set = set()
    for conv in conversations:
        cid = conv.get("id") or conv.get("display_id")
        if cid is None:
            continue
        meta = conv.get("meta", {}) or {}
        assignee = meta.get("assignee") or conv.get("assignee") or {}
        sender = meta.get("sender") or conv.get("contact") or {}
        agent_name = assignee.get("name") or "Não atribuído"
        agent_set.add(agent_name)
        conv_meta[cid] = {
            "agent": agent_name,
            "status": conv.get("status") or "desconhecido",
            "client_name": sender.get("name") or sender.get("identifier") or "Cliente",
            "inbox_id": conv.get("inbox_id"),
        }
    st.session_state["cw_agents_cache"] = sorted(agent_set)

    rows = []
    progress = st.progress(0, text="Buscando mensagens...")
    total_conv = len(conv_meta)
    processed = 0
    for cid, meta in list(conv_meta.items()):
        processed += 1
        progress.progress(min(processed / total_conv, 1.0), text=f"Mensagens da conversa {cid} ({processed}/{total_conv})")
        if conv_filter.strip() and str(cid) != conv_filter.strip():
            continue
        if agent_selected and meta["agent"] not in agent_selected:
            continue
        try:
            msgs = fetch_chatwoot_messages(chatwoot_url, chatwoot_account, chatwoot_token, cid)
        except Exception as e:
            st.warning(f"Falha ao buscar mensagens da conversa {cid}: {e}")
            continue
        for msg in msgs:
            msg_ts_raw = msg.get("created_at") or msg.get("timestamp")
            msg_dt = _parse_ts(msg_ts_raw)
            if not msg_dt:
                continue
            msg_dt_local = msg_dt.astimezone(TZ)
            if msg_dt_local < start_dt or msg_dt_local > end_dt:
                continue
            if hours_selected and msg_dt_local.hour not in hours_selected:
                continue
            direction = "cliente" if (msg.get("message_type") == "incoming" or (msg.get("sender_type") or "").lower() in ["contact", "contact::inbox"]) else "bot"
            if msg_type == "Apenas clientes" and direction != "cliente":
                continue
            if msg_type == "Apenas bot" and direction != "bot":
                continue
            content = msg.get("content") or ""
            if not content:
                content = (msg.get("content_attributes") or {}).get("body") or ""
            rows.append(
                {
                    "conversation_id": str(cid),
                    "client_name": meta.get("client_name"),
                    "agent": meta.get("agent"),
                    "status": meta.get("status"),
                    "inbox_id": meta.get("inbox_id"),
                    "message": content,
                    "created_at": msg_dt_local.strftime("%Y-%m-%d %H:%M:%S"),
                    "created_dt": msg_dt_local,
                    "direction": direction,
                    "message_type": msg.get("message_type"),
                    "sender_type": msg.get("sender_type"),
                }
            )
    progress.empty()

    if not rows:
        st.warning("Nenhum resultado para os filtros informados.")
        return

    df = pd.DataFrame(rows)
    df["created_at_data"] = df["created_dt"].dt.strftime("%d/%m/%Y")
    df["created_at_time"] = df["created_dt"].dt.strftime("%H:%M:%S")
    df["created_dt_data"] = df["created_dt"].dt.strftime("%d/%m/%Y")
    df["created_dt_time"] = df["created_dt"].dt.strftime("%H:%M:%S")

    column_options = list(df.columns)
    columns_selected = st.multiselect(
        "Colunas para visualizar/exportar",
        options=column_options,
        default=column_options,
        help="Selecione pelo menos uma coluna.",
    )
    if not columns_selected:
        st.error("Selecione ao menos uma coluna para visualizar/exportar.")
        return

    conv_summary = df.groupby("conversation_id").agg(
        primeira_mensagem=("created_dt", "min"),
        agente=("agent", lambda x: x.dropna().iloc[0] if not x.dropna().empty else "Não atribuído"),
        cliente=("client_name", lambda x: x.dropna().iloc[0] if not x.dropna().empty else "N/A"),
        status=("status", lambda x: x.dropna().iloc[0] if not x.dropna().empty else "Sem status"),
    ).reset_index()
    conv_summary["dia"] = conv_summary["primeira_mensagem"].dt.date
    conv_summary["hora"] = conv_summary["primeira_mensagem"].dt.strftime("%H:%M")

    hours_label = "Todas (00:00-23:00)" if len(hours_selected) == len(hour_options) else ", ".join(h.strftime("%H:%M") for h in selected_hours_sorted)
    filter_lines = [
        f"Período: {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')}",
        f"Horas: {hours_label}",
        f"Agentes: {', '.join(agent_selected) if agent_selected else 'Todos'}",
        f"Tipo de mensagem: {msg_type}",
        f"Conversation ID: {conv_filter.strip() or 'Todos'}",
    ]
    st.success("\n".join(filter_lines))

    total_conversas = len(conv_summary)
    total_recebidas = int((df["direction"] == "cliente").sum())
    total_enviadas = int((df["direction"] == "bot").sum())
    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("Conversas", total_conversas)
    col_m2.metric("Mensagens recebidas", total_recebidas)
    col_m3.metric("Mensagens enviadas", total_enviadas)

    st.markdown("### Mensagens por hora")
    st.caption(f"Período selecionado: de {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")
    hour_df = build_hourly_df(df)
    hour_styled = (
        hour_df.style.set_properties(
            subset=hour_df.columns,
            **{"text-align": "left", "min-width": "90px", "width": "120px"}
        ).set_table_styles(
            [{"selector": "th", "props": [("text-align", "left"), ("min-width", "90px"), ("width", "120px")]}]
        )
    )
    st.table(hour_styled)

    st.markdown("### Dados detalhados")
    display_cols = columns_selected
    if "status" not in display_cols:
        display_cols = display_cols + ["status"]
    df_display = df.reindex(columns=display_cols)
    st.dataframe(df_display, use_container_width=True)

    csv_data = df_display.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Exportar CSV",
        data=csv_data,
        file_name="relatorio_atendimentos.csv",
        mime="text/csv",
    )


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


__all__ = ["render_atendimentos_dashboard"]
