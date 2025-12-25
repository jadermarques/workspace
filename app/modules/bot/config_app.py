"""UI for bot configuration and integrations."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bot.engine import (
    PRICING_PER_1K,
    load_env_once,
    load_prompt_profiles,
    load_settings,
    save_settings,
    validate_settings,
)
from src.bot.rules import default_schedule


def _provider_options(extra_providers):
    """Return base and expanded provider option lists."""
    base_provider_options = ["openai", "gemini", "anthropic", "azure-openai", "outro"]
    all_provider_options = base_provider_options + [p for p in extra_providers.keys() if p not in base_provider_options]
    return base_provider_options, all_provider_options


def render_config_module():
    """Render the settings module with model, Chatwoot, and schedule tabs."""
    st.header("Configurações Gerais")
    load_env_once()

    current = load_settings() or {}
    stored_providers = current.get("providers") or {}
    extra_providers = st.session_state.get("providers_temp", stored_providers)

    profiles = load_prompt_profiles()
    profile_options = {p["name"] or f"Perfil #{p['id']}": p for p in profiles}
    selected_profile_id = st.session_state.get("selected_profile_id", current.get("prompt_profile_id"))
    if selected_profile_id is None and profiles:
        selected_profile_id = profiles[0]["id"]
    st.session_state["selected_profile_id"] = selected_profile_id
    selected_profile = next((p for p in profiles if p["id"] == selected_profile_id), None)

    # Valores padrão
    vector_store_id = current.get("vector_store_id", "")
    moderation_enabled = current.get("moderation_enabled", False)
    custom_moderation_terms = current.get("custom_moderation_terms", "")
    default_sched = current.get("schedule") or default_schedule()
    bot_enabled = current.get("bot_enabled", True)

    tab_modelo, tab_chatwoot, tab_funcionamento = st.tabs(
        ["Parâmetros do modelo", "Chatwoot", "Funcionamento do Bot (por dia)"]
    )

    # --- Aba Parâmetros do modelo ---
    with tab_modelo:
        st.subheader("Parâmetros do modelo")
        profile_names = list(profile_options.keys())
        selected_name = None
        if profile_names:
            try:
                selected_name = next(k for k, v in profile_options.items() if v["id"] == selected_profile_id)
            except StopIteration:
                selected_name = profile_names[0]
            selected_name = st.selectbox(
                "Selecionar perfil do bot",
                options=profile_names,
                index=profile_names.index(selected_name),
            )
        else:
            st.warning("Nenhum perfil cadastrado. Crie um em Bot Studio.")
        selected_profile = profile_options.get(selected_name) if selected_name else None
        st.text_area(
            "Prompt selecionado (visualização)",
            value=selected_profile["prompt_text"] if selected_profile else "",
            height=180,
            disabled=True,
        )

        base_provider_options, all_provider_options = _provider_options(extra_providers)
        current_provider = current.get("provider", "openai")
        provider = st.selectbox(
            "Provedor LLM",
            options=all_provider_options,
            index=all_provider_options.index(current_provider) if current_provider in all_provider_options else 0,
        )
        base_model_options = {
            "openai": [
                "gpt-5.2",
                "gpt-5.1",
                "gpt-5",
                "gpt-5.2-chat-latest",
                "gpt-5.1-chat-latest",
                "gpt-5-chat-latest",
                "gpt-5.1-codex-max",
                "gpt-5.1-codex",
                "gpt-5-codex",
                "gpt-5.2-pro",
                "gpt-5-pro",
                "gpt-5.1-codex-mini",
                "gpt-5-mini",
                "gpt-5-nano",
                "gpt-4.1",
                "gpt-4.1-mini",
                "gpt-4.1-nano",
                "gpt-4o",
                "gpt-4o-2024-05-13",
                "gpt-4o-mini",
                "gpt-4o-realtime-preview",
                "gpt-4o-mini-realtime-preview",
                "gpt-realtime",
                "gpt-realtime-mini",
                "gpt-audio",
                "gpt-audio-mini",
                "gpt-4o-audio-preview",
                "gpt-4o-mini-audio-preview",
                "o1",
                "o1-pro",
                "o1-mini",
                "o3",
                "o3-pro",
                "o3-deep-research",
                "o3-mini",
                "o4-mini",
                "o4-mini-deep-research",
                "codex-mini-latest",
                "gpt-5-search-api",
            ],
            "gemini": ["gemini-pro", "gemini-1.5-pro"],
            "anthropic": ["claude-3-opus", "claude-3-sonnet"],
            "azure-openai": ["gpt-5.1", "gpt-5.1-mini", "gpt-4o", "gpt-4.1"],
            "outro": [],
        }
        extra_model_options = extra_providers
        default_model = current.get("model", "gpt-4.1-mini")
        provider_models = base_model_options.get(provider) or extra_model_options.get(provider) or []
        if provider_models:
            model = st.selectbox("Modelo", options=provider_models, index=provider_models.index(default_model) if default_model in provider_models else 0)
        else:
            model = st.text_input("Modelo", value=default_model)

        vector_store_id = st.text_input("Vector Store ID", value=current.get("vector_store_id", ""))
        moderation_enabled = st.checkbox("Habilitar moderação (OpenAI)", value=current.get("moderation_enabled", False))
        custom_moderation_terms = st.text_input(
            "Termos personalizados para moderar (separe por ponto e vírgula)",
            value=current.get("custom_moderation_terms", ""),
            help="Ex.: palavra1;palavra2;expressão proibida",
        )

        st.markdown("**Adicionar provedor/modelos**")
        novo_prov = st.text_input("Novo provedor LLM")
        novos_modelos = st.text_input("Modelos (separados por vírgula)")
        atualizar_opcoes = st.button("Atualizar opções", type="secondary")

        if atualizar_opcoes:
            providers_updated = dict(extra_providers)
            if novo_prov.strip():
                modelos_list = [m.strip() for m in novos_modelos.split(",") if m.strip()]
                providers_updated[novo_prov.strip()] = modelos_list
            st.session_state["providers_temp"] = providers_updated
            st.session_state["selected_profile_id"] = selected_profile["id"] if selected_profile else None
            st.success("Opções de provedor/modelo atualizadas (sem validação).")

    # --- Aba Chatwoot ---
    with tab_chatwoot:
        st.subheader("Chatwoot")
        chatwoot_url = st.text_input("CHATWOOT_URL", value=current.get("chatwoot_url", ""))
        chatwoot_api_token = st.text_input("CHATWOOT_API_TOKEN", value=current.get("chatwoot_api_token", ""), type="password")
        chatwoot_account_id = st.text_input("CHATWOOT_ACCOUNT_ID", value=current.get("chatwoot_account_id", ""))

    # --- Aba Funcionamento por dia ---
    with tab_funcionamento:
        st.subheader("Funcionamento do Bot (por dia)")
        dias_semana = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        schedule = {}
        cols = st.columns(3)
        for idx, dia in enumerate(dias_semana):
            with cols[idx % 3]:
                dia_key = str(idx)
                dia_cfg = default_sched.get(dia_key, {"enabled": False, "start": 8, "end": 18})
                enabled = st.checkbox(f"{dia} ativo", value=dia_cfg.get("enabled", False), key=f"ck_{dia}")
                start = st.number_input(f"{dia} início", min_value=0, max_value=23, value=dia_cfg.get("start", 8), key=f"start_{dia}")
                end = st.number_input(f"{dia} fim", min_value=0, max_value=23, value=dia_cfg.get("end", 18), key=f"end_{dia}")
                schedule[dia_key] = {"enabled": enabled, "start": int(start), "end": int(end)}

        bot_enabled = st.checkbox("Bot ligado", value=current.get("bot_enabled", True))

    # --- Salvar ---
    st.markdown("---")
    col_save, _col_spacer = st.columns([1, 3])
    with col_save:
        submitted = st.button("Salvar configurações", type="primary")

    if submitted:
        enabled_days = [i for i in range(7) if schedule.get(str(i), {}).get("enabled")]
        if enabled_days:
            horario_inicio = min(schedule[str(i)]["start"] for i in enabled_days)
            horario_fim = max(schedule[str(i)]["end"] for i in enabled_days)
        else:
            horario_inicio = current.get("horario_inicio", 8)
            horario_fim = current.get("horario_fim", 18)

        providers_saved = dict(st.session_state.get("providers_temp", extra_providers))
        prompt_profile_id = selected_profile["id"] if selected_profile else None
        system_prompt_to_save = selected_profile["prompt_text"] if selected_profile else (current.get("system_prompt", "") or "")

        save_settings(
            {
                "system_prompt": system_prompt_to_save,
                "provider": provider,
                "model": model,
                "vector_store_id": vector_store_id,
                "chatwoot_url": chatwoot_url,
                "chatwoot_api_token": chatwoot_api_token,
                "chatwoot_account_id": chatwoot_account_id,
                "horario_inicio": int(horario_inicio),
                "horario_fim": int(horario_fim),
                "dias_funcionamento": enabled_days,
                "bot_enabled": bot_enabled,
                "schedule": schedule,
                "providers": providers_saved,
                "prompt_blocks": {},
                "prompt_profile_id": prompt_profile_id,
                "moderation_enabled": moderation_enabled,
                "custom_moderation_terms": custom_moderation_terms,
            }
        )
        st.session_state["selected_profile_id"] = prompt_profile_id
        st.success("Configurações salvas.")
        st.subheader("Validações automáticas")
        for name, status, msg in validate_settings(
            {
                "system_prompt": system_prompt_to_save,
                "provider": provider,
                "model": model,
                "vector_store_id": vector_store_id,
                "chatwoot_url": chatwoot_url,
                "chatwoot_api_token": chatwoot_api_token,
                "chatwoot_account_id": chatwoot_account_id,
                "moderation_enabled": moderation_enabled,
                "custom_moderation_terms": custom_moderation_terms,
            }
        ):
            if status == "success":
                st.success(f"{name}: {msg}")
            elif status == "warning":
                st.warning(f"{name}: {msg}")
            else:
                st.error(f"{name}: {msg}")


__all__ = ["render_config_module"]
