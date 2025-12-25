"""Bot runtime service for handling Chatwoot messages via FastAPI."""

import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import pytz
import requests
import uvicorn
from fastapi import BackgroundTasks, FastAPI, Request
from openai import OpenAI
from openai.types.responses import FileSearchToolParam

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bot.engine import (
    load_env_once,
    load_settings,
    get_prompt_profile,
    get_fallback_profile,
    log_conversation,
)
from src.bot.rules import (
    custom_moderation_hit,
    estimar_custo_tokens,
    extrair_primeiro_nome,
    extrair_texto_resposta,
    fora_do_horario_comercial,
    is_audio_attachment,
    moderar_mensagem,
    FUSO_HORARIO,
)

# --- CONFIGURAÇÕES ---
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
ENV_LOADED = False
DEFAULT_SYSTEM_PROMPT = (
    "Você é o Galo Bot, assistente do atendimento da empresa. "
    "Responda em português do Brasil, de forma cordial, direta e útil. "
    "Peça educadamente para reescrever se a mensagem estiver inaudível ou ambígua "
    "e acione um humano quando encontrar pedidos fora do escopo de suporte padrão."
)
ALLOWED_INBOX_ID = os.getenv("ALLOWED_INBOX_ID") or "88473"

app = FastAPI()
client = None  # inicializado após carregar config
historico_conversas = {}  # conversation_id -> lista de mensagens (reinicia a cada deploy)
mensagens_processadas = set()  # ids de mensagens já tratadas (evita duplicidade)


def load_env_local():
    """Load environment variables once for the bot runtime."""
    global ENV_LOADED
    if ENV_LOADED:
        return
    load_env_once()
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())
    ENV_LOADED = True


def responder_cliente(conversation_id, primeiro_nome, user_message, inbox_id=None):
    """Process an incoming message and respond through the configured LLM."""
    load_env_local()
    config = load_settings()
    if not config:
        print("❌ Configurações não encontradas. Use o painel para salvar.")
        return
    if not config.get("bot_enabled", True):
        print("ℹ️ Bot desligado via configuração.")
        return

    if config.get("provider", "openai") != "openai":
        print("❌ Provedor não suportado ainda. Ajuste para openai.")
        return
    global client
    if client is None:
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            print("❌ OPENAI_API_KEY não definida no ambiente.")
            return
        client = OpenAI(api_key=api_key)

    chatwoot_url = config.get("chatwoot_url", "")
    chatwoot_token = config.get("chatwoot_api_token", "")
    chatwoot_account = config.get("chatwoot_account_id", "")

    if not all([chatwoot_url, chatwoot_token, chatwoot_account]):
        print("❌ CHATWOOT_URL/TOKEN/ACCOUNT_ID ausentes na configuração.")
        return

    vector_store_id = config.get("vector_store_id")
    profile_data = get_prompt_profile(config.get("prompt_profile_id")) or get_fallback_profile()
    profile_name = profile_data.get("name") if profile_data else None
    system_prompt = (
        (profile_data.get("prompt_text") if profile_data else None)
        or config.get("system_prompt")
        or DEFAULT_SYSTEM_PROMPT
    )
    moderation_enabled = bool(config.get("moderation_enabled"))
    custom_terms = []
    if config.get("custom_moderation_terms"):
        custom_terms = [t.strip() for t in str(config.get("custom_moderation_terms")).split(";") if t.strip()]

    try:
        url_conv = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account}/conversations/{conversation_id}"
        headers = {"api_access_token": chatwoot_token}
        resp_conv = requests.get(url_conv, headers=headers)

        if resp_conv.status_code == 200:
            conv_data = resp_conv.json()
            status = conv_data.get("status")
            if status not in ("open", "pending"):
                print(f"🛑 Conversa {conversation_id} bloqueada (status: {status}).")
                return
    except Exception as e:
        print(f"Erro no check de Handoff: {e}")

    try:
        mensagens = historico_conversas.setdefault(
            conversation_id,
            [{"role": "system", "content": system_prompt}],
        )
        mensagem_cliente = (
            f"Nome do cliente: {primeiro_nome}\n"
            f"Mensagem do cliente: {user_message or 'Mensagem sem conteúdo.'}"
        )

        moderation_info = {}
        if moderation_enabled:
            custom_hit, termo = custom_moderation_hit(user_message or "", custom_terms)
            if custom_hit:
                moderation_info = {"flagged": True, "custom_term": termo, "source": "custom_terms"}
            else:
                moderation_info = moderar_mensagem(client, user_message or "")

            log_conversation(
                conversation_id,
                primeiro_nome,
                "user",
                mensagem_cliente,
                inbox_id=inbox_id,
                profile_name=profile_name,
                moderation_applied=True,
                moderation_details=str(moderation_info),
            )
            if moderation_info.get("flagged"):
                aviso = (
                    f"Olá, {primeiro_nome}. Detectei conteúdo sensível na mensagem. "
                    "Por favor, reformule ou aguarde para falar com um humano."
                )
                log_conversation(
                    conversation_id,
                    primeiro_nome,
                    "assistant",
                    aviso,
                    inbox_id=inbox_id,
                    profile_name=profile_name,
                    moderation_applied=True,
                    moderation_details=str(moderation_info),
                )
                url_msg = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account}/conversations/{conversation_id}/messages"
                data_out = {"content": aviso, "message_type": "outgoing"}
                resp_out = requests.post(url_msg, json=data_out, headers=headers)
                if 200 <= resp_out.status_code < 300:
                    print("✅ Aviso de moderação enviado.")
                else:
                    print(f"❌ Falha ao enviar aviso de moderação: {resp_out.status_code} {resp_out.text[:200]}")
                return
        else:
            log_conversation(conversation_id, primeiro_nome, "user", mensagem_cliente, inbox_id=inbox_id, profile_name=profile_name)

        mensagens.append({"role": "user", "content": mensagem_cliente})

        tools = []
        if vector_store_id:
            tools.append(
                FileSearchToolParam(type="file_search", vector_store_ids=[vector_store_id])
            )

        completion_kwargs = {
            "model": config.get("model", "gpt-4.1-mini"),
            "input": mensagens,
            "tools": tools or None,
        }
        modelo_atual = str(completion_kwargs["model"]).lower()
        if not modelo_atual.startswith("gpt-5"):
            completion_kwargs["temperature"] = 0.3

        completion = client.responses.create(**completion_kwargs)

        resposta_final = extrair_texto_resposta(completion)
        if not resposta_final:
            print("❌ Sem resposta do modelo.")
            return

        mensagens.append({"role": "assistant", "content": resposta_final})
        usage = getattr(completion, "usage", None)
        in_toks = getattr(usage, "input_tokens", None) if usage else None
        out_toks = getattr(usage, "output_tokens", None) if usage else None
        tot_toks = getattr(usage, "total_tokens", None) if usage else None
        custo = estimar_custo_tokens(completion_kwargs["model"], in_toks, out_toks)

        log_conversation(
            conversation_id,
            primeiro_nome,
            "assistant",
            resposta_final,
            prompt_tokens=in_toks,
            completion_tokens=out_toks,
            total_tokens=tot_toks,
            cost_estimated_usd=custo,
            inbox_id=inbox_id,
            profile_name=profile_name,
            moderation_applied=bool(moderation_info),
            moderation_details=str(moderation_info) if moderation_info else None,
        )

        url_msg = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account}/conversations/{conversation_id}/messages"
        data = {"content": resposta_final, "message_type": "outgoing"}

        resp_out = requests.post(url_msg, json=data, headers=headers)
        try:
            resp_json = resp_out.json()
        except Exception:
            resp_json = resp_out.text[:200]
        if 200 <= resp_out.status_code < 300:
            print(f"✅ Respondido. Chatwoot status={resp_out.status_code} body={resp_json}")
        else:
            print(f"❌ Falha ao enviar para Chatwoot: {resp_out.status_code} {resp_json}")

    except Exception as e:
        print(f"❌ Erro ao processar IA: {e}")
        traceback.print_exc()


@app.post("/webhook")
async def chatwoot_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
        event = data.get("event")
        print(f"🔔 Evento: {event}")
        load_env_local()

        if event == "message_created":
            config = load_settings()
            if not config or not config.get("bot_enabled", True):
                print("ℹ️ Bot desligado ou sem configuração; ignorando mensagem.")
                return {"status": "ok"}
            chatwoot_url = config.get("chatwoot_url", "")
            chatwoot_token = config.get("chatwoot_api_token", "")
            chatwoot_account = config.get("chatwoot_account_id", "")
            if not all([chatwoot_url, chatwoot_token, chatwoot_account]):
                print("❌ CHATWOOT_URL/TOKEN/ACCOUNT_ID ausentes; ignorei mensagem.")
                return {"status": "ok"}

            content = data.get("content")
            msg_type = data.get("message_type")
            is_private = data.get("private")
            attachments = data.get("attachments") or []
            message_id = data.get("id") or data.get("message_id")

            conversation = data.get("conversation", {})
            conversation_id = conversation.get("id")
            inbox_id = conversation.get("inbox_id")

            if not conversation_id:
                nested = data.get("data", {})
                content = nested.get("content")
                msg_type = nested.get("message_type")
                is_private = nested.get("private")
                attachments = nested.get("attachments") or attachments
                message_id = message_id or nested.get("id") or nested.get("message_id")
                conversation_id = nested.get("conversation_id")
                inbox_id = inbox_id or nested.get("inbox_id")

            if inbox_id and str(inbox_id) != str(ALLOWED_INBOX_ID):
                print(f"⛔ Mensagem ignorada: inbox {inbox_id} diferente do permitido {ALLOWED_INBOX_ID}.")
                return {"status": "ok"}

            if message_id:
                if message_id in mensagens_processadas:
                    print(f"⏩ Já tratei a mensagem {message_id}; ignorando duplicata.")
                    return {"status": "ok"}
                mensagens_processadas.add(message_id)

            eh_msg_cliente = (msg_type == 0 or msg_type == "incoming")

            if eh_msg_cliente and is_private is False and conversation_id:
                print(f"ℹ️ Payload recebido: msg_type={msg_type}, private={is_private}, attachments={len(attachments)}, conversation_id={conversation_id}, message_id={message_id}")
                primeiro_nome = extrair_primeiro_nome(data)
                mensagem_cliente = content or ""
                audio_detectado = any(is_audio_attachment(att) for att in attachments)

                if audio_detectado and not mensagem_cliente:
                    print("ℹ️ Áudio recebido; enviando aviso de texto.")
                    url_msg = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account}/conversations/{conversation_id}/messages"
                    headers = {"api_access_token": chatwoot_token}
                    aviso = (
                        f"Olá, {primeiro_nome}, ainda não tenho a capacidade de audição. "
                        "Só sei ler mensagens por enquanto. "
                        "Poderia, por gentileza, enviar sua mensagem por texto?"
                    )
                    data_out = {"content": aviso, "message_type": "outgoing"}
                    resp = requests.post(url_msg, json=data_out, headers=headers)
                    if resp.status_code == 200:
                        print("✅ Aviso de áudio enviado.")
                    else:
                        print(f"❌ Erro ao enviar aviso de áudio: {resp.status_code} {resp.text[:200]}")
                    return

                if not mensagem_cliente:
                    print("⛔ Sem mensagem de texto; nada a enviar ao assistente.")
                    return

                if fora_do_horario_comercial(config):
                    print(f"🌙 Fora do horário. Respondendo {primeiro_nome} (ID {conversation_id})")
                    background_tasks.add_task(responder_cliente, conversation_id, primeiro_nome, mensagem_cliente, inbox_id)
                else:
                    print(f"☀️ Dentro do horário configurado. Bot não responderá automaticamente.")
            else:
                print(f"⛔ Mensagem ignorada (critérios não atendidos). msg_type={msg_type}, private={is_private}, conversation_id={conversation_id}, message_id={message_id}")

    except Exception as e:
        print(f"❌ ERRO NO WEBHOOK: {e}")
        traceback.print_exc()

    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
