import json
import os
from datetime import timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from openai import OpenAI

from src.utils.db_init import DB_PATH, ensure_db
from src.utils.database import get_conn

ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
ENV_LOADED = False
TZ = timezone(timedelta(hours=-3))

# Mantemos a tabela de preços para validações automáticas de modelo.
PRICING_PER_1K = {
    "gpt-5.2": {"input": 0.00175, "output": 0.014},
    "gpt-5.1": {"input": 0.00125, "output": 0.01},
    "gpt-5": {"input": 0.00125, "output": 0.01},
    "gpt-5-mini": {"input": 0.00025, "output": 0.002},
    "gpt-5-nano": {"input": 0.00005, "output": 0.0004},
    "gpt-5.2-chat-latest": {"input": 0.00175, "output": 0.014},
    "gpt-5.1-chat-latest": {"input": 0.00125, "output": 0.01},
    "gpt-5-chat-latest": {"input": 0.00125, "output": 0.01},
    "gpt-5.1-codex-max": {"input": 0.00125, "output": 0.01},
    "gpt-5.1-codex": {"input": 0.00125, "output": 0.01},
    "gpt-5-codex": {"input": 0.00125, "output": 0.01},
    "gpt-5.2-pro": {"input": 0.021, "output": 0.168},
    "gpt-5-pro": {"input": 0.015, "output": 0.12},
    "gpt-4.1": {"input": 0.002, "output": 0.008},
    "gpt-4.1-mini": {"input": 0.0004, "output": 0.0016},
    "gpt-4.1-nano": {"input": 0.0001, "output": 0.0004},
    "gpt-4o": {"input": 0.0025, "output": 0.01},
    "gpt-4o-2024-05-13": {"input": 0.005, "output": 0.015},
    "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
    "gpt-realtime": {"input": 0.004, "output": 0.016},
    "gpt-realtime-mini": {"input": 0.0006, "output": 0.0024},
    "gpt-4o-realtime-preview": {"input": 0.005, "output": 0.02},
    "gpt-4o-mini-realtime-preview": {"input": 0.0006, "output": 0.0024},
    "gpt-audio": {"input": 0.0025, "output": 0.01},
    "gpt-audio-mini": {"input": 0.0006, "output": 0.0024},
    "gpt-4o-audio-preview": {"input": 0.0025, "output": 0.01},
    "gpt-4o-mini-audio-preview": {"input": 0.00015, "output": 0.0006},
    "o1": {"input": 0.015, "output": 0.06},
    "o1-pro": {"input": 0.15, "output": 0.6},
    "o3-pro": {"input": 0.02, "output": 0.08},
    "o3": {"input": 0.002, "output": 0.008},
    "o3-deep-research": {"input": 0.01, "output": 0.04},
    "o4-mini": {"input": 0.0011, "output": 0.0044},
    "o4-mini-deep-research": {"input": 0.002, "output": 0.008},
    "o3-mini": {"input": 0.0011, "output": 0.0044},
    "o1-mini": {"input": 0.0011, "output": 0.0044},
    "gpt-5.1-codex-mini": {"input": 0.00025, "output": 0.002},
    "codex-mini-latest": {"input": 0.0015, "output": 0.006},
    "gpt-5-search-api": {"input": 0.00125, "output": 0.01},
}


def load_env_once():
    """
    Carrega variáveis do .env apenas uma vez sem sobrescrever as existentes.
    """
    global ENV_LOADED
    if ENV_LOADED:
        return
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())
    ENV_LOADED = True


def build_prompt_from_blocks(blocks: Optional[Dict]) -> str:
    blocks = blocks or {}
    sections = [
        ("VOCÊ É...", blocks.get("identity") or ""),
        ("PERSONALIDADE", blocks.get("style") or ""),
        ("ESCOPO E FONTES", blocks.get("scope") or ""),
        ("SAUDAÇÃO INICIAL", blocks.get("greeting") or ""),
        ("REGRAS DE INTERAÇÃO", blocks.get("rules") or ""),
        ("HANDOFF", blocks.get("handoff_phrase") or ""),
        ("DESPEDIDA", blocks.get("goodbye") or ""),
    ]
    parts = [f"{title}\n{content}".strip() for title, content in sections]
    return "\n\n".join(parts).strip()


def load_settings() -> Optional[Dict]:
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT system_prompt, provider, model, vector_store_id, chatwoot_url, chatwoot_api_token,
                   chatwoot_account_id, horario_inicio, horario_fim, dias_funcionamento, bot_enabled,
                   schedule_json, providers_json, prompt_blocks_json, prompt_profile_id,
                   moderation_enabled, custom_moderation_terms
            FROM settings
            WHERE id = 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None
        dias = json.loads(row[9]) if row[9] else list(range(0, 5))
        schedule = json.loads(row[11]) if len(row) > 11 and row[11] else None
        providers = json.loads(row[12]) if len(row) > 12 and row[12] else {}
        prompt_blocks = json.loads(row[13]) if len(row) > 13 and row[13] else {}
        prompt_profile_id = row[14] if len(row) > 14 else None
        moderation_enabled = bool(row[15]) if len(row) > 15 else False
        custom_terms = row[16] if len(row) > 16 else ""
        return {
            "system_prompt": row[0],
            "provider": row[1],
            "model": row[2],
            "vector_store_id": row[3],
            "chatwoot_url": row[4],
            "chatwoot_api_token": row[5],
            "chatwoot_account_id": row[6],
            "horario_inicio": row[7],
            "horario_fim": row[8],
            "dias_funcionamento": dias,
            "bot_enabled": bool(row[10]),
            "schedule": schedule,
            "providers": providers,
            "prompt_blocks": prompt_blocks,
            "prompt_profile_id": prompt_profile_id,
            "moderation_enabled": moderation_enabled,
            "custom_moderation_terms": custom_terms or "",
        }


def save_settings(data: Dict):
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO settings (
                id, system_prompt, provider, model, vector_store_id, chatwoot_url,
                chatwoot_api_token, chatwoot_account_id, horario_inicio, horario_fim,
                dias_funcionamento, bot_enabled, schedule_json, providers_json,
                prompt_blocks_json, prompt_profile_id, moderation_enabled,
                custom_moderation_terms
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                system_prompt=excluded.system_prompt,
                provider=excluded.provider,
                model=excluded.model,
                vector_store_id=excluded.vector_store_id,
                chatwoot_url=excluded.chatwoot_url,
                chatwoot_api_token=excluded.chatwoot_api_token,
                chatwoot_account_id=excluded.chatwoot_account_id,
                horario_inicio=excluded.horario_inicio,
                horario_fim=excluded.horario_fim,
                dias_funcionamento=excluded.dias_funcionamento,
                bot_enabled=excluded.bot_enabled,
                schedule_json=excluded.schedule_json,
                providers_json=excluded.providers_json,
                prompt_blocks_json=excluded.prompt_blocks_json,
                prompt_profile_id=excluded.prompt_profile_id,
                moderation_enabled=excluded.moderation_enabled,
                custom_moderation_terms=excluded.custom_moderation_terms
            """,
            (
                data["system_prompt"],
                data["provider"],
                data["model"],
                data["vector_store_id"],
                data["chatwoot_url"],
                data["chatwoot_api_token"],
                data["chatwoot_account_id"],
                data["horario_inicio"],
                data["horario_fim"],
                json.dumps(data["dias_funcionamento"]),
                int(data["bot_enabled"]),
                json.dumps(data["schedule"]),
                json.dumps(data["providers"]),
                json.dumps(data.get("prompt_blocks", {})),
                data.get("prompt_profile_id"),
                int(data.get("moderation_enabled", False)),
                data.get("custom_moderation_terms", ""),
            ),
        )
        conn.commit()


def load_prompt_profiles() -> List[Dict]:
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, details, prompt_text FROM prompt_profiles ORDER BY name COLLATE NOCASE"
        )
        rows = cur.fetchall()
        return [
            {"id": r[0], "name": r[1] or "", "details": r[2] or "", "prompt_text": r[3] or ""}
            for r in rows
        ]


def get_prompt_profile(profile_id: Optional[int]) -> Optional[Dict]:
    if profile_id is None:
        return None
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, details, prompt_text FROM prompt_profiles WHERE id = ?",
            (profile_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "name": row[1] or "", "details": row[2] or "", "prompt_text": row[3] or ""}


def save_prompt_profile(name: str, details: str, prompt_text: str, profile_id: Optional[int] = None) -> int:
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        if profile_id:
            cur.execute(
                """
                UPDATE prompt_profiles
                SET name = ?, details = ?, prompt_text = ?
                WHERE id = ?
                """,
                (name, details, prompt_text, profile_id),
            )
            saved_id = profile_id
        else:
            cur.execute(
                """
                INSERT INTO prompt_profiles (name, details, prompt_text)
                VALUES (?, ?, ?)
                """,
                (name, details, prompt_text),
            )
            saved_id = cur.lastrowid
        conn.commit()
    return saved_id


def delete_prompt_profile(profile_id: Optional[int]):
    if profile_id is None:
        return
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM prompt_profiles WHERE id = ?", (profile_id,))
        conn.commit()


def get_fallback_profile() -> Optional[Dict]:
    """Retorna o perfil mais recente se nenhum estiver selecionado."""
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, prompt_text FROM prompt_profiles ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "name": row[1] or "", "prompt_text": row[2] or ""}


def load_logs(limit: int = 200) -> List[Dict]:
    ensure_db()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT conversation_id, client_name, direction, message, created_at, profile_name,
                   moderation_applied, moderation_details, inbox_id, prompt_tokens,
                   completion_tokens, total_tokens, cost_estimated_usd
            FROM conversation_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(
                {
                    "conversation_id": r[0],
                    "client_name": r[1],
                    "direction": r[2],
                    "message": r[3],
                    "created_at": r[4],
                    "profile_name": r[5],
                    "moderation_applied": bool(r[6]) if len(r) > 6 else False,
                    "moderation_details": r[7] if len(r) > 7 else None,
                    "inbox_id": r[8] if len(r) > 8 else None,
                    "prompt_tokens": r[9] if len(r) > 9 else None,
                    "completion_tokens": r[10] if len(r) > 10 else None,
                    "total_tokens": r[11] if len(r) > 11 else None,
                    "cost_estimated_usd": r[12] if len(r) > 12 else None,
                }
            )
        return result


def set_bot_enabled(enabled: bool):
    """
    Atualiza apenas o flag de ativação do bot, preservando demais configurações.
    Cria defaults mínimos se ainda não houver registro.
    """
    current = load_settings() or {}
    def_sched = {str(i): {"enabled": i < 5, "start": 8, "end": 18} for i in range(7)}
    data = {
        "system_prompt": current.get("system_prompt", ""),
        "provider": current.get("provider", "openai"),
        "model": current.get("model", "gpt-4.1-mini"),
        "vector_store_id": current.get("vector_store_id", ""),
        "chatwoot_url": current.get("chatwoot_url", ""),
        "chatwoot_api_token": current.get("chatwoot_api_token", ""),
        "chatwoot_account_id": current.get("chatwoot_account_id", ""),
        "horario_inicio": current.get("horario_inicio", 8),
        "horario_fim": current.get("horario_fim", 18),
        "dias_funcionamento": current.get("dias_funcionamento", list(range(0, 5))),
        "bot_enabled": bool(enabled),
        "schedule": current.get("schedule", def_sched),
        "providers": current.get("providers", {}),
        "prompt_blocks": current.get("prompt_blocks", {}),
        "prompt_profile_id": current.get("prompt_profile_id"),
        "moderation_enabled": current.get("moderation_enabled", False),
        "custom_moderation_terms": current.get("custom_moderation_terms", ""),
    }
    save_settings(data)


def log_conversation(
    conversation_id: str,
    client_name: str,
    direction: str,
    message: str,
    prompt_tokens=None,
    completion_tokens=None,
    total_tokens=None,
    cost_estimated_usd=None,
    inbox_id=None,
    profile_name=None,
    moderation_applied=False,
    moderation_details=None,
):
    ensure_db()
    ts = datetime.now(TZ).isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO conversation_logs (
                conversation_id,
                client_name,
                direction,
                message,
                created_at,
                inbox_id,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cost_estimated_usd,
                profile_name,
                moderation_applied,
                moderation_details
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                conversation_id,
                client_name,
                direction,
                message,
                ts,
                inbox_id,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cost_estimated_usd,
                profile_name,
                int(bool(moderation_applied)),
                moderation_details,
            ),
        )
        conn.commit()


def validate_settings(data: Dict):
    """
    Executa validações após salvar configuração.
    Retorna lista de tuplas (nome, status, mensagem).
    """
    load_env_once()
    results = []

    provider = data.get("provider")
    model = data.get("model")
    vector_store_id = data.get("vector_store_id")
    api_key = os.getenv("OPENAI_API_KEY", "")

    if provider != "openai":
        results.append(("Modelo LLM", "warning", f"Validação automática só disponível para provider 'openai' (selecionado: {provider})."))
    else:
        preco = PRICING_PER_1K.get((model or "").lower())
        if not preco:
            results.append(("Modelo LLM", "error", f"Modelo '{model}' não possui preço cadastrado. Atualize o modelo ou cadastre o preço."))
        if not api_key:
            results.append(("Modelo LLM", "error", "OPENAI_API_KEY não encontrada (.env ou ambiente)."))
        else:
            try:
                client = OpenAI(api_key=api_key)
                client.models.retrieve(model)
                results.append(("Modelo LLM", "success", f"Consegui acessar o modelo '{model}'."))
            except Exception as e:
                results.append(("Modelo LLM", "error", f"Não consegui acessar o modelo '{model}': {e}"))

            if vector_store_id:
                try:
                    vs = None
                    if hasattr(client, "vector_stores"):
                        vs = client.vector_stores.retrieve(vector_store_id)
                    elif hasattr(getattr(client, "beta", None), "vector_stores"):
                        vs = client.beta.vector_stores.retrieve(vector_store_id)
                    if vs:
                        results.append(("Vector Store", "success", f"Vector store '{vector_store_id}' acessível."))
                    else:
                        results.append(("Vector Store", "error", "SDK OpenAI não expôs o endpoint de vector store."))
                except Exception as e:
                    results.append(("Vector Store", "error", f"Erro ao acessar vector store '{vector_store_id}': {e}"))
            else:
                results.append(("Vector Store", "warning", "Nenhum vector store configurado para validar."))

    chatwoot_url = (data.get("chatwoot_url") or "").rstrip("/")
    chatwoot_api_token = data.get("chatwoot_api_token") or ""
    chatwoot_account_id = data.get("chatwoot_account_id") or ""
    if chatwoot_url and chatwoot_api_token and chatwoot_account_id:
        try:
            endpoint = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account_id}/conversations"
            resp = requests.get(endpoint, headers={"api_access_token": chatwoot_api_token}, timeout=10)
            if resp.status_code < 400:
                results.append(("Chatwoot API", "success", f"Chatwoot respondeu {resp.status_code}."))
            else:
                results.append(("Chatwoot API", "error", f"Chatwoot respondeu {resp.status_code}: {resp.text[:200]}"))
        except Exception as e:
            results.append(("Chatwoot API", "error", f"Falha ao chamar Chatwoot: {e}"))
    else:
        results.append(("Chatwoot API", "warning", "Preencha CHATWOOT_URL, CHATWOOT_API_TOKEN e CHATWOOT_ACCOUNT_ID para validar."))

    return results


__all__ = [
    "DB_PATH",
    "load_env_once",
    "load_settings",
    "save_settings",
    "load_prompt_profiles",
    "save_prompt_profile",
    "get_prompt_profile",
    "delete_prompt_profile",
    "get_fallback_profile",
    "build_prompt_from_blocks",
    "load_logs",
    "log_conversation",
    "validate_settings",
    "PRICING_PER_1K",
    "set_bot_enabled",
]
