import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple

import pytz
from openai import OpenAI

from src.bot.engine import PRICING_PER_1K

FUSO_HORARIO = pytz.timezone("America/Sao_Paulo")


def default_schedule():
    return {str(i): {"enabled": i < 5, "start": 8, "end": 18} for i in range(7)}


def fora_do_horario_comercial(config: Dict) -> bool:
    """
    Retorna True se estivermos fora do horário comercial configurado.
    """
    agora = datetime.now(FUSO_HORARIO)
    dia = agora.weekday()  # 0 = segunda, 6 = domingo
    hora = agora.hour

    schedule = config.get("schedule") or default_schedule()
    dia_cfg = schedule.get(str(dia)) or {"enabled": False}
    if not dia_cfg.get("enabled"):
        return True
    inicio = dia_cfg.get("start", 8)
    fim = dia_cfg.get("end", 18)
    return not (inicio <= hora < fim)


def custom_moderation_hit(texto: str, termos):
    """
    Retorna (True, termo_encontrado) se algum termo personalizado estiver presente (case-insensitive).
    """
    if not texto or not termos:
        return False, None
    texto_l = texto.lower()
    for termo in termos:
        termo_norm = (termo or "").strip().lower()
        if termo_norm and termo_norm in texto_l:
            return True, termo
    return False, None


def moderar_mensagem(client: OpenAI, texto: str):
    try:
        resp = client.moderations.create(model="omni-moderation-latest", input=texto)
        res = resp.results[0]
        return {
            "flagged": bool(getattr(res, "flagged", False)),
            "categories": res.categories if hasattr(res, "categories") else {},
            "category_scores": res.category_scores if hasattr(res, "category_scores") else {},
            "raw": resp.model_dump_json() if hasattr(resp, "model_dump_json") else str(resp),
        }
    except Exception as e:  # pragma: no cover - API externa
        traceback.print_exc()
        return {"error": str(e)}


def extrair_primeiro_nome(dados_webhook):
    """Extrai nome do cliente para personalizar o atendimento."""
    try:
        sender = dados_webhook.get("sender") or dados_webhook.get("data", {}).get("sender") or {}
        nome_completo = sender.get("name") or sender.get("available_name") or ""
        if not nome_completo:
            return "Cliente"
        return nome_completo.split()[0].title()
    except Exception:
        return "Cliente"


def is_audio_attachment(att):
    """Retorna True se o attachment parecer ser áudio."""
    if not att:
        return False
    mime = (att.get("file_type") or att.get("content_type") or "").lower()
    if "audio" in mime:
        return True
    url = (att.get("data_url") or att.get("url") or "").lower()
    return url.endswith((".ogg", ".oga", ".mp3", ".wav", ".m4a", ".aac"))


def extrair_texto_resposta(response) -> str:
    """
    SDK novo expõe output_text, mas mantemos um fallback defensivo.
    """
    if getattr(response, "output_text", None):
        return response.output_text.strip()
    output = getattr(response, "output", None) or []
    for bloco in output:
        conteudos = getattr(bloco, "content", None) or []
        for item in conteudos:
            texto = getattr(getattr(item, "text", None), "value", None)
            if texto:
                return texto.strip()
    return ""


def estimar_custo_tokens(modelo: str, input_tokens: int, output_tokens: int):
    if input_tokens is None or output_tokens is None:
        return None
    modelo_l = (modelo or "").lower()
    pricing = PRICING_PER_1K.get(modelo_l)
    if not pricing:
        return None
    custo_in = (input_tokens / 1000) * pricing["input"]
    custo_out = (output_tokens / 1000) * pricing["output"]
    return round(custo_in + custo_out, 6)


__all__ = [
    "default_schedule",
    "fora_do_horario_comercial",
    "custom_moderation_hit",
    "moderar_mensagem",
    "extrair_primeiro_nome",
    "is_audio_attachment",
    "extrair_texto_resposta",
    "estimar_custo_tokens",
    "FUSO_HORARIO",
]
