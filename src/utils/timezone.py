"""Timezone configuration shared across the workspace."""

from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Sao_Paulo")

__all__ = ["TZ"]
