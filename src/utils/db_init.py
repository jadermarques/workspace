"""Database initialization and migrations for the workspace."""

import sqlite3
from pathlib import Path

# Caminho centralizado do banco. Mantemos no diretório data/raw para
# separar dados de código e facilitar backup.
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
DB_PATH = DATA_DIR / "bot_config.db"


def ensure_db():
    """Create the database and apply defensive migrations (idempotent)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                system_prompt TEXT,
                provider TEXT,
                model TEXT,
                vector_store_id TEXT,
                chatwoot_url TEXT,
                chatwoot_api_token TEXT,
                chatwoot_account_id TEXT,
                horario_inicio INTEGER,
                horario_fim INTEGER,
                dias_funcionamento TEXT,
                bot_enabled INTEGER,
                schedule_json TEXT,
                providers_json TEXT,
                prompt_blocks_json TEXT,
                prompt_profile_id INTEGER,
                moderation_enabled INTEGER DEFAULT 0,
                custom_moderation_terms TEXT
            )
            """
        )

        cols_settings = [row[1] for row in cur.execute("PRAGMA table_info(settings)")]
        if "schedule_json" not in cols_settings:
            cur.execute("ALTER TABLE settings ADD COLUMN schedule_json TEXT")
        if "providers_json" not in cols_settings:
            cur.execute("ALTER TABLE settings ADD COLUMN providers_json TEXT")
        if "prompt_blocks_json" not in cols_settings:
            cur.execute("ALTER TABLE settings ADD COLUMN prompt_blocks_json TEXT")
        if "prompt_profile_id" not in cols_settings:
            cur.execute("ALTER TABLE settings ADD COLUMN prompt_profile_id INTEGER")
        if "moderation_enabled" not in cols_settings:
            cur.execute("ALTER TABLE settings ADD COLUMN moderation_enabled INTEGER DEFAULT 0")
        if "custom_moderation_terms" not in cols_settings:
            cur.execute("ALTER TABLE settings ADD COLUMN custom_moderation_terms TEXT")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                details TEXT,
                prompt_text TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS insight_prompts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                description TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                prompt_text TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT,
                client_name TEXT,
                direction TEXT,
                message TEXT,
                created_at TEXT,
                inbox_id TEXT,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                total_tokens INTEGER,
                cost_estimated_usd REAL,
                profile_name TEXT,
                moderation_applied INTEGER,
                moderation_details TEXT
            )
            """
        )

        cols_logs = [row[1] for row in cur.execute("PRAGMA table_info(conversation_logs)")]
        if "inbox_id" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN inbox_id TEXT")
        if "prompt_tokens" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN prompt_tokens INTEGER")
        if "completion_tokens" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN completion_tokens INTEGER")
        if "total_tokens" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN total_tokens INTEGER")
        if "cost_estimated_usd" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN cost_estimated_usd REAL")
        if "profile_name" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN profile_name TEXT")
        if "moderation_applied" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN moderation_applied INTEGER")
        if "moderation_details" not in cols_logs:
            cur.execute("ALTER TABLE conversation_logs ADD COLUMN moderation_details TEXT")

        conn.commit()


__all__ = ["ensure_db", "DB_PATH"]
