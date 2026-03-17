"""
Conversation memory backed by Lakebase (Databricks-managed PostgreSQL).

In production, this stores:
- Session history for multi-turn conversations
- Agent decision audit log
- Customer interaction records for compliance

Falls back to in-memory storage when Lakebase is not configured.
"""

import os
import json
from datetime import datetime
from typing import Optional


class ConversationMemory:
    """
    Agent memory with Lakebase (PostgreSQL) backend.

    Lakebase provides:
    - Persistent conversation history across sessions
    - Low-latency reads for agent context retrieval
    - PostgreSQL compatibility for familiar querying
    - Fully managed — no infrastructure to maintain
    - Unity Catalog governed — access control and audit
    """

    def __init__(self):
        self._pg_conn = None
        self._fallback: dict[str, list[dict]] = {}
        self._lakebase_url = os.environ.get("LAKEBASE_URL", "")
        self._init_db()

    def _init_db(self):
        """Try to connect to Lakebase; fall back to in-memory if unavailable."""
        if not self._lakebase_url:
            return

        try:
            import psycopg2
            self._pg_conn = psycopg2.connect(self._lakebase_url)
            self._pg_conn.autocommit = True
            with self._pg_conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS conversation_history (
                        id SERIAL PRIMARY KEY,
                        session_id VARCHAR(128) NOT NULL,
                        role VARCHAR(16) NOT NULL,
                        content TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_session
                    ON conversation_history(session_id, created_at)
                """)
        except Exception:
            self._pg_conn = None

    def status(self) -> str:
        if self._pg_conn:
            return "Lakebase (PostgreSQL) connected"
        if self._lakebase_url:
            return f"Lakebase configured but connection failed ({self._lakebase_url[:30]}...)"
        return "In-memory (set LAKEBASE_URL for Lakebase persistence)"

    def save_message(self, session_id: str, role: str, content: str):
        """Save a message to conversation history."""
        if self._pg_conn:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO conversation_history (session_id, role, content) VALUES (%s, %s, %s)",
                        (session_id, role, content),
                    )
                return
            except Exception:
                pass

        # Fallback: in-memory
        if session_id not in self._fallback:
            self._fallback[session_id] = []
        self._fallback[session_id].append({
            "role": role,
            "content": content,
        })

    def get_history(self, session_id: str, limit: int = 20) -> list[dict]:
        """Retrieve recent conversation history for a session."""
        if self._pg_conn:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute(
                        """SELECT role, content FROM conversation_history
                           WHERE session_id = %s
                           ORDER BY created_at DESC LIMIT %s""",
                        (session_id, limit),
                    )
                    rows = cur.fetchall()
                    return [{"role": r[0], "content": r[1]} for r in reversed(rows)]
            except Exception:
                pass

        # Fallback
        return self._fallback.get(session_id, [])[-limit:]

    def clear_history(self, session_id: str):
        """Clear conversation history for a session."""
        if self._pg_conn:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM conversation_history WHERE session_id = %s",
                        (session_id,),
                    )
                return
            except Exception:
                pass

        self._fallback.pop(session_id, None)
