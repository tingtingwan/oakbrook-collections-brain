"""
Approval queue backed by Unity Catalog Delta table.

In production this would be Lakebase (PostgreSQL) — but since Lakebase
isn't on GCP yet, we use a UC Delta table via SQL connector.
This demonstrates the same governance pattern: approvals are governed
in Unity Catalog with lineage, access control, and audit trail.

Table: main.oakbrook_collections.approval_queue
"""

import json
import logging
from datetime import datetime

from backend.data import _query_uc, _get_sql_connection, UC_CATALOG, UC_SCHEMA

logger = logging.getLogger(__name__)

TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.approval_queue"

# In-memory fallback when UC isn't available
_local_queue: list[dict] = []
_uc_available: bool | None = None


def _ensure_table():
    """Create the approval_queue table if it doesn't exist."""
    global _uc_available
    if _uc_available is False:
        return False
    conn = _get_sql_connection()
    if not conn:
        _uc_available = False
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id STRING,
                customer_id STRING,
                customer_name STRING,
                channel STRING,
                tone STRING,
                message STRING,
                strategy_summary STRING,
                status STRING,
                submitted_at STRING,
                submitted_by STRING,
                reviewed_by STRING,
                reviewed_at STRING
            ) USING DELTA
        """)
        cursor.close()
        _uc_available = True
        logger.info(f"Approval table {TABLE} ready")
        return True
    except Exception as e:
        logger.warning(f"Could not create approval table: {e}")
        _uc_available = False
        return False


def submit_approval(entry: dict) -> dict:
    """Insert a new approval record into UC or local fallback."""
    if _ensure_table():
        try:
            conn = _get_sql_connection()
            cursor = conn.cursor()
            # Escape single quotes in message
            msg = entry["message"].replace("'", "''")
            strategy = entry.get("strategy_summary", "").replace("'", "''")
            cursor.execute(f"""
                INSERT INTO {TABLE} VALUES (
                    '{entry["id"]}',
                    '{entry["customer_id"]}',
                    '{entry["customer_name"]}',
                    '{entry["channel"]}',
                    '{entry["tone"]}',
                    '{msg}',
                    '{strategy}',
                    '{entry["status"]}',
                    '{entry["submitted_at"]}',
                    '{entry["submitted_by"]}',
                    NULL,
                    NULL
                )
            """)
            cursor.close()
            entry["_source"] = f"Unity Catalog ({TABLE})"
            logger.info(f"Approval {entry['id']} saved to UC")
            return entry
        except Exception as e:
            logger.warning(f"UC insert failed, using local: {e}")

    # Fallback to local
    _local_queue.append(entry)
    entry["_source"] = "In-memory (local fallback)"
    return entry


def list_approvals() -> list[dict]:
    """List all approvals from UC or local fallback."""
    if _ensure_table():
        result = _query_uc(f"SELECT * FROM {TABLE} ORDER BY submitted_at DESC")
        if result is not None:
            for r in result:
                r["_source"] = f"Unity Catalog ({TABLE})"
            return result

    return _local_queue


def update_approval_status(approval_id: str, status: str, reviewer: str = "Manager") -> dict | None:
    """Update an approval's status in UC or local fallback."""
    reviewed_at = datetime.now().isoformat()

    if _ensure_table():
        try:
            conn = _get_sql_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE {TABLE}
                SET status = '{status}',
                    reviewed_by = '{reviewer}',
                    reviewed_at = '{reviewed_at}'
                WHERE id = '{approval_id}'
            """)
            cursor.close()

            # Fetch and return the updated record
            result = _query_uc(f"SELECT * FROM {TABLE} WHERE id = '{approval_id}'")
            if result and len(result) > 0:
                result[0]["_source"] = f"Unity Catalog ({TABLE})"
                return result[0]
        except Exception as e:
            logger.warning(f"UC update failed: {e}")

    # Fallback
    for entry in _local_queue:
        if entry["id"] == approval_id:
            entry["status"] = status
            entry["reviewed_by"] = reviewer
            entry["reviewed_at"] = reviewed_at
            return entry

    return None


def get_next_id() -> str:
    """Get the next approval ID."""
    approvals = list_approvals()
    return f"APR-{len(approvals)+1:04d}"
