"""
PostgreSQL Database Backend
Drop-in replacement for SeaTable API client.
Same interface: list_rows, search_rows, get_row, create_row, update_row, etc.
Switch via DB_BACKEND=postgres (default) oder DB_BACKEND=seatable.
"""

import os
import json
import logging
import threading
import uuid
from typing import Optional
from datetime import datetime
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Columns that store JSON – auto-parse strings on INSERT/UPDATE
JSONB_COLUMNS = {
    "fin_cases": {
        "facts_extracted", "answers_user", "manual_overrides", "derived_values",
        "docs_index", "conversation_ids", "readiness", "audit_log",
        "europace_response",
    },
    "fin_documents": {"extracted_data"},
    "processed_emails": {"attachments_hashes"},
    "email_test_log": set(),
}

# ──────────────────────────────────────────
# Connection Pool
# ──────────────────────────────────────────
_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        # Double-check after acquiring lock
        if _pool is not None:
            return _pool
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set!")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            1, 10, DATABASE_URL,
            connect_timeout=10,
            options="-c statement_timeout=30000",  # 30s query timeout
        )
        _init_tables()
        logger.info("PostgreSQL connection pool initialized")
    return _pool


def init_pool():
    """Eagerly initialize the connection pool at app startup."""
    try:
        _get_pool()
    except Exception as e:
        logger.error(f"Failed to init PG pool at startup: {e}")


@contextmanager
def _get_conn():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ──────────────────────────────────────────
# Schema Init
# ──────────────────────────────────────────
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fin_cases (
    _id TEXT PRIMARY KEY,
    case_id TEXT UNIQUE NOT NULL,
    applicant_name TEXT DEFAULT '',
    partner_email TEXT DEFAULT '',
    status TEXT DEFAULT 'INTAKE',
    sources TEXT DEFAULT '',
    facts_extracted JSONB DEFAULT '{}'::jsonb,
    answers_user JSONB DEFAULT '{}'::jsonb,
    manual_overrides JSONB DEFAULT '{}'::jsonb,
    derived_values JSONB DEFAULT '{}'::jsonb,
    docs_index JSONB DEFAULT '{}'::jsonb,
    conversation_ids JSONB DEFAULT '[]'::jsonb,
    readiness JSONB DEFAULT '{}'::jsonb,
    audit_log JSONB DEFAULT '[]'::jsonb,
    onedrive_folder_id TEXT DEFAULT '',
    onedrive_web_url TEXT DEFAULT '',
    last_status_change TEXT DEFAULT '',
    europace_case_id TEXT DEFAULT '',
    europace_response JSONB DEFAULT '{}'::jsonb,
    final_payload_preview TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fin_documents (
    _id TEXT PRIMARY KEY,
    "caseId" TEXT NOT NULL,
    file_name TEXT DEFAULT '',
    doc_type TEXT DEFAULT 'Sonstiges',
    extracted_data JSONB DEFAULT '{}'::jsonb,
    processing_status TEXT DEFAULT 'pending',
    error_message TEXT DEFAULT '',
    onedrive_file_id TEXT DEFAULT '',
    gdrive_file_id TEXT DEFAULT '',
    processed_at TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fin_documents_caseid ON fin_documents ("caseId");

CREATE TABLE IF NOT EXISTS processed_emails (
    _id TEXT PRIMARY KEY,
    provider_message_id TEXT UNIQUE NOT NULL,
    mail_type TEXT DEFAULT '',
    processing_result TEXT DEFAULT '',
    case_id TEXT DEFAULT '',
    from_email TEXT DEFAULT '',
    subject TEXT DEFAULT '',
    conversation_id TEXT DEFAULT '',
    body_text TEXT DEFAULT '',
    body_html TEXT DEFAULT '',
    parsed_result JSONB DEFAULT '{}'::jsonb,
    matched_by TEXT DEFAULT '',
    processed_at TEXT DEFAULT '',
    attachments_count INTEGER DEFAULT 0,
    attachments_hashes JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS email_test_log (
    _id TEXT PRIMARY KEY,
    "to" TEXT DEFAULT '',
    subject TEXT DEFAULT '',
    body_text TEXT DEFAULT '',
    body_html TEXT DEFAULT '',
    logged_at TEXT DEFAULT '',
    dry_run BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fin_partners (
    _id TEXT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fin_errors (
    _id TEXT PRIMARY KEY,
    case_id TEXT DEFAULT '',
    error_type TEXT DEFAULT '',
    message TEXT DEFAULT '',
    source TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fin_errors_case_id ON fin_errors (case_id);
CREATE INDEX IF NOT EXISTS idx_fin_errors_created_at ON fin_errors (created_at DESC);
"""


_MIGRATIONS_SQL = """
ALTER TABLE fin_cases ADD COLUMN IF NOT EXISTS onedrive_web_url TEXT DEFAULT '';
ALTER TABLE fin_cases ADD COLUMN IF NOT EXISTS europace_case_id TEXT DEFAULT '';
ALTER TABLE fin_cases ADD COLUMN IF NOT EXISTS europace_response JSONB DEFAULT '{}'::jsonb;
ALTER TABLE fin_cases ADD COLUMN IF NOT EXISTS final_payload_preview TEXT DEFAULT '';
ALTER TABLE fin_cases ADD COLUMN IF NOT EXISTS partner_name TEXT DEFAULT '';
ALTER TABLE fin_documents ADD COLUMN IF NOT EXISTS gdrive_file_id TEXT DEFAULT '';
ALTER TABLE processed_emails ADD COLUMN IF NOT EXISTS body_html TEXT DEFAULT '';
ALTER TABLE fin_cases ADD COLUMN IF NOT EXISTS finlink_lead_id TEXT DEFAULT '';
ALTER TABLE email_test_log ADD COLUMN IF NOT EXISTS case_id TEXT DEFAULT '';
"""


def _seed_partners(conn):
    """Seed initial allowlist partners if table is empty."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fin_partners")
        if cur.fetchone()[0] > 0:
            return
        INITIAL = [
            ("l.safi@muniqre.com", "MuniqRe"),
            ("a.sergejcuk@invenio-finance.de", "Invenio Finance"),
            ("nicholas.traupe@wohnwerte-deutschland.de", "Wohnwerte Deutschland"),
            ("info@ldp.group", "LDP Group"),
            ("maged@ldp.group", "LDP Group"),
            ("gero.schanze@proper-union.de", "Proper Union"),
            ("pierre.ibanda@proper-api.de", "Proper API"),
            ("kontakt@wgkonzepte.de", "WG Konzepte"),
            ("t.mesletzky@mf-gmbh.immo", "MF GmbH"),
            ("f.mouth@mf-gmbh.immo", "MF GmbH"),
            ("oliver.volz@newego-re.de", "Newego RE"),
            ("info@cdl-immobilien.de", "CDL Immobilien"),
            ("l.schaut@expats-invest.de", "Expats Invest"),
        ]
        for email, name in INITIAL:
            cur.execute(
                "INSERT INTO fin_partners (_id, email, name) VALUES (%s, %s, %s)",
                (_new_id(), email, name),
            )
        logger.info(f"Seeded {len(INITIAL)} initial partners into fin_partners")


def _init_tables():
    """Create all tables if they don't exist, then run migrations."""
    pool = _get_pool.__wrapped__() if hasattr(_get_pool, '__wrapped__') else None
    # We already have _pool set at this point
    conn = _pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
            cur.execute(_MIGRATIONS_SQL)
        _seed_partners(conn)
        conn.commit()
        logger.info("PostgreSQL tables initialized")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to init tables: {e}")
        raise
    finally:
        _pool.putconn(conn)


# ──────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────
def _new_id() -> str:
    return str(uuid.uuid4())


def _prepare_value(table_name: str, column: str, value):
    """Auto-parse JSON strings for JSONB columns."""
    jsonb_cols = JSONB_COLUMNS.get(table_name, set())
    if column in jsonb_cols and isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


def _row_to_dict(cursor, row) -> dict:
    """Convert a DB row to a dict using cursor.description."""
    if row is None:
        return None
    columns = [desc[0] for desc in cursor.description]
    d = {}
    for col, val in zip(columns, row):
        # Convert JSONB back to the same format SeaTable returns
        # (case_logic._parse_json_field handles both dicts and strings)
        d[col] = val
    return d


def _quote_col(col: str) -> str:
    """Quote column names that need it (camelCase, reserved words)."""
    reserved = {"to", "from", "order", "group", "select", "where", "limit", "offset", "user"}
    if col != col.lower() or col in reserved:
        return f'"{col}"'
    return col


# ──────────────────────────────────────────
# Public API (same interface as db_seatable)
# ──────────────────────────────────────────

def invalidate_token():
    """No-op for Postgres (compatibility with SeaTable backend)."""
    pass


def list_rows(table_name: str, view_name: str = "Default View") -> list[dict]:
    """Load all rows from a table."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name} ORDER BY created_at DESC")
                rows = cur.fetchall()
                return [_row_to_dict(cur, r) for r in rows]
    except Exception as e:
        logger.error(f"PG list_rows({table_name}) failed: {e}")
        return []


def search_rows(table_name: str, column: str, value: str) -> list[dict]:
    """Search rows by exact column match (single SQL query, no full scan)."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                sql = f"SELECT * FROM {table_name} WHERE {_quote_col(column)} = %s"
                cur.execute(sql, (value,))
                rows = cur.fetchall()
                return [_row_to_dict(cur, r) for r in rows]
    except Exception as e:
        logger.error(f"PG search_rows({table_name}, {column}={value}) failed: {e}")
        return []


def get_row(table_name: str, row_id: str) -> Optional[dict]:
    """Load a single row by _id."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name} WHERE _id = %s", (row_id,))
                row = cur.fetchone()
                return _row_to_dict(cur, row)
    except Exception as e:
        logger.error(f"PG get_row({table_name}, {row_id}) failed: {e}")
        return None


def create_row(table_name: str, row_data: dict) -> dict:
    """Insert a single row. Returns {"inserted_rows": 1}."""
    row_id = _new_id()
    data = {"_id": row_id}
    for k, v in row_data.items():
        data[k] = _prepare_value(table_name, k, v)

    columns = list(data.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(_quote_col(c) for c in columns)

    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                values = [psycopg2.extras.Json(v) if isinstance(v, (dict, list)) else v for v in data.values()]
                cur.execute(
                    f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
                    values,
                )
        return {"inserted_rows": 1, "_id": row_id}
    except Exception as e:
        logger.error(f"PG create_row({table_name}) failed: {e}")
        raise


def update_row(table_name: str, row_id: str, row_data: dict) -> dict:
    """Update a row by _id."""
    if not row_data:
        return {"updated_rows": 0}

    prepared = {}
    for k, v in row_data.items():
        prepared[k] = _prepare_value(table_name, k, v)

    set_clauses = []
    values = []
    for col, val in prepared.items():
        set_clauses.append(f"{_quote_col(col)} = %s")
        values.append(psycopg2.extras.Json(val) if isinstance(val, (dict, list)) else val)
    values.append(row_id)

    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE _id = %s",
                    values,
                )
        return {"updated_rows": 1}
    except Exception as e:
        logger.error(f"PG update_row({table_name}, {row_id}) failed: {e}")
        raise


def delete_rows(table_name: str, column: str, value) -> dict:
    """Delete rows where column matches value."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {table_name} WHERE {_quote_col(column)} = %s",
                    (value,),
                )
                deleted = cur.rowcount
        return {"deleted_rows": deleted}
    except Exception as e:
        logger.error(f"PG delete_rows({table_name}, {column}={value}) failed: {e}")
        raise


def update_where(table_name: str, where_col: str, where_val, updates: dict) -> int:
    """UPDATE ... SET ... WHERE column = value. Returns number of affected rows."""
    try:
        set_parts = []
        params = []
        for k, v in updates.items():
            set_parts.append(f"{_quote_col(k)} = %s")
            params.append(v)
        params.append(where_val)
        sql = f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {_quote_col(where_col)} = %s"
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount
    except Exception as e:
        logger.error(f"PG update_where({table_name}) failed: {e}")
        return 0


def batch_create_rows(table_name: str, rows: list[dict]) -> dict:
    """Insert multiple rows in a single transaction."""
    if not rows:
        return {"inserted_rows": 0}

    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                for row_data in rows:
                    row_id = _new_id()
                    data = {"_id": row_id}
                    for k, v in row_data.items():
                        data[k] = _prepare_value(table_name, k, v)

                    columns = list(data.keys())
                    placeholders = ", ".join(["%s"] * len(columns))
                    col_names = ", ".join(_quote_col(c) for c in columns)
                    values = [psycopg2.extras.Json(v) if isinstance(v, (dict, list)) else v for v in data.values()]

                    cur.execute(
                        f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
                        values,
                    )
        return {"inserted_rows": len(rows)}
    except Exception as e:
        logger.error(f"PG batch_create_rows({table_name}) failed: {e}")
        raise


def get_columns(table_name: str) -> list[dict]:
    """Return column metadata from information_schema."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                return [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"PG get_columns({table_name}) failed: {e}")
        return []


def ensure_columns(table_name: str, required_columns: list[dict]) -> dict:
    """No-op for Postgres – tables are created with all columns at init."""
    existing = get_columns(table_name)
    existing_names = {c["name"] for c in existing}
    return {
        "table": table_name,
        "created": [],
        "already_existed": [c["column_name"] for c in required_columns if c["column_name"] in existing_names],
        "note": "Postgres tables are auto-created at startup",
    }


# ──────────────────────────────────────────
# Helper functions (same as db_seatable)
# ──────────────────────────────────────────

def is_email_processed(provider_message_id: str) -> bool:
    """Check if email was already fully processed (ignores stale locks)."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM processed_emails WHERE provider_message_id = %s AND processing_result != 'lock' LIMIT 1",
                    (provider_message_id,),
                )
                return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"PG is_email_processed failed: {e}")
        return False


def try_lock_email(provider_message_id: str) -> bool:
    """Atomically lock an email for processing. Returns True if lock acquired, False if already exists."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO processed_emails (_id, provider_message_id, processing_result)
                       VALUES (%s, %s, 'lock')
                       ON CONFLICT (provider_message_id) DO NOTHING""",
                    (_new_id(), provider_message_id),
                )
                return cur.rowcount > 0
    except Exception as e:
        logger.error(f"PG try_lock_email failed: {e}")
        return False


def log_processed_email(
    provider_message_id: str,
    intent: str,
    action: str,
    case_id: str = None,
    from_email: str = None,
    subject: str = None,
    conversation_id: str = None,
    attachments_count: int = 0,
    attachments_hashes: list = None,
    body_text: str = None,
    body_html: str = None,
    parsed_result: dict = None,
    matched_by: str = None,
):
    """Log email as processed. Upserts on provider_message_id. Errors are non-fatal."""
    try:
        hashes = json.dumps(attachments_hashes or [])
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO processed_emails
                        (_id, provider_message_id, mail_type, processing_result, case_id,
                         from_email, subject, conversation_id, body_text, body_html,
                         parsed_result, matched_by, processed_at,
                         attachments_count, attachments_hashes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (provider_message_id) DO UPDATE SET
                        mail_type = EXCLUDED.mail_type,
                        processing_result = EXCLUDED.processing_result,
                        case_id = EXCLUDED.case_id,
                        from_email = EXCLUDED.from_email,
                        subject = EXCLUDED.subject,
                        conversation_id = EXCLUDED.conversation_id,
                        body_text = EXCLUDED.body_text,
                        body_html = EXCLUDED.body_html,
                        parsed_result = EXCLUDED.parsed_result,
                        matched_by = EXCLUDED.matched_by,
                        processed_at = EXCLUDED.processed_at,
                        attachments_count = EXCLUDED.attachments_count,
                        attachments_hashes = EXCLUDED.attachments_hashes
                """, (
                    _new_id(), provider_message_id, intent, action,
                    case_id or "", from_email or "", subject or "",
                    conversation_id or "", (body_text or "")[:5000],
                    (body_html or "")[:50000],
                    psycopg2.extras.Json(parsed_result or {}),
                    matched_by or "", now,
                    attachments_count or 0, psycopg2.extras.Json(json.loads(hashes)),
                ))
    except Exception as e:
        logger.error(f"log_processed_email failed (non-fatal): {e}")


# ──────────────────────────────────────────
# Efficient queries for dashboard
# ──────────────────────────────────────────

def count_rows(table_name: str) -> int:
    """Schnelles COUNT(*) statt SELECT *."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"PG count_rows({table_name}) failed: {e}")
        return 0


def count_grouped(table_name: str, column: str) -> dict[str, int]:
    """GROUP BY COUNT für eine Spalte. Gibt {value: count} zurück."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                col = _quote_col(column)
                cur.execute(f"SELECT {col}, COUNT(*) FROM {table_name} GROUP BY {col}")
                return {row[0] or "": row[1] for row in cur.fetchall()}
    except Exception as e:
        logger.error(f"PG count_grouped({table_name}, {column}) failed: {e}")
        return {}


def query_rows(table_name: str, columns: list[str], where: str = None,
               where_params: tuple = None, order_by: str = "created_at DESC",
               limit: int = None) -> list[dict]:
    """Flexible query mit spezifischen Spalten statt SELECT *."""
    try:
        cols = ", ".join(_quote_col(c) for c in columns)
        sql = f"SELECT {cols} FROM {table_name}"
        params = []
        if where:
            sql += f" WHERE {where}"
            if where_params:
                params.extend(where_params)
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT %s"
            params.append(limit)

        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or None)
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"PG query_rows({table_name}) failed: {e}")
        return []


# ──────────────────────────────────────────
# Error Logging
# ──────────────────────────────────────────

def log_error(error_type: str, message: str, source: str = "", case_id: str = ""):
    """Log an error to fin_errors. Non-fatal: never raises."""
    try:
        create_row("fin_errors", {
            "case_id": case_id,
            "error_type": error_type,
            "message": str(message)[:2000],
            "source": source,
        })
    except Exception:
        pass  # Error-Logging darf nie crashen


def count_recent_errors(hours: int = 24) -> int:
    """Count errors in the last N hours."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM fin_errors WHERE created_at > NOW() - make_interval(hours => %s)",
                    (hours,),
                )
                return cur.fetchone()[0]
    except Exception:
        return 0


# ──────────────────────────────────────────
# SeaTable compat stubs (for debug endpoints)
# ──────────────────────────────────────────

def _get_access_token():
    return "postgres-backend"


def _get_uuid():
    return "postgres"


def _api(path: str) -> str:
    return f"postgres://local/{path}"
