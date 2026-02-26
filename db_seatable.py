"""
SeaTable API Client
Neue API Gateway v2 (SeaTable >= 5.3)
Direkte REST API Anbindung für fin_cases, fin_documents, processed_emails, fin_followups
"""

import os
import logging
import time
import requests
from typing import Optional

logger = logging.getLogger(__name__)

SEATABLE_BASE_URL = os.getenv("SEATABLE_BASE_URL", "https://cloud.seatable.io")
SEATABLE_API_TOKEN = os.getenv("SEATABLE_API_TOKEN", "")
SEATABLE_BASE_UUID = os.getenv("SEATABLE_BASE_UUID", "")

_TOKEN_TTL = 50 * 60  # 50 Minuten (Token läuft nach ~1h ab)
_access_token_cache = {"token": None, "dtable_uuid": None, "expires_at": 0}


def _get_access_token() -> str:
    """Holt JWT Access Token von SeaTable (gecacht, TTL 50min)"""
    if _access_token_cache["token"] and time.time() < _access_token_cache["expires_at"]:
        return _access_token_cache["token"]

    if not SEATABLE_API_TOKEN:
        raise RuntimeError("SEATABLE_API_TOKEN ist nicht gesetzt!")

    resp = requests.get(
        f"{SEATABLE_BASE_URL}/api/v2.1/dtable/app-access-token/",
        headers={"Authorization": f"Bearer {SEATABLE_API_TOKEN}"},
        timeout=10,
    )
    if not resp.ok:
        raise RuntimeError(f"SeaTable Auth failed: {resp.status_code} – {resp.text[:200]}")
    data = resp.json()
    token = data["access_token"]
    uuid = data.get("dtable_uuid", SEATABLE_BASE_UUID)
    _access_token_cache["token"] = token
    _access_token_cache["dtable_uuid"] = uuid
    _access_token_cache["expires_at"] = time.time() + _TOKEN_TTL
    logger.info(f"SeaTable auth ok: uuid={uuid}")
    return token


def _get_uuid() -> str:
    _get_access_token()
    return _access_token_cache["dtable_uuid"] or SEATABLE_BASE_UUID


def _headers() -> dict:
    return {"Authorization": f"Bearer {_get_access_token()}", "Content-Type": "application/json"}


def _api(path: str) -> str:
    """Baut API Gateway v2 URL (SeaTable >= 5.3)"""
    uuid = _get_uuid()
    return f"{SEATABLE_BASE_URL}/api-gateway/api/v2/dtables/{uuid}/{path}"


def invalidate_token():
    _access_token_cache["token"] = None
    _access_token_cache["expires_at"] = 0


def list_rows(table_name: str, view_name: str = "Default View") -> list[dict]:
    """Alle Zeilen einer Tabelle laden (mit Spaltennamen statt Keys)"""
    try:
        resp = requests.get(
            _api("rows/"),
            headers=_headers(),
            params={"table_name": table_name, "view_name": view_name, "convert_keys": "true"},
            timeout=30,
        )
        if resp.status_code == 401:
            invalidate_token()
            resp = requests.get(
                _api("rows/"),
                headers=_headers(),
                params={"table_name": table_name, "view_name": view_name, "convert_keys": "true"},
                timeout=30,
            )
        resp.raise_for_status()
        return resp.json().get("rows", [])
    except Exception as e:
        logger.error(f"SeaTable list_rows({table_name}) failed: {e}")
        return []


def search_rows(table_name: str, column: str, value: str) -> list[dict]:
    """Zeilen nach Spalte/Wert suchen"""
    all_rows = list_rows(table_name)
    return [r for r in all_rows if str(r.get(column, "")) == str(value)]


def get_row(table_name: str, row_id: str) -> Optional[dict]:
    """Einzelne Zeile per ID laden"""
    try:
        resp = requests.get(
            _api(f"rows/{row_id}/"),
            headers=_headers(),
            params={"table_name": table_name, "convert_keys": "true"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"SeaTable get_row({table_name}, {row_id}) failed: {e}")
        return None


def create_row(table_name: str, row_data: dict) -> dict:
    """Neue Zeile erstellen (v2 API: rows Array)"""
    try:
        resp = requests.post(
            _api("rows/"),
            headers=_headers(),
            json={"table_name": table_name, "rows": [row_data]},
            timeout=15,
        )
        if resp.status_code == 401:
            invalidate_token()
            resp = requests.post(
                _api("rows/"),
                headers=_headers(),
                json={"table_name": table_name, "rows": [row_data]},
                timeout=15,
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"SeaTable create_row({table_name}) failed: {e}")
        raise


def update_row(table_name: str, row_id: str, row_data: dict) -> dict:
    """Zeile aktualisieren"""
    try:
        resp = requests.put(
            _api("rows/"),
            headers=_headers(),
            json={"table_name": table_name, "row_id": row_id, "row": row_data},
            timeout=15,
        )
        if resp.status_code == 401:
            invalidate_token()
            resp = requests.put(
                _api("rows/"),
                headers=_headers(),
                json={"table_name": table_name, "row_id": row_id, "row": row_data},
                timeout=15,
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"SeaTable update_row({table_name}, {row_id}) failed: {e}")
        raise


def batch_create_rows(table_name: str, rows: list[dict]) -> dict:
    """Mehrere Zeilen auf einmal erstellen"""
    try:
        resp = requests.post(
            _api("rows/"),
            headers=_headers(),
            json={"table_name": table_name, "rows": rows},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"SeaTable batch_create_rows({table_name}) failed: {e}")
        raise


def get_columns(table_name: str) -> list[dict]:
    """Gibt alle Spalten einer Tabelle zurück"""
    try:
        token = _get_access_token()
        uuid = _get_uuid()
        url = f"{SEATABLE_BASE_URL}/api-gateway/api/v2/dtables/{uuid}/metadata/"
        resp = requests.get(url, headers=_headers(), timeout=10)
        resp.raise_for_status()
        tables = resp.json().get("metadata", {}).get("tables", [])
        for t in tables:
            if t["name"] == table_name:
                return t.get("columns", [])
        return []
    except Exception as e:
        logger.error(f"SeaTable get_columns({table_name}) failed: {e}")
        return []


def ensure_columns(table_name: str, required_columns: list[dict]) -> dict:
    """
    Stellt sicher dass alle Spalten existieren. Fehlende werden angelegt.
    required_columns: [{"column_name": "xyz", "column_type": "text"}, ...]

    SeaTable column_type: text, long-text, number, checkbox, date, single-select, ...
    """
    existing = get_columns(table_name)
    existing_names = {c["name"] for c in existing}
    created = []
    skipped = []

    for col in required_columns:
        name = col["column_name"]
        if name in existing_names:
            skipped.append(name)
            continue
        try:
            uuid = _get_uuid()
            url = f"{SEATABLE_BASE_URL}/api-gateway/api/v2/dtables/{uuid}/columns/"
            resp = requests.post(
                url,
                headers=_headers(),
                json={"table_name": table_name, "column_name": name, "column_type": col.get("column_type", "text")},
                timeout=10,
            )
            if resp.status_code == 401:
                invalidate_token()
                resp = requests.post(
                    url,
                    headers=_headers(),
                    json={"table_name": table_name, "column_name": name, "column_type": col.get("column_type", "text")},
                    timeout=10,
                )
            resp.raise_for_status()
            created.append(name)
            logger.info(f"SeaTable column created: {table_name}.{name}")
        except Exception as e:
            logger.error(f"SeaTable ensure_columns: failed to create {table_name}.{name}: {e}")

    return {"table": table_name, "created": created, "already_existed": skipped}


def is_email_processed(provider_message_id: str) -> bool:
    """Prüft ob E-Mail bereits verarbeitet wurde"""
    rows = list_rows("processed_emails")
    for r in rows:
        if r.get("provider_message_id") == provider_message_id:
            return True
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
):
    """E-Mail als verarbeitet markieren. Fehler werden geloggt aber nicht geworfen."""
    from datetime import datetime
    import json
    try:
        create_row("processed_emails", {
            "provider_message_id": provider_message_id,
            "mail_type": intent,
            "processing_result": action,
            "case_id": case_id or "",
            "from_email": from_email or "",
            "subject": subject or "",
            "conversation_id": conversation_id or "",
            "processed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            "attachments_count": attachments_count or 0,
            "attachments_hashes": json.dumps(attachments_hashes or []),
        })
    except Exception as e:
        logger.error(f"log_processed_email failed (non-fatal): {e}")
