"""
SeaTable API Client
Direkte REST API Anbindung für fin_cases, fin_documents, processed_emails, fin_followups
"""

import os
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

SEATABLE_BASE_URL = os.getenv("SEATABLE_BASE_URL", "https://cloud.seatable.io")
SEATABLE_API_TOKEN = os.getenv("SEATABLE_API_TOKEN", "")
SEATABLE_BASE_UUID = os.getenv("SEATABLE_BASE_UUID", "")

_access_token_cache = {"token": None, "dtable_uuid": None}


def _get_access_token() -> str:
    """Holt JWT Access Token von SeaTable (gecacht)"""
    if _access_token_cache["token"]:
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
    resp.raise_for_status()
    data = resp.json()
    token = data["access_token"]
    uuid = data.get("dtable_uuid", SEATABLE_BASE_UUID)
    _access_token_cache["token"] = token
    _access_token_cache["dtable_uuid"] = uuid
    logger.debug(f"SeaTable access token refreshed, uuid={uuid}")
    return token


def _get_uuid() -> str:
    _get_access_token()
    return _access_token_cache["dtable_uuid"] or SEATABLE_BASE_UUID


def _headers() -> dict:
    return {"Authorization": f"Bearer {_get_access_token()}", "Content-Type": "application/json"}


def _api(path: str) -> str:
    uuid = _get_uuid()
    return f"{SEATABLE_BASE_URL}/dtable-server/api/v1/dtables/{uuid}/{path}"


def invalidate_token():
    _access_token_cache["token"] = None


def list_rows(table_name: str, view_name: str = "Default View") -> list[dict]:
    """Alle Zeilen einer Tabelle laden"""
    try:
        resp = requests.get(
            _api("rows/"),
            headers=_headers(),
            params={"table_name": table_name, "view_name": view_name},
            timeout=30,
        )
        if resp.status_code == 401:
            invalidate_token()
            resp = requests.get(
                _api("rows/"),
                headers=_headers(),
                params={"table_name": table_name, "view_name": view_name},
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
            params={"table_name": table_name},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"SeaTable get_row({table_name}, {row_id}) failed: {e}")
        return None


def create_row(table_name: str, row_data: dict) -> dict:
    """Neue Zeile erstellen"""
    try:
        resp = requests.post(
            _api("rows/"),
            headers=_headers(),
            json={"table_name": table_name, "row": row_data},
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


def is_email_processed(provider_message_id: str) -> bool:
    """Prüft ob E-Mail bereits verarbeitet wurde"""
    rows = list_rows("processed_emails")
    for r in rows:
        if r.get("provider_message_id") == provider_message_id:
            return True
    return False


def log_processed_email(provider_message_id: str, intent: str, action: str, case_id: str = None):
    """E-Mail als verarbeitet markieren"""
    from datetime import datetime
    create_row("processed_emails", {
        "provider_message_id": provider_message_id,
        "intent": intent,
        "action": action,
        "case_id": case_id or "",
        "processed_at": datetime.utcnow().isoformat(),
    })
