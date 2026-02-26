"""
Readiness Check Logic
Prüft ob ein Case vollständig ist und bestimmt den nächsten Status.
Portiert aus dem n8n Readiness Router.
"""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Optional
import case_logic as cases

logger = logging.getLogger(__name__)

# ============================================================
# PFLICHTFELDER (Finanzierungsdaten)
# ============================================================
REQUIRED_FINANCING_KEYS = ["purchase_price", "loan_amount", "equity_to_use", "object_type", "usage"]

KEY_SEARCH_PATHS = {
    "purchase_price": ["purchase_price", "property_data.purchase_price", "financing_data.purchase_price"],
    "loan_amount": ["loan_amount", "financing_data.loan_amount"],
    "equity_to_use": ["equity_to_use", "financing_data.equity_to_use"],
    "object_type": ["object_type", "property_data.object_type"],
    "usage": ["usage", "property_data.usage"],
}

BROKER_REQUIRED = ["partnerId"]

# ============================================================
# DOKUMENT-ANFORDERUNGEN
# ============================================================
DOCS_REQUIRED_ALWAYS = {
    "Selbstauskunft": {"count": 1, "max_age_days": None},
    "Ausweiskopie": {"count": 1, "max_age_days": None, "warn_expiry_days": 90},
    "Eigenkapitalnachweis": {"count": 1, "max_age_days": 30},
    "Renteninfo": {"count": 1, "max_age_days": None},
}

DOCS_REQUIRED_EMPLOYED = {
    "Gehaltsnachweis": {"count": 3, "max_age_days": 90},
    "Kontoauszug": {"count": 3, "max_age_days": 90},
    "Steuerbescheid": {"count": 1, "max_age_days": None},
    "Steuererklärung": {"count": 1, "max_age_days": None},
    "Lohnsteuerbescheinigung": {"count": 1, "max_age_days": None, "alternative": "Gehaltsabrechnung Dezember"},
}

DOCS_REQUIRED_SELF_EMPLOYED = {
    "BWA": {"count": 3, "max_age_days": None},
    "Summen und Saldenliste": {"count": 3, "max_age_days": None},
    "Jahresabschluss": {"count": 3, "max_age_days": None},
    "Steuerbescheid": {"count": 2, "max_age_days": None},
    "Steuererklärung": {"count": 2, "max_age_days": None},
    "Kontoauszug": {"count": 3, "max_age_days": 90},
    "Nachweis Krankenversicherung": {"count": 1, "max_age_days": None},
}

DOCS_REQUIRED_PROPERTY = {
    "Exposé": {"count": 1, "max_age_days": None},
    "Objektbild Innen": {"count": 1, "max_age_days": None},
    "Objektbild Außen": {"count": 1, "max_age_days": None},
    "Baubeschreibung": {"count": 1, "max_age_days": None},
    "Grundbuch": {"count": 1, "max_age_days": 90},
    "Wohnflächenberechnung": {"count": 1, "max_age_days": None},
    "Grundriss": {"count": 1, "max_age_days": None},
    "Energieausweis": {"count": 1, "max_age_days": None},
}


def _get_nested(obj: dict, path: str):
    """Tiefensuche per Punkt-Pfad: 'property_data.purchase_price'"""
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _compute_effective_view(case: dict) -> dict:
    """
    Baut effektive Sicht: manual_overrides > answers_user > facts_extracted > derived_values
    Flacht answers_user (partner + broker) in eine Ebene.
    """
    derived = case.get("_derived_values", {})
    facts = case.get("_facts_extracted", {})
    answers_raw = case.get("_answers_user", {})
    overrides = case.get("_manual_overrides", {})

    # answers_user: partner dann broker (broker hat Priorität)
    answers_flat = {}
    for actor in ["partner", "broker"]:
        actor_data = answers_raw.get(actor, {})
        if isinstance(actor_data, dict):
            answers_flat.update(actor_data)
    # Flat-Keys direkt (Legacy-Format)
    for k, v in answers_raw.items():
        if k not in ("partner", "broker", "_meta") and not k.startswith("_"):
            answers_flat[k] = v

    view = {}
    for src in [derived, facts, answers_flat, overrides]:
        if isinstance(src, dict):
            view.update({k: v for k, v in src.items() if v is not None and v != ""})
    return view


def _doc_age_ok(doc: dict, max_age_days: Optional[int]) -> bool:
    """Prüft ob Dokument nicht zu alt ist"""
    if not max_age_days:
        return True
    analyzed = doc.get("analyzed_at") or doc.get("meta", {}) and doc.get("meta", {}).get("doc_date")
    if not analyzed:
        return True  # Kein Datum → nicht prüfbar → akzeptieren
    try:
        dt = datetime.fromisoformat(analyzed.replace("Z", "+00:00"))
        return (datetime.now(dt.tzinfo) - dt).days <= max_age_days
    except Exception:
        return True


def _doc_expiry_warn(doc: dict, warn_days: Optional[int]) -> bool:
    """Prüft ob Dokument bald abläuft (z.B. Ausweis)"""
    if not warn_days:
        return False
    extracted = doc.get("extracted") or {}
    expiry_str = extracted.get("Gültig bis") or extracted.get("expiry_date")
    if not expiry_str:
        return False
    try:
        # Deutsches Format: DD.MM.YYYY
        if re.match(r"\d{2}\.\d{2}\.\d{4}", expiry_str):
            parts = expiry_str.split(".")
            expiry = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
        else:
            expiry = datetime.fromisoformat(expiry_str)
        return (expiry - datetime.now()).days <= warn_days
    except Exception:
        return False


def check_readiness(case_id: str) -> dict:
    """
    Vollständige Readiness-Prüfung für einen Case.

    Rückgabe:
    {
        status: str,          # nächster Status
        missing_financing: list,
        missing_broker: list,
        missing_docs: list,
        stale_docs: list,
        warnings: list,
        manual_overrides_applied: list,
        effective_view: dict,  # berechnete Gesamtsicht
    }
    """
    case = cases.load_case(case_id)
    if not case:
        raise ValueError(f"Case nicht gefunden: {case_id}")

    view = _compute_effective_view(case)
    overrides = case.get("_manual_overrides", {})
    docs_index = cases.build_docs_index(case_id)

    # Ergebnisse
    missing_financing = []
    missing_broker = []
    missing_docs = []
    stale_docs = []
    warnings = []
    overrides_applied = []

    # ──────────────────────────────────────────
    # 1. Pflichtfelder Finanzierung
    # ──────────────────────────────────────────
    for key in REQUIRED_FINANCING_KEYS:
        paths = KEY_SEARCH_PATHS.get(key, [key])
        value = None
        for path in paths:
            value = _get_nested(view, path)
            if value is not None and value != "":
                break
        if value is None or value == "":
            missing_financing.append(key)

    # ──────────────────────────────────────────
    # 2. Pflichtfelder Broker
    # ──────────────────────────────────────────
    for key in BROKER_REQUIRED:
        if not view.get(key):
            missing_broker.append(key)

    # ──────────────────────────────────────────
    # 3. Dokument-Checks
    # ──────────────────────────────────────────
    is_couple = bool(view.get("is_couple"))
    employment = str(view.get("employment_type") or view.get("employment_status") or "Angestellter")
    is_self_employed = "selbst" in employment.lower() or "freiberuf" in employment.lower()
    has_joint_account = bool(overrides.get("has_joint_account") or view.get("has_joint_account"))

    person_count = 2 if is_couple else 1

    def check_doc(doc_type: str, req: dict, label: str = None):
        label = label or doc_type
        docs = docs_index.get(doc_type, [])
        alternative = req.get("alternative")

        # Alternative prüfen
        if not docs and alternative:
            docs = docs_index.get(alternative, [])
            if docs:
                doc_type = alternative

        # Override: Accept missing?
        if overrides.get(f"accept_missing_{doc_type.lower().replace(' ', '_')}"):
            overrides_applied.append(f"accept_missing:{doc_type}")
            return

        required_count = req.get("count", 1)
        max_age = req.get("max_age_days")
        warn_days = req.get("warn_expiry_days")

        # Override: Accept stale?
        accept_stale = bool(overrides.get(f"accept_stale_{doc_type.lower().replace(' ', '_')}"))

        fresh_docs = [d for d in docs if accept_stale or _doc_age_ok(d, max_age)]

        if len(fresh_docs) < required_count:
            if len(docs) >= required_count and not accept_stale:
                stale_docs.append({"type": label, "required": required_count, "found": len(docs), "fresh": len(fresh_docs)})
            else:
                missing_docs.append({"type": label, "required": required_count, "found": len(fresh_docs)})
        else:
            # Ablaufdatum-Warnung
            if warn_days:
                for doc in fresh_docs:
                    if _doc_expiry_warn(doc, warn_days):
                        warnings.append(f"{label} läuft bald ab")

    # Immer erforderlich
    for doc_type, req in DOCS_REQUIRED_ALWAYS.items():
        for p in range(person_count):
            check_doc(doc_type, req)

    # Angestellte oder Selbstständige
    if is_self_employed:
        for doc_type, req in DOCS_REQUIRED_SELF_EMPLOYED.items():
            check_doc(doc_type, req)
    else:
        for doc_type, req in DOCS_REQUIRED_EMPLOYED.items():
            # Kontoauszug: Bei Gemeinschaftskonto nur 3x statt 6x
            if doc_type == "Kontoauszug" and is_couple and has_joint_account:
                req = dict(req)
                req["count"] = 3
            check_doc(doc_type, req)

    # Objektdokumente
    for doc_type, req in DOCS_REQUIRED_PROPERTY.items():
        check_doc(doc_type, req)

    # ──────────────────────────────────────────
    # 4. Manual Overrides prüfen
    # ──────────────────────────────────────────
    approve_import = bool(overrides.get("APPROVE_IMPORT"))
    wait_for_docs = bool(overrides.get("WAIT_FOR_DOCS"))

    # ──────────────────────────────────────────
    # 5. Status bestimmen
    # Priorität: APPROVE_IMPORT > WAIT_FOR_DOCS > sonstige Blocker
    # ──────────────────────────────────────────
    if approve_import:
        # Broker-Override: Direkt zum Import, übersteuert alle anderen Checks
        status = "READY_FOR_IMPORT"

    elif wait_for_docs:
        status = "WAITING_FOR_DOCUMENTS"

    elif stale_docs and not any(overrides.get(f"accept_stale_{d['type'].lower().replace(' ', '_')}") for d in stale_docs):
        status = "NEEDS_MANUAL_REVIEW_BROKER"

    elif missing_financing:
        status = "NEEDS_QUESTIONS_PARTNER"

    elif missing_docs:
        status = "NEEDS_QUESTIONS_PARTNER"

    elif missing_broker:
        status = "NEEDS_QUESTIONS_BROKER"

    else:
        status = "AWAITING_BROKER_CONFIRMATION"

    result = {
        "status": status,
        "missing_financing": missing_financing,
        "missing_broker": missing_broker,
        "missing_docs": missing_docs,
        "stale_docs": stale_docs,
        "warnings": warnings,
        "manual_overrides_applied": overrides_applied,
        "effective_view": view,
        "approve_import": approve_import,
        "is_complete": status in ("READY_FOR_IMPORT", "AWAITING_BROKER_CONFIRMATION"),
    }

    # In DB speichern (cached case durchreichen → spart 1 DB call)
    cases.update_status(case_id, status, result, _cached_case=case)
    logger.info(f"Readiness check for {case_id}: {status} | missing_fin={missing_financing} | missing_docs={len(missing_docs)}")

    return result
