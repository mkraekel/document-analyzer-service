"""
Case Management Logic
Erstellen, Laden, Aktualisieren von fin_cases
Inklusive Case Matching Logic aus dem n8n Mail Gateway
"""

import json
import logging
import time
from datetime import datetime
from typing import Optional
import seatable as db

logger = logging.getLogger(__name__)

# Erlaubte Absender-E-Mails (Allowlist aus Gatekeeper Node)
ALLOWLIST = [
    "l.safi@muniqre.com",
    "a.sergejcuk@invenio-finance.de",
    "nicholas.traupe@wohnwerte-deutschland.de",
    "info@ldp.group",
    "maged@ldp.group",
    "gero.schanze@proper-union.de",
    "pierre.ibanda@proper-api.de",
    "kontakt@wgkonzepte.de",
    "t.mesletzky@mf-gmbh.immo",
    "f.mouth@mf-gmbh.immo",
    "oliver.volz@newego-re.de",
    "info@cdl-immobilien.de",
    "l.schaut@expats-invest.de",
]

OWN_DOMAIN = "@alexander-heil.com"

# Blocklist: Diese Absender werden IMMER geblockt (Systembenachrichtigungen etc.)
BLOCKLIST = [
    "alexanderheil1@alexander-heil.com",
]


def gatekeeper(from_email: str, subject: str, conversation_id: str = None) -> dict:
    """
    Prüft ob E-Mail verarbeitet werden soll.
    Rückgabe: {pass: bool, reason: str, actor: str, is_internal_reply: bool}
    """
    import re
    from_email = (from_email or "").lower().strip()
    subject = subject or ""

    # Blocklist: sofort abweisen
    if from_email in BLOCKLIST:
        return {"pass": False, "reason": "sender_blocklisted", "actor": None, "is_internal_reply": False}

    REPLY_PATTERN = re.compile(r"^(re:|aw:|fwd:|wg:|antw:)", re.IGNORECASE)
    NON_FINANCE = re.compile(
        r"(your receipt|receipt|invoice|rechnung|quittung|railway|booking|buchung|order\s*#|"
        r"newsletter|webinar|statusänderung|abgesagt|nausys|funnelcockpit|"
        r"beitrag.*erhöhung|erhöhung.*beitrag|instagram|login|notification|"
        r"mitteilungs.center|datenraum|angebotsbeziehung|microsoft bookings|"
        r"neue buchung von|terminbestätigung)", re.IGNORECASE
    )
    FINANCE_HINT = re.compile(r"(finanzierung|baufinanz|darlehen|kredit|objekt|kaufpreis|eigenkapital|unterlagen)", re.IGNORECASE)

    # Interne E-Mail
    if from_email.endswith(OWN_DOMAIN):
        is_reply = bool(re.match(r"^(re:|aw:|antw:)", subject, re.IGNORECASE))
        is_forward = bool(re.match(r"^(fwd:|wg:)", subject, re.IGNORECASE))
        has_conv = bool(conversation_id)
        has_finance = bool(FINANCE_HINT.search(subject))

        # Forwards: nur mit Finance-Keyword durchlassen
        if is_forward:
            if has_finance:
                return {"pass": True, "reason": None, "actor": "broker", "is_internal_reply": False}
            return {"pass": False, "reason": "internal_forward_no_finance", "actor": None, "is_internal_reply": False}

        # Replies: durchlassen (könnte Case-bezogene Antwort sein)
        if is_reply or has_conv:
            # Non-Finance Replies trotzdem blocken
            if NON_FINANCE.search(subject) and not has_finance:
                return {"pass": False, "reason": "internal_non_finance", "actor": None, "is_internal_reply": False}
            return {"pass": True, "reason": None, "actor": "broker", "is_internal_reply": True}

        # Neue interne Mail mit Finanz-Keyword aber KEIN Forward/Reply
        # → wahrscheinlich ausgehende Mail an Bankpartner/Geschäftskontakt
        # → Triage statt automatisch Case erstellen
        if has_finance:
            return {"pass": True, "reason": "internal_new_finance", "actor": "broker", "is_internal_reply": False, "force_triage": True}
        return {"pass": False, "reason": "outgoing_system_mail", "actor": None, "is_internal_reply": False}

    # Externe Allowlist
    if from_email not in ALLOWLIST:
        return {"pass": False, "reason": "sender_not_allowlisted", "actor": None, "is_internal_reply": False}

    # Non-Finance Filter
    if NON_FINANCE.search(subject) and not FINANCE_HINT.search(subject):
        return {"pass": False, "reason": "non_finance_subject", "actor": None, "is_internal_reply": False}

    return {"pass": True, "reason": None, "actor": "partner", "is_internal_reply": False}


def _parse_json_field(case: dict, field: str, default=None):
    """Parst JSON-String-Felder sicher. Gibt dict oder list zurück."""
    if default is None:
        default = {}
    val = case.get(field)
    if val is None:
        return default
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str) and val.strip() == "":
        return default
    try:
        return json.loads(val)
    except Exception:
        return default


def load_case(case_id: str) -> Optional[dict]:
    """Lädt einen Case aus SeaTable per case_id"""
    rows = db.search_rows("fin_cases", "case_id", case_id)
    if not rows:
        return None
    case = rows[0]
    # JSON-Felder parsen (dict-Felder → default {}, list-Felder → default [])
    for field in ["facts_extracted", "answers_user", "manual_overrides", "derived_values",
                  "docs_index", "readiness", "actors", "europace_response"]:
        case[f"_{field}"] = _parse_json_field(case, field, default={})
    for field in ["audit_log", "conversation_ids"]:
        case[f"_{field}"] = _parse_json_field(case, field, default=[])
    return case


def get_all_active_cases() -> list[dict]:
    """Alle aktiven Cases laden"""
    all_cases = db.list_rows("fin_cases")
    inactive = {"IMPORTED", "ERROR", "ARCHIVED", "DECLINED"}
    return [c for c in all_cases if c.get("status") not in inactive]


def match_case(
    from_email: str,
    applicant_last_name: str,
    referenced_case_id: str,
    conversation_id: str,
    mail_type: str,
    actor: str,
) -> dict:
    """
    Findet passenden Case oder bestimmt 'create' / 'triage'.
    Rückgabe: {action: 'create'|'update'|'triage', case_id: str|None, matched_by: str}
    """
    all_cases = get_all_active_cases()

    # 1. Explizite CASE-ID in referenced_case_id
    if referenced_case_id:
        for c in all_cases:
            if c.get("case_id") == referenced_case_id:
                return {"action": "update", "case_id": referenced_case_id, "matched_by": "referenced_case_id"}

    # 2. Conversation-ID Match
    if conversation_id:
        for c in all_cases:
            conv_ids = _parse_json_field(c, "conversation_ids")
            ids = conv_ids if isinstance(conv_ids, list) else []
            if conversation_id in ids:
                return {"action": "update", "case_id": c["case_id"], "matched_by": "conversation_id"}

    # 3. Partner E-Mail + Nachname Match
    if from_email and applicant_last_name:
        ln = applicant_last_name.lower().strip()
        for c in all_cases:
            case_email = (c.get("partner_email") or "").lower().strip()
            case_name = (c.get("applicant_name") or "").lower()
            if case_email == from_email and ln and ln in case_name:
                return {"action": "update", "case_id": c["case_id"], "matched_by": "email_name"}

    # 4. Neuer Vorgang
    if mail_type == "new_request":
        new_id = f"CASE-{int(time.time() * 1000)}"
        return {"action": "create", "case_id": new_id, "matched_by": "new"}

    # 5. Broker-Reply ohne Match → Triage
    if actor == "broker":
        return {"action": "triage", "case_id": None, "matched_by": "no_match_broker"}

    return {"action": "triage", "case_id": None, "matched_by": "no_match"}


def create_case(
    case_id: str,
    applicant_name: str,
    partner_email: str,
    partner_phone: str,
    conversation_id: str,
    facts: dict,
    source: str = "email",
    partner_name: str = "",
) -> dict:
    """Erstellt neuen Case in SeaTable"""
    now = datetime.utcnow().isoformat()
    row = {
        "case_id": case_id,
        "applicant_name": applicant_name or "",
        "partner_email": partner_email or "",
        "partner_name": partner_name or "",
        "status": "INTAKE",
        "sources": source,
        "facts_extracted": json.dumps(facts or {}),
        "answers_user": json.dumps({}),
        "manual_overrides": json.dumps({}),
        "derived_values": json.dumps({}),
        "docs_index": json.dumps({}),
        "conversation_ids": json.dumps([conversation_id] if conversation_id else []),
        "audit_log": json.dumps([{"event": "case_created", "ts": now, "source": source}]),
        "last_status_change": now,
    }
    result = db.create_row("fin_cases", row)
    logger.info(f"Case created: {case_id}")
    return result


def update_case_conversation(case_id: str, conversation_id: str):
    """Fügt conversation_id zum bestehenden Case hinzu"""
    case = load_case(case_id)
    if not case:
        return
    conv_ids = case.get("_conversation_ids", [])
    if isinstance(conv_ids, dict):
        conv_ids = []
    if conversation_id and conversation_id not in conv_ids:
        conv_ids.append(conversation_id)
        db.update_row("fin_cases", case["_id"], {
            "conversation_ids": json.dumps(conv_ids)
        })


_JUNK_VALUES = {"N/A", "n/a", "N.A.", "n.a.", "nicht verfügbar", "unbekannt", "-", "–", "k.A.", "k. A.", "null", "None", "none"}


def _is_junk(val) -> bool:
    """Prüft ob ein Wert ein Platzhalter/Junk ist der wie null behandelt werden soll."""
    return isinstance(val, str) and val.strip() in _JUNK_VALUES


def merge_facts(existing: dict, new_facts: dict) -> dict:
    """
    Deep Merge: Last-write-wins — neuere Werte überschreiben ältere.
    Für Objekte wird rekursiv gemergt.
    Junk-Werte (N/A, -, etc.) und leere Werte werden ignoriert.
    """
    result = dict(existing)
    for key, val in new_facts.items():
        if _is_junk(val):
            continue
        if val is None or val == "":
            continue
        existing_val = result.get(key)
        if isinstance(val, dict) and isinstance(existing_val, dict):
            result[key] = merge_facts(existing_val, val)
        else:
            result[key] = val
    return result


def save_facts(case_id: str, new_facts: dict, source: str = "document") -> dict:
    """Mergt neue Facts mit bestehenden und speichert"""
    case = load_case(case_id)
    if not case:
        raise ValueError(f"Case nicht gefunden: {case_id}")

    existing = case.get("_facts_extracted", {})
    merged = merge_facts(existing, new_facts)

    audit = case.get("_audit_log", [])
    audit.append({"event": "facts_updated", "ts": datetime.utcnow().isoformat(), "source": source})
    audit = audit[-100:]  # max 100 Einträge

    db.update_row("fin_cases", case["_id"], {
        "facts_extracted": json.dumps(merged),
        "audit_log": json.dumps(audit),
    })
    return merged


def save_answers(case_id: str, answers: dict, actor: str = "partner", overrides: dict = None) -> dict:
    """Speichert Antworten in answers_user oder manual_overrides"""
    case = load_case(case_id)
    if not case:
        raise ValueError(f"Case nicht gefunden: {case_id}")

    existing_answers = case.get("_answers_user", {})
    existing_overrides = case.get("_manual_overrides", {})

    # Antworten mergen (actor-spezifisch)
    if actor == "broker":
        actor_answers = existing_answers.get("broker", {})
        actor_answers.update(answers)
        existing_answers["broker"] = actor_answers
        # Broker Overrides separat
        if overrides:
            existing_overrides.update(overrides)
    else:
        actor_answers = existing_answers.get("partner", {})
        actor_answers.update(answers)
        existing_answers["partner"] = actor_answers

    audit = case.get("_audit_log", [])
    audit.append({
        "event": "answers_updated",
        "ts": datetime.utcnow().isoformat(),
        "actor": actor,
        "keys": list(answers.keys()),
    })
    audit = audit[-100:]

    update_data = {
        "answers_user": json.dumps(existing_answers),
        "audit_log": json.dumps(audit),
    }
    if overrides:
        update_data["manual_overrides"] = json.dumps(existing_overrides)

    db.update_row("fin_cases", case["_id"], update_data)
    return existing_answers


def update_status(case_id: str, status: str, readiness: dict = None, _cached_case: dict = None):
    """Status + Readiness Result speichern. _cached_case vermeidet doppelten DB-Load."""
    case = _cached_case or load_case(case_id)
    if not case:
        raise ValueError(f"Case nicht gefunden: {case_id}")

    now = datetime.utcnow().isoformat()
    update_data = {
        "status": status,
        "last_status_change": now,
    }
    if readiness:
        update_data["readiness"] = json.dumps(readiness)

    audit = case.get("_audit_log", [])
    audit.append({"event": "status_changed", "ts": now, "status": status})
    audit = audit[-100:]
    update_data["audit_log"] = json.dumps(audit)

    db.update_row("fin_cases", case["_id"], update_data)


def update_onedrive_folder(case_id: str, folder_id: str, web_url: str = None):
    """OneDrive Folder ID + optional Web URL speichern"""
    rows = db.search_rows("fin_cases", "case_id", case_id)
    if not rows:
        return
    update = {"onedrive_folder_id": folder_id}
    if web_url:
        update["onedrive_web_url"] = web_url
    db.update_row("fin_cases", rows[0]["_id"], update)


def build_docs_index(case_id: str) -> dict:
    """Dokumente aus fin_documents laden und nach Typ indexieren.
    Dedupliziert nach Dateiname (behält neueste Version)."""
    docs = db.search_rows("fin_documents", "caseId", case_id)

    # Deduplizieren: pro Dateiname nur den neuesten Eintrag behalten
    seen_filenames = {}  # filename → doc (mit neuestem processed_at)
    for doc in docs:
        if doc.get("processing_status") != "completed":
            continue
        fname = doc.get("file_name", "")
        existing = seen_filenames.get(fname)
        if existing:
            # Behalte den neueren
            if (doc.get("processed_at") or "") > (existing.get("processed_at") or ""):
                seen_filenames[fname] = doc
        else:
            seen_filenames[fname] = doc

    index = {}
    for doc in seen_filenames.values():
        doc_type = doc.get("doc_type", "Sonstiges")
        if doc_type not in index:
            index[doc_type] = []
        extracted = doc.get("extracted_data")
        if isinstance(extracted, str):
            try:
                extracted = json.loads(extracted)
            except Exception:
                extracted = {}
        index[doc_type].append({
            "filename": doc.get("file_name"),
            "analyzed_at": doc.get("processed_at"),
            "extracted": extracted,
        })
    # Neueste zuerst
    for doc_type in index:
        index[doc_type].sort(key=lambda x: x.get("analyzed_at") or "", reverse=True)
    return index
