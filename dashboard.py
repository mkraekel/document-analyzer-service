"""
Admin Dashboard API
Endpoints fuer Triage, Case-Management, Overrides
"""

import json
import logging
import os
import traceback
import httpx
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

import db_postgres as db
import case_logic as cases
import readiness as rdns
from readiness import _compute_effective_view, _find_value, KEY_SEARCH_PATHS
import notify

N8N_SCAN_WEBHOOK = os.getenv("N8N_SCAN_WEBHOOK", "")
N8N_SETUP_CASE_WEBHOOK = os.getenv("N8N_SETUP_CASE_WEBHOOK", "")
N8N_WEBHOOK_API_KEY = os.getenv("N8N_WEBHOOK_API_KEY", "")

logger = logging.getLogger(__name__)
router = APIRouter()

# ── OpenAI Credits Cache ─────────────────────────────────────────
_openai_credits_cache: dict = {"data": None, "fetched_at": None}
_CREDITS_CACHE_TTL = 3600  # 1 hour

def _n8n_headers() -> dict:
    """Returns auth headers for n8n webhook calls."""
    if N8N_WEBHOOK_API_KEY:
        return {"X-API-Key": N8N_WEBHOOK_API_KEY}
    return {}

# Interne Domains die NICHT als partner_email verwendet werden sollen
_INTERNAL_DOMAINS = {"alexander-heil.com"}


def _safe_partner_email(extracted_email: str | None, fallback_email: str) -> str:
    """Gibt eine sichere partner_email zurueck. Interne Adressen werden NICHT verwendet."""
    for email in [extracted_email, fallback_email]:
        if email and "@" in email:
            domain = email.rsplit("@", 1)[1].lower()
            if domain not in _INTERNAL_DOMAINS:
                return email
    return ""


# ──────────────────────────────────────────
# Dashboard Page
# ──────────────────────────────────────────

# ──────────────────────────────────────────
# API: Stats
# ──────────────────────────────────────────

@router.get("/api/dashboard/stats")
async def dashboard_stats():
    try:
        # Effiziente COUNT-Queries statt SELECT * auf alle Tabellen
        cases_by_status = db.count_grouped("fin_cases", "status")
        emails_by_result = db.count_grouped("processed_emails", "processing_result")
        docs_total = db.count_rows("fin_documents")

        cases_total = sum(cases_by_status.values())
        emails_total = sum(emails_by_result.values())

        return {
            "cases_total": cases_total,
            "cases_by_status": cases_by_status,
            "emails_total": emails_total,
            "emails_by_result": emails_by_result,
            "documents_total": docs_total,
            "triage_count": (
                emails_by_result.get("no_case_match", 0)
                + emails_by_result.get("triage", 0)
                + emails_by_result.get("irrelevant", 0)
            ),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: OpenAI Credits
# ──────────────────────────────────────────

async def _fetch_openai_credits() -> dict:
    """Fetch current billing info from OpenAI API. Cached for 1 hour."""
    import time
    from datetime import date

    now = time.time()
    if (_openai_credits_cache["data"] is not None
            and _openai_credits_cache["fetched_at"]
            and now - _openai_credits_cache["fetched_at"] < _CREDITS_CACHE_TTL):
        return _openai_credits_cache["data"]

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        return {"error": "OPENAI_API_KEY not set"}

    headers = {"Authorization": f"Bearer {api_key}"}
    result = {}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # 1. Subscription → plan + hard limit
            sub_resp = await client.get(
                "https://api.openai.com/v1/dashboard/billing/subscription",
                headers=headers,
            )
            if sub_resp.status_code == 200:
                sub = sub_resp.json()
                result["hard_limit_usd"] = sub.get("hard_limit_usd")
                result["plan"] = sub.get("plan", {}).get("title") if isinstance(sub.get("plan"), dict) else sub.get("plan")
            else:
                logger.warning(f"OpenAI subscription endpoint returned {sub_resp.status_code}: {sub_resp.text[:200]}")

            # 2. Credit grants → remaining credits
            credits_resp = await client.get(
                "https://api.openai.com/v1/dashboard/billing/credit_grants",
                headers=headers,
            )
            if credits_resp.status_code == 200:
                cg = credits_resp.json()
                result["total_granted"] = cg.get("total_granted")
                result["total_used"] = cg.get("total_used")
                result["total_available"] = cg.get("total_available")
            else:
                logger.warning(f"OpenAI credit_grants endpoint returned {credits_resp.status_code}: {credits_resp.text[:200]}")

            # 3. Usage this month
            today = date.today()
            start = today.replace(day=1).isoformat()
            usage_resp = await client.get(
                f"https://api.openai.com/v1/dashboard/billing/usage?start_date={start}&end_date={today.isoformat()}",
                headers=headers,
            )
            if usage_resp.status_code == 200:
                usage = usage_resp.json()
                # total_usage is in cents
                result["used_usd"] = round(usage.get("total_usage", 0) / 100, 2)
            else:
                logger.warning(f"OpenAI usage endpoint returned {usage_resp.status_code}: {usage_resp.text[:200]}")

        result["fetched_at"] = datetime.utcnow().isoformat()
        _openai_credits_cache["data"] = result
        _openai_credits_cache["fetched_at"] = now
        logger.info(f"OpenAI credits fetched: {result}")
    except Exception as e:
        logger.error(f"OpenAI credits fetch failed: {e}")
        result["error"] = str(e)

    return result


@router.get("/api/dashboard/openai-credits")
async def dashboard_openai_credits():
    return await _fetch_openai_credits()


# ──────────────────────────────────────────
# API: Triage Queue
# ──────────────────────────────────────────

@router.get("/api/dashboard/triage")
async def dashboard_triage():
    try:
        # Nur benötigte Spalten laden
        triage_cols = [
            "provider_message_id", "from_email", "subject", "body_text",
            "conversation_id", "parsed_result", "matched_by",
            "processed_at", "attachments_count",
        ]
        rows = db.query_rows(
            "processed_emails", triage_cols,
            where="processing_result IN (%s, %s, %s)",
            where_params=("no_case_match", "triage", "irrelevant"),
            order_by="created_at DESC",
            limit=200,
        )
        items = []
        for e in rows:
            parsed = e.get("parsed_result", {})
            if isinstance(parsed, str):
                try:
                    parsed = json.loads(parsed)
                except Exception:
                    parsed = {}
            items.append({
                "provider_message_id": e.get("provider_message_id"),
                "from_email": e.get("from_email"),
                "subject": e.get("subject"),
                "body_text": (e.get("body_text") or "")[:500],
                "conversation_id": e.get("conversation_id"),
                "parsed_result": parsed,
                "matched_by": e.get("matched_by", ""),
                "processed_at": e.get("processed_at"),
                "attachments_count": e.get("attachments_count", 0),
            })
        return {"triage": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Cases List
# ──────────────────────────────────────────

@router.get("/api/dashboard/cases")
async def dashboard_cases():
    try:
        # Nur benötigte Spalten statt SELECT * mit allen JSONB-Feldern
        case_cols = [
            "case_id", "applicant_name", "partner_email", "status",
            "onedrive_folder_id", "last_status_change", "readiness",
        ]
        rows = db.query_rows("fin_cases", case_cols, order_by="created_at DESC")
        items = []
        for c in rows:
            readiness = c.get("readiness", {})
            if isinstance(readiness, str):
                try:
                    readiness = json.loads(readiness)
                except Exception:
                    readiness = {}
            items.append({
                "case_id": c.get("case_id"),
                "applicant_name": c.get("applicant_name"),
                "partner_email": c.get("partner_email"),
                "status": c.get("status"),
                "onedrive_folder_id": c.get("onedrive_folder_id", ""),
                "last_status_change": c.get("last_status_change"),
                "missing_financing": readiness.get("missing_financing", []),
                "missing_applicant_data": readiness.get("missing_applicant_data", []),
                "missing_docs_count": len(readiness.get("missing_docs", [])),
                "total_docs_required": len(readiness.get("missing_docs", [])),
                "overrides_applied": readiness.get("manual_overrides_applied", []),
                "is_complete": readiness.get("is_complete", False),
                "completeness_pct": readiness.get("completeness_percent", 0),
            })
        return {"cases": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Case Detail
# ──────────────────────────────────────────

@router.get("/api/dashboard/case/{case_id}")
async def dashboard_case_detail(case_id: str):
    try:
        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

        docs = db.search_rows("fin_documents", "caseId", case_id)
        emails = db.search_rows("processed_emails", "case_id", case_id)

        # Parse JSON fields
        facts = case.get("_facts_extracted", {})
        answers = case.get("_answers_user", {})
        overrides = case.get("_manual_overrides", {})
        readiness = case.get("_readiness", {})
        # Always compute fresh effective_view from current facts/answers/overrides
        view = _compute_effective_view(case)
        # Resolve KEY_SEARCH_PATHS so canonical field names are always present
        for key in KEY_SEARCH_PATHS:
            if key not in view or view[key] is None or view[key] == "":
                val = _find_value(view, key)
                if val is not None and val != "":
                    view[key] = val
        readiness["effective_view"] = view
        audit = case.get("_audit_log", [])
        conv_ids = case.get("_conversation_ids", [])

        case_web_url = case.get("onedrive_web_url", "")
        doc_list = []
        for d in docs:
            extracted = d.get("extracted_data", {})
            if isinstance(extracted, str):
                try:
                    extracted = json.loads(extracted)
                except Exception:
                    extracted = {}
            file_name = d.get("file_name", "")
            # Strip legacy "gdrive:" prefix for clean display
            if file_name.startswith("gdrive:"):
                file_name = file_name[7:]
            gdrive_id = d.get("gdrive_file_id", "")
            onedrive_id = d.get("onedrive_file_id", "")
            # OneDrive-Link: Ordner-URL + Dateiname (Business/SharePoint)
            onedrive_url = ""
            if onedrive_id and case_web_url:
                import urllib.parse
                encoded_name = urllib.parse.quote(file_name)
                onedrive_url = f"{case_web_url}/{encoded_name}"
            doc_list.append({
                "_id": d.get("_id", ""),
                "file_name": file_name,
                "doc_type": d.get("doc_type"),
                "processing_status": d.get("processing_status"),
                "processed_at": d.get("processed_at"),
                "extracted_fields": list(extracted.keys()) if isinstance(extracted, dict) else [],
                "extracted_data": extracted or {},
                "gdrive_file_id": gdrive_id,
                "gdrive_url": f"https://drive.google.com/file/d/{gdrive_id}/view" if gdrive_id else "",
                "onedrive_url": onedrive_url,
            })

        email_list = []
        for e in emails:
            parsed = e.get("parsed_result") or {}
            if isinstance(parsed, str):
                try:
                    parsed = json.loads(parsed)
                except Exception:
                    parsed = {}
            email_list.append({
                "subject": e.get("subject"),
                "from_email": e.get("from_email"),
                "mail_type": e.get("mail_type"),
                "processing_result": e.get("processing_result"),
                "processed_at": e.get("processed_at"),
                "matched_by": e.get("matched_by", ""),
                "body_text": e.get("body_text", ""),
                "body_html": e.get("body_html", ""),
                "parsed_result": parsed,
            })

        # Google Drive + Investagon Links aus E-Mails sammeln (unique)
        gdrive_links_set = set()
        investagon_links_set = set()
        for e in emails:
            parsed_e = e.get("parsed_result") or {}
            if isinstance(parsed_e, str):
                try:
                    parsed_e = json.loads(parsed_e)
                except Exception:
                    parsed_e = {}
            for link in parsed_e.get("google_drive_links", []):
                if link:
                    gdrive_links_set.add(link)
            for link in parsed_e.get("investagon_links", []):
                if link:
                    investagon_links_set.add(link)
        gdrive_links_list = sorted(gdrive_links_set)
        investagon_links_list = sorted(investagon_links_set)

        # Europace-Felder
        europace_response = case.get("_europace_response", {})

        return {
            "case_id": case_id,
            "applicant_name": case.get("applicant_name"),
            "partner_email": case.get("partner_email"),
            "status": case.get("status"),
            "onedrive_folder_id": case.get("onedrive_folder_id", ""),
            "onedrive_web_url": case.get("onedrive_web_url", ""),
            "google_drive_links": gdrive_links_list,
            "investagon_links": investagon_links_list,
            "last_status_change": case.get("last_status_change"),
            "europace_case_id": case.get("europace_case_id", ""),
            "finlink_lead_id": case.get("finlink_lead_id", ""),
            "europace_response": europace_response,
            "conversation_ids": conv_ids,
            "facts_extracted": facts,
            "answers_user": answers,
            "manual_overrides": overrides,
            "readiness": readiness,
            "audit_log": audit[-20:],  # letzte 20
            "documents": doc_list,
            "emails": email_list,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Assign Triage Email to Case
# ──────────────────────────────────────────

class AssignRequest(BaseModel):
    provider_message_id: str
    case_id: str

@router.post("/api/dashboard/assign")
async def dashboard_assign(req: AssignRequest):
    try:
        # 1. Triage-E-Mail laden
        emails = db.search_rows("processed_emails", "provider_message_id", req.provider_message_id)
        if not emails:
            raise HTTPException(status_code=404, detail="E-Mail nicht gefunden")
        email = emails[0]

        # 2. Case pruefen
        case = cases.load_case(req.case_id)
        if not case:
            raise HTTPException(status_code=404, detail=f"Case {req.case_id} nicht gefunden")

        # 3. Conversation-ID zum Case hinzufuegen
        conv_id = email.get("conversation_id")
        if conv_id:
            cases.update_case_conversation(req.case_id, conv_id)

        # 4. Parsed-Daten als Answers/Facts mergen
        parsed = email.get("parsed_result", {})
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                parsed = {}

        answers = {}
        for key in ["purchase_price", "loan_amount", "equity_to_use", "object_type", "usage"]:
            if parsed.get(key) is not None:
                answers[key] = parsed[key]
        if answers:
            cases.save_answers(req.case_id, answers, actor="partner")

        extracted = parsed.get("extracted_answers", {})
        if extracted:
            cases.save_answers(req.case_id, {}, actor="broker", overrides=extracted)

        # 5. Processed-Email updaten
        db.update_row("processed_emails", email["_id"], {
            "processing_result": "assigned",
            "case_id": req.case_id,
        })

        # 6. Readiness recheck
        result = rdns.check_readiness(req.case_id)

        # 7. Setup triggern wenn Case keinen OneDrive-Ordner hat oder E-Mail Attachments hatte
        if N8N_SETUP_CASE_WEBHOOK:
            needs_setup = not case.get("onedrive_folder_id")
            has_attachments = email.get("attachments_count", 0) > 0
            if needs_setup or has_attachments:
                import asyncio
                asyncio.create_task(_trigger_setup_case(
                    case_id=req.case_id,
                    outlook_message_id=req.provider_message_id,
                    applicant_name=case.get("applicant_name", ""),
                ))

        return {"success": True, "case_id": req.case_id, "status": result.get("status")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Assign failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Set Override on Case
# ──────────────────────────────────────────

class OverrideRequest(BaseModel):
    key: str
    value: Optional[str] = "true"

@router.post("/api/dashboard/case/{case_id}/override")
async def dashboard_override(case_id: str, req: OverrideRequest):
    try:
        # Parse value
        val = req.value
        if val.lower() in ("true", "1", "yes"):
            val = True
        elif val.lower() in ("false", "0", "no"):
            val = False

        cases.save_answers(case_id, {}, actor="broker", overrides={req.key: val})
        result = rdns.check_readiness(case_id)
        return {"success": True, "case_id": case_id, "status": result.get("status")}
    except Exception as e:
        logger.error(f"Override failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Quick Actions
# ──────────────────────────────────────────

class ActionRequest(BaseModel):
    action: str  # FREIGABE, WAIT_FOR_DOCS, RECHECK, REANALYZE

@router.post("/api/dashboard/case/{case_id}/action")
async def dashboard_action(case_id: str, req: ActionRequest):
    try:
        action = req.action.upper().strip()

        if action == "FREIGABE":
            cases.save_answers(case_id, {}, actor="broker", overrides={"APPROVE_IMPORT": True})
        elif action == "WAIT_FOR_DOCS":
            cases.save_answers(case_id, {}, actor="broker", overrides={"WAIT_FOR_DOCS": True})
        elif action == "RECHECK":
            # Scan Google Drive for new files in background before readiness check
            gdrive_links = _collect_gdrive_links(case_id)
            if gdrive_links:
                import asyncio
                asyncio.create_task(_scan_gdrive_and_recheck(case_id, gdrive_links))
        elif action == "REANALYZE":
            import asyncio
            asyncio.create_task(_do_reanalyze(case_id))
            return {"success": True, "case_id": case_id, "message": "Neuanalyse gestartet (läuft im Hintergrund)"}
        elif action == "DECLINE":
            cases.update_status(case_id, "DECLINED")
            return {"success": True, "case_id": case_id, "status": "DECLINED"}
        elif action == "ARCHIVE":
            cases.update_status(case_id, "ARCHIVED")
            return {"success": True, "case_id": case_id, "status": "ARCHIVED"}
        else:
            raise HTTPException(status_code=400, detail=f"Unbekannte Aktion: {action}")

        result = rdns.check_readiness(case_id)
        return {"success": True, "case_id": case_id, "status": result.get("status")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Action failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


async def _scan_gdrive_and_recheck(case_id: str, links: list):
    """Background: sync GDrive → OneDrive, trigger n8n scan, then recheck."""
    import asyncio
    try:
        _case = cases.load_case(case_id)
        folder_id = _case.get("onedrive_folder_id", "") if _case else ""
        if not folder_id:
            logger.warning(f"[{case_id}] No OneDrive folder — GDrive RECHECK skipped")
            return

        import gdrive
        sync_result = await asyncio.to_thread(
            gdrive.sync_to_onedrive,
            case_id=case_id, links=links, onedrive_folder_id=folder_id,
        )
        uploaded = sync_result.get("files_uploaded", 0)
        logger.info(f"[{case_id}] GDrive sync on RECHECK: {uploaded} uploaded")

        # Trigger n8n scan to analyze any new files
        if uploaded > 0 and N8N_SCAN_WEBHOOK:
            async with httpx.AsyncClient(timeout=300) as client:
                resp = await client.post(N8N_SCAN_WEBHOOK, headers=_n8n_headers(), json={
                    "case_id": case_id,
                    "onedrive_folder_id": folder_id,
                    "force_reanalyze": False,
                })
                resp.raise_for_status()

        # Remap facts after GDrive scan
        from document_processor import DocumentProcessor
        await asyncio.to_thread(DocumentProcessor.remap_facts, case_id)

        readiness_result = await asyncio.to_thread(rdns.check_readiness, case_id)
        await asyncio.to_thread(notify.dispatch_notifications, case_id, readiness_result)
    except Exception as e:
        logger.error(f"[{case_id}] GDrive scan on RECHECK failed: {e}")


# ──────────────────────────────────────────
# API: Dismiss Triage Email
# ──────────────────────────────────────────

class DismissRequest(BaseModel):
    provider_message_id: str

@router.post("/api/dashboard/dismiss")
async def dashboard_dismiss(req: DismissRequest):
    try:
        emails = db.search_rows("processed_emails", "provider_message_id", req.provider_message_id)
        if not emails:
            raise HTTPException(status_code=404, detail="E-Mail nicht gefunden")
        db.update_row("processed_emails", emails[0]["_id"], {
            "processing_result": "dismissed",
        })
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Create Case from Triage
# ──────────────────────────────────────────

class CreateCaseFromTriageRequest(BaseModel):
    provider_message_id: str
    applicant_name: str
    partner_email: Optional[str] = ""

@router.post("/api/dashboard/create-case")
async def dashboard_create_case(req: CreateCaseFromTriageRequest):
    try:
        import time
        import asyncio

        # 1. E-Mail laden
        emails = db.search_rows("processed_emails", "provider_message_id", req.provider_message_id)
        if not emails:
            raise HTTPException(status_code=404, detail="E-Mail nicht gefunden")
        email = emails[0]

        # 2. Case anlegen
        case_id = f"CASE-{int(time.time() * 1000)}"
        conv_id = email.get("conversation_id", "")

        parsed = email.get("parsed_result", {})
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                parsed = {}

        facts = {}
        for key in ["purchase_price", "loan_amount", "equity_to_use", "object_type", "usage"]:
            if parsed.get(key) is not None:
                facts[key] = parsed[key]
        prop = parsed.get("property_data", {})
        fin = parsed.get("financing_data", {})
        if prop:
            facts["property_data"] = prop
        if fin:
            facts["financing_data"] = fin

        cases.create_case(
            case_id=case_id,
            applicant_name=req.applicant_name,
            partner_email=_safe_partner_email(req.partner_email, email.get("from_email", "")),
            partner_phone="",
            conversation_id=conv_id,
            facts=facts,
            partner_name=email.get("parsed_result", {}).get("sender_first_name") or email.get("from_name", ""),
        )

        # 3. E-Mail zuordnen
        db.update_row("processed_emails", email["_id"], {
            "processing_result": "assigned",
            "case_id": case_id,
        })

        # 4. Readiness check
        result = rdns.check_readiness(case_id)

        # 5. n8n Setup-Case Webhook triggern (OneDrive-Ordner + Attachments re-analysieren)
        #    Läuft async im Hintergrund – Dashboard wartet nicht darauf
        if N8N_SETUP_CASE_WEBHOOK:
            asyncio.create_task(_trigger_setup_case(
                case_id=case_id,
                outlook_message_id=req.provider_message_id,
                applicant_name=req.applicant_name,
            ))

        return {"success": True, "case_id": case_id, "status": result.get("status")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create case failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


async def _trigger_setup_case(case_id: str, outlook_message_id: str, applicant_name: str = ""):
    """Triggert n8n Setup-Case Webhook im Hintergrund (OneDrive + Attachment-Analyse)."""
    try:
        async with httpx.AsyncClient(timeout=180) as client:
            resp = await client.post(N8N_SETUP_CASE_WEBHOOK, headers=_n8n_headers(), json={
                "case_id": case_id,
                "outlook_message_id": outlook_message_id,
                "applicant_name": applicant_name,
            })
            resp.raise_for_status()
            result = resp.json()
            logger.info(f"Setup-case webhook OK for {case_id}: {result.get('message', '')}")
    except Exception as e:
        logger.error(f"Setup-case webhook failed for {case_id}: {e}")


# ──────────────────────────────────────────
# API: Update Case Fields (Facts/Answers)
# ──────────────────────────────────────────

def _unflatten_key(dotted_key: str, value) -> dict:
    """Convert 'a.b.c' + val into {'a': {'b': {'c': val}}}"""
    parts = dotted_key.split(".")
    result = {}
    current = result
    for part in parts[:-1]:
        current[part] = {}
        current = current[part]
    current[parts[-1]] = value
    return result

class UpdateFieldRequest(BaseModel):
    field: str       # z.B. "purchase_price", "applicant_name", "object_type"
    value: str       # Wert als String (wird ggf. zu Zahl konvertiert)
    target: str = "answers"  # "answers", "facts", "case" (top-level fields)

@router.post("/api/dashboard/case/{case_id}/update-field")
async def dashboard_update_field(case_id: str, req: UpdateFieldRequest):
    try:
        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

        # Wert konvertieren
        val = req.value
        try:
            val = float(val)
            if val == int(val):
                val = int(val)
        except (ValueError, TypeError):
            pass

        if req.target == "case":
            # Top-level Felder (applicant_name, partner_email)
            allowed = {"applicant_name", "partner_email"}
            if req.field not in allowed:
                raise HTTPException(status_code=400, detail=f"Feld {req.field} nicht erlaubt")
            db.update_row("fin_cases", case["_id"], {req.field: str(val)})
        elif req.target == "facts":
            # Unflatten dotted keys: "applicant_data.employment_type" -> {"applicant_data": {"employment_type": val}}
            facts_dict = _unflatten_key(req.field, val)
            cases.save_facts(case_id, facts_dict, source="dashboard")
        else:
            override_dict = _unflatten_key(req.field, val)
            cases.save_answers(case_id, override_dict, actor="broker")

        result = rdns.check_readiness(case_id)
        return {"success": True, "case_id": case_id, "status": result.get("status")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update field failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Check if OneDrive file is already processed
# ──────────────────────────────────────────

class CheckFileRequest(BaseModel):
    case_id: str
    onedrive_file_id: str
    force_reanalyze: bool = False

@router.post("/api/dashboard/check-file-processed")
async def check_file_processed(req: CheckFileRequest):
    """Prüft ob eine OneDrive-Datei bereits analysiert wurde."""
    try:
        if req.force_reanalyze:
            return {"already_processed": False}
        docs = db.search_rows("fin_documents", "caseId", req.case_id)
        for d in docs:
            if d.get("onedrive_file_id") == req.onedrive_file_id:
                return {"already_processed": True, "doc_type": d.get("doc_type")}
        return {"already_processed": False}
    except Exception as e:
        logger.error(f"check-file-processed failed: {e}")
        return {"already_processed": False}


# ──────────────────────────────────────────
# API: Google Drive Processing
# ──────────────────────────────────────────

class GDriveRequest(BaseModel):
    google_drive_links: Optional[list] = None  # Optional: override links from case
    force_reanalyze: bool = False

@router.post("/api/dashboard/case/{case_id}/process-gdrive")
async def dashboard_process_gdrive(case_id: str, req: GDriveRequest = None):
    """Downloads and analyzes files from Google Drive links for a case."""
    try:
        import gdrive

        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

        # Bei force_reanalyze: facts leeren
        if req and req.force_reanalyze:
            _clear_facts_for_reanalyze(case)

        # Use provided links or look in case audit_log/emails for stored links
        links = (req.google_drive_links if req and req.google_drive_links else None)

        if not links:
            # Try to find google_drive_links from processed emails
            emails = db.search_rows("processed_emails", "case_id", case_id)
            for email in emails:
                parsed = email.get("parsed_result") or {}
                if isinstance(parsed, str):
                    try:
                        parsed = json.loads(parsed)
                    except Exception:
                        parsed = {}
                email_links = parsed.get("google_drive_links", [])
                if email_links:
                    links = email_links
                    break

        if not links:
            raise HTTPException(
                status_code=400,
                detail="Keine Google Drive Links gefunden. Bitte Links manuell angeben.",
            )

        import asyncio
        # Fire-and-forget: GDrive-Analyse im Hintergrund
        asyncio.create_task(_run_gdrive_background(case_id, links))

        return {
            "success": True,
            "case_id": case_id,
            "message": "Google Drive Analyse gestartet (läuft im Hintergrund)",
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Google Drive processing failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# Reanalyze Helpers
# ──────────────────────────────────────────

def _clear_facts_for_reanalyze(case: dict):
    """Leert facts_extracted und löscht Dokument-Records für Neuanalyse."""
    case_id = case.get("case_id")
    # 1. facts_extracted leeren (merge_facts füllt nur leere Slots → muss leer sein)
    audit = case.get("_audit_log", [])
    audit.append({"event": "reanalyze_started", "ts": datetime.utcnow().isoformat(), "source": "dashboard"})
    audit = audit[-100:]
    db.update_row("fin_cases", case["_id"], {
        "facts_extracted": json.dumps({}),
        "audit_log": json.dumps(audit),
    })
    # 2. Alle Dokument-Records löschen (werden bei Neuanalyse neu erstellt)
    result = db.delete_rows("fin_documents", "caseId", case_id)
    logger.info(f"Reanalyze: cleared facts + deleted {result.get('deleted_rows', 0)} docs for {case_id}")


async def _do_reanalyze(case_id: str):
    """Neuanalyse: 1) GDrive → OneDrive sync, 2) OneDrive-Scan analysiert alles. Läuft als Background-Task."""
    import asyncio

    case = cases.load_case(case_id)
    if not case:
        logger.error(f"Reanalyze: Case {case_id} nicht gefunden")
        return

    _clear_facts_for_reanalyze(case)

    folder_id = case.get("onedrive_folder_id")
    results = {}

    # 1. Sync Google Drive → OneDrive (upload only, no analysis)
    gdrive_links = _collect_gdrive_links(case_id)
    if gdrive_links and folder_id:
        try:
            import gdrive
            sync_result = await asyncio.to_thread(
                gdrive.sync_to_onedrive,
                case_id=case_id, links=gdrive_links, onedrive_folder_id=folder_id,
            )
            results["gdrive_uploaded"] = sync_result.get("files_uploaded", 0)
            results["gdrive_skipped"] = sync_result.get("files_skipped", 0)
            logger.info(f"Reanalyze GDrive sync: {sync_result}")
        except Exception as e:
            logger.error(f"Reanalyze GDrive sync failed: {e}")
            results["gdrive_error"] = str(e)

    # 2. OneDrive Scan → analysiert alle Dateien (GDrive + bestehende)
    if folder_id and N8N_SCAN_WEBHOOK:
        try:
            async with httpx.AsyncClient(timeout=300) as client:
                resp = await client.post(N8N_SCAN_WEBHOOK, headers=_n8n_headers(), json={
                    "case_id": case_id,
                    "onedrive_folder_id": folder_id,
                    "force_reanalyze": True,
                })
                resp.raise_for_status()
                scan_result = resp.json()
                results["onedrive_scanned"] = scan_result.get("scanned", 0)
        except Exception as e:
            logger.error(f"Reanalyze OneDrive scan failed: {e}")
            results["onedrive_error"] = str(e)

    # 3. Remap facts with full couple knowledge
    try:
        from document_processor import DocumentProcessor
        remap_result = DocumentProcessor.remap_facts(case_id)
        results["remap"] = remap_result
        logger.info(f"Reanalyze remap for {case_id}: {remap_result}")
    except Exception as e:
        logger.error(f"Remap after reanalyze failed for {case_id}: {e}")

    # 4. Readiness Check
    try:
        readiness_result = rdns.check_readiness(case_id)
        results["status"] = readiness_result.get("status")
    except Exception as e:
        logger.error(f"Readiness after reanalyze failed: {e}")

    results["success"] = True
    results["case_id"] = case_id
    return results


def _collect_gdrive_links(case_id: str) -> list[str]:
    """Sammelt alle Google Drive Links aus den E-Mails eines Cases."""
    emails = db.search_rows("processed_emails", "case_id", case_id)
    links = []
    for email in emails:
        parsed = email.get("parsed_result") or {}
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                parsed = {}
        links.extend(parsed.get("google_drive_links", []))
    return links


async def _run_scan_background(case_id: str, folder_id: str, force: bool):
    """GDrive → OneDrive sync + n8n OneDrive-Scan im Hintergrund."""
    import asyncio
    scanned = 0

    # 1. Sync new GDrive files to OneDrive first
    gdrive_links = _collect_gdrive_links(case_id)
    if gdrive_links and folder_id:
        try:
            import gdrive
            sync_result = await asyncio.to_thread(
                gdrive.sync_to_onedrive,
                case_id=case_id, links=gdrive_links, onedrive_folder_id=folder_id,
            )
            logger.info(f"Scan background GDrive sync for {case_id}: {sync_result.get('files_uploaded', 0)} uploaded")
        except Exception as e:
            logger.error(f"Scan background GDrive sync failed for {case_id}: {e}")

    # 2. OneDrive Scan → analyses all files (including freshly synced GDrive files)
    try:
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(N8N_SCAN_WEBHOOK, headers=_n8n_headers(), json={
                "case_id": case_id,
                "onedrive_folder_id": folder_id,
                "force_reanalyze": force,
            })
            resp.raise_for_status()
            result = resp.json()
        scanned = result.get("scanned", 0)
        logger.info(f"Scan background OneDrive done for {case_id}: {scanned} scanned")
    except Exception as e:
        logger.error(f"Scan background OneDrive failed for {case_id}: {e}")

    # 3. Remap facts (couple detection) after scan
    try:
        from document_processor import DocumentProcessor
        remap_result = await asyncio.to_thread(DocumentProcessor.remap_facts, case_id)
        logger.info(f"Scan background remap for {case_id}: couple={remap_result.get('is_couple')}")
    except Exception as e:
        logger.error(f"Remap after scan failed for {case_id}: {e}")

    # 4. Readiness + Notifications
    try:
        readiness_result = await asyncio.to_thread(rdns.check_readiness, case_id)
        if scanned > 0:
            await asyncio.to_thread(notify.dispatch_notifications, case_id, readiness_result, True)
    except Exception as e:
        logger.error(f"Readiness after scan failed for {case_id}: {e}")


async def _run_gdrive_background(case_id: str, links: list):
    """GDrive → OneDrive sync im Hintergrund, dann OneDrive-Scan."""
    import asyncio
    try:
        import case_logic as _cases
        _case = _cases.load_case(case_id)
        folder_id = _case.get("onedrive_folder_id", "") if _case else ""

        if not folder_id:
            # Warte auf n8n Setup Case
            import time
            await asyncio.to_thread(time.sleep, 15)
            _case = _cases.load_case(case_id)
            folder_id = _case.get("onedrive_folder_id", "") if _case else ""

        if folder_id:
            import gdrive
            sync_result = await asyncio.to_thread(
                gdrive.sync_to_onedrive,
                case_id=case_id, links=links, onedrive_folder_id=folder_id,
            )
            uploaded = sync_result.get("files_uploaded", 0)
            logger.info(f"GDrive background sync for {case_id}: {uploaded} uploaded")

            # Trigger OneDrive scan to analyze the uploaded files
            if uploaded > 0 and N8N_SCAN_WEBHOOK:
                async with httpx.AsyncClient(timeout=300) as client:
                    resp = await client.post(N8N_SCAN_WEBHOOK, headers=_n8n_headers(), json={
                        "case_id": case_id,
                        "onedrive_folder_id": folder_id,
                        "force_reanalyze": False,
                    })
                    resp.raise_for_status()
                    logger.info(f"GDrive background scan triggered for {case_id}")

                # Remap facts after GDrive scan
                from document_processor import DocumentProcessor
                await asyncio.to_thread(DocumentProcessor.remap_facts, case_id)

                readiness_result = await asyncio.to_thread(rdns.check_readiness, case_id)
                await asyncio.to_thread(notify.dispatch_notifications, case_id, readiness_result, True)
        else:
            logger.warning(f"[{case_id}] No OneDrive folder — GDrive sync skipped")
    except Exception as e:
        logger.error(f"GDrive background failed for {case_id}: {e}")


# ──────────────────────────────────────────
# API: Scan Documents (triggers n8n OneDrive scan)
# ──────────────────────────────────────────

class ScanRequest(BaseModel):
    force_reanalyze: bool = False

@router.post("/api/dashboard/case/{case_id}/scan-documents")
async def dashboard_scan_documents(case_id: str, req: ScanRequest = None):
    """Triggert n8n Webhook zum Scannen des OneDrive-Ordners."""
    try:
        force = req.force_reanalyze if req else False
        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

        folder_id = case.get("onedrive_folder_id")
        if not folder_id:
            raise HTTPException(status_code=400, detail="Kein OneDrive-Ordner vorhanden")

        if not N8N_SCAN_WEBHOOK:
            raise HTTPException(status_code=503, detail="N8N_SCAN_WEBHOOK nicht konfiguriert")

        # Bei force_reanalyze: facts_extracted leeren damit merge_facts frisch startet
        if force:
            _clear_facts_for_reanalyze(case)

        import asyncio
        # Fire-and-forget: n8n Scan im Hintergrund
        asyncio.create_task(_run_scan_background(case_id, folder_id, force))

        return {"success": True, "case_id": case_id, "message": "Scan gestartet (läuft im Hintergrund)"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Scan documents failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Update Document Type
# ──────────────────────────────────────────

class UpdateDocTypeRequest(BaseModel):
    doc_id: str
    new_doc_type: str

@router.post("/api/dashboard/case/{case_id}/update-doc-type")
async def dashboard_update_doc_type(case_id: str, req: UpdateDocTypeRequest):
    """Ändert den Dokumenttyp eines bereits analysierten Dokuments."""
    try:
        db.update_row("fin_documents", req.doc_id, {"doc_type": req.new_doc_type})

        # Facts neu mergen da sich der Dokumenttyp geändert hat
        import readiness as rdns
        result = rdns.check_readiness(case_id)

        return {
            "success": True,
            "case_id": case_id,
            "doc_id": req.doc_id,
            "new_doc_type": req.new_doc_type,
            "status": result.get("status"),
        }
    except Exception as e:
        logger.error(f"Update doc type failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Outgoing Emails (Notifications Log)
# ──────────────────────────────────────────

@router.get("/api/dashboard/outgoing-emails")
async def dashboard_outgoing_emails(case_id: Optional[str] = None):
    try:
        email_cols = ["to", "subject", "body_text", "body_html", "logged_at", "dry_run", "case_id"]
        if case_id:
            rows = db.query_rows(
                "email_test_log", email_cols,
                where="case_id = %s",
                where_params=(case_id,),
                order_by="created_at DESC",
                limit=100,
            )
        else:
            rows = db.query_rows(
                "email_test_log", email_cols,
                order_by="created_at DESC",
                limit=100,
            )
        items = []
        for e in rows:
            items.append({
                "to": e.get("to"),
                "subject": e.get("subject", ""),
                "body_text": (e.get("body_text") or "")[:500],
                "body_html": (e.get("body_html") or "")[:1000],
                "logged_at": e.get("logged_at"),
                "dry_run": e.get("dry_run", True),
                "case_id": e.get("case_id", ""),
            })
        return {"emails": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/dashboard/outgoing-emails")
async def dashboard_clear_outgoing_emails():
    """Alle dry-run Einträge in email_test_log löschen."""
    try:
        result = db.delete_rows("email_test_log", "dry_run", True)
        return {"deleted": result.get("deleted_rows", 0)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Test-Mail (Dry-Run Notification)
# ──────────────────────────────────────────

@router.post("/api/dashboard/case/{case_id}/test-mail")
async def dashboard_test_mail(case_id: str):
    """Generiert eine Dry-Run Notification unabhängig vom Case-Status."""
    try:
        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

        readiness_result = rdns.check_readiness(case_id)
        notify.dispatch_notifications(case_id, readiness_result, force=True, dry_run_override=True)
        return {"success": True, "status": readiness_result.get("status")}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Import Case to Europace
# ──────────────────────────────────────────

class ImportRequest(BaseModel):
    dry_run: Optional[bool] = False

@router.post("/api/dashboard/case/{case_id}/import")
async def dashboard_import_case(case_id: str, req: ImportRequest = None):
    """Triggert den Europace-Import fuer einen Case aus dem Dashboard."""
    try:
        import import_builder

        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

        # Status pruefen
        if case.get("status") != "READY_FOR_IMPORT":
            raise HTTPException(
                status_code=400,
                detail=f"Case hat Status '{case.get('status')}', erwartet 'READY_FOR_IMPORT'"
            )

        # APPROVE_IMPORT Override pruefen
        overrides = case.get("_manual_overrides", {})
        if not overrides.get("APPROVE_IMPORT"):
            raise HTTPException(
                status_code=400,
                detail="Import nicht freigegeben. Bitte zuerst FREIGABE erteilen."
            )

        dry_run = req.dry_run if req else False
        result = import_builder.execute_import(case_id=case_id, dry_run=dry_run)

        return {
            "success": result["success"],
            "case_id": case_id,
            "europace_case_id": result.get("europace_case_id"),
            "finlink_lead_id": result.get("finlink_lead_id"),
            "errors": result.get("errors", []),
            "warnings": result.get("warnings", []),
            "dry_run": dry_run,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dashboard import failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
