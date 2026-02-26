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
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional

import seatable as db
import case_logic as cases
import readiness as rdns
import notify

N8N_SCAN_WEBHOOK = os.getenv("N8N_SCAN_WEBHOOK", "")
N8N_SETUP_CASE_WEBHOOK = os.getenv("N8N_SETUP_CASE_WEBHOOK", "")

logger = logging.getLogger(__name__)
router = APIRouter()

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

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page():
    import os
    html_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


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
            "triage_count": emails_by_result.get("no_case_match", 0),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
            where="processing_result = %s",
            where_params=("no_case_match",),
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
                "missing_docs_count": len(readiness.get("missing_docs", [])),
                "total_docs_required": len(readiness.get("missing_docs", [])),
                "overrides_applied": readiness.get("manual_overrides_applied", []),
                "is_complete": readiness.get("is_complete", False),
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
        audit = case.get("_audit_log", [])
        conv_ids = case.get("_conversation_ids", [])

        doc_list = []
        for d in docs:
            extracted = d.get("extracted_data", {})
            if isinstance(extracted, str):
                try:
                    extracted = json.loads(extracted)
                except Exception:
                    extracted = {}
            doc_list.append({
                "file_name": d.get("file_name"),
                "doc_type": d.get("doc_type"),
                "processing_status": d.get("processing_status"),
                "processed_at": d.get("processed_at"),
                "extracted_fields": list(extracted.keys()) if extracted else [],
            })

        email_list = []
        for e in emails:
            email_list.append({
                "subject": e.get("subject"),
                "from_email": e.get("from_email"),
                "mail_type": e.get("mail_type"),
                "processing_result": e.get("processing_result"),
                "processed_at": e.get("processed_at"),
                "matched_by": e.get("matched_by", ""),
            })

        # Europace-Felder
        europace_response = case.get("_europace_response", {})

        return {
            "case_id": case_id,
            "applicant_name": case.get("applicant_name"),
            "partner_email": case.get("partner_email"),
            "status": case.get("status"),
            "onedrive_folder_id": case.get("onedrive_folder_id", ""),
            "onedrive_web_url": case.get("onedrive_web_url", ""),
            "last_status_change": case.get("last_status_change"),
            "europace_case_id": case.get("europace_case_id", ""),
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
    action: str  # FREIGABE, WAIT_FOR_DOCS, RECHECK

@router.post("/api/dashboard/case/{case_id}/action")
async def dashboard_action(case_id: str, req: ActionRequest):
    try:
        action = req.action.upper().strip()

        if action == "FREIGABE":
            cases.save_answers(case_id, {}, actor="broker", overrides={"APPROVE_IMPORT": True})
        elif action == "WAIT_FOR_DOCS":
            cases.save_answers(case_id, {}, actor="broker", overrides={"WAIT_FOR_DOCS": True})
        elif action == "RECHECK":
            pass  # nur Recheck, kein Override
        else:
            raise HTTPException(status_code=400, detail=f"Unbekannte Aktion: {action}")

        result = rdns.check_readiness(case_id)
        return {"success": True, "case_id": case_id, "status": result.get("status")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Action failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


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
            ))

        return {"success": True, "case_id": case_id, "status": result.get("status")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create case failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


async def _trigger_setup_case(case_id: str, outlook_message_id: str):
    """Triggert n8n Setup-Case Webhook im Hintergrund (OneDrive + Attachment-Analyse)."""
    try:
        async with httpx.AsyncClient(timeout=180) as client:
            resp = await client.post(N8N_SETUP_CASE_WEBHOOK, json={
                "case_id": case_id,
                "outlook_message_id": outlook_message_id,
            })
            resp.raise_for_status()
            result = resp.json()
            logger.info(f"Setup-case webhook OK for {case_id}: {result.get('message', '')}")
    except Exception as e:
        logger.error(f"Setup-case webhook failed for {case_id}: {e}")


# ──────────────────────────────────────────
# API: Update Case Fields (Facts/Answers)
# ──────────────────────────────────────────

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
            cases.save_facts(case_id, {req.field: val}, source="dashboard")
        else:
            cases.save_answers(case_id, {req.field: val}, actor="broker")

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

@router.post("/api/dashboard/check-file-processed")
async def check_file_processed(req: CheckFileRequest):
    """Prüft ob eine OneDrive-Datei bereits analysiert wurde."""
    try:
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

@router.post("/api/dashboard/case/{case_id}/process-gdrive")
async def dashboard_process_gdrive(case_id: str, req: GDriveRequest = None):
    """Downloads and analyzes files from Google Drive links for a case."""
    try:
        import gdrive

        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

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
        # Run blocking Google Drive + GPT analysis in thread pool
        result = await asyncio.to_thread(
            gdrive.process_google_drive_links, case_id=case_id, links=links
        )

        # Readiness check after processing
        readiness_result = None
        if result.get("files_processed", 0) > 0:
            try:
                readiness_result = await asyncio.to_thread(rdns.check_readiness, case_id)
                notify.dispatch_notifications(case_id, readiness_result, force=True)
            except Exception as e:
                logger.error(f"Readiness after gdrive failed: {e}")

        return {
            "success": result.get("success", False),
            "case_id": case_id,
            "files_found": result.get("files_found", 0),
            "files_processed": result.get("files_processed", 0),
            "files_skipped": result.get("files_skipped", 0),
            "results": result.get("results", []),
            "errors": result.get("errors", []),
            "readiness": readiness_result,
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Google Drive processing failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Scan Documents (triggers n8n OneDrive scan)
# ──────────────────────────────────────────

@router.post("/api/dashboard/case/{case_id}/scan-documents")
async def dashboard_scan_documents(case_id: str):
    """Triggert n8n Webhook zum Scannen des OneDrive-Ordners."""
    try:
        case = cases.load_case(case_id)
        if not case:
            raise HTTPException(status_code=404, detail="Case nicht gefunden")

        folder_id = case.get("onedrive_folder_id")
        if not folder_id:
            raise HTTPException(status_code=400, detail="Kein OneDrive-Ordner vorhanden")

        if not N8N_SCAN_WEBHOOK:
            raise HTTPException(status_code=503, detail="N8N_SCAN_WEBHOOK nicht konfiguriert")

        # n8n Webhook aufrufen – n8n listet Dateien und ruft /process-document pro Datei
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(N8N_SCAN_WEBHOOK, json={
                "case_id": case_id,
                "onedrive_folder_id": folder_id,
            })
            resp.raise_for_status()
            result = resp.json()

        scanned = result.get("scanned", 0)

        # Nach dem Scan: Readiness Check + Notifications
        try:
            readiness_result = rdns.check_readiness(case_id)
            if scanned > 0:
                notify.dispatch_notifications(case_id, readiness_result, force=True)
        except Exception as e:
            logger.error(f"Readiness check after scan failed: {e}")

        return {"success": True, "case_id": case_id, "scanned": scanned, "message": result.get("message", "")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Scan documents failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Outgoing Emails (Notifications Log)
# ──────────────────────────────────────────

@router.get("/api/dashboard/outgoing-emails")
async def dashboard_outgoing_emails(case_id: Optional[str] = None):
    try:
        email_cols = ["to", "subject", "body_text", "body_html", "logged_at", "dry_run"]
        if case_id:
            # Filter direkt in SQL statt alle laden + Python-Filter
            rows = db.query_rows(
                "email_test_log", email_cols,
                where="subject LIKE %s",
                where_params=(f"%{case_id}%",),
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
            })
        return {"emails": items}
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
            "errors": result.get("errors", []),
            "warnings": result.get("warnings", []),
            "dry_run": dry_run,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dashboard import failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
