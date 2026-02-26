"""
Admin Dashboard API
Endpoints fuer Triage, Case-Management, Overrides
"""

import json
import logging
import traceback
from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional

import seatable as db
import case_logic as cases
import readiness as rdns

logger = logging.getLogger(__name__)
router = APIRouter()


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
        all_cases = db.list_rows("fin_cases")
        all_emails = db.list_rows("processed_emails")
        all_docs = db.list_rows("fin_documents")

        status_counts = {}
        for c in all_cases:
            s = c.get("status", "UNKNOWN")
            status_counts[s] = status_counts.get(s, 0) + 1

        result_counts = {}
        for e in all_emails:
            r = e.get("processing_result", "unknown")
            result_counts[r] = result_counts.get(r, 0) + 1

        return {
            "cases_total": len(all_cases),
            "cases_by_status": status_counts,
            "emails_total": len(all_emails),
            "emails_by_result": result_counts,
            "documents_total": len(all_docs),
            "triage_count": result_counts.get("no_case_match", 0),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────
# API: Triage Queue
# ──────────────────────────────────────────

@router.get("/api/dashboard/triage")
async def dashboard_triage():
    try:
        emails = db.search_rows("processed_emails", "processing_result", "no_case_match")
        items = []
        for e in emails:
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
        all_cases = db.list_rows("fin_cases")
        items = []
        for c in all_cases:
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

        return {
            "case_id": case_id,
            "applicant_name": case.get("applicant_name"),
            "partner_email": case.get("partner_email"),
            "status": case.get("status"),
            "onedrive_folder_id": case.get("onedrive_folder_id", ""),
            "last_status_change": case.get("last_status_change"),
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
