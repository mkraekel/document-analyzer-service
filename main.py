"""
Document Analyzer Service
Analysiert PDFs und Bilder mit GPT-4o Vision
"""

import os
import sys
import base64
import json
import io

# Logging als erstes
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

logger.info("=== Document Analyzer Service Starting ===")
logger.info(f"Python version: {sys.version}")
logger.info(f"PORT env: {os.getenv('PORT', 'not set')}")
logger.info(f"OPENAI_API_KEY set: {bool(os.getenv('OPENAI_API_KEY'))}")

try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("dotenv loaded")
except Exception as e:
    logger.warning(f"dotenv not available: {e}")

from pypdf import PdfReader
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from openai import OpenAI
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Document Analyzer", version="1.0.0")

# OpenAI Client
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    logger.warning("OPENAI_API_KEY nicht gesetzt! /analyze wird nicht funktionieren.")
    client = None
else:
    logger.info("OpenAI client initialized")
    client = OpenAI(api_key=api_key)

# Dokument-Typen für Klassifizierung
DOC_TYPES = [
    "Selbstauskunft", "Ausweiskopie", "Gehaltsnachweis", "Gehaltsabrechnung Dezember",
    "Kontoauszug", "Eigenkapitalnachweis", "Bausparvertrag", "Depotnachweis",
    "Renteninfo", "Steuerbescheid", "Steuererklärung", "Lohnsteuerbescheinigung",
    "Darlehensvertrag", "Exposé", "BWA", "Summen und Saldenliste", "Jahresabschluss",
    "Nachweis Krankenversicherung", "Private Rentenversicherung", "Private Lebensversicherung",
    "Objektbild Innen", "Objektbild Außen", "Baubeschreibung", "Grundriss",
    "Teilungserklärung", "Wohnflächenberechnung", "Modernisierungsaufstellung",
    "Grundbuch", "Energieausweis", "Sonstiges"
]

EXTRACTION_PROMPT = """Analysiere dieses Dokument und extrahiere alle relevanten Daten.

Dokumenttyp erkennen aus: {doc_types}

Extrahiere je nach Dokumenttyp:

Für Ausweise:
- Vorname, Nachname, Geburtsdatum, Geburtsort, Nationalität
- Ausweisnummer, Gültig bis, Ausstellungsbehörde

Für Gehaltsnachweise:
- Arbeitgeber, Brutto, Netto, Monat/Jahr
- Steuerklasse, Sozialversicherungsbeiträge

Für Kontoauszüge:
- Bank, IBAN, Kontostand, Zeitraum
- Regelmäßige Eingänge/Ausgänge

Für Selbstauskunft:
- Alle persönlichen Daten, Adresse, Kontaktdaten
- Familienstatus, Kinder, Beruf, Einkommen

Für Immobilien-Dokumente:
- Adresse, Wohnfläche, Baujahr, Objekttyp
- Kaufpreis, Grundstücksgröße

Antworte NUR mit validem JSON in diesem Format:
{{
  "doc_type": "erkannter Typ",
  "confidence": "high|medium|low",
  "meta": {{
    "doc_date": "YYYY-MM-DD oder null",
    "person_name": "Name der Person im Dokument oder null"
  }},
  "extracted_data": {{
    // Alle extrahierten Felder hier
  }}
}}
"""


def extract_text_from_pdf(pdf_bytes: bytes) -> tuple[str, int]:
    """Extrahiert Text aus PDF mit pypdf"""
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = ""
        for page in reader.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n"
        return text.strip(), len(reader.pages)
    except Exception as e:
        logger.error(f"PDF Text-Extraktion fehlgeschlagen: {e}")
        return "", 0


def analyze_with_gpt4o(file_bytes: bytes, mime_type: str, filename: str) -> dict:
    """Analysiert Dokument mit GPT-4o Vision"""

    if not client:
        raise HTTPException(status_code=500, detail="OpenAI API Key nicht konfiguriert")

    base64_data = base64.standard_b64encode(file_bytes).decode("utf-8")

    # Für PDFs: Erst Text extrahieren
    extracted_text = ""
    if mime_type == "application/pdf":
        extracted_text, page_count = extract_text_from_pdf(file_bytes)
        logger.info(f"PDF {filename}: {len(extracted_text)} Zeichen Text, {page_count} Seiten")

    # Prompt bauen
    prompt = EXTRACTION_PROMPT.format(doc_types=", ".join(DOC_TYPES))

    # PDFs: Immer Text-basiert analysieren (Vision API akzeptiert keine PDFs)
    if mime_type == "application/pdf":
        if extracted_text:
            logger.info(f"PDF mit Text: {len(extracted_text)} Zeichen")
            messages = [{
                "role": "user",
                "content": f"{prompt}\n\nDokument: {filename}\n\nExtrahierter Text:\n{extracted_text[:15000]}"
            }]
            model = "gpt-4o-mini"  # Günstiger für Text-Only
        else:
            # PDF ohne Text (gescannt) - braucht OCR/Bildkonvertierung
            logger.warning(f"PDF {filename} hat keinen extrahierbaren Text - Scan-Dokument?")
            return {
                "doc_type": "Sonstiges",
                "confidence": "low",
                "error": "PDF enthält keinen extrahierbaren Text. Bitte als Bild hochladen.",
                "meta": {"requires_ocr": True}
            }
    else:
        # Bilder: Vision-Analyse
        logger.info(f"Nutze Vision-Analyse für Bild: {filename}")
        content = [
            {"type": "text", "text": f"{prompt}\n\nDokument: {filename}"},
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{mime_type};base64,{base64_data}",
                    "detail": "high"
                }
            }
        ]
        messages = [{"role": "user", "content": content}]
        model = "gpt-4o"

    # API Call
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=4000,
            temperature=0.1
        )

        result_text = response.choices[0].message.content

        # JSON parsen
        # Entferne mögliche Markdown Code Blocks
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0]
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0]

        return json.loads(result_text.strip())

    except json.JSONDecodeError as e:
        logger.error(f"JSON Parse Error: {e}")
        return {
            "doc_type": "Sonstiges",
            "confidence": "low",
            "error": "JSON Parse Error",
            "raw_response": result_text[:500]
        }
    except Exception as e:
        logger.error(f"OpenAI API Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class AnalyzeResponse(BaseModel):
    success: bool
    filename: str
    doc_type: str
    confidence: str
    meta: Optional[dict] = None
    extracted_data: Optional[dict] = None
    error: Optional[str] = None


@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze_document(file: UploadFile = File(...)):
    """
    Analysiert ein Dokument (PDF oder Bild)

    - Erkennt Dokumenttyp
    - Extrahiert alle relevanten Daten
    - Gibt strukturiertes JSON zurück
    """

    # Datei lesen
    file_bytes = await file.read()
    filename = file.filename or "unknown"

    # MIME Type bestimmen
    content_type = file.content_type or "application/octet-stream"

    # Unterstützte Typen
    supported_types = [
        "application/pdf",
        "image/jpeg", "image/jpg", "image/png", "image/webp", "image/gif"
    ]

    if content_type not in supported_types:
        # Versuche anhand der Dateiendung
        ext = filename.lower().split(".")[-1]
        type_map = {
            "pdf": "application/pdf",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "png": "image/png",
            "webp": "image/webp",
            "gif": "image/gif"
        }
        content_type = type_map.get(ext, content_type)

    if content_type not in supported_types:
        return AnalyzeResponse(
            success=False,
            filename=filename,
            doc_type="unknown",
            confidence="none",
            error=f"Unsupported file type: {content_type}"
        )

    logger.info(f"Analyzing: {filename} ({content_type}, {len(file_bytes)} bytes)")

    # Analyse durchführen
    try:
        result = analyze_with_gpt4o(file_bytes, content_type, filename)

        return AnalyzeResponse(
            success=True,
            filename=filename,
            doc_type=result.get("doc_type", "Sonstiges"),
            confidence=result.get("confidence", "low"),
            meta=result.get("meta"),
            extracted_data=result.get("extracted_data")
        )

    except Exception as e:
        logger.error(f"Analysis failed for {filename}: {e}")
        return AnalyzeResponse(
            success=False,
            filename=filename,
            doc_type="error",
            confidence="none",
            error=str(e)
        )


@app.get("/health")
async def health_check():
    """Health Check Endpoint"""
    return {"status": "healthy", "service": "document-analyzer"}


# ============================================
# READINESS CHECK ENDPOINT
# ============================================

READINESS_CONFIG = {
    "STATUS": {
        "INTAKE": "INTAKE",
        "EXTRACTING": "EXTRACTING",
        "NEEDS_QUESTIONS_PARTNER": "NEEDS_QUESTIONS_PARTNER",
        "NEEDS_QUESTIONS_BROKER": "NEEDS_QUESTIONS_BROKER",
        "NEEDS_MANUAL_REVIEW_BROKER": "NEEDS_MANUAL_REVIEW_BROKER",
        "AWAITING_BROKER_CONFIRMATION": "AWAITING_BROKER_CONFIRMATION",
        "WAITING_FOR_DOCUMENTS": "WAITING_FOR_DOCUMENTS",
        "READY_FOR_IMPORT": "READY_FOR_IMPORT",
        "IMPORTED": "IMPORTED",
        "ERROR": "ERROR"
    },
    "REQUIRED_FINANCING_KEYS": ["purchase_price", "loan_amount", "equity_to_use", "object_type", "usage"],
    "KEY_SEARCH_PATHS": {
        "purchase_price": ["purchase_price", "property_data.purchase_price", "financing_data.purchase_price"],
        "loan_amount": ["loan_amount", "financing_data.loan_amount"],
        "equity_to_use": ["equity_to_use", "financing_data.equity_to_use"],
        "object_type": ["object_type", "property_data.object_type"],
        "usage": ["usage", "property_data.usage"]
    },
    "REQUIRED_DOCS": {
        "customer_always": [
            {"type": "Selbstauskunft", "count": 1, "per_person": False},
            {"type": "Ausweiskopie", "count": 1, "per_person": True},
            {"type": "Eigenkapitalnachweis", "count": 1, "per_person": True, "freshness_months": 1},
            {"type": "Renteninfo", "count": 1, "per_person": True}
        ],
        "customer_employed": [
            {"type": "Gehaltsnachweis", "count": 3, "per_person": True, "freshness_months": 3},
            {"type": "Kontoauszug", "count": 3, "per_person": True, "freshness_months": 3},
            {"type": "Steuerbescheid", "count": 1, "per_person": True},
            {"type": "Steuererklärung", "count": 1, "per_person": True},
            {"type": "Lohnsteuerbescheinigung", "count": 1, "per_person": True, "alternatives": ["Gehaltsabrechnung Dezember"]}
        ],
        "customer_self_employed": [
            {"type": "BWA", "count": 1, "per_person": True},
            {"type": "Summen und Saldenliste", "count": 1, "per_person": True},
            {"type": "Jahresabschluss", "count": 3, "per_person": True},
            {"type": "Steuerbescheid", "count": 2, "per_person": True},
            {"type": "Steuererklärung", "count": 2, "per_person": True},
            {"type": "Kontoauszug", "count": 3, "per_person": True, "freshness_months": 3},
            {"type": "Nachweis Krankenversicherung", "count": 1, "per_person": True}
        ],
        "object_always": [
            {"type": "Exposé", "count": 1},
            {"type": "Objektbild Innen", "count": 1},
            {"type": "Objektbild Außen", "count": 1},
            {"type": "Baubeschreibung", "count": 1},
            {"type": "Grundbuch", "count": 1, "freshness_months": 3},
            {"type": "Teilungserklärung", "count": 1},
            {"type": "Wohnflächenberechnung", "count": 1},
            {"type": "Grundriss", "count": 1},
            {"type": "Energieausweis", "count": 1},
            {"type": "Modernisierungsaufstellung", "count": 1}
        ]
    },
    "QUESTION_TEMPLATES": {
        "purchase_price": {"question": "Wie hoch ist der Kaufpreis der Immobilie?", "example": "z.B. 350000"},
        "loan_amount": {"question": "Wie hoch soll das Darlehen sein?", "example": "z.B. 280000"},
        "equity_to_use": {"question": "Wie viel Eigenkapital soll eingesetzt werden?", "example": "z.B. 70000"},
        "object_type": {"question": "Um welche Art von Immobilie handelt es sich?", "example": "ETW, EFH, DHH, RH, MFH"},
        "usage": {"question": "Wie soll die Immobilie genutzt werden?", "example": "Eigennutzung oder Kapitalanlage"}
    },
    "WARNINGS": {"ID_EXPIRY_DAYS": 90}
}


def get_nested_value(obj: dict, path: str):
    """Get value from nested dict by dot-notation path"""
    if not obj or not path:
        return None
    parts = path.split(".")
    current = obj
    for part in parts:
        if current is None or not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def compute_effective_view(case_data: dict) -> dict:
    """Merge all data sources with priority"""
    def safe_json_parse(val):
        if isinstance(val, dict):
            return val
        if isinstance(val, str):
            try:
                return json.loads(val)
            except:
                return {}
        return {}

    manual_overrides = safe_json_parse(case_data.get("manual_overrides", "{}"))
    answers_user_raw = safe_json_parse(case_data.get("answers_user", "{}"))
    facts_extracted = safe_json_parse(case_data.get("facts_extracted", "{}"))
    derived_values = safe_json_parse(case_data.get("derived_values", "{}"))

    # Flatten answers_user
    answers_user = {}
    for section in ["partner", "broker"]:
        if section in answers_user_raw and isinstance(answers_user_raw[section], dict):
            for k, v in answers_user_raw[section].items():
                if not k.startswith("_") and v is not None:
                    answers_user[k] = v

    # Priority merge: derived < facts < answers < manual
    result = {}
    for source in [derived_values, facts_extracted, answers_user, manual_overrides]:
        if not isinstance(source, dict):
            continue
        for k, v in source.items():
            if k.startswith("_") or v is None:
                continue
            if isinstance(v, dict) and isinstance(result.get(k), dict):
                result[k] = {**result[k], **v}
            else:
                result[k] = v
    return result


class ReadinessRequest(BaseModel):
    case_id: str
    facts_extracted: Optional[dict] = None
    answers_user: Optional[dict] = None
    manual_overrides: Optional[dict] = None
    derived_values: Optional[dict] = None
    docs_index: Optional[dict] = None  # {doc_type: [{doc_date: "...", ...}, ...]}
    applicant_name: Optional[str] = None


class ReadinessResponse(BaseModel):
    case_id: str
    ready: bool
    new_status: str
    completeness_percent: int
    missing_required: list
    missing_docs: list
    stale_docs: list
    warnings: list
    questions_partner: list
    questions_broker: list
    next_action: Optional[str] = None


@app.post("/check-readiness", response_model=ReadinessResponse)
async def check_readiness(request: ReadinessRequest):
    """
    Prüft ob ein Fall bereit für den Europace Import ist.

    Gibt zurück:
    - ready: Boolean ob bereit
    - missing_required: Fehlende Pflichtdaten
    - missing_docs: Fehlende Dokumente
    - questions_partner/broker: Offene Fragen
    - next_action: Empfohlene nächste Aktion
    """
    from datetime import datetime

    CONFIG = READINESS_CONFIG

    # Build case_data dict from request
    case_data = {
        "case_id": request.case_id,
        "facts_extracted": request.facts_extracted or {},
        "answers_user": request.answers_user or {},
        "manual_overrides": request.manual_overrides or {},
        "derived_values": request.derived_values or {}
    }

    effective_view = compute_effective_view(case_data)
    docs_index = request.docs_index or {}
    manual_overrides = request.manual_overrides or {}

    # Initialize readiness
    readiness = {
        "missing_required": [],
        "missing_docs": [],
        "stale_docs": [],
        "warnings": [],
        "questions_broker": [],
        "questions_partner": []
    }

    now = datetime.now()

    # 1. Check required financing keys
    for key in CONFIG["REQUIRED_FINANCING_KEYS"]:
        search_paths = CONFIG["KEY_SEARCH_PATHS"].get(key, [key])
        value = None

        for path in search_paths:
            value = get_nested_value(effective_view, path)
            if value is not None and value != "":
                break

        if value is None or value == "":
            readiness["missing_required"].append(key)
            template = CONFIG["QUESTION_TEMPLATES"].get(key, {})
            if template:
                readiness["questions_partner"].append({
                    "key": key,
                    "question": template.get("question", f"Bitte {key} angeben"),
                    "example": template.get("example", "")
                })

    # 2. Detect couple and employment type
    is_couple = (
        get_nested_value(effective_view, "is_couple") is True or
        get_nested_value(effective_view, "applicant_2_first_name") is not None or
        get_nested_value(effective_view, "applicant_data_2.first_name") is not None
    )

    emp_type_1 = str(get_nested_value(effective_view, "applicant_data.employment_type") or "").lower()
    emp_type_2 = str(get_nested_value(effective_view, "applicant_data_2.employment_type") or "").lower()

    is_self_employed_1 = "selbst" in emp_type_1 or "freiberuf" in emp_type_1
    is_self_employed_2 = "selbst" in emp_type_2 or "freiberuf" in emp_type_2
    has_self_employed = is_self_employed_1 or is_self_employed_2
    has_employed = (emp_type_1 and not is_self_employed_1) or (is_couple and emp_type_2 and not is_self_employed_2)

    # 3. Build required docs list
    required_docs = (
        CONFIG["REQUIRED_DOCS"]["customer_always"] +
        CONFIG["REQUIRED_DOCS"]["object_always"]
    )
    if has_employed:
        required_docs += CONFIG["REQUIRED_DOCS"]["customer_employed"]
    if has_self_employed:
        required_docs += CONFIG["REQUIRED_DOCS"]["customer_self_employed"]

    # 4. Check each required document
    has_joint_account = get_nested_value(effective_view, "has_joint_account") is True

    for doc_req in required_docs:
        doc_type = doc_req["type"]
        required_count = doc_req.get("count", 1)
        is_per_person = doc_req.get("per_person", False)
        alternatives = doc_req.get("alternatives", [])
        freshness_months = doc_req.get("freshness_months")

        # Adjust for couples
        is_per_person_effective = is_per_person
        if doc_type == "Kontoauszug" and has_joint_account and is_couple:
            is_per_person_effective = False

        person_multiplier = 2 if (is_couple and is_per_person_effective) else 1
        total_required = required_count * person_multiplier

        # Get docs including alternatives
        docs = docs_index.get(doc_type, [])
        for alt in alternatives:
            docs += docs_index.get(alt, [])

        # Check count
        if len(docs) < total_required:
            override_key = f"accept_missing_{doc_type.lower().replace(' ', '_')}"
            if not manual_overrides.get(override_key):
                missing = total_required - len(docs)
                if required_count > 1:
                    readiness["missing_docs"].append(f"{doc_type} ({missing}x von {total_required} fehlt)")
                else:
                    readiness["missing_docs"].append(doc_type)

        # Check freshness
        if freshness_months and docs:
            for i, doc in enumerate(docs[:total_required]):
                doc_date_str = doc.get("doc_date") or doc.get("date")
                if doc_date_str:
                    try:
                        doc_date = datetime.fromisoformat(doc_date_str.replace("Z", ""))
                        age_months = (now - doc_date).days / 30
                        if age_months > freshness_months:
                            readiness["stale_docs"].append({
                                "doc_type": doc_type,
                                "doc_date": doc_date_str,
                                "age_months": round(age_months),
                                "max_age_months": freshness_months
                            })
                    except:
                        pass

    # 5. Check ID expiry warning
    id_valid_until = get_nested_value(effective_view, "id_data.valid_until")
    if id_valid_until:
        try:
            expiry = datetime.fromisoformat(id_valid_until.replace("Z", ""))
            days_until = (expiry - now).days
            if days_until < 0:
                readiness["warnings"].append(f"Ausweis ist abgelaufen ({id_valid_until})")
            elif days_until < CONFIG["WARNINGS"]["ID_EXPIRY_DAYS"]:
                readiness["warnings"].append(f"Ausweis läuft in {days_until} Tagen ab")
        except:
            pass

    # 6. Determine status
    if manual_overrides.get("WAIT_FOR_DOCS"):
        new_status = CONFIG["STATUS"]["WAITING_FOR_DOCUMENTS"]
    elif manual_overrides.get("APPROVE_IMPORT") and not readiness["missing_required"] and not readiness["missing_docs"]:
        new_status = CONFIG["STATUS"]["READY_FOR_IMPORT"]
    elif readiness["missing_required"] or readiness["missing_docs"]:
        new_status = CONFIG["STATUS"]["NEEDS_QUESTIONS_PARTNER"]
    elif readiness["stale_docs"]:
        new_status = CONFIG["STATUS"]["NEEDS_MANUAL_REVIEW_BROKER"]
    else:
        new_status = CONFIG["STATUS"]["AWAITING_BROKER_CONFIRMATION"]

    # 7. Calculate completeness
    total_checks = len(CONFIG["REQUIRED_FINANCING_KEYS"]) + len(required_docs)
    passed_checks = total_checks - len(readiness["missing_required"]) - len(readiness["missing_docs"])
    completeness = round((passed_checks / total_checks) * 100) if total_checks > 0 else 0

    # 8. Next action recommendation
    next_action = None
    if readiness["missing_required"]:
        first_missing = readiness["missing_required"][0]
        template = CONFIG["QUESTION_TEMPLATES"].get(first_missing, {})
        next_action = template.get("question", f"Bitte {first_missing} angeben")
    elif readiness["missing_docs"]:
        next_action = f"Bitte hochladen: {readiness['missing_docs'][0]}"
    elif readiness["stale_docs"]:
        next_action = f"Dokument veraltet: {readiness['stale_docs'][0]['doc_type']} - bitte aktualisieren"
    elif new_status == CONFIG["STATUS"]["AWAITING_BROKER_CONFIRMATION"]:
        next_action = "Alle Unterlagen vollständig - bereit für Freigabe"

    ready = new_status == CONFIG["STATUS"]["READY_FOR_IMPORT"]

    return ReadinessResponse(
        case_id=request.case_id,
        ready=ready,
        new_status=new_status,
        completeness_percent=completeness,
        missing_required=readiness["missing_required"],
        missing_docs=readiness["missing_docs"],
        stale_docs=readiness["stale_docs"],
        warnings=readiness["warnings"],
        questions_partner=readiness["questions_partner"],
        questions_broker=readiness["questions_broker"],
        next_action=next_action
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
