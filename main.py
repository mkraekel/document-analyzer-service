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

    # Flatten answers_user (handle both nested partner/broker and flat keys)
    answers_user = {}
    # First: nested keys under partner/broker
    for section in ["partner", "broker"]:
        if section in answers_user_raw and isinstance(answers_user_raw[section], dict):
            for k, v in answers_user_raw[section].items():
                if not k.startswith("_") and v is not None:
                    answers_user[k] = v
    # Second: flat keys at top level (higher priority)
    for k, v in answers_user_raw.items():
        if k in ["partner", "broker", "_meta"] or k.startswith("_"):
            continue
        if v is not None and not isinstance(v, dict):
            answers_user[k] = v
        elif isinstance(v, dict):
            # Also include nested dicts at top level
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


# ============================================
# EUROPACE PAYLOAD BUILDER ENDPOINT
# ============================================

EUROPACE_ENUMS = {
    "anrede": {"Herr": "HERR", "Frau": "FRAU"},
    "familienstand": {
        "ledig": "LEDIG",
        "verheiratet": "VERHEIRATET",
        "geschieden": "GESCHIEDEN",
        "verwitwet": "VERWITWET",
        "eingetragene Lebenspartnerschaft": "LEBENSPARTNERSCHAFT"
    },
    "objektart": {
        "ETW": "EIGENTUMSWOHNUNG",
        "EFH": "EINFAMILIENHAUS",
        "DHH": "DOPPELHAUSHAELFTE",
        "RH": "REIHENHAUS",
        "MFH": "MEHRFAMILIENHAUS",
        "Eigentumswohnung": "EIGENTUMSWOHNUNG",
        "Einfamilienhaus": "EINFAMILIENHAUS"
    },
    "nutzungsart": {
        "Eigennutzung": "EIGENGENUTZT",
        "Kapitalanlage": "VERMIETET",
        "Teilvermietet": "TEILWEISE_VERMIETET"
    },
    "beschaeftigungsart": {
        "Angestellter": "ANGESTELLTER",
        "Selbstständig": "SELBSTAENDIGER",
        "Beamter": "BEAMTER",
        "Rentner": "RENTNER",
        "Sonstiges": "SONSTIGES"
    }
}


def map_enum(value, enum_map):
    """Map value to Europace enum"""
    if not value or not enum_map:
        return None
    return enum_map.get(value, value)


def clean_payload(obj):
    """Remove None/empty values from nested dict"""
    if obj is None:
        return None
    if not isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        cleaned = [clean_payload(v) for v in obj if clean_payload(v) is not None]
        return cleaned if cleaned else None

    cleaned = {}
    for k, v in obj.items():
        if v is None or v == "" or v == []:
            continue
        if isinstance(v, dict):
            nested = clean_payload(v)
            if nested:
                cleaned[k] = nested
        elif isinstance(v, list):
            nested = clean_payload(v)
            if nested:
                cleaned[k] = nested
        else:
            cleaned[k] = v
    return cleaned if cleaned else None


class EuropaceRequest(BaseModel):
    case_id: str
    facts_extracted: Optional[dict] = None
    answers_user: Optional[dict] = None
    manual_overrides: Optional[dict] = None
    derived_values: Optional[dict] = None
    partner_id: Optional[str] = None
    tippgeber_partner_id: Optional[str] = None


class EuropaceResponse(BaseModel):
    case_id: str
    success: bool
    payload: Optional[dict] = None
    validation_errors: list = []
    validation_warnings: list = []
    is_valid: bool = False
    debug_effective_view: Optional[dict] = None  # Temporarily for debugging


@app.post("/build-europace-payload", response_model=EuropaceResponse)
async def build_europace_payload(request: EuropaceRequest):
    """
    Baut den Europace API Payload aus den Case-Daten.

    Führt auch Validierung durch:
    - Strukturelle Prüfung (Pflichtfelder)
    - Wertbereichs-Prüfung (Kaufpreis, Alter, etc.)
    - Plausibilitäts-Prüfung (Verhältnisse)
    """
    from datetime import datetime

    # Build case_data dict from request
    case_data = {
        "case_id": request.case_id,
        "facts_extracted": request.facts_extracted or {},
        "answers_user": request.answers_user or {},
        "manual_overrides": request.manual_overrides or {},
        "derived_values": request.derived_values or {}
    }

    effective_view = compute_effective_view(case_data)

    # Debug logging
    logger.info(f"case_data keys: {case_data.keys()}")
    logger.info(f"facts_extracted: {request.facts_extracted}")
    logger.info(f"answers_user: {request.answers_user}")
    logger.info(f"effective_view: {effective_view}")

    def get_value(primary, *fallbacks):
        """Get value with fallback paths"""
        val = get_nested_value(effective_view, primary)
        if val is not None:
            return val
        for fb in fallbacks:
            val = get_nested_value(effective_view, fb)
            if val is not None:
                return val
        return None

    def build_kunde(prefix, id_suffix):
        """Build customer object for Europace"""
        p = f"{prefix}." if prefix else "applicant_data."
        p_alt = prefix.replace("applicant_data", "applicant") if prefix else "applicant"

        return {
            "externeKundenId": f"{request.case_id}{id_suffix}",
            "personendaten": {
                "anrede": map_enum(get_value(f"{p}salutation", f"{p_alt}_salutation"), EUROPACE_ENUMS["anrede"]),
                "titel": get_value(f"{p}title", f"{p_alt}_title"),
                "vorname": get_value(f"{p}first_name", f"{p_alt}_first_name"),
                "nachname": get_value(f"{p}last_name", f"{p_alt}_last_name"),
                "geburtsdatum": get_value(f"{p}birth_date", f"{p_alt}_birth_date"),
                "geburtsort": get_value(f"{p}birth_place", f"{p_alt}_birth_place"),
                "staatsangehoerigkeit": get_value(f"{p}nationality", f"{p_alt}_nationality") or "DE",
                "steuerId": get_value(f"{p}tax_id", f"{p_alt}_tax_id")
            },
            "kontakt": {
                "telefonPrivat": get_value(f"{p}phone", f"{p_alt}_phone"),
                "email": get_value(f"{p}email", f"{p_alt}_email")
            },
            "wohnsituation": None if prefix == "applicant_data_2" else {
                "anschrift": {
                    "strasse": get_value("address_data.street"),
                    "hausnummer": get_value("address_data.house_number"),
                    "plz": get_value("address_data.zip"),
                    "ort": get_value("address_data.city")
                },
                "wohnhaftSeit": get_value("address_data.resident_since")
            },
            "familienstand": {
                "familienstand": map_enum(get_value("household_data.marital_status"), EUROPACE_ENUMS["familienstand"])
            },
            "beschaeftigung": {
                "beschaeftigungsverhaeltnis": {
                    "beschaeftigungsart": map_enum(get_value(f"{p}employment_type", f"{p_alt}_employment_type"), EUROPACE_ENUMS["beschaeftigungsart"]),
                    "beruf": get_value(f"{p}occupation", f"{p_alt}_occupation"),
                    "arbeitgeber": {
                        "name": get_value(f"{p}employer", f"{p_alt}_employer"),
                        "inDeutschland": get_value(f"{p}employer_in_germany", f"{p_alt}_employer_in_germany") is not False
                    },
                    "beschaeftigtSeit": get_value(f"{p}employed_since", f"{p_alt}_employed_since"),
                    "befristet": get_value(f"{p}employment_status", f"{p_alt}_employment_status") == "befristet",
                    "inProbezeit": get_value(f"{p}probation", f"{p_alt}_probation") or False
                }
            },
            "einkommenNetto": {
                "monatlichesNettoEinkommen": get_value(f"{p}net_income", f"{p}monthly_income", f"{p_alt}_monthly_income"),
                "anzahlGehaelterProJahr": get_value(f"{p}salaries_per_year", f"{p_alt}_salaries_per_year") or 12
            }
        }

    # Build customers
    kunde1 = build_kunde("applicant_data", "_1")
    kunden = [kunde1]

    # Check for couple
    is_couple = (
        get_value("is_couple") is True or
        get_value("applicant_2_first_name") is not None or
        get_value("applicant_data_2.first_name") is not None
    )

    if is_couple:
        kunde2 = build_kunde("applicant_data_2", "_2")
        if kunde2.get("personendaten", {}).get("vorname") or kunde2.get("personendaten", {}).get("nachname"):
            kunden.append(kunde2)

    # Build payload - mit allen möglichen Pfaden für jeden Wert
    kaufpreis = get_value("purchase_price", "property_data.purchase_price", "financing_data.purchase_price")
    loan_amount = get_value("loan_amount", "financing_data.loan_amount")
    equity = get_value("equity_to_use", "financing_data.equity_to_use", "equity")
    object_type = get_value("object_type", "property_data.object_type")
    usage = get_value("usage", "property_data.usage")

    payload = {
        "kundenangaben": {
            "haushalte": [{
                "kunden": kunden,
                "finanzielleSituation": {
                    "vermoegen": {
                        "summeBankUndSparguthaben": get_value("assets.bank_savings", "bank_savings"),
                        "summeBausparvertraege": get_value("assets.bauspar", "bauspar")
                    }
                },
                "finanzbedarf": {
                    "fahrzeuge": {
                        "anzahlPKWGesamt": get_value("household_data.cars_in_household", "cars_in_household") or 0
                    }
                }
            }],
            "finanzierungsobjekt": {
                "immobilie": {
                    "objektart": map_enum(object_type, EUROPACE_ENUMS["objektart"]),
                    "nutzungsart": map_enum(usage, EUROPACE_ENUMS["nutzungsart"]),
                    "adresse": {
                        "strasse": get_value("property_data.street", "object_street"),
                        "hausnummer": get_value("property_data.house_number", "object_house_number"),
                        "plz": get_value("property_data.zip", "object_zip"),
                        "ort": get_value("property_data.city", "object_city")
                    },
                    "wohnflaeche": get_value("property_data.living_space", "living_space"),
                    "baujahr": get_value("property_data.year_built", "year_built"),
                    "kaufpreis": kaufpreis,
                    "marktwert": get_value("property_data.market_value", "market_value") or kaufpreis
                }
            },
            "finanzierungswunsch": {
                "darlehenssumme": loan_amount,
                "eigenkapital": equity,
                "zinsbindungInJahren": get_value("zinsbindung") or 10,
                "wunschrate": get_value("wunschrate")
            }
        },
        "bearbeiter": {
            "partnerId": request.partner_id or get_value("partnerId"),
            "tippgeberPartnerId": request.tippgeber_partner_id or get_value("tippgeberPartnerId")
        }
    }

    # Clean payload
    cleaned_payload = clean_payload(payload)

    # ============================================
    # VALIDATION
    # ============================================
    errors = []
    warnings = []
    current_year = datetime.now().year

    kundenangaben = cleaned_payload.get("kundenangaben", {}) if cleaned_payload else {}
    haushalte = kundenangaben.get("haushalte", [{}])[0] if kundenangaben.get("haushalte") else {}
    kunde = haushalte.get("kunden", [{}])[0] if haushalte.get("kunden") else {}
    immobilie = kundenangaben.get("finanzierungsobjekt", {}).get("immobilie", {})
    finanzierungswunsch = kundenangaben.get("finanzierungswunsch", {})

    # Required fields
    if not kunde.get("personendaten", {}).get("vorname"):
        errors.append("Pflichtfeld: Vorname fehlt")
    if not kunde.get("personendaten", {}).get("nachname"):
        errors.append("Pflichtfeld: Nachname fehlt")
    if not immobilie.get("objektart"):
        errors.append("Pflichtfeld: Objektart fehlt")
    if not immobilie.get("nutzungsart"):
        errors.append("Pflichtfeld: Nutzungsart fehlt")
    if not immobilie.get("kaufpreis") and not immobilie.get("marktwert"):
        errors.append("Pflichtfeld: Kaufpreis/Marktwert fehlt")
    if not finanzierungswunsch.get("darlehenssumme"):
        errors.append("Pflichtfeld: Darlehenssumme fehlt")
    if finanzierungswunsch.get("eigenkapital") is None:
        errors.append("Pflichtfeld: Eigenkapital fehlt")

    # Value ranges
    kp = immobilie.get("kaufpreis") or immobilie.get("marktwert") or 0
    darlehen = finanzierungswunsch.get("darlehenssumme") or 0
    ek = finanzierungswunsch.get("eigenkapital") or 0
    wohnflaeche = immobilie.get("wohnflaeche")
    baujahr = immobilie.get("baujahr")

    if kp > 0:
        if kp < 30000:
            errors.append(f"Kaufpreis zu niedrig ({kp:,} €) - Minimum 30.000 €")
        if kp > 10000000:
            errors.append(f"Kaufpreis zu hoch ({kp:,} €) - Maximum 10.000.000 €")

    if darlehen > 0:
        if darlehen < 10000:
            errors.append(f"Darlehenssumme zu niedrig ({darlehen:,} €) - Minimum 10.000 €")
        if darlehen > 10000000:
            errors.append(f"Darlehenssumme zu hoch ({darlehen:,} €) - Maximum 10.000.000 €")

    if ek < 0:
        errors.append(f"Eigenkapital kann nicht negativ sein ({ek:,} €)")
    if kp > 0 and ek > kp * 1.5:
        errors.append(f"Eigenkapital ({ek:,} €) > 150% des Kaufpreises")

    if wohnflaeche is not None:
        if wohnflaeche < 15:
            warnings.append(f"Wohnfläche sehr klein ({wohnflaeche} m²)")
        if wohnflaeche > 2000:
            errors.append(f"Wohnfläche zu groß ({wohnflaeche} m²)")

    if baujahr is not None:
        if baujahr < 1800:
            errors.append(f"Baujahr zu alt ({baujahr})")
        if baujahr > current_year + 2:
            errors.append(f"Baujahr in der Zukunft ({baujahr})")

    # Ratio checks
    if kp > 0 and darlehen > 0:
        if darlehen > kp * 1.2:
            warnings.append(f"Darlehenssumme > 120% des Kaufpreises - Vollfinanzierung?")
        ek_quote = (ek / kp) * 100
        if ek_quote < 5 and ek > 0:
            warnings.append(f"Eigenkapitalquote sehr niedrig ({ek_quote:.1f}%)")

    # Age check
    geb = kunde.get("personendaten", {}).get("geburtsdatum")
    if geb:
        try:
            birth = datetime.fromisoformat(geb.replace("Z", ""))
            age = (datetime.now() - birth).days // 365
            if age < 18:
                errors.append(f"Antragsteller unter 18 Jahre ({age})")
            if age > 99:
                errors.append(f"Alter über 99 Jahre ({age})")
            if age > 75:
                warnings.append(f"Antragsteller über 75 Jahre ({age})")
        except:
            pass

    is_valid = len(errors) == 0

    return EuropaceResponse(
        case_id=request.case_id,
        success=is_valid,
        payload=cleaned_payload,
        validation_errors=errors,
        validation_warnings=warnings,
        is_valid=is_valid,
        debug_effective_view=effective_view  # Remove after debugging
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
