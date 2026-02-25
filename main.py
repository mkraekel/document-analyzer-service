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
        is_valid=is_valid
    )


# ============================================
# EMAIL PARSER ENDPOINT
# ============================================

EMAIL_PARSE_PROMPT = """Analysiere diese E-Mail und extrahiere strukturierte Daten.

E-Mail:
Von: {from_address} ({from_name})
Betreff: {subject}
Text:
{body}

Aufgaben:
1. Erkenne den Intent (Absicht) der E-Mail
2. Extrahiere alle relevanten Daten
3. Erkenne ob es eine Antwort auf eine bestehende Anfrage ist

Antworte NUR mit validem JSON:
{{
  "intent": "new_request|document_upload|question_answer|followup|status_inquiry|unknown",
  "confidence": 0.0-1.0,
  "is_reply": true|false,
  "reply_to_case_id": "CASE-XXX oder null",
  "language": "de|en",
  "urgency": "low|normal|high",

  "applicant_data": {{
    "name": "Vor- und Nachname oder null",
    "first_name": "Vorname oder null",
    "last_name": "Nachname oder null",
    "email": "E-Mail oder null",
    "phone": "Telefon oder null"
  }},

  "property_data": {{
    "address": "Vollständige Adresse oder null",
    "street": "Straße oder null",
    "house_number": "Hausnummer oder null",
    "zip": "PLZ oder null",
    "city": "Stadt oder null",
    "purchase_price": Zahl oder null,
    "object_type": "ETW|EFH|DHH|RH|MFH oder null",
    "usage": "Eigennutzung|Kapitalanlage oder null",
    "living_space": Zahl oder null
  }},

  "financing_data": {{
    "loan_amount": Zahl oder null,
    "equity": Zahl oder null
  }},

  "answered_questions": [
    {{"key": "feldname", "value": "antwort", "original_text": "Originaltext"}}
  ],

  "mentioned_documents": ["Gehaltsnachweis", "Ausweis", ...],

  "summary": "Kurze Zusammenfassung in einem Satz"
}}
"""


class EmailParseRequest(BaseModel):
    from_address: str
    from_name: Optional[str] = None
    subject: str
    body: str
    attachments: Optional[list] = None


class EmailParseResponse(BaseModel):
    success: bool
    intent: str
    confidence: float
    is_reply: bool
    reply_to_case_id: Optional[str] = None
    language: str = "de"
    urgency: str = "normal"
    applicant_data: dict = {}
    property_data: dict = {}
    financing_data: dict = {}
    answered_questions: list = []
    mentioned_documents: list = []
    summary: Optional[str] = None
    error: Optional[str] = None


@app.post("/parse-email", response_model=EmailParseResponse)
async def parse_email(request: EmailParseRequest):
    """
    Parst eine eingehende E-Mail und extrahiert strukturierte Daten.

    - Erkennt Intent (neue Anfrage, Dokument-Upload, Antwort, etc.)
    - Extrahiert Kontakt-, Objekt- und Finanzierungsdaten
    - Erkennt Antworten auf bestehende Cases
    """
    import re

    if not client:
        return EmailParseResponse(
            success=False,
            intent="error",
            confidence=0.0,
            is_reply=False,
            error="OpenAI API Key nicht konfiguriert"
        )

    # Case-ID aus Subject extrahieren (z.B. [CASE-123] oder Re: CASE-123)
    case_id_match = re.search(r'(?:CASE|Case|case)[-_]?(\w{6,})', request.subject + " " + request.body[:500])
    detected_case_id = case_id_match.group(0) if case_id_match else None

    # Reply-Detection
    is_likely_reply = any(x in request.subject.lower() for x in ['re:', 'aw:', 'fwd:', 'wg:'])

    # GPT für Analyse
    prompt = EMAIL_PARSE_PROMPT.format(
        from_address=request.from_address,
        from_name=request.from_name or "Unbekannt",
        subject=request.subject,
        body=request.body[:8000]  # Limit
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            temperature=0.1
        )

        result_text = response.choices[0].message.content

        # JSON parsen
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0]
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0]

        result = json.loads(result_text.strip())

        # Override mit detected values
        if detected_case_id and not result.get("reply_to_case_id"):
            result["reply_to_case_id"] = detected_case_id
            result["is_reply"] = True

        if is_likely_reply and not result.get("is_reply"):
            result["is_reply"] = True

        # Attachments in mentioned_documents
        if request.attachments:
            mentioned = result.get("mentioned_documents", [])
            for att in request.attachments:
                if att not in mentioned:
                    mentioned.append(att)
            result["mentioned_documents"] = mentioned

        return EmailParseResponse(
            success=True,
            intent=result.get("intent", "unknown"),
            confidence=result.get("confidence", 0.5),
            is_reply=result.get("is_reply", False),
            reply_to_case_id=result.get("reply_to_case_id"),
            language=result.get("language", "de"),
            urgency=result.get("urgency", "normal"),
            applicant_data=result.get("applicant_data", {}),
            property_data=result.get("property_data", {}),
            financing_data=result.get("financing_data", {}),
            answered_questions=result.get("answered_questions", []),
            mentioned_documents=result.get("mentioned_documents", []),
            summary=result.get("summary")
        )

    except json.JSONDecodeError as e:
        logger.error(f"Email parse JSON error: {e}")
        return EmailParseResponse(
            success=False,
            intent="error",
            confidence=0.0,
            is_reply=is_likely_reply,
            reply_to_case_id=detected_case_id,
            error=f"JSON Parse Error: {e}"
        )
    except Exception as e:
        logger.error(f"Email parse error: {e}")
        return EmailParseResponse(
            success=False,
            intent="error",
            confidence=0.0,
            is_reply=is_likely_reply,
            error=str(e)
        )


# ============================================
# QUESTION GENERATOR ENDPOINT
# ============================================

QUESTION_PROMPT = """Du bist ein freundlicher Finanzierungsberater.

Generiere natürliche Fragen für fehlende Informationen.

Kontext:
- Antragsteller: {applicant_name}
- Bekannte Daten: {known_data}
- Fehlende Felder: {missing_fields}
- Zielgruppe: {target} (Partner = Kunde, Broker = Makler)
- Ton: {tone}

Erstelle für jedes fehlende Feld eine natürliche Frage.
Kombiniere die Fragen zu einer freundlichen E-Mail.

Antworte NUR mit validem JSON:
{{
  "questions": [
    {{
      "field": "purchase_price",
      "question": "Wie hoch ist der Kaufpreis der Immobilie?",
      "example": "z.B. 350.000 €"
    }}
  ],
  "email_subject": "Betreff der E-Mail",
  "email_body": "Vollständiger E-Mail-Text mit allen Fragen",
  "email_greeting": "Anrede",
  "email_closing": "Grußformel"
}}
"""


class QuestionGeneratorRequest(BaseModel):
    case_id: str
    applicant_name: Optional[str] = None
    missing_fields: list  # ["purchase_price", "loan_amount"]
    known_data: Optional[dict] = None
    target: str = "partner"  # "partner" oder "broker"
    tone: str = "formal"  # "formal" oder "friendly"
    language: str = "de"


class QuestionGeneratorResponse(BaseModel):
    success: bool
    questions: list = []
    email_subject: str = ""
    email_body: str = ""
    error: Optional[str] = None


# Field descriptions for question generation
FIELD_DESCRIPTIONS = {
    "purchase_price": "Kaufpreis der Immobilie",
    "loan_amount": "Gewünschte Darlehenssumme",
    "equity_to_use": "Einzusetzendes Eigenkapital",
    "object_type": "Art der Immobilie (ETW, EFH, DHH, etc.)",
    "usage": "Nutzungsart (Eigennutzung oder Kapitalanlage)",
    "living_space": "Wohnfläche in m²",
    "year_built": "Baujahr",
    "object_address": "Adresse der Immobilie",
    "applicant_birth_date": "Geburtsdatum",
    "applicant_employer": "Arbeitgeber",
    "applicant_income": "Monatliches Nettoeinkommen"
}


@app.post("/generate-questions", response_model=QuestionGeneratorResponse)
async def generate_questions(request: QuestionGeneratorRequest):
    """
    Generiert natürliche Fragen für fehlende Daten.

    - Kontextbezogene Formulierung
    - Kombiniert zu einer E-Mail
    - Anpassbar an Zielgruppe (Kunde vs. Makler)
    """

    if not request.missing_fields:
        return QuestionGeneratorResponse(
            success=True,
            questions=[],
            email_subject="Keine Fragen erforderlich",
            email_body=""
        )

    # Fallback ohne GPT für einfache Fälle
    if not client or len(request.missing_fields) <= 2:
        questions = []
        for field in request.missing_fields:
            desc = FIELD_DESCRIPTIONS.get(field, field)
            questions.append({
                "field": field,
                "question": f"Bitte teilen Sie uns mit: {desc}",
                "example": ""
            })

        name = request.applicant_name or "Interessent"
        if request.target == "partner":
            greeting = f"Sehr geehrte/r {name},"
        else:
            greeting = "Sehr geehrte Damen und Herren,"

        body = f"""{greeting}

vielen Dank für Ihre Finanzierungsanfrage.

Um diese bearbeiten zu können, benötigen wir noch folgende Informationen:

"""
        for q in questions:
            body += f"• {q['question']}\n"

        body += """
Bitte antworten Sie einfach auf diese E-Mail.

Mit freundlichen Grüßen
Ihr Finanzierungsteam"""

        return QuestionGeneratorResponse(
            success=True,
            questions=questions,
            email_subject=f"Finanzierungsanfrage - Rückfragen",
            email_body=body
        )

    # GPT für komplexere Fälle
    known_str = json.dumps(request.known_data or {}, ensure_ascii=False, indent=2)
    missing_str = ", ".join([f"{f} ({FIELD_DESCRIPTIONS.get(f, f)})" for f in request.missing_fields])

    prompt = QUESTION_PROMPT.format(
        applicant_name=request.applicant_name or "Kunde",
        known_data=known_str[:2000],
        missing_fields=missing_str,
        target=request.target,
        tone=request.tone
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1500,
            temperature=0.3
        )

        result_text = response.choices[0].message.content

        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0]
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0]

        result = json.loads(result_text.strip())

        return QuestionGeneratorResponse(
            success=True,
            questions=result.get("questions", []),
            email_subject=result.get("email_subject", "Rückfragen zu Ihrer Finanzierungsanfrage"),
            email_body=result.get("email_body", "")
        )

    except Exception as e:
        logger.error(f"Question generator error: {e}")
        # Fallback
        return QuestionGeneratorResponse(
            success=False,
            questions=[{"field": f, "question": FIELD_DESCRIPTIONS.get(f, f)} for f in request.missing_fields],
            email_subject="Rückfragen",
            email_body="",
            error=str(e)
        )


# ============================================
# NOTIFICATION COMPOSER ENDPOINT
# ============================================

NOTIFICATION_TEMPLATES = {
    "case_created": {
        "subject": "Ihre Finanzierungsanfrage ist eingegangen",
        "body": """Sehr geehrte/r {applicant_name},

vielen Dank für Ihre Finanzierungsanfrage.

Wir haben Ihre Anfrage erhalten und werden diese schnellstmöglich bearbeiten.

Ihre Vorgangsnummer: {case_id}

Bei Rückfragen stehen wir Ihnen gerne zur Verfügung.

Mit freundlichen Grüßen
{broker_name}"""
    },
    "documents_received": {
        "subject": "Dokumente erhalten - {case_id}",
        "body": """Sehr geehrte/r {applicant_name},

wir haben folgende Dokumente erhalten:
{document_list}

{status_message}

Mit freundlichen Grüßen
{broker_name}"""
    },
    "documents_missing": {
        "subject": "Fehlende Unterlagen - {case_id}",
        "body": """Sehr geehrte/r {applicant_name},

für die Bearbeitung Ihrer Finanzierungsanfrage benötigen wir noch folgende Unterlagen:

{missing_documents}

Bitte laden Sie diese Dokumente hoch oder senden Sie sie per E-Mail.

Mit freundlichen Grüßen
{broker_name}"""
    },
    "ready_for_review": {
        "subject": "Finanzierungsanfrage bereit zur Prüfung - {case_id}",
        "body": """Sehr geehrte/r {broker_name},

die Finanzierungsanfrage von {applicant_name} ist vollständig und bereit zur Prüfung.

Vorgangsnummer: {case_id}
Kaufpreis: {purchase_price}
Darlehenssumme: {loan_amount}
Eigenkapital: {equity}

Bitte prüfen und freigeben.

Mit freundlichen Grüßen
Ihr Automatisierungssystem"""
    },
    "imported_to_europace": {
        "subject": "Erfolgreich an Europace übermittelt - {case_id}",
        "body": """Sehr geehrte/r {broker_name},

die Finanzierungsanfrage wurde erfolgreich an Europace übermittelt.

Vorgangsnummer: {case_id}
Europace-ID: {europace_id}

Sie können den Vorgang jetzt in Europace weiter bearbeiten.

Mit freundlichen Grüßen
Ihr Automatisierungssystem"""
    },
    "error": {
        "subject": "Fehler bei Finanzierungsanfrage - {case_id}",
        "body": """Sehr geehrte/r {broker_name},

bei der Verarbeitung der Finanzierungsanfrage {case_id} ist ein Fehler aufgetreten:

{error_message}

Bitte prüfen Sie den Vorgang manuell.

Mit freundlichen Grüßen
Ihr Automatisierungssystem"""
    }
}


class NotificationRequest(BaseModel):
    case_id: str
    notification_type: str  # case_created, documents_received, etc.
    applicant_name: Optional[str] = None
    applicant_email: Optional[str] = None
    broker_name: Optional[str] = "Ihr Finanzierungsteam"
    broker_email: Optional[str] = None
    context: Optional[dict] = None  # Additional data for templates
    language: str = "de"


class NotificationResponse(BaseModel):
    success: bool
    notification_type: str
    email_to: Optional[str] = None
    email_cc: Optional[str] = None
    email_subject: str = ""
    email_body: str = ""
    email_html: Optional[str] = None
    preview: str = ""
    error: Optional[str] = None


@app.post("/compose-notification", response_model=NotificationResponse)
async def compose_notification(request: NotificationRequest):
    """
    Erstellt eine Benachrichtigung basierend auf dem Typ und Kontext.

    - Vordefinierte Templates für häufige Status
    - Personalisierung mit Kontext-Daten
    - HTML und Plain-Text Output
    """

    template = NOTIFICATION_TEMPLATES.get(request.notification_type)

    if not template:
        return NotificationResponse(
            success=False,
            notification_type=request.notification_type,
            error=f"Unknown notification type: {request.notification_type}"
        )

    # Merge context with request data
    ctx = {
        "case_id": request.case_id,
        "applicant_name": request.applicant_name or "Kunde",
        "applicant_email": request.applicant_email or "",
        "broker_name": request.broker_name or "Ihr Finanzierungsteam",
        "broker_email": request.broker_email or "",
        **(request.context or {})
    }

    # Format lists
    if "missing_documents" in ctx and isinstance(ctx["missing_documents"], list):
        ctx["missing_documents"] = "\n".join(f"• {doc}" for doc in ctx["missing_documents"])

    if "document_list" in ctx and isinstance(ctx["document_list"], list):
        ctx["document_list"] = "\n".join(f"• {doc}" for doc in ctx["document_list"])

    # Default status message
    if "status_message" not in ctx:
        ctx["status_message"] = "Wir werden diese prüfen und uns bei Ihnen melden."

    # Format currency values
    for key in ["purchase_price", "loan_amount", "equity"]:
        if key in ctx and ctx[key]:
            try:
                val = float(ctx[key])
                ctx[key] = f"{val:,.0f} €".replace(",", ".")
            except:
                pass

    try:
        subject = template["subject"].format(**ctx)
        body = template["body"].format(**ctx)
    except KeyError as e:
        # Fill missing keys with placeholder
        ctx[str(e).strip("'")] = f"[{e}]"
        subject = template["subject"].format(**ctx)
        body = template["body"].format(**ctx)

    # Determine recipient
    if request.notification_type in ["ready_for_review", "imported_to_europace", "error"]:
        email_to = request.broker_email
    else:
        email_to = request.applicant_email

    # Simple HTML wrapper
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
<div style="max-width: 600px; margin: 0 auto; padding: 20px;">
{body.replace(chr(10), '<br>')}
</div>
</body>
</html>"""

    return NotificationResponse(
        success=True,
        notification_type=request.notification_type,
        email_to=email_to,
        email_cc=request.broker_email if email_to == request.applicant_email else None,
        email_subject=subject,
        email_body=body,
        email_html=html,
        preview=body[:100] + "..." if len(body) > 100 else body
    )


# ============================================
# DATA VALIDATOR ENDPOINT
# ============================================

import re
from datetime import datetime

VALIDATION_RULES = {
    "email": {
        "pattern": r"^[\w\.\-\+]+@[\w\.\-]+\.[a-zA-Z]{2,}$",
        "message": "Ungültige E-Mail-Adresse"
    },
    "phone": {
        "pattern": r"^[\+]?[\d\s\-\/\(\)]{6,20}$",
        "message": "Ungültige Telefonnummer"
    },
    "plz": {
        "pattern": r"^\d{5}$",
        "message": "PLZ muss 5 Ziffern haben"
    },
    "birth_date": {
        "min_age": 18,
        "max_age": 99,
        "message": "Alter muss zwischen 18 und 99 Jahren sein"
    },
    "purchase_price": {
        "min": 30000,
        "max": 10000000,
        "message": "Kaufpreis muss zwischen 30.000 und 10.000.000 € liegen"
    },
    "loan_amount": {
        "min": 10000,
        "max": 10000000,
        "message": "Darlehenssumme muss zwischen 10.000 und 10.000.000 € liegen"
    },
    "equity": {
        "min": 0,
        "message": "Eigenkapital kann nicht negativ sein"
    },
    "living_space": {
        "min": 15,
        "max": 2000,
        "message": "Wohnfläche muss zwischen 15 und 2.000 m² liegen"
    },
    "year_built": {
        "min": 1800,
        "max_offset": 2,  # Current year + 2
        "message": "Baujahr ungültig"
    }
}


class ValidationRequest(BaseModel):
    data: dict
    schema_type: str = "full"  # "applicant", "property", "financing", "full"
    strict: bool = False  # If true, warnings become errors


class ValidationResponse(BaseModel):
    valid: bool
    normalized_data: dict = {}
    errors: list = []
    warnings: list = []
    auto_corrections: list = []


def normalize_phone(phone: str) -> str:
    """Normalize phone number to German format"""
    if not phone:
        return phone
    # Remove all non-digit except +
    cleaned = re.sub(r'[^\d\+]', '', phone)
    # Add German prefix if missing
    if cleaned.startswith('0'):
        cleaned = '+49' + cleaned[1:]
    elif not cleaned.startswith('+'):
        cleaned = '+49' + cleaned
    return cleaned


def normalize_name(name: str) -> str:
    """Capitalize name properly"""
    if not name:
        return name
    return ' '.join(word.capitalize() for word in name.split())


@app.post("/validate-data", response_model=ValidationResponse)
async def validate_data(request: ValidationRequest):
    """
    Validiert und normalisiert Eingabedaten.

    - Prüft Formate (E-Mail, Telefon, PLZ)
    - Prüft Wertbereiche (Kaufpreis, Alter, etc.)
    - Normalisiert Daten (Telefon, Namen)
    - Gibt Fehler und Warnungen zurück
    """

    data = request.data.copy()
    errors = []
    warnings = []
    corrections = []
    current_year = datetime.now().year

    # Normalize and validate each field
    for key, value in list(data.items()):
        if value is None or value == "":
            continue

        # Email validation
        if key in ["email", "applicant_email", "broker_email"] and value:
            rule = VALIDATION_RULES["email"]
            if not re.match(rule["pattern"], str(value)):
                errors.append({"field": key, "message": rule["message"], "value": value})
            else:
                data[key] = str(value).lower().strip()
                if data[key] != value:
                    corrections.append({"field": key, "original": value, "corrected": data[key], "reason": "Kleinschreibung"})

        # Phone normalization
        if key in ["phone", "applicant_phone", "broker_phone"] and value:
            rule = VALIDATION_RULES["phone"]
            if not re.match(rule["pattern"], str(value)):
                warnings.append({"field": key, "message": rule["message"], "value": value})
            else:
                normalized = normalize_phone(str(value))
                if normalized != value:
                    corrections.append({"field": key, "original": value, "corrected": normalized, "reason": "Telefon normalisiert"})
                data[key] = normalized

        # PLZ validation
        if key in ["plz", "zip", "object_zip"] and value:
            rule = VALIDATION_RULES["plz"]
            plz_str = str(value).strip()
            if not re.match(rule["pattern"], plz_str):
                errors.append({"field": key, "message": rule["message"], "value": value})
            else:
                data[key] = plz_str

        # Name normalization
        if key in ["name", "first_name", "last_name", "applicant_name"] and value:
            normalized = normalize_name(str(value))
            if normalized != value:
                corrections.append({"field": key, "original": value, "corrected": normalized, "reason": "Großschreibung"})
            data[key] = normalized

        # Birth date validation
        if key in ["birth_date", "applicant_birth_date", "geburtsdatum"] and value:
            try:
                if isinstance(value, str):
                    birth = datetime.fromisoformat(value.replace("Z", ""))
                else:
                    birth = value
                age = (datetime.now() - birth).days // 365
                rule = VALIDATION_RULES["birth_date"]
                if age < rule["min_age"]:
                    errors.append({"field": key, "message": f"Antragsteller muss mindestens {rule['min_age']} Jahre alt sein", "value": f"{age} Jahre"})
                elif age > rule["max_age"]:
                    errors.append({"field": key, "message": f"Alter über {rule['max_age']} Jahre", "value": f"{age} Jahre"})
            except:
                warnings.append({"field": key, "message": "Datumsformat nicht erkannt", "value": value})

        # Numeric range validations
        if key in ["purchase_price", "kaufpreis"] and value:
            try:
                val = float(value)
                rule = VALIDATION_RULES["purchase_price"]
                if val < rule["min"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
                elif val > rule["max"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
            except:
                warnings.append({"field": key, "message": "Keine gültige Zahl", "value": value})

        if key in ["loan_amount", "darlehenssumme"] and value:
            try:
                val = float(value)
                rule = VALIDATION_RULES["loan_amount"]
                if val < rule["min"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
                elif val > rule["max"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
            except:
                warnings.append({"field": key, "message": "Keine gültige Zahl", "value": value})

        if key in ["equity", "equity_to_use", "eigenkapital"] and value:
            try:
                val = float(value)
                if val < 0:
                    errors.append({"field": key, "message": "Eigenkapital kann nicht negativ sein", "value": val})
            except:
                warnings.append({"field": key, "message": "Keine gültige Zahl", "value": value})

        if key in ["living_space", "wohnflaeche"] and value:
            try:
                val = float(value)
                rule = VALIDATION_RULES["living_space"]
                if val < rule["min"]:
                    warnings.append({"field": key, "message": f"Wohnfläche unter {rule['min']} m² unüblich", "value": val})
                elif val > rule["max"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
            except:
                pass

        if key in ["year_built", "baujahr"] and value:
            try:
                val = int(value)
                rule = VALIDATION_RULES["year_built"]
                max_year = current_year + rule["max_offset"]
                if val < rule["min"]:
                    errors.append({"field": key, "message": f"Baujahr vor {rule['min']} ungültig", "value": val})
                elif val > max_year:
                    errors.append({"field": key, "message": "Baujahr in der Zukunft", "value": val})
            except:
                pass

    # Cross-field validation
    purchase_price = data.get("purchase_price") or data.get("kaufpreis")
    loan_amount = data.get("loan_amount") or data.get("darlehenssumme")
    equity = data.get("equity") or data.get("equity_to_use") or data.get("eigenkapital")

    if purchase_price and equity:
        try:
            if float(equity) > float(purchase_price) * 1.5:
                warnings.append({
                    "field": "equity",
                    "message": "Eigenkapital > 150% des Kaufpreises",
                    "value": f"{equity} vs {purchase_price}"
                })
        except:
            pass

    if purchase_price and loan_amount:
        try:
            if float(loan_amount) > float(purchase_price) * 1.2:
                warnings.append({
                    "field": "loan_amount",
                    "message": "Darlehenssumme > 120% des Kaufpreises (Vollfinanzierung?)",
                    "value": f"{loan_amount} vs {purchase_price}"
                })
        except:
            pass

    # If strict mode, warnings become errors
    if request.strict:
        errors.extend(warnings)
        warnings = []

    return ValidationResponse(
        valid=len(errors) == 0,
        normalized_data=data,
        errors=errors,
        warnings=warnings,
        auto_corrections=corrections
    )


# ============================================================
# PIPELINE ENDPOINTS (volle Business-Logik in Python)
# n8n ruft diese auf – macht selbst nur noch Trigger + OneDrive
# ============================================================

import seatable as db
import case_logic as cases
import readiness as rdns
import notify
import traceback


@app.get("/debug/seatable")
async def debug_seatable():
    """Testet SeaTable Verbindung und gibt Details zurück"""
    import requests as _req
    results = {}
    results["env"] = {
        "SEATABLE_API_TOKEN_set": bool(os.getenv("SEATABLE_API_TOKEN")),
        "SEATABLE_BASE_UUID_set": bool(os.getenv("SEATABLE_BASE_UUID")),
        "SEATABLE_BASE_URL": os.getenv("SEATABLE_BASE_URL", "https://cloud.seatable.io"),
    }

    try:
        db.invalidate_token()
        token = db._get_access_token()
        results["auth"] = "ok"
        results["uuid"] = db._get_uuid()
        results["api_url"] = db._api("rows/")
    except Exception as e:
        results["auth"] = f"FAILED: {e}"
        return results

    # Tabellen auflisten
    try:
        meta_url = db._api("metadata/")
        r = _req.get(meta_url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if r.ok:
            tables = [t["name"] for t in r.json().get("metadata", {}).get("tables", [])]
            results["tables_in_base"] = tables
        else:
            results["tables_in_base"] = f"{r.status_code}: {r.text[:200]}"
    except Exception as e:
        results["tables_in_base"] = f"FAILED: {e}"

    # fin_cases laden
    try:
        rows = db.list_rows("fin_cases")
        results["fin_cases"] = f"ok – {len(rows)} rows"
    except Exception as e:
        results["fin_cases"] = f"FAILED: {e}"

    # processed_emails laden
    try:
        rows = db.list_rows("processed_emails")
        results["processed_emails"] = f"ok – {len(rows)} rows"
    except Exception as e:
        results["processed_emails"] = f"FAILED: {e}"

    return results

class ProcessEmailRequest(BaseModel):
    provider_message_id: str
    conversation_id: Optional[str] = None
    from_email: str
    from_name: Optional[str] = ""
    subject: Optional[str] = ""
    body_text: Optional[str] = ""
    body_html: Optional[str] = ""
    received_at: Optional[str] = None
    # Anhänge als base64 (key = Dateiname, value = base64-String)
    attachments: Optional[dict] = {}
    # OneDrive Folder ID wenn bereits bekannt (z.B. nach Upload durch n8n)
    onedrive_folder_id: Optional[str] = None

class ProcessEmailResponse(BaseModel):
    action: str          # 'processed' | 'skipped' | 'triage' | 'error'
    case_id: Optional[str] = None
    is_new_case: bool = False
    status: Optional[str] = None
    reason: Optional[str] = None
    # Instruktionen für n8n
    onedrive_folder_id: Optional[str] = None
    needs_onedrive_folder: bool = False
    files_to_upload: list = []
    readiness: Optional[dict] = None

@app.post("/process-email")
async def process_email(request: ProcessEmailRequest):
    """
    Vollständige E-Mail-Verarbeitungs-Pipeline.
    n8n sendet rohe E-Mail-Daten + Anhänge (base64), Python macht den Rest.
    """
    logger.info(f"process-email: {request.from_email} / {request.subject[:60] if request.subject else ''}")
    try:
        return await _process_email_impl(request)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"process-email unhandled error: {tb}")
        raise HTTPException(status_code=500, detail={"error": str(e), "traceback": tb})


async def _process_email_impl(request: ProcessEmailRequest):

    # 1. Dedup-Check
    if db.is_email_processed(request.provider_message_id):
        logger.info(f"E-Mail bereits verarbeitet: {request.provider_message_id}")
        return ProcessEmailResponse(action="skipped", reason="already_processed")

    # 2. Gatekeeper
    gate = cases.gatekeeper(request.from_email, request.subject, request.conversation_id)
    _att_filenames = list((request.attachments or {}).keys())
    _log_kwargs = dict(
        from_email=request.from_email,
        subject=request.subject,
        conversation_id=request.conversation_id,
        attachments_count=len(_att_filenames),
        attachments_hashes=_att_filenames,
    )
    if not gate["pass"]:
        db.log_processed_email(request.provider_message_id, "skipped", gate["reason"], **_log_kwargs)
        return ProcessEmailResponse(action="skipped", reason=gate["reason"])

    # Sofort als "in Verarbeitung" markieren (Dedup-Lock)
    db.log_processed_email(request.provider_message_id, "processing", "lock", **_log_kwargs)

    # 3. KI-Parsing über bestehenden parse-email Logic
    parsed = {}
    try:
        email_text = f"Subject: {request.subject}\nFrom: {request.from_email}\n\n{request.body_text}"
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": """Du bist ein E-Mail-Parser für Baufinanzierungsfälle.
Analysiere die E-Mail und extrahiere strukturierte Daten.
Antworte NUR mit JSON:
{
  "mail_type": "new_request" | "reply",
  "is_relevant": true | false,
  "applicant_firstName": string | null,
  "applicant_lastName": string | null,
  "partner_email": string | null,
  "referenced_case_id": string | null,
  "purchase_price": number | null,
  "loan_amount": number | null,
  "equity_to_use": number | null,
  "object_type": "ETW"|"EFH"|"DHH"|"RH"|"MFH"|null,
  "usage": "Eigennutzung"|"Kapitalanlage"|null,
  "extracted_answers": {
    "APPROVE_IMPORT": null,
    "WAIT_FOR_DOCS": null,
    "accept_stale_kontoauszug": null,
    "accept_stale_gehaltsnachweis": null,
    "has_joint_account": null
  },
  "notes": string | null
}"""},
                {"role": "user", "content": f"is_broker_reply: {gate['is_internal_reply']}\n\n{email_text[:4000]}"},
            ],
            response_format={"type": "json_object"},
        )
        parsed = json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"AI parse failed: {e}")
        parsed = {"mail_type": "new_request", "is_relevant": True}

    # Relevanz prüfen
    is_broker = gate["is_internal_reply"]
    is_relevant = is_broker or (
        parsed.get("is_relevant") and
        parsed.get("applicant_firstName") and
        parsed.get("applicant_lastName")
    )
    if not is_relevant:
        db.log_processed_email(request.provider_message_id, "not_relevant", "irrelevant", **_log_kwargs)
        return ProcessEmailResponse(action="skipped", reason="not_relevant")

    # 4. Case Matching
    first_name = parsed.get("applicant_firstName") or ""
    last_name = parsed.get("applicant_lastName") or ""
    applicant_name = f"{first_name} {last_name}".strip()

    match = cases.match_case(
        from_email=request.from_email,
        applicant_last_name=last_name,
        referenced_case_id=parsed.get("referenced_case_id"),
        conversation_id=request.conversation_id,
        mail_type=parsed.get("mail_type", "new_request"),
        actor=gate["actor"],
    )

    case_id = match["case_id"]
    is_new = match["action"] == "create"
    needs_folder = False

    # 5. Case erstellen oder aktualisieren
    if match["action"] == "create":
        facts = {
            "property_data": {
                "purchase_price": parsed.get("purchase_price"),
                "object_type": parsed.get("object_type"),
                "usage": parsed.get("usage"),
            },
            "financing_data": {
                "loan_amount": parsed.get("loan_amount"),
                "equity_to_use": parsed.get("equity_to_use"),
            },
        }
        cases.create_case(
            case_id=case_id,
            applicant_name=applicant_name,
            partner_email=parsed.get("partner_email") or request.from_email,
            partner_phone="",
            conversation_id=request.conversation_id,
            facts=facts,
        )
        needs_folder = True  # n8n soll OneDrive-Ordner erstellen

    elif match["action"] == "update":
        cases.update_case_conversation(case_id, request.conversation_id)

        # Antworten verarbeiten
        extracted = parsed.get("extracted_answers") or {}
        answers = {k: v for k, v in {
            "purchase_price": parsed.get("purchase_price"),
            "loan_amount": parsed.get("loan_amount"),
            "equity_to_use": parsed.get("equity_to_use"),
            "object_type": parsed.get("object_type"),
            "usage": parsed.get("usage"),
        }.items() if v is not None}

        overrides = {k: v for k, v in extracted.items() if v is not None}

        if answers:
            cases.save_answers(case_id, answers, actor=gate["actor"])
        if overrides:
            cases.save_answers(case_id, {}, actor=gate["actor"], overrides=overrides)

    elif match["action"] == "triage":
        db.log_processed_email(request.provider_message_id, "triage", "no_case_match", **_log_kwargs)
        return ProcessEmailResponse(action="triage", reason="no_case_match")

    # 6. Anhänge für Upload vorbereiten
    files_to_upload = []
    if request.attachments:
        for filename, b64_data in request.attachments.items():
            files_to_upload.append({"filename": filename, "data_base64": b64_data})

    # 7. Readiness Check (nur bei Update oder nach kurzer Verarbeitung)
    readiness_result = None
    if match["action"] == "update" and not needs_folder:
        try:
            readiness_result = rdns.check_readiness(case_id)
            notify.dispatch_notifications(case_id, readiness_result)
        except Exception as e:
            logger.error(f"Readiness check failed: {e}")

    # 8. Log
    db.log_processed_email(request.provider_message_id, parsed.get("mail_type", "new_request"), match["action"], case_id, **_log_kwargs)

    return ProcessEmailResponse(
        action="processed",
        case_id=case_id,
        is_new_case=is_new,
        status=readiness_result.get("status") if readiness_result else "INTAKE",
        onedrive_folder_id=request.onedrive_folder_id,
        needs_onedrive_folder=needs_folder,
        files_to_upload=files_to_upload,
        readiness=readiness_result,
    )


class ProcessDocumentRequest(BaseModel):
    case_id: str
    filename: str
    data_base64: str        # Base64 kodierte Datei
    mime_type: Optional[str] = "application/pdf"
    onedrive_file_id: Optional[str] = None

class ProcessDocumentResponse(BaseModel):
    success: bool
    case_id: str
    doc_type: Optional[str] = None
    confidence: Optional[str] = None
    facts_merged: bool = False
    readiness: Optional[dict] = None
    error: Optional[str] = None

@app.post("/process-document", response_model=ProcessDocumentResponse)
async def process_document(request: ProcessDocumentRequest):
    """
    Analysiert ein Dokument und mergt die Facts in den Case.
    n8n lädt Datei von OneDrive herunter und sendet sie base64-kodiert.
    """
    logger.info(f"process-document: {request.case_id} / {request.filename}")

    # 1. Datei dekodieren
    try:
        file_bytes = base64.b64decode(request.data_base64)
    except Exception as e:
        return ProcessDocumentResponse(success=False, case_id=request.case_id, error=f"Base64 decode failed: {e}")

    # 2. Dokument analysieren (bestehende Logik)
    try:
        result = analyze_with_gpt4o(file_bytes, request.mime_type, request.filename)
    except Exception as e:
        logger.error(f"Document analysis failed: {e}")
        # In SeaTable als Fehler speichern
        db.create_row("fin_documents", {
            "caseId": request.case_id,
            "onedrive_file_id": request.onedrive_file_id or "",
            "file_name": request.filename,
            "doc_type": "error",
            "processing_status": "error",
            "error_message": str(e),
            "processed_at": __import__("datetime").datetime.utcnow().isoformat(),
        })
        return ProcessDocumentResponse(success=False, case_id=request.case_id, error=str(e))

    # 3. In fin_documents speichern
    extracted = result.get("extracted_data") or {}
    db.create_row("fin_documents", {
        "caseId": request.case_id,
        "onedrive_file_id": request.onedrive_file_id or "",
        "file_name": request.filename,
        "doc_type": result.get("doc_type", "Sonstiges"),
        "extracted_data": json.dumps(extracted),
        "processing_status": "completed",
        "processed_at": __import__("datetime").datetime.utcnow().isoformat(),
    })

    # 4. Facts in Case mergen
    try:
        doc_type = result.get("doc_type", "")
        new_facts = _map_extracted_to_facts(doc_type, extracted)
        if new_facts:
            cases.save_facts(request.case_id, new_facts, source=f"document:{doc_type}")
        facts_merged = bool(new_facts)
    except Exception as e:
        logger.error(f"Facts merge failed: {e}")
        facts_merged = False

    # 5. Readiness Check
    readiness_result = None
    try:
        readiness_result = rdns.check_readiness(request.case_id)
        notify.dispatch_notifications(request.case_id, readiness_result)
    except Exception as e:
        logger.error(f"Readiness check after document failed: {e}")

    return ProcessDocumentResponse(
        success=True,
        case_id=request.case_id,
        doc_type=result.get("doc_type"),
        confidence=result.get("confidence"),
        facts_merged=facts_merged,
        readiness=readiness_result,
    )


def _map_extracted_to_facts(doc_type: str, extracted: dict) -> dict:
    """Mappt extrahierte Dokument-Daten auf facts_extracted Struktur"""
    facts = {}

    if doc_type in ("Ausweiskopie",):
        facts["applicant_data"] = {
            "vorname": extracted.get("Vorname"),
            "nachname": extracted.get("Nachname"),
            "geburtsdatum": extracted.get("Geburtsdatum"),
            "geburtsort": extracted.get("Geburtsort"),
            "nationalitaet": extracted.get("Nationalität") or extracted.get("Nationalitaet"),
        }
        facts["id_data"] = {
            "ausweisnummer": extracted.get("Ausweisnummer"),
            "gueltig_bis": extracted.get("Gültig bis"),
        }

    elif doc_type in ("Gehaltsnachweis", "Gehaltsabrechnung", "Gehaltsabrechnung Dezember", "Lohnsteuerbescheinigung"):
        facts["income_data"] = {
            "arbeitgeber": extracted.get("Arbeitgeber"),
            "brutto": extracted.get("Brutto"),
            "netto": extracted.get("Netto"),
            "steuerklasse": extracted.get("Steuerklasse"),
        }
        facts["employment_data"] = {
            "arbeitgeber": extracted.get("Arbeitgeber"),
            "employment_type": "Angestellter",
        }

    elif doc_type in ("Kontoauszug",):
        facts["banking_data"] = {
            "bank": extracted.get("Bank"),
            "iban": extracted.get("IBAN"),
            "kontostand": extracted.get("Kontostand"),
        }

    elif doc_type in ("Exposé",):
        facts["property_data"] = {
            "purchase_price": extracted.get("Kaufpreis") or extracted.get("purchase_price"),
            "address": extracted.get("Adresse"),
            "object_type": extracted.get("Objekttyp") or extracted.get("object_type"),
            "living_area": extracted.get("Wohnfläche"),
            "year_built": extracted.get("Baujahr"),
        }

    elif doc_type in ("Selbstauskunft",):
        facts["applicant_data"] = {
            "vorname": extracted.get("Vorname") or extracted.get("applicant_first_name"),
            "nachname": extracted.get("Nachname") or extracted.get("applicant_last_name"),
            "email": extracted.get("E-Mail") or extracted.get("applicant_email"),
            "telefon": extracted.get("Telefon") or extracted.get("applicant_phone"),
            "geburtsdatum": extracted.get("Geburtsdatum"),
            "familienstand": extracted.get("Familienstand"),
        }

    # Leere Werte entfernen
    def clean(d):
        if isinstance(d, dict):
            return {k: clean(v) for k, v in d.items() if v is not None and v != ""}
        return d

    return clean(facts)


class IngestAnswersRequest(BaseModel):
    case_id: str
    actor: str = "partner"      # "partner" | "broker"
    source: str = "webhook"
    answers: dict = {}
    overrides: Optional[dict] = None  # APPROVE_IMPORT, WAIT_FOR_DOCS, etc.

class IngestAnswersResponse(BaseModel):
    success: bool
    case_id: str
    status: Optional[str] = None
    readiness: Optional[dict] = None
    ready_for_import: bool = False
    error: Optional[str] = None

@app.post("/ingest-answers", response_model=IngestAnswersResponse)
async def ingest_answers(request: IngestAnswersRequest):
    """
    Verarbeitet Antworten/Korrekturen von Partner oder Broker.
    Speichert in answers_user / manual_overrides und führt Readiness Check durch.
    """
    logger.info(f"ingest-answers: {request.case_id} / actor={request.actor}")

    try:
        # Antworten speichern
        cases.save_answers(
            case_id=request.case_id,
            answers=request.answers,
            actor=request.actor,
            overrides=request.overrides,
        )

        # Readiness Check
        readiness_result = rdns.check_readiness(request.case_id)
        try:
            notify.dispatch_notifications(request.case_id, readiness_result)
        except Exception as notify_err:
            logger.error(f"dispatch_notifications failed (non-fatal): {notify_err}")

        return IngestAnswersResponse(
            success=True,
            case_id=request.case_id,
            status=readiness_result["status"],
            readiness=readiness_result,
            ready_for_import=readiness_result["status"] == "READY_FOR_IMPORT",
        )
    except Exception as e:
        logger.error(f"ingest-answers failed: {e}")
        return IngestAnswersResponse(success=False, case_id=request.case_id, error=str(e))


class FullReadinessRequest(BaseModel):
    case_id: str
    send_notifications: bool = True

@app.post("/full-readiness-check")
async def full_readiness_check(request: FullReadinessRequest):
    """
    Vollständiger Readiness Check mit SeaTable-Zugriff und E-Mail-Versand.
    Ersetzt den n8n Readiness Router komplett.
    """
    logger.info(f"full-readiness-check: {request.case_id}")
    try:
        result = rdns.check_readiness(request.case_id)
        if request.send_notifications:
            notify.dispatch_notifications(request.case_id, result)
        return result
    except Exception as e:
        logger.error(f"full-readiness-check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class UpdateOneDriveFolderRequest(BaseModel):
    case_id: str
    onedrive_folder_id: str

@app.post("/update-onedrive-folder")
async def update_onedrive_folder(request: UpdateOneDriveFolderRequest):
    """n8n meldet erstellten OneDrive-Ordner zurück"""
    try:
        cases.update_onedrive_folder(request.case_id, request.onedrive_folder_id)
        # Readiness Check starten
        result = rdns.check_readiness(request.case_id)
        # Notifications sind nicht-kritisch – Fehler hier dürfen nie einen 500 erzeugen
        try:
            notify.dispatch_notifications(request.case_id, result)
        except Exception as notify_err:
            logger.error(f"dispatch_notifications failed (non-fatal): {notify_err}")
        return {"success": True, "case_id": request.case_id, "status": result["status"]}
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"update-onedrive-folder failed: {tb}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
