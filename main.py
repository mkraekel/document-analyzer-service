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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
