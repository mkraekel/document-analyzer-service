"""
Document Analyzer Service
Analysiert PDFs und Bilder mit GPT-4o Vision
"""

import os
import sys
import asyncio
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
import fitz  # PyMuPDF - PDF to image for scanned docs
from PIL import Image, ImageOps
from io import BytesIO
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from openai import OpenAI
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Document Analyzer", version="1.0.0")

# ── Globale JWT Auth Middleware ───────────────────────────────────
from auth import JWTAuthMiddleware
app.add_middleware(JWTAuthMiddleware)

# ── Legacy Redirect ───────────────────────────────────────────────
from fastapi.responses import RedirectResponse as _Redirect

@app.get("/dashboard")
async def legacy_dashboard_redirect():
    return _Redirect(url="/app", status_code=302)

# ── Auth ──────────────────────────────────────────────────────────
import auth

@app.post("/api/auth/login")
async def login(req: auth.LoginRequest):
    user = auth.authenticate_user(req.username, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="Ungueltige Anmeldedaten")
    token = auth.create_access_token(user)
    return auth.TokenResponse(access_token=token, user=user)

# ── Dashboard API (geschuetzt) ───────────────────────────────────
from dashboard import router as dashboard_router
app.include_router(dashboard_router, dependencies=[auth.Depends(auth.get_current_user)])

# ── React Dashboard SPA ──────────────────────────────────────────
_DASHBOARD_DIST = Path(__file__).parent / "dashboard" / "dist"

if _DASHBOARD_DIST.is_dir():
    # Static assets (JS, CSS, etc.)
    app.mount("/app/assets", StaticFiles(directory=_DASHBOARD_DIST / "assets"), name="dashboard-assets")

    @app.get("/app/{rest:path}")
    async def serve_spa(rest: str = ""):
        """Serve React SPA – alle /app/* Routen liefern index.html aus."""
        index = _DASHBOARD_DIST / "index.html"
        if index.exists():
            return HTMLResponse(index.read_text())
        raise HTTPException(404, "Dashboard nicht gebaut. Bitte 'npm run build' im dashboard/ Ordner ausfuehren.")
else:
    logger.info("React Dashboard nicht gefunden (dashboard/dist/) – nur API verfuegbar")


@app.on_event("startup")
async def startup_event():
    """Start DB pool init in background so health endpoint responds immediately."""
    import asyncio
    import seatable as _db
    if hasattr(_db, 'init_pool'):
        # Fire-and-forget: DB init runs in background thread, doesn't block startup
        asyncio.get_event_loop().run_in_executor(None, _db.init_pool)
        logger.info("DB pool initialization started (background)")

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
    "Grundbuch", "Energieausweis",
    "Kaufvertrag", "Mietvertrag",
    "Handelsregisterauszug", "Gesellschaftsvertrag",
    "Sonstiges"
]

EXTRACTION_PROMPT = """Analysiere dieses Dokument und extrahiere alle relevanten Daten.

Dokumenttyp erkennen aus: {doc_types}

WICHTIGE KLASSIFIZIERUNGS-HINWEISE (MUSS beachtet werden):
- Reisepass, Personalausweis (Vorder- UND Rückseite!), Aufenthaltstitel, Identitaetsdokument → "Ausweiskopie"
- Personalausweis-Rückseite erkennt man an: Anschrift/Adresse, Augenfarbe, Größe, Behörde/Authority, MRZ-Zeile (IDD<<...), Bundesdruckerei → IMMER "Ausweiskopie"!
- Gehaltsabrechnung, Entgeltnachweis, Entgeltabrechnung, Lohnabrechnung, Lohnausweis, Verdienstbescheinigung, Brutto-Netto-Abrechnung, Bezuegemitteilung → "Gehaltsnachweis"
- Renteninformation, Rentenauskunft, Deutsche Rentenversicherung, Renteninfo 20XX → "Renteninfo"
- Grundbuchauszug, Grundbuchblatt, GB-Auszug, Amtsgericht Grundbuch → "Grundbuch"
- Immobilienexposé, Verkaufsexposé, Objektbeschreibung mit Kaufpreis, Immobilienangebot → "Exposé"
- Finanzstatus, Vermoegensaufstellung, Depotauszug, Depotnachweis, Sparkontoauszug, Kontouebersicht mit Salden/Guthaben, Bankkonten-Uebersicht, Kontosalden, Sparkassenkonten-Uebersicht → "Eigenkapitalnachweis"
- Wohnungsgrundriss, Grundrissplan, Grundrisszeichnung, Flurkarte, Lageplan → "Grundriss"
- Energiepass, Energetischer Ausweis, Energieverbrauchsausweis, Energiebedarfsausweis → "Energieausweis"
- Hausbeschreibung, Hausunterlagen mit technischen Details, Objektbeschreibung (technisch) → "Baubeschreibung"
- Teilungserklaerung, Aufteilungsplan, Gemeinschaftsordnung → "Teilungserklärung"
- Wohnflaechenberechnung, Flaechenberechnung, DIN277 Berechnung → "Wohnflächenberechnung"
- Lohnsteuerbescheinigung, Elektronische Lohnsteuerbescheinigung → "Lohnsteuerbescheinigung"
- Einkommensteuerbescheid, Bescheid fuer 20XX → "Steuerbescheid"
- Einkommensteuererklaerung, Anlage N, Anlage V, Anlage Vorsorgeaufwand → "Steuererklärung"
- SCHUFA-Auskunft, Bonitaetsauskunft, Selbstauskunft SCHUFA → "Selbstauskunft"
- Kaufvertrag, Notarieller Kaufvertrag, Kaufvertragsentwurf → "Kaufvertrag"
- Mietvertrag, Wohnungsmietvertrag → "Mietvertrag"
- Handelsregisterauszug, HRA, HRB Auszug → "Handelsregisterauszug"
- Foto vom Haus/Wohnung von aussen, Strassenseite, Fassade → "Objektbild Außen"
- Foto vom Haus/Wohnung von innen, Zimmer, Kueche, Bad → "Objektbild Innen"
- Bausparvertrag, Bausparkasse, Schwäbisch Hall, Wüstenrot → "Bausparvertrag"
- Darlehensvertrag, Kreditvertrag, bestehender Kredit → "Darlehensvertrag"
- Betriebswirtschaftliche Auswertung → "BWA"
- Krankenversicherungsnachweis, Krankenversicherungskarte, PKV Bescheinigung → "Nachweis Krankenversicherung"
- WICHTIG: Kontoauszuege die MEHRERE Konten mit Salden zeigen = "Eigenkapitalnachweis", NICHT "Kontoauszug". "Kontoauszug" ist NUR fuer einzelne Kontoauszuege mit Transaktionsliste.
- WICHTIG: Depotauszuege, Wertpapieraufstellungen, Fondsanteile = "Depotnachweis", NICHT "Kontoauszug" oder "Sonstiges".
- WICHTIG: Private Rentenversicherung, Riester-Vertrag, Ruerup-Rente = "Private Rentenversicherung", NICHT "Sonstiges".
- WICHTIG: Private Lebensversicherung, Risikolebensversicherung, Kapitallebensversicherung = "Private Lebensversicherung", NICHT "Sonstiges".
- Verwende "Sonstiges" WIRKLICH NUR als letzten Ausweg, wenn das Dokument in KEINEN der obigen Typen passt. Lieber eine Kategorie waehlen die ungefaehr passt als "Sonstiges".

Extrahiere je nach Dokumenttyp:

Für Ausweise (inkl. Reisepass, Personalausweis - Vorder- UND Rückseite):
- Vorname, Nachname, Geburtsdatum, Geburtsort, Nationalität
- Ausweisnummer, Gültig bis, Ausstellungsbehörde
- Bei Rückseite: Anschrift (Strasse, Hausnummer, PLZ, Ort), Augenfarbe, Größe
- MRZ-Zeile auslesen: Name und Ausweisnummer verifizieren

Für Gehaltsnachweise (inkl. Entgeltnachweis, Gehaltsabrechnung):
- Vorname, Nachname
- Strasse, Hausnummer, PLZ, Ort (Wohnadresse des Arbeitnehmers, falls angegeben)
- Arbeitgeber, Brutto, Netto, Auszahlungsbetrag, Monat/Jahr
- Steuerklasse, Sozialversicherungsbeiträge
- WICHTIG: "Netto" ist das steuerliche Netto VOR Abzügen (VWL, Kirchensteuer, etc.). "Auszahlungsbetrag" ist der tatsächlich ausgezahlte Betrag. Beide Werte extrahieren wenn vorhanden!

Für Kontoauszüge:
- Bank, IBAN, Kontostand, Zeitraum
- months_covered (Anzahl der abgedeckten Monate als Zahl, z.B. 1 wenn nur ein Monat, 3 wenn Quartal/3 Monate enthalten)
- Regelmäßige Eingänge/Ausgänge

Für Selbstauskunft:
- Anrede (Herr/Frau), Vorname, Nachname, Geburtsdatum, Familienstand
- Telefon, E-Mail, Steuer-ID
- Strasse, Hausnummer, PLZ, Ort (Wohnadresse)
- Beruf, Beschäftigt seit (Datum), Einkommen
- Anzahl Kinder

Für Immobilien-Dokumente (Exposé, Grundbuch, Kaufvertrag, Energieausweis, etc.):
- Straße, Hausnummer, PLZ, Ort (Objektadresse - EINZELN aufteilen, nicht als ein String!)
- Wohnfläche, Baujahr, Grundstücksgröße
- Kaufpreis
- Objekttyp (MUSS einer sein: ETW, EFH, DHH, RH, MFH, Grundstück)
- Nutzungsart (MUSS einer sein: Eigennutzung, Kapitalanlage, Teilvermietet)

Für Steuerbescheide:
- Steuerjahr, zu versteuerndes Einkommen
- Einkünfte aus nichtselbständiger Arbeit
- Einkünfte aus Gewerbebetrieb/selbständiger Arbeit
- Einkünfte aus Vermietung und Verpachtung
- Erstattung/Nachzahlung

Für Steuererklärungen:
- Steuerjahr
- Einkünfte aus nichtselbständiger Arbeit
- Einkünfte aus Vermietung und Verpachtung
- Werbungskosten

Für BWA (Betriebswirtschaftliche Auswertung):
- Zeitraum (Monat/Jahr), Firma/Unternehmen
- Umsatzerlöse, Gesamtkosten
- Vorläufiges Ergebnis (Gewinn/Verlust)

Für Jahresabschluss:
- Jahr, Firma/Unternehmen
- Bilanzsumme, Umsatzerlöse
- Jahresüberschuss/Gewinn

Für Summen und Saldenliste:
- Zeitraum, Firma/Unternehmen
- Kontensalden (Zusammenfassung)

Für Renteninfo:
- Prognostizierte monatliche Rente (bei Regelaltersgrenze)
- Bisher erworbene Rentenansprüche
- Rentenversicherungsnummer

Für Eigenkapitalnachweise (Finanzstatus, Vermögensaufstellung):
- Gesamtguthaben/Gesamtvermögen
- Einzelne Konten mit jeweiligem Saldo
- Bank/Institut

Für Depotnachweis:
- Gesamtdepotwert
- Bank/Broker
- Einzelne Positionen (optional)

Für Kaufvertrag:
- Kaufpreis, Straße, Hausnummer, PLZ, Ort (Objektadresse - EINZELN!)
- Käufer, Verkäufer, Notar
- Datum

Für Darlehensvertrag (bestehende Kredite):
- Bank/Kreditgeber, Restschuld, Zinssatz
- Monatliche Rate, Laufzeitende

Für Bausparvertrag:
- Bausparkasse, Bausparsumme, Angespartes Guthaben
- Tarif, Zuteilungsreif (ja/nein)

Für Mietvertrag:
- Kaltmiete, Warmmiete/Nebenkosten
- Mieter, Vermieter
- Objektadresse

Für Nachweis Krankenversicherung:
- PKV oder GKV
- Monatlicher Beitrag, Versicherer

Für Handelsregisterauszug:
- Firma, Sitz, Geschäftsführer
- HRB/HRA-Nummer, Rechtsform

Für Energieausweis:
- Energiekennwert (kWh/m²a), Energieeffizienzklasse
- Heizungsart, Baujahr (des Gebäudes)

WICHTIG: Verwende null für fehlende/unbekannte Werte. NIEMALS "N/A", "n/a", "nicht verfügbar", "unbekannt" oder "-" als Wert verwenden!

Antworte NUR mit validem JSON in diesem Format:
{{
  "doc_type": "erkannter Typ (MUSS einer der vorgegebenen Typen sein!)",
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


def pdf_pages_to_images(pdf_bytes: bytes, max_pages: int = 2, dpi: int = 200) -> list[tuple[bytes, str]]:
    """Rendert PDF-Seiten als PNG-Bilder (für gescannte Dokumente)."""
    images = []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for i in range(min(len(doc), max_pages)):
            pix = doc[i].get_pixmap(dpi=dpi)
            images.append((pix.tobytes("png"), "image/png"))
        doc.close()
    except Exception as e:
        logger.error(f"PDF-to-image Konvertierung fehlgeschlagen: {e}")
    return images


# Filename-Keywords → doc_type Mapping (Fallback wenn GPT "Sonstiges" sagt)
FILENAME_DOC_TYPE_HINTS = {
    "selbstauskunft": "Selbstauskunft",
    "schufa": "Selbstauskunft",
    "gehaltsnachweis": "Gehaltsnachweis",
    "gehaltsabrechnung": "Gehaltsnachweis",
    "gehaltsab": "Gehaltsnachweis",
    "lohnabrechnung": "Gehaltsnachweis",
    "entgeltnachweis": "Gehaltsnachweis",
    "entgeltabrechnung": "Gehaltsnachweis",
    "bezügemitteilung": "Gehaltsnachweis",
    "verdienstbescheinigung": "Gehaltsnachweis",
    "kontoauszug": "Kontoauszug",
    "kontoauszüge": "Kontoauszug",
    "ausweis": "Ausweiskopie",
    "personalausweis": "Ausweiskopie",
    "reisepass": "Ausweiskopie",
    "renteninfo": "Renteninfo",
    "renteninformation": "Renteninfo",
    "rentenauskunft": "Renteninfo",
    "steuerbescheid": "Steuerbescheid",
    "steuererklärung": "Steuererklärung",
    "steuererklaerung": "Steuererklärung",
    "einkommensteuer": "Steuererklärung",
    "lohnsteuerbescheinigung": "Lohnsteuerbescheinigung",
    "lohnsteuerbesch": "Lohnsteuerbescheinigung",
    "grundbuch": "Grundbuch",
    "grundbuchauszug": "Grundbuch",
    "exposé": "Exposé",
    "expose": "Exposé",
    "energieausweis": "Energieausweis",
    "energiepass": "Energieausweis",
    "baubeschreibung": "Baubeschreibung",
    "grundriss": "Grundriss",
    "teilungserklärung": "Teilungserklärung",
    "teilungserklaerung": "Teilungserklärung",
    "aufteilungsplan": "Teilungserklärung",
    "gemeinschaftsordnung": "Teilungserklärung",
    "wohnflächenberechnung": "Wohnflächenberechnung",
    "flächenberechnung": "Wohnflächenberechnung",
    "eigenkapitalnachweis": "Eigenkapitalnachweis",
    "depotauszug": "Depotnachweis",
    "depotnachweis": "Depotnachweis",
    "wertpapier": "Depotnachweis",
    "bausparvertrag": "Bausparvertrag",
    "kaufvertrag": "Kaufvertrag",
    "mietvertrag": "Mietvertrag",
    "bwa": "BWA",
    "jahresabschluss": "Jahresabschluss",
    "krankenversicherung": "Nachweis Krankenversicherung",
    "handelsregister": "Handelsregisterauszug",
}


def _filename_fallback_doc_type(filename: str) -> Optional[str]:
    """Versucht doc_type aus Dateiname zu erkennen (Fallback wenn GPT Sonstiges sagt)."""
    name_lower = filename.lower().rsplit(".", 1)[0]  # Extension entfernen
    # Direkte Keywords
    for keyword, doc_type in FILENAME_DOC_TYPE_HINTS.items():
        if keyword in name_lower:
            return doc_type
    return None


def _fix_image_orientation(file_bytes: bytes, mime_type: str) -> tuple[bytes, str]:
    """EXIF-Auto-Rotation für Kamerafotos. Gibt korrigierte Bytes + MIME zurück."""
    if not mime_type.startswith("image/"):
        return file_bytes, mime_type
    try:
        img = Image.open(BytesIO(file_bytes))
        rotated = ImageOps.exif_transpose(img)
        if rotated is not img:
            buf = BytesIO()
            fmt = "JPEG" if mime_type in ("image/jpeg", "image/jpg") else "PNG"
            rotated.save(buf, format=fmt)
            logger.info(f"EXIF-Rotation angewendet ({mime_type})")
            return buf.getvalue(), mime_type
    except Exception as e:
        logger.debug(f"EXIF-Rotation nicht möglich: {e}")
    return file_bytes, mime_type


def analyze_with_gpt4o(file_bytes: bytes, mime_type: str, filename: str) -> dict:
    """Analysiert Dokument mit GPT-4o Vision"""

    if not client:
        raise HTTPException(status_code=500, detail="OpenAI API Key nicht konfiguriert")

    # EXIF-Auto-Rotation für Kamerafotos (Personalausweis etc.)
    file_bytes, mime_type = _fix_image_orientation(file_bytes, mime_type)

    base64_data = base64.standard_b64encode(file_bytes).decode("utf-8")

    # Für PDFs: Erst Text extrahieren
    extracted_text = ""
    if mime_type == "application/pdf":
        extracted_text, page_count = extract_text_from_pdf(file_bytes)
        logger.info(f"PDF {filename}: {len(extracted_text)} Zeichen Text, {page_count} Seiten")

    # Prompt bauen
    prompt = EXTRACTION_PROMPT.format(doc_types=", ".join(DOC_TYPES))

    # System-Prompt separat für OpenAI Prompt Caching (statischer Prefix → 50% Input-Rabatt)
    system_msg = {"role": "system", "content": prompt}

    # PDFs: Immer Text-basiert analysieren (Vision API akzeptiert keine PDFs)
    if mime_type == "application/pdf":
        if extracted_text:
            logger.info(f"PDF mit Text: {len(extracted_text)} Zeichen")
            messages = [
                system_msg,
                {"role": "user", "content": f"Dokument: {filename}\n\nExtrahierter Text:\n{extracted_text[:15000]}"},
            ]
            model = "gpt-4o-mini"  # Günstiger für Text-Only
        else:
            # PDF ohne Text (gescannt) → als Bild rendern und Vision API nutzen
            page_images = pdf_pages_to_images(file_bytes, max_pages=2)
            if page_images:
                logger.info(f"Scan-PDF {filename}: {len(page_images)} Seiten als Bilder gerendert → Vision API")
                content = [{"type": "text", "text": f"Dokument: {filename}"}]
                for img_bytes_page, img_mime in page_images:
                    b64 = base64.standard_b64encode(img_bytes_page).decode("utf-8")
                    content.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:{img_mime};base64,{b64}", "detail": "high"}
                    })
                messages = [system_msg, {"role": "user", "content": content}]
                model = "gpt-4o"
            else:
                # Konvertierung fehlgeschlagen - Filename-Fallback
                fallback_type = _filename_fallback_doc_type(filename)
                logger.warning(f"PDF {filename}: kein Text, Bild-Rendering fehlgeschlagen. Fallback: {fallback_type}")
                return {
                    "doc_type": fallback_type or "Sonstiges",
                    "confidence": "low",
                    "error": "PDF ohne Text, Bild-Konvertierung fehlgeschlagen",
                    "meta": {"requires_ocr": True, "filename_fallback": bool(fallback_type)}
                }
    else:
        # Bilder: Vision-Analyse
        logger.info(f"Nutze Vision-Analyse für Bild: {filename}")
        content = [
            {"type": "text", "text": f"Dokument: {filename}"},
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{mime_type};base64,{base64_data}",
                    "detail": "high"
                }
            }
        ]
        messages = [system_msg, {"role": "user", "content": content}]
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

        result = json.loads(result_text.strip())

        # Filename-Fallback: wenn GPT "Sonstiges" sagt, aber Dateiname eindeutig ist
        if result.get("doc_type") == "Sonstiges":
            fallback = _filename_fallback_doc_type(filename)
            if fallback:
                logger.info(f"GPT sagte Sonstiges für {filename}, Filename-Fallback → {fallback}")
                result["doc_type"] = fallback
                result["meta"] = result.get("meta") or {}
                result["meta"]["filename_fallback"] = True

        return result

    except json.JSONDecodeError as e:
        logger.error(f"JSON Parse Error: {e}")
        fallback = _filename_fallback_doc_type(filename)
        return {
            "doc_type": fallback or "Sonstiges",
            "confidence": "low",
            "error": "JSON Parse Error",
            "raw_response": result_text[:500],
            "meta": {"filename_fallback": bool(fallback)}
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
    """Health Check Endpoint - responds immediately, even before DB is ready."""
    return {"status": "healthy", "service": "document-analyzer"}


# ============================================
# READINESS CHECK ENDPOINT
# (delegiert an readiness.py – EINZIGE Implementierung)
# ============================================

QUESTION_TEMPLATES = {
    # Finanzierungsdaten
    "purchase_price": {"question": "Wie hoch ist der Kaufpreis der Immobilie?", "example": "z.B. 350000"},
    "loan_amount": {"question": "Wie hoch soll das Darlehen sein?", "example": "z.B. 280000"},
    "equity_to_use": {"question": "Wie viel Eigenkapital soll eingesetzt werden?", "example": "z.B. 70000"},
    "object_type": {"question": "Um welche Art von Immobilie handelt es sich?", "example": "ETW, EFH, DHH, RH, MFH"},
    "usage": {"question": "Wie soll die Immobilie genutzt werden?", "example": "Eigennutzung oder Kapitalanlage"},
    # Antragsteller Stammdaten
    "applicant_first_name": {"question": "Wie lautet der Vorname des Antragstellers?", "example": "z.B. Max"},
    "applicant_last_name": {"question": "Wie lautet der Nachname des Antragstellers?", "example": "z.B. Mustermann"},
    "applicant_birth_date": {"question": "Was ist das Geburtsdatum des Antragstellers?", "example": "Format: JJJJ-MM-TT"},
    "employment_type": {"question": "Welche Beschäftigungsart liegt vor?", "example": "Angestellter, Selbstständig, Beamter, Rentner"},
    "net_income": {"question": "Wie hoch ist das monatliche Nettoeinkommen?", "example": "z.B. 3500"},
    # Wohnadresse
    "address_street": {"question": "Wie lautet die Straße der Wohnadresse?", "example": "z.B. Musterstraße"},
    "address_house_number": {"question": "Wie lautet die Hausnummer?", "example": "z.B. 12a"},
    "address_zip": {"question": "Wie lautet die PLZ der Wohnadresse?", "example": "z.B. 60311"},
    "address_city": {"question": "Wie lautet der Wohnort?", "example": "z.B. Frankfurt"},
    # Selbstständige Zusatz
    "self_employed_since": {"question": "Seit wann ist der Antragsteller selbstständig?", "example": "Format: JJJJ-MM-TT"},
    "profit_last_year": {"question": "Wie hoch war der Gewinn im Vorjahr?", "example": "z.B. 65000"},
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

    # Top-level Felder aus case_data uebernehmen (nicht in JSON-Blobs gespeichert)
    for top_key in ["applicant_name", "partner_email"]:
        if not result.get(top_key) and case_data.get(top_key):
            result[top_key] = case_data[top_key]

    return result


class ReadinessRequest(BaseModel):
    case_id: str


class ReadinessResponse(BaseModel):
    case_id: str
    ready: bool
    new_status: str
    completeness_percent: int
    missing_required: list
    missing_applicant_data: list
    missing_docs: list
    stale_docs: list
    warnings: list
    recommended_missing: list
    questions_partner: list
    questions_broker: list
    next_action: Optional[str] = None


@app.post("/check-readiness", response_model=ReadinessResponse)
async def check_readiness_endpoint(request: ReadinessRequest):
    """
    Prüft ob ein Fall bereit für den Europace Import ist.
    Delegiert an readiness.py – EINZIGE Implementierung.
    """
    import readiness as rdns

    result = await asyncio.to_thread(rdns.check_readiness, request.case_id)

    # Map readiness.py result → ReadinessResponse format
    missing_financing = result.get("missing_financing", [])
    missing_applicant_data = result.get("missing_applicant_data", [])
    missing_docs_raw = result.get("missing_docs", [])

    # missing_docs: readiness.py liefert dicts, Endpoint erwartet Strings
    missing_docs_str = []
    for d in missing_docs_raw:
        if isinstance(d, dict):
            dtype = d.get("type", "?")
            req = d.get("required", 1)
            found = d.get("found", 0)
            if req > 1:
                missing_docs_str.append(f"{dtype} ({req - found}x von {req} fehlt)")
            else:
                missing_docs_str.append(dtype)
        else:
            missing_docs_str.append(str(d))

    # Questions für fehlende Finanzierungsdaten + Antragstellerdaten
    questions_partner = []
    for key in missing_financing + missing_applicant_data:
        tpl = QUESTION_TEMPLATES.get(key)
        if tpl:
            questions_partner.append({
                "key": key,
                "question": tpl["question"],
                "example": tpl.get("example", ""),
            })

    # Next action
    next_action = None
    status = result.get("status", "")
    all_missing_data = missing_financing + missing_applicant_data
    if all_missing_data:
        tpl = QUESTION_TEMPLATES.get(all_missing_data[0], {})
        next_action = tpl.get("question", f"Bitte {all_missing_data[0]} angeben")
    elif missing_docs_str:
        next_action = f"Bitte hochladen: {missing_docs_str[0]}"
    elif result.get("stale_docs"):
        first_stale = result["stale_docs"][0]
        dtype = first_stale.get("type", "?") if isinstance(first_stale, dict) else str(first_stale)
        next_action = f"Dokument veraltet: {dtype} - bitte aktualisieren"
    elif status == "AWAITING_BROKER_CONFIRMATION":
        next_action = "Alle Unterlagen vollständig - bereit für Freigabe"

    return ReadinessResponse(
        case_id=request.case_id,
        ready=status == "READY_FOR_IMPORT",
        new_status=status,
        completeness_percent=result.get("completeness_percent", 0),
        missing_required=missing_financing,
        missing_applicant_data=missing_applicant_data,
        missing_docs=missing_docs_str,
        stale_docs=result.get("stale_docs", []),
        warnings=result.get("warnings", []),
        recommended_missing=result.get("recommended_missing", []),
        questions_partner=questions_partner,
        questions_broker=[],
        next_action=next_action,
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

EMAIL_PARSE_SYSTEM_PROMPT = """Analysiere E-Mails und extrahiere strukturierte Daten.

Aufgaben:
1. Erkenne den Intent (Absicht) der E-Mail
2. Extrahiere alle relevanten Daten
3. Erkenne ob es eine Antwort auf eine bestehende Anfrage ist

Antworte NUR mit validem JSON:
{
  "intent": "new_request|document_upload|question_answer|followup|status_inquiry|unknown",
  "confidence": 0.0-1.0,
  "is_reply": true|false,
  "reply_to_case_id": "CASE-XXX oder null",
  "language": "de|en",
  "urgency": "low|normal|high",

  "applicant_data": {
    "name": "Vor- und Nachname oder null",
    "first_name": "Vorname oder null",
    "last_name": "Nachname oder null",
    "email": "E-Mail oder null",
    "phone": "Telefon oder null"
  },

  "property_data": {
    "address": "Vollständige Adresse oder null",
    "street": "Straße oder null",
    "house_number": "Hausnummer oder null",
    "zip": "PLZ oder null",
    "city": "Stadt oder null",
    "purchase_price": Zahl oder null,
    "object_type": "ETW|EFH|DHH|RH|MFH oder null",
    "usage": "Eigennutzung|Kapitalanlage oder null",
    "living_space": Zahl oder null
  },

  "financing_data": {
    "loan_amount": Zahl oder null,
    "equity": Zahl oder null
  },

  "answered_questions": [
    {"key": "feldname", "value": "antwort", "original_text": "Originaltext"}
  ],

  "mentioned_documents": ["Gehaltsnachweis", "Ausweis", ...],

  "google_drive_links": ["https://drive.google.com/drive/folders/..." oder leere Liste],

  "summary": "Kurze Zusammenfassung in einem Satz"
}"""


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
    google_drive_links: list = []
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

    # GPT für Analyse — System/User getrennt für OpenAI Prompt Caching
    email_content = (
        f"E-Mail:\nVon: {request.from_address} ({request.from_name or 'Unbekannt'})\n"
        f"Betreff: {request.subject}\nText:\n{request.body[:8000]}"
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": EMAIL_PARSE_SYSTEM_PROMPT},
                {"role": "user", "content": email_content},
            ],
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
            google_drive_links=result.get("google_drive_links", []),
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
    "applicant_income": "Monatliches Nettoeinkommen",
    "monthly_rental_income": "Monatliche Mieteinnahmen (Kaltmiete)"
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

# DB pool is now initialized via FastAPI startup event (see startup_event above)
# This avoids blocking the server start and allows health checks to pass immediately


# ============================================
# TABLE SETUP – fehlende Spalten automatisch anlegen
# ============================================

REQUIRED_TABLE_COLUMNS = {
    "processed_emails": [
        {"column_name": "provider_message_id", "column_type": "text"},
        {"column_name": "mail_type", "column_type": "text"},
        {"column_name": "processing_result", "column_type": "text"},
        {"column_name": "case_id", "column_type": "text"},
        {"column_name": "from_email", "column_type": "text"},
        {"column_name": "subject", "column_type": "text"},
        {"column_name": "conversation_id", "column_type": "text"},
        {"column_name": "processed_at", "column_type": "text"},
        {"column_name": "attachments_count", "column_type": "number"},
        {"column_name": "attachments_hashes", "column_type": "long-text"},
    ],
    "fin_cases": [
        {"column_name": "case_id", "column_type": "text"},
        {"column_name": "applicant_name", "column_type": "text"},
        {"column_name": "partner_email", "column_type": "text"},
        {"column_name": "status", "column_type": "text"},
        {"column_name": "sources", "column_type": "text"},
        {"column_name": "facts_extracted", "column_type": "long-text"},
        {"column_name": "answers_user", "column_type": "long-text"},
        {"column_name": "manual_overrides", "column_type": "long-text"},
        {"column_name": "derived_values", "column_type": "long-text"},
        {"column_name": "docs_index", "column_type": "long-text"},
        {"column_name": "readiness", "column_type": "long-text"},
        {"column_name": "audit_log", "column_type": "long-text"},
        {"column_name": "conversation_ids", "column_type": "long-text"},
        {"column_name": "onedrive_folder_id", "column_type": "text"},
        {"column_name": "last_status_change", "column_type": "text"},
    ],
    "fin_documents": [
        {"column_name": "caseId", "column_type": "text"},
        {"column_name": "onedrive_file_id", "column_type": "text"},
        {"column_name": "file_name", "column_type": "text"},
        {"column_name": "doc_type", "column_type": "text"},
        {"column_name": "extracted_data", "column_type": "long-text"},
        {"column_name": "processing_status", "column_type": "text"},
        {"column_name": "error_message", "column_type": "text"},
        {"column_name": "processed_at", "column_type": "text"},
    ],
    "email_test_log": [
        {"column_name": "to", "column_type": "text"},
        {"column_name": "subject", "column_type": "text"},
        {"column_name": "body_text", "column_type": "long-text"},
        {"column_name": "body_html", "column_type": "long-text"},
        {"column_name": "logged_at", "column_type": "text"},
        {"column_name": "dry_run", "column_type": "checkbox"},
    ],
}


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
    google_drive_links: list = []
    readiness: Optional[dict] = None

@app.post("/process-email")
async def process_email(request: ProcessEmailRequest):
    """
    Vollständige E-Mail-Verarbeitungs-Pipeline.
    n8n sendet rohe E-Mail-Daten + Anhänge (base64), Python macht den Rest.
    Runs in thread pool to avoid blocking the event loop during GPT analysis.
    """
    import asyncio
    logger.info(f"process-email: {request.from_email} / {request.subject[:60] if request.subject else ''}")
    try:
        result = await asyncio.to_thread(_process_email_impl, request)

        # Google Drive async task must be started from the event loop (not from thread)
        gdrive_links = result.get("_gdrive_links")
        if gdrive_links and result.get("case_id"):
            logger.info(f"[{result['case_id']}] Google Drive links detected: {len(gdrive_links)} → triggering async processing")
            asyncio.create_task(_process_gdrive_async(result["case_id"], gdrive_links))

        # Remove internal field before returning
        result.pop("_gdrive_links", None)
        return ProcessEmailResponse(**result)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"process-email unhandled error: {tb}")
        raise HTTPException(status_code=500, detail={"error": str(e), "traceback": tb})


async def _process_gdrive_async(case_id: str, links: list):
    """Background task: process Google Drive links after email processing.
    Runs blocking work in thread pool to avoid blocking the event loop."""
    import asyncio
    try:
        import gdrive
        # Run blocking Google Drive + GPT analysis in thread pool
        result = await asyncio.to_thread(
            gdrive.process_google_drive_links, case_id=case_id, links=links
        )
        processed = result.get("files_processed", 0)
        logger.info(f"[{case_id}] Google Drive async: {processed} files processed")

        # Re-run readiness check after Google Drive files are analyzed
        if processed > 0:
            try:
                readiness_result = await asyncio.to_thread(rdns.check_readiness, case_id)
                notify.dispatch_notifications(case_id, readiness_result)
            except Exception as e:
                logger.error(f"[{case_id}] Readiness after gdrive failed: {e}")
    except Exception as e:
        logger.error(f"[{case_id}] Google Drive async processing failed: {e}")


# Interne Domains die NICHT als partner_email verwendet werden sollen
_INTERNAL_DOMAINS = {"alexander-heil.com"}


def _safe_partner_email(extracted_email: str | None, fallback_email: str) -> str:
    """Gibt eine sichere partner_email zurueck. Interne Adressen werden NICHT verwendet."""
    for email in [extracted_email, fallback_email]:
        if email and "@" in email:
            domain = email.rsplit("@", 1)[1].lower()
            if domain not in _INTERNAL_DOMAINS:
                return email
    # Alle Adressen sind intern - leeren String zurueckgeben
    logger.warning(f"Keine externe partner_email gefunden (extracted={extracted_email}, from={fallback_email})")
    return ""


def _process_email_impl(request: ProcessEmailRequest):
    """Synchronous implementation - runs in thread pool via asyncio.to_thread."""

    # 1. Dedup-Check
    if db.is_email_processed(request.provider_message_id):
        logger.info(f"E-Mail bereits verarbeitet: {request.provider_message_id}")
        return {"action": "skipped", "reason": "already_processed"}

    # 2. Gatekeeper
    gate = cases.gatekeeper(request.from_email, request.subject, request.conversation_id)
    _att_filenames = list((request.attachments or {}).keys())
    _body_short = (request.body_text or "")[:5000]
    _body_html_short = (request.body_html or "")[:50000]
    _log_kwargs = dict(
        from_email=request.from_email,
        subject=request.subject,
        conversation_id=request.conversation_id,
        attachments_count=len(_att_filenames),
        attachments_hashes=_att_filenames,
        body_text=_body_short,
        body_html=_body_html_short,
    )
    if not gate["pass"]:
        # Nicht loggen – diese Mails sind irrelevant (Newsletter, System, nicht-allowlisted)
        logger.debug(f"Gatekeeper blocked: {gate['reason']} | {request.from_email} | {request.subject}")
        return {"action": "skipped", "reason": gate["reason"]}

    # 3. KI-Parsing über bestehenden parse-email Logic
    parsed = {}
    try:
        email_text = f"Subject: {request.subject}\nFrom: {request.from_email}\n\n{request.body_text}"
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": """Du bist ein E-Mail-Parser für Baufinanzierungsfälle.
Analysiere die E-Mail und extrahiere strukturierte Daten.

WICHTIG ZUR NAMEN-ERKENNUNG:
- Der Absender (From) ist oft ein MAKLER/FINANZBERATER, NICHT der Antragsteller!
- Der Antragsteller-Name steht typischerweise im E-Mail-BETREFF (z.B. "Max Mustermann - München", "Carsten Brand")
- Der Antragsteller-Name kann auch im E-Mail-Text stehen ("Anfrage von Max Mustermann", "Kunde: ...")
- Die Signatur am Ende der E-Mail gehoert dem ABSENDER (Makler), NICHT dem Antragsteller
- Wenn der Betreff einen Personennamen enthaelt, ist das hoechstwahrscheinlich der Antragsteller
- applicant_firstName/lastName = Name des ANTRAGSTELLERS (Kunde), NICHT des Absenders!

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
    // Broker-Overrides – NUR bei is_broker_reply=true relevant:
    // APPROVE_IMPORT: true bei "FREIGABE", "GENEHMIGT", "approved", "ok starten", "Freigabe erteilt"
    // WAIT_FOR_DOCS: true bei "warte auf Dokumente", "WAIT_FOR_DOCS", "noch nicht vollständig"
    // has_joint_account: true wenn "Gemeinschaftskonto" oder "gemeinsames Konto" erwähnt
    // accept_stale_{typ}: true bei "ACCEPT_STALE {Typ}" oder "veraltete {Typ} akzeptiert"
    //   Beispiele: accept_stale_kontoauszug, accept_stale_gehaltsnachweis, accept_stale_grundbuch
    //   Namensregel: Dokumenttyp kleingeschrieben, Leerzeichen → Unterstrich
    // accept_missing_{typ}: true bei "ACCEPT_MISSING {Typ}" oder "{Typ} wird nicht benötigt"
    //   Beispiele: accept_missing_renteninfo, accept_missing_energieausweis
    // partnerId: string wenn Broker eine Europace-Partner-ID angibt
    // Gib NUR die Keys zurück die tatsächlich gesetzt werden sollen (keine null-Werte)
  },
  "notes": string | null
}
WICHTIG bei is_broker_reply=true: Scanne gezielt nach Kommandos wie FREIGABE, ACCEPT_STALE, ACCEPT_MISSING, WAIT_FOR_DOCS.
Der Broker kann mehrere Overrides in einer Mail setzen, z.B. "ACCEPT_STALE Kontoauszug" UND "ACCEPT_MISSING Renteninfo"."""},
                {"role": "user", "content": f"is_broker_reply: {gate['is_internal_reply']}\n\n{email_text[:4000]}"},
            ],
            response_format={"type": "json_object"},
        )
        parsed = json.loads(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"AI parse failed: {e}")
        parsed = {"mail_type": "new_request", "is_relevant": True}

    # 3b. Google Drive Links per Regex extrahieren (zuverlässiger als GPT)
    import re as _re
    _email_body_combined = (request.body_text or "") + " " + (request.body_html or "")
    _gdrive_pattern = r'https?://drive\.google\.com/(?:drive/folders|file/d|open\?id=)[^\s<>"\')\]]+'
    _gdrive_matches = _re.findall(_gdrive_pattern, _email_body_combined)
    if _gdrive_matches:
        # Deduplizieren
        _seen = set()
        _unique_links = []
        for link in _gdrive_matches:
            clean = link.rstrip(".,;:)")
            if clean not in _seen:
                _seen.add(clean)
                _unique_links.append(clean)
        parsed["google_drive_links"] = _unique_links
        logger.info(f"Google Drive links found in email: {len(_unique_links)}")

    # 3c. Investagon Links erkennen (manuelle Aktion erforderlich)
    _investagon_pattern = r'https?://[^\s<>"\')\]]*investagon\.[^\s<>"\')\]]+'
    _investagon_matches = _re.findall(_investagon_pattern, _email_body_combined)
    if _investagon_matches:
        _seen_inv = set()
        _unique_inv = []
        for link in _investagon_matches:
            clean = link.rstrip(".,;:)")
            if clean not in _seen_inv:
                _seen_inv.add(clean)
                _unique_inv.append(clean)
        parsed["investagon_links"] = _unique_inv
        logger.info(f"Investagon links found in email: {len(_unique_inv)}")

    # Relevanz prüfen
    is_broker = gate["is_internal_reply"]
    is_relevant = is_broker or (
        parsed.get("is_relevant") and
        parsed.get("applicant_firstName") and
        parsed.get("applicant_lastName")
    )
    if not is_relevant:
        db.log_processed_email(request.provider_message_id, "not_relevant", "irrelevant",
                               parsed_result=parsed, **_log_kwargs)
        return {"action": "skipped", "reason": "not_relevant"}

    # 4. Case Matching
    first_name = parsed.get("applicant_firstName") or ""
    last_name = parsed.get("applicant_lastName") or ""
    applicant_name = f"{first_name} {last_name}".strip()

    # Sanity check: Wenn der extrahierte Name dem Absender entspricht,
    # wurde wahrscheinlich der Broker statt des Antragstellers erkannt.
    # In dem Fall: Namen aus dem Betreff verwenden.
    if applicant_name and request.from_email and "@" in request.from_email:
        sender_prefix = request.from_email.split("@")[0].lower()
        sender_parts = set(sender_prefix.replace(".", " ").replace("-", " ").replace("_", " ").split())
        name_parts = set(applicant_name.lower().split())
        # Wenn >= 50% der Namensteile im Absender-Prefix vorkommen → Broker-Name
        if name_parts and sender_parts and len(name_parts & sender_parts) >= len(name_parts) * 0.5:
            import re as _re_name
            subject_clean = (request.subject or "").strip()
            # Reply-Prefixes entfernen
            subject_clean = _re_name.sub(r"^(Re:|AW:|Fwd:|WG:|Antw:)\s*", "", subject_clean, flags=_re_name.IGNORECASE).strip()
            # Orts-Suffix entfernen (z.B. " - Stuttgart")
            if " - " in subject_clean:
                subject_clean = subject_clean.split(" - ")[0].strip()
            if subject_clean and not subject_clean.startswith("CASE-"):
                logger.info(f"Applicant name corrected: '{applicant_name}' -> '{subject_clean}' (sender match detected)")
                applicant_name = subject_clean
                # Auch first/last für Case-Matching updaten
                parts = subject_clean.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = parts[-1]
                elif len(parts) == 1:
                    last_name = parts[0]
                    first_name = ""

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

    # 4b. Interne neue Mails (kein FWD/Reply) → Triage statt Case
    if gate.get("force_triage") and match["action"] == "create":
        logger.info(f"Internal new finance mail → triage (not a forwarded request): {request.subject}")
        db.log_processed_email(request.provider_message_id, parsed.get("mail_type", "new_request"), "triage",
                               parsed_result=parsed, matched_by="internal_new_finance", **_log_kwargs)
        return {"action": "triage", "reason": "internal_new_finance"}

    # 4c. Kein Case erstellen ohne Antragstellername → Triage
    if match["action"] == "create" and not applicant_name:
        logger.info(f"No applicant name found, redirecting to triage: {request.subject}")
        db.log_processed_email(request.provider_message_id, parsed.get("mail_type", "new_request"), "triage",
                               parsed_result=parsed, matched_by="no_applicant_name", **_log_kwargs)
        return {"action": "triage", "reason": "no_applicant_name"}

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
            partner_email=_safe_partner_email(parsed.get("partner_email"), request.from_email),
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
        db.log_processed_email(request.provider_message_id, "triage", "no_case_match",
                               parsed_result=parsed, matched_by=match.get("matched_by"), **_log_kwargs)
        return {"action": "triage", "reason": "no_case_match"}

    # 6. Anhänge direkt intern verarbeiten (statt zurück an n8n)
    docs_processed = []
    doc_rows_to_insert = []  # Batch: alle Dokumente sammeln, dann einmal speichern
    all_new_facts = {}       # Gesammelte Facts aus allen Dokumenten
    if request.attachments:
        now_ts = datetime.utcnow().isoformat()
        for filename, b64_data in request.attachments.items():
            try:
                file_bytes = base64.b64decode(b64_data)
                ext = filename.split('.')[-1].lower()
                mime_map = {
                    'pdf': 'application/pdf', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
                    'png': 'image/png', 'tiff': 'image/tiff', 'tif': 'image/tiff',
                    'webp': 'image/webp',
                }
                mime = mime_map.get(ext, 'application/octet-stream')

                result = analyze_with_gpt4o(file_bytes, mime, filename)
                extracted = result.get("extracted_data") or {}

                # Zeile sammeln (nicht einzeln speichern)
                doc_rows_to_insert.append({
                    "caseId": case_id,
                    "file_name": filename,
                    "doc_type": result.get("doc_type", "Sonstiges"),
                    "extracted_data": json.dumps(extracted),
                    "processing_status": "completed",
                    "processed_at": now_ts,
                })

                # Facts sammeln (einmal am Ende mergen)
                _person = (result.get("meta") or {}).get("person_name")
                _is_couple = _detect_is_couple(applicant_name, all_new_facts)
                new_facts = _map_extracted_to_facts(
                    result.get("doc_type", ""), extracted,
                    person_name=_person,
                    case_applicant_name=applicant_name,
                    is_couple=_is_couple,
                )
                if new_facts:
                    all_new_facts = cases.merge_facts(all_new_facts, new_facts)

                # Applicant name ggf. korrigieren (bei erster Ausweiskopie)
                if result.get("doc_type") in ("Ausweiskopie", "Selbstauskunft") and _person:
                    _maybe_update_applicant_name(case_id, _person)

                docs_processed.append({"filename": filename, "doc_type": result.get("doc_type"), "success": True})
                logger.info(f"Attachment processed: {filename} → {result.get('doc_type')}")

            except Exception as e:
                logger.error(f"Attachment processing failed for {filename}: {e}")
                docs_processed.append({"filename": filename, "success": False, "error": str(e)})

        # Batch: alle Dokumente auf einmal speichern (1 DB call statt N)
        if doc_rows_to_insert:
            try:
                db.batch_create_rows("fin_documents", doc_rows_to_insert)
            except Exception as e:
                logger.error(f"Batch insert fin_documents failed: {e}")

        # Gesammelte Facts einmal mergen (1 DB call statt N)
        if all_new_facts:
            try:
                cases.save_facts(case_id, all_new_facts, source="document:batch")
            except Exception as e:
                logger.error(f"Batch facts merge failed: {e}")

    # 7. Readiness Check + Notifications (immer, nachdem alles verarbeitet ist)
    readiness_result = None
    try:
        readiness_result = rdns.check_readiness(case_id)
        notify.dispatch_notifications(case_id, readiness_result)
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")

    # 7b. Google Drive Links sammeln (werden vom Endpoint als async Task gestartet)
    gdrive_links = parsed.get("google_drive_links", [])

    # 8. Log
    db.log_processed_email(request.provider_message_id, parsed.get("mail_type", "new_request"), match["action"], case_id,
                           parsed_result=parsed, matched_by=match.get("matched_by"), **_log_kwargs)

    return {
        "action": "processed",
        "case_id": case_id,
        "is_new_case": is_new,
        "status": readiness_result.get("status") if readiness_result else "INTAKE",
        "onedrive_folder_id": request.onedrive_folder_id,
        "needs_onedrive_folder": needs_folder,
        "files_to_upload": [],
        "google_drive_links": gdrive_links,
        "readiness": readiness_result,
        "_gdrive_links": gdrive_links if gdrive_links else None,  # internal: triggers async GDrive task
    }


class ProcessDocumentRequest(BaseModel):
    case_id: str
    filename: str
    data_base64: str        # Base64 kodierte Datei
    mime_type: Optional[str] = "application/pdf"
    onedrive_file_id: Optional[str] = None

class ProcessDocumentResponse(BaseModel):
    success: bool
    case_id: str
    queued: bool = False
    doc_type: Optional[str] = None
    confidence: Optional[str] = None
    facts_merged: bool = False
    readiness: Optional[dict] = None
    error: Optional[str] = None

# Per-Case Locks: Dokumente desselben Cases sequenziell, verschiedene Cases parallel
_case_processing_locks: dict[str, asyncio.Lock] = {}

# ── Processing Queue Tracker ──────────────────────────────────────
# In-Memory Tracking aller laufenden/wartenden Dokument-Verarbeitungen
# Struktur: { case_id: [ {filename, status, queued_at, started_at, finished_at, doc_type, error} ] }
_processing_queue: dict[str, list[dict]] = {}
_QUEUE_MAX_FINISHED = 50  # Max erledigte Items pro Case behalten


def _queue_add(case_id: str, filename: str):
    """Fügt Dokument zur Queue hinzu."""
    if case_id not in _processing_queue:
        _processing_queue[case_id] = []
    _processing_queue[case_id].append({
        "filename": filename,
        "status": "queued",
        "queued_at": datetime.utcnow().isoformat(),
        "started_at": None,
        "finished_at": None,
        "doc_type": None,
        "error": None,
    })


def _queue_update(case_id: str, filename: str, **kwargs):
    """Aktualisiert Status eines Queue-Items."""
    items = _processing_queue.get(case_id, [])
    for item in items:
        if item["filename"] == filename and item["status"] not in ("done", "error"):
            item.update(kwargs)
            break


def _queue_cleanup(case_id: str):
    """Entfernt alte erledigte Items."""
    items = _processing_queue.get(case_id, [])
    active = [i for i in items if i["status"] in ("queued", "processing")]
    finished = [i for i in items if i["status"] in ("done", "error")]
    _processing_queue[case_id] = active + finished[-_QUEUE_MAX_FINISHED:]


@app.get("/api/dashboard/case/{case_id}/queue")
async def get_processing_queue(case_id: str):
    """Gibt den aktuellen Verarbeitungsstatus der Queue für einen Case zurück."""
    items = _processing_queue.get(case_id, [])
    active = [i for i in items if i["status"] in ("queued", "processing")]
    recent_done = [i for i in items if i["status"] in ("done", "error")][-20:]
    return {
        "case_id": case_id,
        "active": active,
        "recent": recent_done,
        "total_queued": len([i for i in items if i["status"] == "queued"]),
        "total_processing": len([i for i in items if i["status"] == "processing"]),
        "total_done": len([i for i in items if i["status"] == "done"]),
        "total_error": len([i for i in items if i["status"] == "error"]),
    }


@app.post("/process-document", response_model=ProcessDocumentResponse)
async def process_document(request: ProcessDocumentRequest):
    """
    Nimmt Dokument an und verarbeitet es im Hintergrund.
    Gibt sofort 200 OK zurück — keine Timeouts mehr.
    """
    logger.info(f"process-document: {request.case_id} / {request.filename} → queued")

    # Base64 sofort validieren (schnell, fängt kaputte Requests ab)
    try:
        file_bytes = base64.b64decode(request.data_base64)
    except Exception as e:
        return ProcessDocumentResponse(success=False, case_id=request.case_id, error=f"Base64 decode failed: {e}")

    # In Queue eintragen + Background-Task starten
    _queue_add(request.case_id, request.filename)

    asyncio.create_task(_process_document_background(
        case_id=request.case_id,
        filename=request.filename,
        file_bytes=file_bytes,
        mime_type=request.mime_type or "application/pdf",
        onedrive_file_id=request.onedrive_file_id,
    ))

    return ProcessDocumentResponse(
        success=True,
        case_id=request.case_id,
        queued=True,
    )


async def _process_document_background(
    case_id: str, filename: str, file_bytes: bytes,
    mime_type: str, onedrive_file_id: str = None,
):
    """Verarbeitet ein Dokument im Hintergrund mit per-Case Lock."""

    # Per-Case Lock holen/erstellen
    if case_id not in _case_processing_locks:
        _case_processing_locks[case_id] = asyncio.Lock()
    lock = _case_processing_locks[case_id]

    async with lock:
        _queue_update(case_id, filename, status="processing", started_at=datetime.utcnow().isoformat())
        logger.info(f"[{case_id}] Processing {filename} (background)")

        # 1. Dokument analysieren (synchron → in Thread auslagern)
        try:
            result = await asyncio.to_thread(analyze_with_gpt4o, file_bytes, mime_type, filename)
        except Exception as e:
            logger.error(f"[{case_id}] Document analysis failed for {filename}: {e}")
            _queue_update(case_id, filename, status="error", error=str(e), finished_at=datetime.utcnow().isoformat())
            db.create_row("fin_documents", {
                "caseId": case_id,
                "onedrive_file_id": onedrive_file_id or "",
                "file_name": filename,
                "doc_type": "error",
                "processing_status": "error",
                "error_message": str(e),
                "processed_at": datetime.utcnow().isoformat(),
            })
            _queue_cleanup(case_id)
            return

        # 2. In fin_documents speichern (Upsert)
        extracted = result.get("extracted_data") or {}
        doc_data = {
            "doc_type": result.get("doc_type", "Sonstiges"),
            "extracted_data": json.dumps(extracted),
            "processing_status": "completed",
            "processed_at": datetime.utcnow().isoformat(),
        }
        existing_doc = None
        existing_docs = db.search_rows("fin_documents", "caseId", case_id)
        for d in existing_docs:
            if onedrive_file_id and d.get("onedrive_file_id") == onedrive_file_id:
                existing_doc = d
                break
            if d.get("file_name") == filename:
                existing_doc = d
                break
        if existing_doc:
            db.update_row("fin_documents", existing_doc["_id"], doc_data)
        else:
            doc_data["caseId"] = case_id
            doc_data["onedrive_file_id"] = onedrive_file_id or ""
            doc_data["file_name"] = filename
            db.create_row("fin_documents", doc_data)

        # 3. Facts in Case mergen
        try:
            doc_type = result.get("doc_type", "")
            _person = (result.get("meta") or {}).get("person_name")
            _case = cases.load_case(case_id)
            _case_name = _case.get("applicant_name") if _case else None
            _existing_facts = _case.get("_facts_extracted", {}) if _case else {}
            _is_couple = _detect_is_couple(_case_name, _existing_facts)
            new_facts = _map_extracted_to_facts(
                doc_type, extracted,
                person_name=_person,
                case_applicant_name=_case_name,
                is_couple=_is_couple,
            )
            if new_facts:
                cases.save_facts(case_id, new_facts, source=f"document:{doc_type}")

            if doc_type in ("Ausweiskopie", "Selbstauskunft") and _person:
                _maybe_update_applicant_name(case_id, _person)
        except Exception as e:
            logger.error(f"[{case_id}] Facts merge failed for {filename}: {e}")

        # 4. Readiness Check
        try:
            rdns.check_readiness(case_id)
        except Exception as e:
            logger.error(f"[{case_id}] Readiness check failed after {filename}: {e}")

        _queue_update(case_id, filename,
                      status="done",
                      doc_type=result.get("doc_type"),
                      finished_at=datetime.utcnow().isoformat())
        _queue_cleanup(case_id)
        logger.info(f"[{case_id}] Done processing {filename} → {result.get('doc_type', '?')}")


def _detect_is_couple(applicant_name: str, facts: dict) -> bool:
    """Erkennt ob es sich um ein Paar handelt (zwei Antragsteller)."""
    # 1. "und" / "&" im Case-Namen → Paar
    if applicant_name:
        name_lower = applicant_name.lower()
        if " und " in name_lower or " & " in name_lower:
            return True
    # 2. Bereits applicant_data_2 mit Vorname/Nachname in Facts → Paar
    ad2 = facts.get("applicant_data_2", {})
    if isinstance(ad2, dict) and (ad2.get("first_name") or ad2.get("vorname")):
        return True
    return False


def _clean_person_name(name: str) -> str:
    """Remove titles and honorifics from a person name."""
    if not name:
        return ""
    prefixes = {"herr", "frau", "dr.", "prof.", "dr", "prof", "dipl.", "ing."}
    parts = name.strip().split()
    cleaned = [p for p in parts if p.lower() not in prefixes]
    return " ".join(cleaned)


def _is_primary_applicant(person_name: str, case_applicant_name: str) -> bool:
    """
    Check if person_name matches the case's primary applicant.
    Returns True if they share at least one name part (first/last name).
    Returns True by default if we can't determine.
    """
    if not person_name or not case_applicant_name:
        return True  # Can't determine → default to primary

    pn = _clean_person_name(person_name).lower()
    cn = _clean_person_name(case_applicant_name).lower()

    # Handle "und"/"&" in names
    pn_parts = set(pn.replace(" und ", " ").replace(" & ", " ").split())
    cn_parts = set(cn.replace(" und ", " ").replace(" & ", " ").split())

    # Remove common filler words
    fillers = {"und", "&", "von", "van", "de", "der", "die", "das"}
    pn_parts -= fillers
    cn_parts -= fillers

    if not pn_parts or not cn_parts:
        return True

    return bool(pn_parts & cn_parts)


def _maybe_update_applicant_name(case_id: str, person_name: str):
    """
    Update applicant_name if the current one appears to be the broker's name.
    Only called for identity documents (Ausweiskopie, Selbstauskunft).
    """
    if not person_name:
        return
    case = cases.load_case(case_id)
    if not case:
        return
    current = (case.get("applicant_name") or "").strip()
    partner_email = (case.get("partner_email") or "").lower()

    if not current:
        # No name set yet → set it from document
        db.update_row("fin_cases", case["_id"], {"applicant_name": person_name})
        logger.info(f"[{case_id}] applicant_name set from document: '{person_name}'")
        return

    # Check if current name matches the broker/partner email prefix
    if not partner_email or "@" not in partner_email:
        return
    email_prefix = partner_email.split("@")[0]
    email_parts = set(email_prefix.replace(".", " ").replace("-", " ").replace("_", " ").lower().split())
    current_parts = set(current.lower().split())

    # If >= 50% of current name parts appear in the broker email → likely broker name
    if email_parts and current_parts:
        overlap = current_parts & email_parts
        if len(overlap) >= len(current_parts) * 0.5:
            logger.info(f"[{case_id}] Updating applicant_name: '{current}' -> '{person_name}' (broker name detected)")
            db.update_row("fin_cases", case["_id"], {"applicant_name": person_name})


def _map_extracted_to_facts(doc_type: str, extracted: dict,
                             person_name: str = None,
                             case_applicant_name: str = None,
                             is_couple: bool = False) -> dict:
    """
    Mappt extrahierte Dokument-Daten auf facts_extracted Struktur.

    person_name: Name der Person im Dokument (aus meta.person_name)
    case_applicant_name: Name des Hauptantragstellers aus dem Case
    is_couple: True wenn bekannt ist, dass es einen zweiten Antragsteller gibt

    Bei Paaren: Wenn person_name nicht zum case_applicant_name passt,
    werden die Daten unter _2-Keys gespeichert (applicant_data_2, income_data_2, etc.)
    Bei Einzel-Antragstellern: Immer Primary (verhindert _2 durch OCR-Fehler).
    """
    facts = {}

    # Determine person suffix for person-specific doc types
    is_primary = _is_primary_applicant(person_name, case_applicant_name)
    if not is_primary and not is_couple:
        # Kein Paar bekannt → Name-Mismatch ist wahrscheinlich ein OCR-/Extraktionsfehler
        logger.warning(
            f"Name mismatch '{person_name}' vs '{case_applicant_name}' "
            f"aber kein Paar erkannt → behandle als Primary"
        )
        is_primary = True
    suffix = "" if is_primary else "_2"

    if doc_type in ("Ausweiskopie",):
        _vorname = extracted.get("Vorname")
        _nachname = extracted.get("Nachname")
        _gebdat = extracted.get("Geburtsdatum")
        _gebort = extracted.get("Geburtsort")
        _nat = extracted.get("Nationalität") or extracted.get("Nationalitaet")
        facts[f"applicant_data{suffix}"] = {
            "first_name": _vorname, "vorname": _vorname,
            "last_name": _nachname, "nachname": _nachname,
            "birth_date": _gebdat, "geburtsdatum": _gebdat,
            "birth_place": _gebort, "geburtsort": _gebort,
            "nationality": _nat, "nationalitaet": _nat,
        }
        facts[f"id_data{suffix}"] = {
            "ausweisnummer": extracted.get("Ausweisnummer"),
            "gueltig_bis": extracted.get("Gültig bis"),
        }

    elif doc_type in ("Gehaltsnachweis", "Gehaltsabrechnung", "Gehaltsabrechnung Dezember", "Lohnsteuerbescheinigung"):
        _arbeitgeber = extracted.get("Arbeitgeber")
        _netto = extracted.get("Netto")
        _auszahlung = extracted.get("Auszahlungsbetrag")
        # Auszahlungsbetrag hat Vorrang vor Netto (Netto ist vor Abzügen wie VWL etc.)
        _effective_net = _auszahlung or _netto
        facts[f"income_data{suffix}"] = {
            "arbeitgeber": _arbeitgeber,
            "employer": _arbeitgeber,
            "brutto": extracted.get("Brutto"),
            "netto": _netto,
            "auszahlungsbetrag": _auszahlung,
            "net_income": _effective_net,
            "steuerklasse": extracted.get("Steuerklasse"),
        }
        facts[f"employment_data{suffix}"] = {
            "arbeitgeber": _arbeitgeber,
            "employer": _arbeitgeber,
            "employment_type": "Angestellter",
        }
        # Also write to applicant_data so KEY_SEARCH_PATHS can find it
        _vorname = extracted.get("Vorname")
        _nachname = extracted.get("Nachname")
        ad = {
            "employer": _arbeitgeber,
            "net_income": _effective_net,
            "employment_type": "Angestellter",
        }
        if _vorname:
            ad["first_name"] = _vorname
            ad["vorname"] = _vorname
        if _nachname:
            ad["last_name"] = _nachname
            ad["nachname"] = _nachname
        facts[f"applicant_data{suffix}"] = ad
        # Wohnadresse aus Gehaltsnachweis (nur Hauptantragsteller)
        _street = extracted.get("Strasse") or extracted.get("Straße")
        _hnr = extracted.get("Hausnummer")
        _plz = extracted.get("PLZ")
        _city = extracted.get("Ort") or extracted.get("Stadt")
        if not suffix and (_street or _plz or _city):
            facts["address_data"] = {
                "street": _street,
                "house_number": _hnr,
                "zip": _plz,
                "city": _city,
            }

    elif doc_type in ("Kontoauszug",):
        # Kontoauszug is shared / not person-specific
        facts["banking_data"] = {
            "bank": extracted.get("Bank"),
            "iban": extracted.get("IBAN"),
            "kontostand": extracted.get("Kontostand"),
        }

    elif doc_type in ("Exposé",):
        # Property data is shared
        facts["property_data"] = {
            "purchase_price": extracted.get("Kaufpreis") or extracted.get("purchase_price"),
            "address": extracted.get("Adresse"),
            "street": extracted.get("Straße") or extracted.get("Strasse") or extracted.get("street"),
            "house_number": extracted.get("Hausnummer") or extracted.get("house_number"),
            "zip": extracted.get("PLZ") or extracted.get("zip"),
            "plz": extracted.get("PLZ"),
            "city": extracted.get("Ort") or extracted.get("Stadt") or extracted.get("city"),
            "ort": extracted.get("Ort") or extracted.get("Stadt"),
            "object_type": extracted.get("Objekttyp") or extracted.get("object_type"),
            "usage": extracted.get("Nutzungsart") or extracted.get("usage"),
            "living_space": extracted.get("Wohnfläche") or extracted.get("Wohnflaeche"),
            "year_built": extracted.get("Baujahr"),
            "plot_size": extracted.get("Grundstücksgröße") or extracted.get("Grundstuecksgroesse"),
        }

    elif doc_type in ("Selbstauskunft",):
        _vorname = extracted.get("Vorname") or extracted.get("applicant_first_name")
        _nachname = extracted.get("Nachname") or extracted.get("applicant_last_name")
        _telefon = extracted.get("Telefon") or extracted.get("applicant_phone")
        _gebdat = extracted.get("Geburtsdatum")
        _famstand = extracted.get("Familienstand")
        _beruf = extracted.get("Beruf") or extracted.get("occupation")
        _anrede = extracted.get("Anrede") or extracted.get("salutation")
        _steuer_id = extracted.get("Steuer-ID") or extracted.get("Steuer_ID") or extracted.get("tax_id")
        _besch_seit = extracted.get("Beschäftigt seit") or extracted.get("Beschaeftigt_seit") or extracted.get("employed_since")
        _kinder = extracted.get("Anzahl Kinder") or extracted.get("Kinder") or extracted.get("children")
        facts[f"applicant_data{suffix}"] = {
            "first_name": _vorname, "vorname": _vorname,
            "last_name": _nachname, "nachname": _nachname,
            "phone": _telefon, "telefon": _telefon,
            "email": extracted.get("E-Mail") or extracted.get("applicant_email"),
            "birth_date": _gebdat, "geburtsdatum": _gebdat,
            "marital_status": _famstand, "familienstand": _famstand,
            "occupation": _beruf, "beruf": _beruf,
            "salutation": _anrede, "anrede": _anrede,
            "tax_id": _steuer_id, "steuer_id": _steuer_id,
            "employed_since": _besch_seit, "beschaeftigt_seit": _besch_seit,
            "children": _kinder, "kinder": _kinder,
        }
        if not suffix:  # Adresse nur für Hauptantragsteller
            facts["address_data"] = {
                "street": extracted.get("Strasse") or extracted.get("Straße"),
                "house_number": extracted.get("Hausnummer"),
                "zip": extracted.get("PLZ"),
                "city": extracted.get("Ort") or extracted.get("Stadt"),
            }
        # Einkommen aus Selbstauskunft
        _einkommen = extracted.get("Einkommen") or extracted.get("Netto")
        if _einkommen:
            facts[f"income_data{suffix}"] = {
                "net_income": _einkommen,
                "netto": _einkommen,
            }

    elif doc_type in ("Kaufvertrag",):
        facts["property_data"] = {
            "purchase_price": extracted.get("Kaufpreis") or extracted.get("purchase_price"),
            "address": extracted.get("Adresse"),
            "street": extracted.get("Straße") or extracted.get("Strasse") or extracted.get("street"),
            "house_number": extracted.get("Hausnummer") or extracted.get("house_number"),
            "zip": extracted.get("PLZ") or extracted.get("zip"),
            "plz": extracted.get("PLZ"),
            "city": extracted.get("Ort") or extracted.get("Stadt") or extracted.get("city"),
            "ort": extracted.get("Ort") or extracted.get("Stadt"),
        }

    elif doc_type in ("Steuerbescheid",):
        facts[f"tax_data{suffix}"] = {
            "tax_year": extracted.get("Steuerjahr"),
            "taxable_income": extracted.get("zu versteuerndes Einkommen"),
            "income_employment": extracted.get("Einkünfte aus nichtselbständiger Arbeit") or extracted.get("Einkuenfte_nichtselbstaendig"),
            "income_self_employment": extracted.get("Einkünfte aus Gewerbebetrieb/selbständiger Arbeit") or extracted.get("Einkuenfte_selbstaendig") or extracted.get("Einkünfte aus Gewerbebetrieb"),
            "income_rental": extracted.get("Einkünfte aus Vermietung und Verpachtung") or extracted.get("Einkuenfte_vermietung"),
            "refund_or_payment": extracted.get("Erstattung/Nachzahlung") or extracted.get("Erstattung"),
        }

    elif doc_type in ("Steuererklärung",):
        facts[f"tax_data{suffix}"] = {
            "tax_year": extracted.get("Steuerjahr"),
            "income_employment": extracted.get("Einkünfte aus nichtselbständiger Arbeit") or extracted.get("Einkuenfte_nichtselbstaendig"),
            "income_rental": extracted.get("Einkünfte aus Vermietung und Verpachtung") or extracted.get("Einkuenfte_vermietung"),
            "deductions": extracted.get("Werbungskosten"),
        }

    elif doc_type in ("BWA",):
        _gewinn = extracted.get("Vorläufiges Ergebnis") or extracted.get("Gewinn/Verlust") or extracted.get("Gewinn") or extracted.get("profit")
        facts[f"income_data{suffix}"] = {
            "profit_last_year": _gewinn,
            "gewinn_vorjahr": _gewinn,
            "revenue": extracted.get("Umsatzerlöse") or extracted.get("Umsatz"),
            "total_costs": extracted.get("Gesamtkosten"),
        }
        facts[f"business_data{suffix}"] = {
            "company_name": extracted.get("Firma") or extracted.get("Unternehmen"),
            "period": extracted.get("Zeitraum"),
        }
        facts[f"employment_data{suffix}"] = {
            "employment_type": "Selbständiger",
        }
        facts[f"applicant_data{suffix}"] = {
            "employment_type": "Selbständiger",
            "profit_last_year": _gewinn,
        }

    elif doc_type in ("Jahresabschluss",):
        _gewinn = extracted.get("Jahresüberschuss") or extracted.get("Gewinn") or extracted.get("profit")
        facts[f"income_data{suffix}"] = {
            "profit_last_year": _gewinn,
            "gewinn_vorjahr": _gewinn,
            "revenue": extracted.get("Umsatzerlöse") or extracted.get("Umsatz"),
            "balance_total": extracted.get("Bilanzsumme"),
        }
        facts[f"business_data{suffix}"] = {
            "company_name": extracted.get("Firma") or extracted.get("Unternehmen"),
            "year": extracted.get("Jahr"),
        }
        facts[f"applicant_data{suffix}"] = {
            "employment_type": "Selbständiger",
            "profit_last_year": _gewinn,
        }

    elif doc_type in ("Summen und Saldenliste",):
        facts[f"business_data{suffix}"] = {
            "company_name": extracted.get("Firma") or extracted.get("Unternehmen"),
            "period": extracted.get("Zeitraum"),
            "account_balances": extracted.get("Kontensalden"),
        }

    elif doc_type in ("Renteninfo",):
        facts[f"pension_data{suffix}"] = {
            "projected_monthly_pension": extracted.get("Prognostizierte monatliche Rente") or extracted.get("monatliche Rente"),
            "current_pension_entitlement": extracted.get("Bisher erworbene Rentenansprüche") or extracted.get("erworbene Rentenansprüche"),
            "insurance_number": extracted.get("Rentenversicherungsnummer"),
        }

    elif doc_type in ("Eigenkapitalnachweis",):
        facts["equity_data"] = {
            "total_equity": extracted.get("Gesamtguthaben") or extracted.get("Gesamtvermögen") or extracted.get("Gesamtvermoegen"),
            "accounts": extracted.get("Einzelne Konten") or extracted.get("Konten"),
            "bank": extracted.get("Bank") or extracted.get("Institut"),
        }

    elif doc_type in ("Depotnachweis",):
        facts["equity_data"] = {
            "depot_value": extracted.get("Gesamtdepotwert") or extracted.get("Depotwert"),
            "bank": extracted.get("Bank") or extracted.get("Broker"),
        }

    elif doc_type in ("Darlehensvertrag",):
        facts["existing_loans"] = {
            "bank": extracted.get("Bank") or extracted.get("Kreditgeber"),
            "remaining_debt": extracted.get("Restschuld"),
            "interest_rate": extracted.get("Zinssatz"),
            "monthly_rate": extracted.get("Monatliche Rate") or extracted.get("Rate"),
            "end_date": extracted.get("Laufzeitende"),
        }

    elif doc_type in ("Bausparvertrag",):
        facts["savings_data"] = {
            "bausparkasse": extracted.get("Bausparkasse"),
            "target_amount": extracted.get("Bausparsumme"),
            "saved_amount": extracted.get("Angespartes Guthaben") or extracted.get("Guthaben"),
            "tariff": extracted.get("Tarif"),
            "ready_for_allocation": extracted.get("Zuteilungsreif"),
        }

    elif doc_type in ("Mietvertrag",):
        facts["rental_data"] = {
            "cold_rent": extracted.get("Kaltmiete"),
            "warm_rent": extracted.get("Warmmiete") or extracted.get("Warmmiete/Nebenkosten"),
            "tenant": extracted.get("Mieter"),
            "landlord": extracted.get("Vermieter"),
            "address": extracted.get("Objektadresse") or extracted.get("Adresse"),
        }

    elif doc_type in ("Nachweis Krankenversicherung",):
        facts[f"insurance_data{suffix}"] = {
            "type": extracted.get("PKV oder GKV") or extracted.get("PKV/GKV") or extracted.get("Versicherungsart"),
            "monthly_premium": extracted.get("Monatlicher Beitrag") or extracted.get("Beitrag"),
            "insurer": extracted.get("Versicherer") or extracted.get("Versicherung"),
        }

    elif doc_type in ("Energieausweis",):
        facts["property_data"] = {
            "energy_value": extracted.get("Energiekennwert"),
            "energy_class": extracted.get("Energieeffizienzklasse"),
            "heating_type": extracted.get("Heizungsart"),
            "year_built": extracted.get("Baujahr"),
        }

    elif doc_type in ("Handelsregisterauszug", "Gesellschaftsvertrag"):
        facts[f"business_data{suffix}"] = {
            "company_name": extracted.get("Firma"),
            "seat": extracted.get("Sitz"),
            "managing_director": extracted.get("Geschäftsführer") or extracted.get("Geschaeftsfuehrer"),
            "register_number": extracted.get("HRB/HRA-Nummer") or extracted.get("HRB") or extracted.get("HRA"),
            "legal_form": extracted.get("Rechtsform"),
        }

    else:
        # Generischer Fallback: Bekannte Felder aus JEDEM Dokumenttyp extrahieren
        _GENERIC_PROPERTY_MAP = {
            "Kaufpreis": "purchase_price", "purchase_price": "purchase_price",
            "Adresse": "address", "address": "address",
            "Objekttyp": "object_type", "object_type": "object_type",
            "Nutzungsart": "usage", "usage": "usage",
            "Wohnfläche": "living_space", "living_space": "living_space",
            "Baujahr": "year_built", "year_built": "year_built",
            "Grundstücksgröße": "plot_size", "plot_size": "plot_size",
        }
        _GENERIC_INCOME_MAP = {
            "Netto": "net_income", "net_income": "net_income",
            "Brutto": "brutto", "brutto": "brutto",
            "Arbeitgeber": "employer", "employer": "employer",
        }
        _GENERIC_APPLICANT_MAP = {
            "Vorname": "first_name", "first_name": "first_name",
            "Nachname": "last_name", "last_name": "last_name",
            "Geburtsdatum": "birth_date", "birth_date": "birth_date",
        }

        prop = {}
        for src_key, dst_key in _GENERIC_PROPERTY_MAP.items():
            val = extracted.get(src_key)
            if val is not None and val != "":
                prop[dst_key] = val
        if prop:
            facts["property_data"] = prop

        inc = {}
        for src_key, dst_key in _GENERIC_INCOME_MAP.items():
            val = extracted.get(src_key)
            if val is not None and val != "":
                inc[dst_key] = val
        if inc:
            facts[f"income_data{suffix}"] = inc

        app = {}
        for src_key, dst_key in _GENERIC_APPLICANT_MAP.items():
            val = extracted.get(src_key)
            if val is not None and val != "":
                app[dst_key] = val
        if app:
            facts[f"applicant_data{suffix}"] = app

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
    force_notifications: bool = False  # Cooldown überspringen (manueller Recheck)

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
            notify.dispatch_notifications(request.case_id, result, force=request.force_notifications)
        return result
    except Exception as e:
        logger.error(f"full-readiness-check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class UpdateOneDriveFolderRequest(BaseModel):
    case_id: str
    onedrive_folder_id: str
    web_url: Optional[str] = None

@app.post("/update-onedrive-folder")
async def update_onedrive_folder(request: UpdateOneDriveFolderRequest):
    """n8n meldet erstellten OneDrive-Ordner zurück.
    Kein Notification-Dispatch hier – n8n ruft danach /full-readiness-check auf."""
    try:
        cases.update_onedrive_folder(request.case_id, request.onedrive_folder_id, web_url=request.web_url)
        # Nur Status prüfen, KEINE Notification (verhindert Doppel-Notification)
        result = rdns.check_readiness(request.case_id)
        return {"success": True, "case_id": request.case_id, "status": result["status"]}
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"update-onedrive-folder failed: {tb}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# DRY-RUN LOG ENDPOINT
# ============================================

@app.get("/dry-run-log")
async def dry_run_log(limit: int = 20):
    """
    Zeigt die letzten Test-E-Mails aus dem Dry-Run-Modus.
    Liest aus SeaTable 'email_test_log' oder aus der lokalen dry_run_emails.log.
    """
    import notify as ntf
    results = {"dry_run_active": ntf.EMAIL_DRY_RUN, "emails": [], "source": None}

    # Aus SeaTable laden
    try:
        rows = db.list_rows("email_test_log")
        rows_sorted = sorted(rows, key=lambda r: r.get("logged_at", ""), reverse=True)[:limit]
        results["emails"] = [
            {
                "to": r.get("to"),
                "subject": r.get("subject"),
                "body_text": r.get("body_text", "")[:500],
                "logged_at": r.get("logged_at"),
            }
            for r in rows_sorted
        ]
        results["source"] = "seatable"
        return results
    except Exception:
        pass

    # Fallback: lokale Logdatei
    log_path = os.path.join(os.path.dirname(__file__), "dry_run_emails.log")
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            content = f.read()
        entries = [e.strip() for e in content.split("=" * 60) if e.strip()]
        results["emails"] = entries[-limit:]
        results["source"] = "logfile"

    return results


@app.delete("/dry-run-log")
async def clear_dry_run_log():
    """Löscht die lokale dry_run_emails.log (SeaTable-Einträge manuell löschen)."""
    log_path = os.path.join(os.path.dirname(__file__), "dry_run_emails.log")
    if os.path.exists(log_path):
        os.remove(log_path)
    return {"cleared": True, "note": "SeaTable email_test_log bitte manuell in SeaTable leeren"}


@app.get("/admin/inspect")
async def admin_inspect():
    """Gibt einen Ueberblick ueber alle DB-Inhalte. Nur fuer Development."""
    import db_postgres as _pg
    result = {}
    with _pg._get_conn() as conn:
        with conn.cursor() as cur:
            # Cases
            cur.execute("SELECT case_id, applicant_name, partner_email, status, facts_extracted, answers_user, manual_overrides, onedrive_folder_id, last_status_change FROM fin_cases ORDER BY created_at DESC")
            cols = [d[0] for d in cur.description]
            cases = [dict(zip(cols, row)) for row in cur.fetchall()]
            for c in cases:
                for k in ["facts_extracted", "answers_user", "manual_overrides"]:
                    if isinstance(c[k], str):
                        try:
                            import json as _j
                            c[k] = _j.loads(c[k])
                        except Exception:
                            pass
            result["cases"] = cases

            # Documents per case
            cur.execute("SELECT \"caseId\", doc_type, file_name, processing_status FROM fin_documents ORDER BY \"caseId\", processed_at DESC")
            cols = [d[0] for d in cur.description]
            docs = [dict(zip(cols, row)) for row in cur.fetchall()]
            # Group by case
            docs_by_case = {}
            for d in docs:
                cid = d["caseId"]
                if cid not in docs_by_case:
                    docs_by_case[cid] = {"total": 0, "by_type": {}}
                docs_by_case[cid]["total"] += 1
                t = d["doc_type"] or "Sonstiges"
                docs_by_case[cid]["by_type"][t] = docs_by_case[cid]["by_type"].get(t, 0) + 1
            result["documents_by_case"] = docs_by_case

            # Emails
            cur.execute("SELECT provider_message_id, from_email, subject, processing_result, case_id, matched_by FROM processed_emails ORDER BY processed_at DESC LIMIT 50")
            cols = [d[0] for d in cur.description]
            result["recent_emails"] = [dict(zip(cols, row)) for row in cur.fetchall()]

            # Outgoing
            cur.execute("SELECT \"to\", subject, logged_at, dry_run FROM email_test_log ORDER BY logged_at DESC LIMIT 20")
            cols = [d[0] for d in cur.description]
            result["outgoing_emails"] = [dict(zip(cols, row)) for row in cur.fetchall()]

    return result


@app.post("/admin/delete-case")
async def admin_delete_case(request: dict):
    """Löscht einen einzelnen Case und zugehörige Dokumente/E-Mails."""
    import db_postgres as _pg
    case_id = request.get("case_id")
    if not case_id:
        raise HTTPException(status_code=400, detail="case_id required")
    results = {}
    for table, col in [("fin_documents", '"caseId"'), ("processed_emails", "case_id"), ("fin_cases", "case_id")]:
        try:
            with _pg._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {table} WHERE {col} = %s", (case_id,))
                    results[table] = cur.rowcount
        except Exception as e:
            results[table] = f"error: {e}"
    return {"deleted": results, "case_id": case_id}


@app.post("/admin/cleanup-emails")
async def admin_cleanup_emails():
    """Löscht irrelevante E-Mail-Logs (sender_not_allowlisted, skipped, no_case_match ohne Case)."""
    import db_postgres as _pg
    results = {}
    for result_type in ("sender_not_allowlisted", "no_case_match", "skipped", "irrelevant", "outgoing_system_mail",
                        "internal_forward_no_finance", "internal_non_finance"):
        try:
            with _pg._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM processed_emails WHERE processing_result = %s", (result_type,))
                    results[result_type] = cur.rowcount
        except Exception as e:
            results[result_type] = f"error: {e}"
    return {"cleaned": results}


# ============================================
# REMINDER CHECK ENDPOINT
# ============================================

REMINDER_DAYS = int(os.getenv("REMINDER_DAYS", "3"))
MAX_REMINDERS = int(os.getenv("MAX_REMINDERS", "3"))


def _count_reminders_in_audit(audit_log: list, current_status: str) -> int:
    """Zählt wie viele Erinnerungen im audit_log für den aktuellen Status gesendet wurden."""
    count = 0
    for entry in audit_log:
        if (entry.get("event") == "reminder_sent"
                and entry.get("status") == current_status):
            count += 1
    return count


def _has_recent_emails(case_id: str, days: int) -> bool:
    """Prüft ob in den letzten X Tagen eine E-Mail für diesen Case eingegangen ist."""
    from datetime import datetime, timedelta
    import seatable as db

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
    try:
        rows = db.query_rows(
            "processed_emails",
            columns=["case_id", "processed_at"],
            where="case_id = %s AND processed_at > %s",
            where_params=(case_id, cutoff),
            limit=1,
        )
        return len(rows) > 0
    except Exception as e:
        logger.warning(f"Fehler beim Prüfen neuer E-Mails für {case_id}: {e}")
        return False


def check_and_send_reminders() -> dict:
    """
    Prüft alle aktiven Cases und sendet Erinnerungen falls nötig.
    Gibt zurück: {sent: int, checked: int, skipped_reasons: dict}
    """
    from datetime import datetime
    import case_logic as cases
    import readiness as rdns
    import notify

    active_cases = cases.get_all_active_cases()
    reminder_statuses = {"NEEDS_QUESTIONS_PARTNER"}

    sent = 0
    checked = 0
    skipped_reasons = {
        "wrong_status": 0,
        "no_last_change": 0,
        "too_recent": 0,
        "max_reminders_reached": 0,
        "recent_email": 0,
        "error": 0,
    }

    for case in active_cases:
        status = case.get("status", "")
        case_id = case.get("case_id", "")

        if status not in reminder_statuses:
            skipped_reasons["wrong_status"] += 1
            continue

        checked += 1

        # Prüfe wie lange der Case schon in diesem Status ist
        last_change = case.get("last_status_change", "")
        if not last_change:
            skipped_reasons["no_last_change"] += 1
            continue

        try:
            last_change_dt = datetime.fromisoformat(last_change.replace("Z", "+00:00").replace("+00:00", ""))
            days_since = (datetime.utcnow() - last_change_dt).days
        except Exception as e:
            logger.warning(f"Kann last_status_change nicht parsen für {case_id}: {last_change} ({e})")
            skipped_reasons["error"] += 1
            continue

        if days_since < REMINDER_DAYS:
            skipped_reasons["too_recent"] += 1
            continue

        # Prüfe wie viele Erinnerungen schon gesendet wurden
        audit_log = case.get("audit_log", [])
        if isinstance(audit_log, str):
            try:
                audit_log = json.loads(audit_log)
            except Exception:
                audit_log = []
        if not isinstance(audit_log, list):
            audit_log = []

        reminder_count = _count_reminders_in_audit(audit_log, status)
        if reminder_count >= MAX_REMINDERS:
            skipped_reasons["max_reminders_reached"] += 1
            continue

        # Prüfe ob kürzlich eine neue E-Mail eingegangen ist
        if _has_recent_emails(case_id, REMINDER_DAYS):
            skipped_reasons["recent_email"] += 1
            continue

        # Readiness Check durchführen für aktuelle Daten
        try:
            readiness_result = rdns.check_readiness(case_id)
        except Exception as e:
            logger.error(f"Readiness check fehlgeschlagen für {case_id}: {e}")
            skipped_reasons["error"] += 1
            continue

        # Erinnerung senden
        target = "partner" if status == "NEEDS_QUESTIONS_PARTNER" else "broker"
        try:
            notify.send_reminder(case_id, readiness_result, reminder_count + 1, target=target)
        except Exception as e:
            logger.error(f"Reminder-Versand fehlgeschlagen für {case_id}: {e}")
            skipped_reasons["error"] += 1
            continue

        # Audit log updaten
        try:
            case_fresh = cases.load_case(case_id)
            if case_fresh:
                import seatable as db
                audit = case_fresh.get("_audit_log", [])
                audit.append({
                    "event": "reminder_sent",
                    "ts": datetime.utcnow().isoformat(),
                    "reminder_count": reminder_count + 1,
                    "status": status,
                    "target": target,
                })
                # Max 100 Einträge
                audit = audit[-100:]
                db.update_row("fin_cases", case_fresh["_id"], {
                    "audit_log": json.dumps(audit),
                })
        except Exception as e:
            logger.warning(f"Audit-Log Update fehlgeschlagen für {case_id}: {e}")

        sent += 1
        logger.info(f"Reminder #{reminder_count + 1} gesendet für Case {case_id} (Status: {status}, Target: {target})")

    return {
        "sent": sent,
        "checked": checked,
        "total_active": len(active_cases),
        "skipped_reasons": skipped_reasons,
    }


@app.post("/check-reminders")
async def check_reminders():
    """
    Prüft alle aktiven Cases und sendet Erinnerungen falls nötig.

    Wird von einem n8n Schedule Trigger (1x täglich) aufgerufen.

    Logik:
    - Cases mit Status NEEDS_QUESTIONS_PARTNER
    - Wenn last_status_change älter als REMINDER_DAYS (default: 3)
    - UND keine neue E-Mail in den letzten REMINDER_DAYS eingegangen
    - UND weniger als MAX_REMINDERS (default: 3) Erinnerungen gesendet
    - DANN: Erinnerung senden
    """
    try:
        result = check_and_send_reminders()
        logger.info(f"Reminder-Check abgeschlossen: {result}")
        return {
            "success": True,
            "reminders_sent": result["sent"],
            "cases_checked": result["checked"],
            "total_active_cases": result["total_active"],
            "skipped_reasons": result["skipped_reasons"],
            "config": {
                "reminder_days": REMINDER_DAYS,
                "max_reminders": MAX_REMINDERS,
            },
        }
    except Exception as e:
        logger.error(f"Reminder-Check fehlgeschlagen: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


# ============================================
# IMPORT CASE ENDPOINT
# ============================================

# ============================================
# GOOGLE DRIVE PROCESSING
# ============================================

class ProcessGoogleDriveRequest(BaseModel):
    case_id: str
    google_drive_links: list  # ["https://drive.google.com/drive/folders/..."]

class ProcessGoogleDriveResponse(BaseModel):
    success: bool
    case_id: str
    files_found: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    results: list = []
    errors: list = []
    readiness: Optional[dict] = None

@app.post("/process-google-drive", response_model=ProcessGoogleDriveResponse)
async def process_google_drive(request: ProcessGoogleDriveRequest):
    """
    Downloads files from Google Drive links and analyzes them.
    Called after email processing when google_drive_links are detected,
    or manually from the dashboard.
    """
    logger.info(f"process-google-drive: {request.case_id} / {len(request.google_drive_links)} links")

    if not request.google_drive_links:
        return ProcessGoogleDriveResponse(
            success=False, case_id=request.case_id,
            errors=["Keine Google Drive Links angegeben"],
        )

    try:
        import gdrive
        import asyncio
        # Run blocking Google Drive + GPT analysis in thread pool
        result = await asyncio.to_thread(
            gdrive.process_google_drive_links,
            case_id=request.case_id,
            links=request.google_drive_links,
        )

        # Run readiness check after processing
        readiness_result = None
        if result.get("files_processed", 0) > 0:
            try:
                readiness_result = await asyncio.to_thread(rdns.check_readiness, request.case_id)
                notify.dispatch_notifications(request.case_id, readiness_result)
            except Exception as e:
                logger.error(f"Readiness check after gdrive failed: {e}")

        return ProcessGoogleDriveResponse(
            case_id=request.case_id,
            readiness=readiness_result,
            **result,
        )

    except ValueError as e:
        # Missing GOOGLE_SERVICE_ACCOUNT_JSON
        logger.error(f"Google Drive config error: {e}")
        return ProcessGoogleDriveResponse(
            success=False, case_id=request.case_id,
            errors=[str(e)],
        )
    except Exception as e:
        logger.error(f"process-google-drive failed: {traceback.format_exc()}")
        return ProcessGoogleDriveResponse(
            success=False, case_id=request.case_id,
            errors=[f"Unerwarteter Fehler: {e}"],
        )


class ImportCaseRequest(BaseModel):
    case_id: str
    dry_run: Optional[bool] = False

class ImportCaseResponse(BaseModel):
    success: bool
    case_id: str
    europace_case_id: Optional[str] = None
    errors: list = []
    warnings: list = []
    payload_preview: Optional[dict] = None
    dry_run: bool = False

@app.post("/import-case", response_model=ImportCaseResponse)
async def import_case(request: ImportCaseRequest):
    """
    Triggert den Europace-Import fuer einen Case.
    dry_run=True: Nur Payload bauen und validieren, nicht an API senden.
    """
    import import_builder

    try:
        result = import_builder.execute_import(
            case_id=request.case_id,
            dry_run=request.dry_run,
        )
        return ImportCaseResponse(**result)
    except Exception as e:
        logger.error(f"Import failed for {request.case_id}: {e}")
        return ImportCaseResponse(
            success=False,
            case_id=request.case_id,
            errors=[str(e)],
            dry_run=request.dry_run,
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
