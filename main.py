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
    import db_postgres as _db
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
    client = OpenAI(api_key=api_key, timeout=60.0)

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
- WICHTIG: Personalausweise können GEDREHT/SEITLICH fotografiert sein! Erkennungsmerkmale: Passfoto, "BUNDESREPUBLIK DEUTSCHLAND", "PERSONALAUSWEIS/IDENTITY CARD", Name, Geburtsdatum, Ausweisnummer, Unterschrift, Hologramm → IMMER "Ausweiskopie" auch wenn das Bild um 90° oder 180° gedreht ist!
- WICHTIG: Aufenthaltstitel-Zusatzblätter, Aufenthaltstitel-Rückseiten, Nebenbestimmungen zum Aufenthaltstitel → IMMER "Ausweiskopie", NIEMALS "BWA" oder "Sonstiges"!
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
- Flurkarte, Liegenschaftskarte, Liegenschaftsplan, Katasterkarte, Lageplan (amtliche Karte mit Grundstücksgrenzen/Flurstücken) → "Grundriss"
- Modernisierungsstandard, Modernisierungsübersicht, Sanierungsübersicht → "Modernisierungsaufstellung"
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
- months_covered (PFLICHT! Zähle wie viele SEPARATE Gehaltsabrechnungen/Monate in diesem Dokument enthalten sind. Zähle die Anzahl der "Abrechnung der Brutto/Netto-Bezüge" Abschnitte bzw. verschiedene Monat/Jahr-Angaben. Ein PDF mit 3 Seiten für Nov, Dez, Jan = months_covered: 3)
- WICHTIG: "Netto" ist das steuerliche Netto VOR Abzügen (VWL, Kirchensteuer, etc.). "Auszahlungsbetrag" ist der tatsächlich ausgezahlte Betrag. Beide Werte extrahieren wenn vorhanden!
- WICHTIG: Wenn das PDF MEHRERE Gehaltsabrechnungen enthält (z.B. 3 Monate), extrahiere die Daten der NEUESTEN Abrechnung, aber setze months_covered auf die GESAMTZAHL aller enthaltenen Abrechnungen!

Für Kontoauszüge:
- Bank, IBAN, Kontostand, Zeitraum
- months_covered (PFLICHT! Zähle wie viele Monate dieser Kontoauszug abdeckt. Prüfe den Zeitraum: Jan-März = 3. Ein einzelner Monatsauszug = 1.)
- Regelmäßige Eingänge/Ausgänge
- Monatliche_Miete (Suche nach regelmäßigen Abbuchungen mit Verwendungszweck "Miete", "Kaltmiete", "Warmmiete", "Mietzahlung" o.ä. Gib den monatlichen Betrag als Zahl an. Wenn mehrere Monate sichtbar sind, nimm den typischen/wiederkehrenden Betrag.)

Für Selbstauskunft:
- Anrede (Herr/Frau), Vorname, Nachname, Geburtsdatum, Familienstand
- Telefon, E-Mail, Steuer-ID
- Strasse, Hausnummer, PLZ, Ort (Wohnadresse)
- Beruf, Beschäftigt seit (Datum), Einkommen
- Anzahl Kinder

Für Immobilien-Dokumente (Exposé, Grundbuch, Kaufvertrag, Energieausweis, etc.):
- Straße, Hausnummer, PLZ, Ort (Objektadresse - EINZELN aufteilen, nicht als ein String!)
- Wohnfläche (NUR die tatsächliche Wohnfläche in m², NICHT die Grundstücksgröße! Bei ETW typisch 30-200 m², bei Häusern 80-400 m². Werte über 500 m² sind fast sicher die Grundstücksgröße!)
- Baujahr
- Grundstücksgröße (separat von Wohnfläche! Kann mehrere hundert oder tausend m² sein)
- Kaufpreis
- Objekttyp (MUSS einer sein: ETW, EFH, DHH, RH, MFH, Grundstück)
- Nutzungsart (MUSS einer sein: Eigennutzung, Kapitalanlage, Teilvermietet)

Für Steuerbescheide:
- Steuerjahr, zu versteuerndes Einkommen
- Einkünfte aus nichtselbständiger Arbeit
- Einkünfte aus Gewerbebetrieb/selbständiger Arbeit
- Einkünfte aus Vermietung und Verpachtung
- Erstattung/Nachzahlung
- documents_covered (Anzahl der enthaltenen Steuerbescheide/Jahre als Zahl, z.B. 1 wenn nur ein Jahr, 2 wenn zwei Jahre in einem PDF)

Für Steuererklärungen:
- Steuerjahr
- Einkünfte aus nichtselbständiger Arbeit
- Einkünfte aus Vermietung und Verpachtung
- Werbungskosten
- documents_covered (Anzahl der enthaltenen Steuererklärungen/Jahre als Zahl)

Für BWA (Betriebswirtschaftliche Auswertung):
- Zeitraum (Monat/Jahr), Firma/Unternehmen
- Umsatzerlöse, Gesamtkosten
- Vorläufiges Ergebnis (Gewinn/Verlust)

Für Jahresabschluss:
- Jahr, Firma/Unternehmen
- Bilanzsumme, Umsatzerlöse
- Jahresüberschuss/Gewinn
- documents_covered (Anzahl der enthaltenen Jahresabschlüsse als Zahl, z.B. 1 wenn nur ein Jahr, 3 wenn drei Jahre in einem PDF)

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


def pdf_pages_to_images(pdf_bytes: bytes, max_pages: int = 5, dpi: int = 200) -> list[tuple[bytes, str]]:
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


############################
# Post-GPT Sanitization
############################

def _coerce_number(val) -> Optional[float]:
    """Versucht einen Wert in eine Zahl umzuwandeln. Handles '3.500,00', '3500.00', '3500 EUR' etc."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if not isinstance(val, str):
        return None
    s = val.strip()
    # Einheiten/Währung entfernen
    for suffix in (" EUR", " €", " Euro", " qm", " m²", " m2", "%"):
        s = s.replace(suffix, "")
    s = s.strip()
    if not s:
        return None
    # Deutsches Format: 3.500,00 → 3500.00
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif "." in s:
        # Heuristik: "3.500" oder "1.250.000" = deutsche Tausender-Punkte, NICHT Dezimal
        import re
        if re.match(r"^\d{1,3}(\.\d{3})+$", s):
            s = s.replace(".", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


_VALID_OBJECT_TYPES = {"ETW", "EFH", "DHH", "RH", "MFH", "Grundstück", "Grundstueck", "ZFH"}
_VALID_USAGE = {"Eigennutzung", "Kapitalanlage", "Teilvermietet", "Vermietet"}
_VALID_EMPLOYMENT = {"Angestellter", "Selbständig", "Selbstständig", "Freiberufler", "Beamter", "Rentner"}

# Felder die als Zahlen erwartet werden (pro extracted_data key)
_NUMERIC_FIELDS = {
    "Brutto", "Netto", "Auszahlungsbetrag", "Kaufpreis", "Wohnfläche", "Wohnflaeche",
    "Grundstücksgröße", "Kontostand", "Monatliche_Miete", "Monatliche Miete",
    "Kaltmiete", "Warmmiete", "Gesamtguthaben", "Gesamtdepotwert", "Restschuld",
    "Bausparsumme", "Angespartes Guthaben", "Monatliche Rate", "Monatlicher Beitrag",
    "Umsatzerlöse", "Gesamtkosten", "Jahresüberschuss", "Bilanzsumme",
    "zu versteuerndes Einkommen", "Einkommen",
    "Einkünfte aus nichtselbständiger Arbeit",
    "Einkünfte aus Gewerbebetrieb",
    "Einkünfte aus Vermietung und Verpachtung",
    "months_covered", "documents_covered",
}

# Enum-Felder: key → erlaubte Werte
_ENUM_FIELDS = {
    "object_type": _VALID_OBJECT_TYPES,
    "usage": _VALID_USAGE,
    "employment_type": _VALID_EMPLOYMENT,
}

# Plausibilitäts-Checks: Feld → (min, max, Warnung)
_PLAUSIBILITY = {
    "Wohnfläche": (10, 500, "Wohnfläche {val} m² unplausibel (vermutlich Grundstücksfläche?)"),
    "Wohnflaeche": (10, 500, "Wohnfläche {val} m² unplausibel (vermutlich Grundstücksfläche?)"),
    "Kaufpreis": (10000, 50000000, "Kaufpreis {val} € unplausibel"),
    "months_covered": (1, 24, "months_covered {val} unplausibel"),
    "documents_covered": (1, 10, "documents_covered {val} unplausibel"),
}


def _sanitize_extracted_data(result: dict) -> dict:
    """
    Post-GPT Validierung + Bereinigung der extrahierten Daten.
    - Typ-Coercion für Zahlenfelder
    - Enum-Validierung (object_type, usage, employment_type)
    - Datumsformat-Normalisierung
    - Plausibilitäts-Warnings in meta
    Mutiert result in-place und gibt es zurück.
    """
    extracted = result.get("extracted_data")
    if not isinstance(extracted, dict):
        return result

    warnings = []

    # 1. Typ-Coercion: Zahlenfelder normalisieren
    for key in list(extracted.keys()):
        if key in _NUMERIC_FIELDS:
            raw = extracted[key]
            if raw is None:
                continue
            coerced = _coerce_number(raw)
            if coerced is not None:
                # Ganzzahlen als int speichern (months_covered etc.)
                extracted[key] = int(coerced) if coerced == int(coerced) else coerced
            elif isinstance(raw, str) and raw.strip():
                # Nicht konvertierbar → auf null setzen + warnen
                warnings.append(f"Feld '{key}' nicht als Zahl erkannt: '{raw}'")
                extracted[key] = None

    # 2. Enum-Validierung
    for key, valid_values in _ENUM_FIELDS.items():
        val = extracted.get(key)
        if val and isinstance(val, str) and val not in valid_values:
            # Fuzzy-Match: case-insensitive Suche
            match = next((v for v in valid_values if v.lower() == val.lower()), None)
            if match:
                extracted[key] = match
            else:
                warnings.append(f"Ungültiger Wert für '{key}': '{val}' (erwartet: {', '.join(sorted(valid_values))})")
                extracted[key] = None

    # 3. Datumsformat-Normalisierung: DD.MM.YYYY → YYYY-MM-DD
    import re
    for key in list(extracted.keys()):
        val = extracted[key]
        if isinstance(val, str):
            # DD.MM.YYYY → YYYY-MM-DD
            m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", val.strip())
            if m:
                d, mo, y = m.groups()
                extracted[key] = f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    # 4. Plausibilitäts-Checks
    for key, (vmin, vmax, msg_tpl) in _PLAUSIBILITY.items():
        val = extracted.get(key)
        if isinstance(val, (int, float)) and (val < vmin or val > vmax):
            warnings.append(msg_tpl.format(val=val))

    # Warnings in meta speichern
    if warnings:
        meta = result.get("meta") or {}
        meta["validation_warnings"] = warnings
        result["meta"] = meta
        for w in warnings:
            logger.warning(f"Sanitize: {w}")

    return result


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
    pages_truncated = False  # True wenn gescanntes PDF mehr Seiten hat als gerendert wurden
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
                {"role": "user", "content": f"Dokument: {filename} ({page_count} Seiten)\n\nExtrahierter Text:\n{extracted_text[:15000]}"},
            ]
            model = "gpt-4o-mini"  # Günstiger für Text-Only
        else:
            # PDF ohne Text (gescannt) → als Bild rendern und Vision API nutzen
            page_images = pdf_pages_to_images(file_bytes, max_pages=5)
            if page_images:
                if page_count > len(page_images):
                    pages_truncated = True
                    logger.warning(f"Scan-PDF {filename}: {page_count} Seiten, aber nur {len(page_images)} gerendert — months_covered wird ignoriert")
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

        # Seiten abgeschnitten? → months_covered entfernen (GPT hat nicht alle Seiten gesehen)
        if pages_truncated and isinstance(result.get("extracted_data"), dict):
            ed = result["extracted_data"]
            for cov_key in ("months_covered", "documents_covered"):
                if ed.get(cov_key):
                    logger.warning(f"Entferne {cov_key}={ed[cov_key]} weil PDF abgeschnitten ({page_count} Seiten, nur 5 gerendert)")
                    ed.pop(cov_key)
            meta = result.get("meta") or {}
            meta["pages_truncated"] = True
            meta["total_pages"] = page_count
            result["meta"] = meta

        # Post-GPT Sanitization
        result = _sanitize_extracted_data(result)

        # Rotation-Retry: Bild könnte physisch gedreht sein (kein EXIF-Fix möglich)
        if result.get("doc_type") == "Sonstiges" and mime_type.startswith("image/") and not _filename_fallback_doc_type(filename):
            try:
                img = Image.open(BytesIO(file_bytes))
                rotated_img = img.rotate(-90, expand=True)
                buf = BytesIO()
                fmt = "JPEG" if mime_type in ("image/jpeg", "image/jpg") else "PNG"
                rotated_img.save(buf, format=fmt)
                rotated_b64 = base64.standard_b64encode(buf.getvalue()).decode("utf-8")
                logger.info(f"Sonstiges bei Bild {filename} — Retry mit 90°-Rotation")
                retry_content = [
                    {"type": "text", "text": f"Dokument: {filename} (gedrehte Version)"},
                    {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{rotated_b64}", "detail": "high"}}
                ]
                retry_resp = client.chat.completions.create(
                    model="gpt-4o", messages=[system_msg, {"role": "user", "content": retry_content}],
                    max_tokens=4000, temperature=0.1
                )
                retry_text = retry_resp.choices[0].message.content
                if "```json" in retry_text:
                    retry_text = retry_text.split("```json")[1].split("```")[0]
                elif "```" in retry_text:
                    retry_text = retry_text.split("```")[1].split("```")[0]
                retry_result = json.loads(retry_text.strip())
                if retry_result.get("doc_type") != "Sonstiges":
                    logger.info(f"Rotation-Retry erfolgreich: {retry_result.get('doc_type')} (war Sonstiges)")
                    retry_result["meta"] = retry_result.get("meta") or {}
                    retry_result["meta"]["rotation_retry"] = True
                    return _sanitize_extracted_data(retry_result)
            except Exception as e:
                logger.debug(f"Rotation-Retry fehlgeschlagen: {e}")

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
        db.log_error("gpt_analysis", str(e), source="analyze_with_gpt4o")
        raise HTTPException(status_code=500, detail=str(e))


# ── Document Processor (delegates to document_processor.py) ───────
from document_processor import DocumentProcessor, FileInput, MIME_MAP as DOC_MIME_MAP
processor = DocumentProcessor(analyze_fn=analyze_with_gpt4o)


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
            except (json.JSONDecodeError, TypeError, ValueError):
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

class EuropaceRequest(BaseModel):
    case_id: str


class EuropaceResponse(BaseModel):
    case_id: str
    success: bool
    payload: Optional[dict] = None
    validation_errors: list = []
    validation_warnings: list = []
    is_valid: bool = False


@app.post("/build-europace-payload", response_model=EuropaceResponse)
async def build_europace_payload_endpoint(request: EuropaceRequest):
    """
    Baut den Europace API Payload aus den Case-Daten.
    Delegiert an import_builder fuer korrekte Europace API v1.0 Struktur.
    """
    import import_builder

    try:
        build_result = import_builder.build_europace_payload(request.case_id)
    except ValueError as e:
        return EuropaceResponse(
            case_id=request.case_id, success=False,
            validation_errors=[str(e)], is_valid=False,
        )

    validation = import_builder.validate_payload(
        build_result["payload"], build_result["effective_view"]
    )

    return EuropaceResponse(
        case_id=request.case_id,
        success=validation["is_valid"],
        payload=build_result["payload"],
        validation_errors=validation["errors"],
        validation_warnings=validation["warnings"],
        is_valid=validation["is_valid"],
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

  "sender_first_name": "Vorname des Absenders/Kontaktperson (aus Signatur, Grußformel, oder From-Name extrahieren). Nur den Vornamen, nicht den Nachnamen. Bei Firmennamen null.",

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
            except (ValueError, TypeError):
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
            except (ValueError, TypeError):
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
            except (ValueError, TypeError):
                warnings.append({"field": key, "message": "Keine gültige Zahl", "value": value})

        if key in ["loan_amount", "darlehenssumme"] and value:
            try:
                val = float(value)
                rule = VALIDATION_RULES["loan_amount"]
                if val < rule["min"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
                elif val > rule["max"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
            except (ValueError, TypeError):
                warnings.append({"field": key, "message": "Keine gültige Zahl", "value": value})

        if key in ["equity", "equity_to_use", "eigenkapital"] and value:
            try:
                val = float(value)
                if val < 0:
                    errors.append({"field": key, "message": "Eigenkapital kann nicht negativ sein", "value": val})
            except (ValueError, TypeError):
                warnings.append({"field": key, "message": "Keine gültige Zahl", "value": value})

        if key in ["living_space", "wohnflaeche"] and value:
            try:
                val = float(value)
                rule = VALIDATION_RULES["living_space"]
                if val < rule["min"]:
                    warnings.append({"field": key, "message": f"Wohnfläche unter {rule['min']} m² unüblich", "value": val})
                elif val > rule["max"]:
                    errors.append({"field": key, "message": rule["message"], "value": val})
            except (ValueError, TypeError):
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
            except (ValueError, TypeError):
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
        except (ValueError, TypeError):
            pass

    if purchase_price and loan_amount:
        try:
            if float(loan_amount) > float(purchase_price) * 1.2:
                warnings.append({
                    "field": "loan_amount",
                    "message": "Darlehenssumme > 120% des Kaufpreises (Vollfinanzierung?)",
                    "value": f"{loan_amount} vs {purchase_price}"
                })
        except (ValueError, TypeError):
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

import db_postgres as db
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
        db.log_error("email_process", str(e), source="process_email")
        raise HTTPException(status_code=500, detail={"error": str(e)})


async def _process_gdrive_async(case_id: str, links: list):
    """Background task: sync Google Drive files to OneDrive, then trigger scan.
    Runs blocking work in thread pool to avoid blocking the event loop."""
    import asyncio
    try:
        import gdrive
        import case_logic as _cases

        _case = _cases.load_case(case_id)
        folder_id = _case.get("onedrive_folder_id", "") if _case else ""

        # Wait for n8n to create OneDrive folder if needed
        if not folder_id:
            import time
            logger.info(f"[{case_id}] Waiting for OneDrive folder (n8n setup)...")
            await asyncio.to_thread(time.sleep, 15)
            _case = _cases.load_case(case_id)
            folder_id = _case.get("onedrive_folder_id", "") if _case else ""

        if not folder_id:
            logger.warning(f"[{case_id}] No OneDrive folder — GDrive sync skipped")
            return

        # 1. Sync GDrive → OneDrive
        sync_result = await asyncio.to_thread(
            gdrive.sync_to_onedrive,
            case_id=case_id, links=links, onedrive_folder_id=folder_id,
        )
        uploaded = sync_result.get("files_uploaded", 0)
        logger.info(f"[{case_id}] GDrive sync: {uploaded} uploaded, {sync_result.get('files_skipped', 0)} skipped")

        # 2. Trigger n8n OneDrive scan to analyze uploaded files
        if uploaded > 0:
            n8n_scan = os.getenv("N8N_SCAN_WEBHOOK", "")
            n8n_key = os.getenv("N8N_WEBHOOK_API_KEY", "")
            if n8n_scan:
                import httpx
                headers = {"X-API-Key": n8n_key} if n8n_key else {}
                async with httpx.AsyncClient(timeout=300) as http_client:
                    resp = await http_client.post(n8n_scan, headers=headers, json={
                        "case_id": case_id,
                        "onedrive_folder_id": folder_id,
                        "force_reanalyze": False,
                    })
                    resp.raise_for_status()
                    logger.info(f"[{case_id}] OneDrive scan triggered after GDrive sync")

                readiness_result = await asyncio.to_thread(rdns.check_readiness, case_id)
                await asyncio.to_thread(notify.dispatch_notifications, case_id, readiness_result)
    except Exception as e:
        logger.error(f"[{case_id}] Google Drive async processing failed: {e}")
        db.log_error("background_task", str(e), source="gdrive_async", case_id=case_id)


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

    # 1. Atomic Dedup-Lock (prevents race condition with parallel requests)
    if not db.try_lock_email(request.provider_message_id):
        logger.info(f"E-Mail bereits verarbeitet/gesperrt: {request.provider_message_id}")
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
  "sender_first_name": "Vorname des ABSENDERS (nicht des Antragstellers!) - aus Signatur, Grußformel oder From-Header extrahieren. Nur Vorname, kein Nachname. Bei reinen Firmennamen null.",
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
        partner_email_for_case = _safe_partner_email(parsed.get("partner_email"), request.from_email)
        partner_name = parsed.get("sender_first_name") or request.from_name or ""
        cases.create_case(
            case_id=case_id,
            applicant_name=applicant_name,
            partner_email=partner_email_for_case,
            partner_phone="",
            conversation_id=request.conversation_id,
            facts=facts,
            partner_name=partner_name,
        )
        needs_folder = True  # n8n soll OneDrive-Ordner erstellen

        # Finlink Lead erstellen (wir sind bereits im Thread)
        import import_builder
        try:
            import_builder.create_finlink_lead(case_id, facts, applicant_name, partner_email_for_case)
        except Exception as _fl_err:
            logger.error(f"[{case_id}] Finlink lead creation failed: {_fl_err}")
            db.log_error("import", str(_fl_err), source="finlink_lead", case_id=case_id)

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

    # 6. Anhänge direkt intern verarbeiten (delegiert an DocumentProcessor)
    docs_processed = []
    if request.attachments:
        files = []
        for filename, b64_data in request.attachments.items():
            try:
                file_bytes = base64.b64decode(b64_data)
                ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
                mime = DOC_MIME_MAP.get(ext, 'application/octet-stream')
                files.append(FileInput(filename=filename, file_bytes=file_bytes, mime_type=mime, source="email"))
            except Exception as e:
                logger.error(f"Attachment decode failed for {filename}: {e}")
                docs_processed.append({"filename": filename, "success": False, "error": str(e)})

        if files:
            batch_result = processor.process_batch(case_id, files)
            docs_processed = batch_result.get("results", [])

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

    # applicant_name für n8n Ordnerbenennung
    _case_for_response = cases.load_case(case_id)
    _applicant_name = _case_for_response.get("applicant_name", "") if _case_for_response else ""

    return {
        "action": "processed",
        "case_id": case_id,
        "applicant_name": _applicant_name,
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


@app.get("/api/dashboard/case/{case_id}/queue")
async def get_processing_queue(case_id: str):
    """Gibt den aktuellen Verarbeitungsstatus der Queue für einen Case zurück."""
    return processor.get_queue(case_id)


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


async def _process_document_background(case_id, filename, file_bytes, mime_type, onedrive_file_id=None):
    """Delegates to processor.process_single() in a background thread."""
    import asyncio
    file_input = FileInput(
        filename=filename, file_bytes=file_bytes, mime_type=mime_type,
        onedrive_file_id=onedrive_file_id, source="onedrive"
    )
    await asyncio.to_thread(processor.process_single, case_id, file_input)


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
            db.log_error("email_send", str(notify_err), source="dispatch_notifications", case_id=request.case_id)

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
        db.log_error("readiness", str(e), source="full_readiness_check", case_id=request.case_id)
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
            cur.execute("SELECT \"to\", subject, logged_at, dry_run, case_id FROM email_test_log ORDER BY logged_at DESC LIMIT 20")
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
    for table, col in [("fin_documents", '"caseId"'), ("processed_emails", "case_id"), ("email_test_log", "case_id"), ("fin_cases", "case_id")]:
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
    # Auch alle Einträge mit leerem from_email löschen (kaputte Lock-Einträge)
    try:
        with _pg._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM processed_emails WHERE from_email = '' OR from_email IS NULL")
                results["empty_from_email"] = cur.rowcount
    except Exception as e:
        results["empty_from_email"] = f"error: {e}"
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
    import db_postgres as db

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
            db.log_error("email_send", str(e), source="send_reminder", case_id=case_id)
            skipped_reasons["error"] += 1
            continue

        # Audit log updaten
        try:
            case_fresh = cases.load_case(case_id)
            if case_fresh:
                import db_postgres as db
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
        db.log_error("background_task", str(e), source="reminder_check")
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
    files_uploaded: int = 0
    files_skipped: int = 0
    errors: list = []

@app.post("/process-google-drive", response_model=ProcessGoogleDriveResponse)
async def process_google_drive(request: ProcessGoogleDriveRequest):
    """
    Syncs files from Google Drive to OneDrive (no analysis).
    Analysis happens via n8n OneDrive scan.
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

        case = cases.load_case(request.case_id)
        folder_id = case.get("onedrive_folder_id", "") if case else ""
        if not folder_id:
            return ProcessGoogleDriveResponse(
                success=False, case_id=request.case_id,
                errors=["Kein OneDrive-Ordner konfiguriert"],
            )

        result = await asyncio.to_thread(
            gdrive.sync_to_onedrive,
            case_id=request.case_id,
            links=request.google_drive_links,
            onedrive_folder_id=folder_id,
        )

        return ProcessGoogleDriveResponse(
            case_id=request.case_id,
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
        db.log_error("background_task", str(e), source="process_google_drive", case_id=request.case_id)
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
    finlink_lead_id: Optional[str] = None
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
        db.log_error("import", str(e), source="execute_import", case_id=request.case_id)
        return ImportCaseResponse(
            success=False,
            case_id=request.case_id,
            errors=[str(e)],
            dry_run=request.dry_run,
        )


# Re-exports for backward compatibility (gdrive.py imports these from main)
from document_processor import (  # noqa: E402, F401
    _detect_is_couple,
    _map_extracted_to_facts,
    _maybe_update_applicant_name,
    _collect_person_names_from_docs,
)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
