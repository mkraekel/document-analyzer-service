# Document Analyzer Service — Projektdokumentation

> Automatisierte Baufinanzierungs-Pipeline: E-Mail-Eingang → Dokumentenanalyse → Readiness Check → Europace-Export

## Architektur

```
┌─────────────┐     ┌──────────────────────────┐     ┌────────────┐
│   n8n        │────▶│  FastAPI Backend (Python) │────▶│ PostgreSQL │
│  Workflows   │     │  Railway.app              │     │  Railway   │
└─────────────┘     └──────────┬───────────────┘     └────────────┘
                               │
                    ┌──────────┴───────────┐
                    │  React Dashboard     │
                    │  (Vite + TypeScript)  │
                    └──────────────────────┘
```

**Stack:** Python FastAPI · React + Vite + TS · PostgreSQL · OpenAI GPT-4o / GPT-4o-mini · Railway

---

## Projektstruktur

```
document-analyzer-service/
├── main.py                 # FastAPI App, GPT-Analyse, E-Mail-Verarbeitung
├── dashboard.py            # Dashboard API-Routen
├── auth.py                 # JWT-Auth Middleware + Login
├── case_logic.py           # Case CRUD, Matching, Facts-Merging, Gatekeeper
├── readiness.py            # Readiness Check (EINZIGE autoritative Implementierung)
├── notify.py               # E-Mail-Versand (Templates, kein GPT mehr)
├── import_builder.py       # Europace-Payload Builder
├── gdrive.py               # Google Drive Integration
├── db_postgres.py          # PostgreSQL Backend + Schema + Migrationen
├── seatable.py             # DB-Backend Switch (Legacy)
├── requirements.txt        # Python Dependencies
│
├── dashboard/              # React SPA
│   └── src/
│       ├── pages/          # Login, Overview, Cases, CaseDetail, Triage, OutgoingEmails
│       ├── components/     # Layout, StatusBadge, Toast, etc.
│       ├── lib/format.ts   # Field Labels, Formatter
│       ├── api/client.ts   # Axios API Client
│       └── types/api.ts    # TypeScript Interfaces
│
└── railway.json            # Deployment Config
```

---

## Datenfluss

```
1. E-Mail kommt rein (n8n Webhook)
   │
   ▼
2. POST /process-email
   ├── Gatekeeper: Blocklist → Allowlist → Subject-Filter
   ├── E-Mail parsen (GPT-4o-mini): Typ, Name, Referenzen
   ├── Case Matching: case_id → conversation_id → email+name → neu/triage
   └── Case erstellen oder aktualisieren
   │
   ▼
3. Dokument-Anhänge verarbeiten (POST /process-document)
   ├── EXIF-Auto-Rotation (Pillow)
   ├── PDF: Text extrahieren (pypdf) oder als Bilder rendern (PyMuPDF, max 5 Seiten)
   ├── GPT-4o Vision: Klassifizierung + Datenextraktion
   ├── Post-GPT Sanitization: Typ-Coercion, Enum-Validierung, Plausibilitäts-Checks
   ├── Abgeschnittene PDFs (>5 Seiten): months_covered wird entfernt
   ├── Fallback: Dateiname-Heuristik wenn "Sonstiges"
   ├── Fallback: 90°-Rotation-Retry wenn immer noch "Sonstiges"
   └── Facts in Case mergen (last-write-wins, mit Overwrite-Logging)
   │
   ▼
4. Readiness Check (POST /check-readiness)
   ├── Effective View berechnen: defaults < derived < facts < answers < overrides
   ├── Pflichtfelder prüfen (Finanzierung, Antragsteller, Adresse)
   ├── Dokumente zählen (inkl. months_covered für Kontoauszüge)
   ├── Warnungen generieren (Ablauf, Mieteinnahmen, etc.)
   └── Status bestimmen: INTAKE → NEEDS_QUESTIONS → READY → etc.
   │
   ▼
5. Benachrichtigung (notify.py)
   ├── Partner-E-Mail: "Hallo [Vorname], für [Antragsteller] bräuchte ich noch..."
   ├── Broker-Confirmation: Zusammenfassung + FREIGABE-Kommando
   └── Erinnerungen mit Cooldown (10 Min)
   │
   ▼
6. Europace-Export (POST /build-europace-payload)
   └── Effective View → Europace XML-Schema Mapping
```

---

## Datenbank-Schema (PostgreSQL)

### fin_cases
| Feld | Typ | Beschreibung |
|------|-----|-------------|
| case_id | TEXT UNIQUE | z.B. CASE-1707234567000 |
| applicant_name | TEXT | Top-Level (nicht in JSON) |
| partner_email | TEXT | E-Mail des Vertriebspartners |
| partner_name | TEXT | Name des Partners (aus from_name) |
| status | TEXT | INTAKE, READY, COMPLETE, ARCHIVED, etc. |
| facts_extracted | JSONB | GPT-Extraktionen aus Dokumenten |
| answers_user | JSONB | {partner: {...}, broker: {...}} |
| manual_overrides | JSONB | Manuelle Korrekturen |
| derived_values | JSONB | Berechnete Werte |
| conversation_ids | JSONB | Array von E-Mail-Thread-IDs |
| readiness | JSONB | Letztes Readiness-Ergebnis |
| audit_log | JSONB | Audit-Trail (max 100 Einträge) |
| onedrive_folder_id | TEXT | OneDrive Ordner |
| onedrive_web_url | TEXT | OneDrive URL |
| europace_case_id | TEXT | Europace Vorgangs-ID |

### fin_documents
| Feld | Typ | Beschreibung |
|------|-----|-------------|
| caseId | TEXT | FK zu fin_cases |
| file_name | TEXT | Dateiname |
| doc_type | TEXT | Klassifizierter Dokumenttyp |
| extracted_data | JSONB | GPT-Extraktion |
| processing_status | TEXT | pending, completed, error |
| onedrive_file_id | TEXT | OneDrive Datei-ID |
| gdrive_file_id | TEXT | Google Drive Datei-ID |

### processed_emails
| Feld | Typ | Beschreibung |
|------|-----|-------------|
| provider_message_id | TEXT UNIQUE | Exchange/Gmail Message-ID |
| mail_type | TEXT | new_request, reply, forward |
| processing_result | TEXT | case_created, triage, etc. |
| case_id | TEXT | Zugeordneter Case |
| from_email / subject | TEXT | Absender / Betreff |
| body_text / body_html | TEXT | E-Mail-Inhalt |

### email_test_log
Dry-Run E-Mails (wenn EMAIL_DRY_RUN=true).

---

## GPT-Modelle

| Kontext | Modell | Warum |
|---------|--------|-------|
| Dokument-Analyse (Bilder, Scans) | **GPT-4o** | Vision-Fähigkeit für Fotos/Scans |
| Dokument-Analyse (Text-PDFs) | **GPT-4o-mini** | Günstiger, Text reicht |
| E-Mail-Parsing | **GPT-4o-mini** | Strukturierte Extraktion aus Text |
| Prompt Caching | Automatisch | System-Message separiert → 50% Input-Rabatt |

### Post-GPT Sanitization (_sanitize_extracted_data)

Nach jeder GPT-Analyse wird das Ergebnis validiert und bereinigt:

| Prüfung | Details |
|---------|---------|
| **Typ-Coercion** | Zahlenfelder: "3.500,00 EUR" → 3500.0, deutsches Kommaformat, Währungssuffixe |
| **Tausender-Heuristik** | "3.500" → 3500 (nicht 3.5), erkennt `\d{1,3}(\.\d{3})+` Pattern |
| **Enum-Validierung** | object_type (ETW/EFH/...), usage, employment_type — case-insensitiv |
| **Datumsformat** | DD.MM.YYYY → YYYY-MM-DD automatisch |
| **Plausibilität** | Wohnfläche 10-500m², Kaufpreis >10k, months_covered 1-24 |

Warnings werden in `meta.validation_warnings` gespeichert und geloggt.

Zentrale Dicts: `_NUMERIC_FIELDS`, `_ENUM_FIELDS`, `_PLAUSIBILITY` — erweiterbar ohne Spaghetti-Code.

---

## Gatekeeper (case_logic.py)

Entscheidet ob eine E-Mail verarbeitet wird:

1. **BLOCKLIST** — Sofort abweisen (z.B. System-Notifications wie Microsoft Bookings)
2. **Intern** (@alexander-heil.com):
   - Replies/Forwards mit Finanz-Keyword → durchlassen
   - Neue Mails mit Finanz-Keyword → Triage (force_triage)
   - Non-Finance → blocken
3. **Extern** — Nur ALLOWLIST-Absender, Non-Finance gefiltert
4. **NON_FINANCE Filter**: receipt, booking, buchung, newsletter, webinar, statusänderung, etc.

---

## Readiness Check (readiness.py)

### Effective View Merge-Reihenfolge
```
defaults (ETW, Vermietet) < derived < facts_extracted < answers_user < manual_overrides
```

### Pflichtfelder
- **Finanzierung:** Kaufpreis, Darlehenssumme, Eigenkapital, Objektart, Nutzungsart
- **Antragsteller:** Vor-/Nachname, Geburtsdatum, Beschäftigung, Nettoeinkommen, Adresse
- **Selbstständige zusätzlich:** Selbstständig seit, Gewinn Vorjahr

### Dokument-Anforderungen
- Gehaltsnachweis: 3x (max 90 Tage), per_person
- Kontoauszug: 3x (max 90 Tage), per_person, **zählt months_covered** statt Dateien
- Ausweiskopie: 1x, per_person, warnt wenn < 90 Tage gültig
- Steuerbescheid/Steuererklärung: 1-2x je nach Beschäftigung
- Und ~15 weitere Dokumenttypen

### Besonderheiten
- **months_covered**: Ein PDF-Kontoauszug über 5 Monate zählt als 5/3
- **Abgeschnittene PDFs**: Wenn ein gescanntes PDF >5 Seiten hat, wird `months_covered` entfernt (→ zählt als 1). `meta.pages_truncated=true` und `meta.total_pages` werden gesetzt.
- **DOC_TYPE_ALIASES**: GPT-Varianten auf kanonische Typen mappen
- **Kontoauszüge als Eigenkapitalnachweis**: ≥3 Monate Kontoauszüge ersetzen fehlenden Eigenkapitalnachweis
- **Gemeinschaftskonto**: Kontoauszüge nicht verdoppeln bei Paaren

---

## Facts-Merging (case_logic.py)

### merge_facts() — Last-Write-Wins mit Overwrite-Logging
```python
# Neuere Werte überschreiben ältere (kein fill-empty-only mehr)
# Junk-Werte (N/A, null, -, k.A.) werden ignoriert
# Verschachtelte Dicts werden rekursiv gemergt
# Überschreibungen werden geloggt: {field, old, new}
```

Jede Überschreibung wird:
1. Per `logger.info()` geloggt: `income_data.net_income: 3200.0 → 3500.0`
2. Im **audit_log** des Cases gespeichert (Feld `overwrites` im Audit-Eintrag)
3. Für den Broker im Dashboard nachvollziehbar

### Junk-Werte
`N/A, n/a, null, None, none, nicht verfügbar, unbekannt, -, –, k.A., k. A.`

### Per-Person Routing
- `_is_primary_applicant()`: Name-Matching mit Case-Antragsteller
- `_detect_is_couple()`: Prüft ob tatsächlich ein Paar vorliegt
- Primary → `applicant_data`, `income_data`, `id_data`
- Secondary → `applicant_data_2`, `income_data_2`, `id_data_2`

---

## E-Mail-Benachrichtigungen (notify.py)

### Partner-E-Mail (kein GPT mehr)
```
Hallo [Vorname des Partners],

für die Finanzierungsanfrage von [Antragsteller] bräuchte ich noch
folgende Informationen, um die Anfrage bei der Bank einreichen zu können:

Folgende Angaben:
  - Kaufpreis
  - Darlehenssumme

Folgende Dokumente:
  - Kontoauszug (3x benötigt)
  - Gehaltsnachweis (3x benötigt)

Könnten Sie mir diese Unterlagen/Informationen zukommen lassen?

Mit freundlichen Grüßen
Alexander Heil Finanzierung
```

### Cooldown
10 Minuten pro (case_id, status) — verhindert Doppelversand.

---

## Europace-Export (import_builder.py)

Baut den Europace-Payload aus der Effective View:
- Antragsteller(daten) inkl. Person 2
- Beschäftigung & Einkommen (inkl. monthly_rental_income → monatlicheEinnahmenAusNebentaetigkeit)
- Objekt (Adresse, Typ, Nutzung, Kaufpreis)
- Finanzierung (Darlehenssumme, Eigenkapital, Zinsbindung)
- Partner-ID: CZU26 (Alexander Heil) oder XET70 (Matthias Lächele)
- `_clean_payload()` entfernt None-Werte rekursiv

---

## Dashboard (React SPA)

| Seite | Route | Beschreibung |
|-------|-------|-------------|
| Login | /app/login | JWT-Auth |
| Overview | /app | Statistiken, Quick-Links |
| Cases | /app/cases | Case-Liste mit Status |
| Case Detail | /app/cases/:id | Vollansicht, editierbare Felder, Europace-Gruppen |
| Triage | /app/triage | E-Mails ohne Case-Zuordnung |
| Outgoing Emails | /app/emails | Dry-Run E-Mail-Log |

### CaseDetail Features
- **Editierbare Europace-Felder** in Gruppen (Objekt, Antragsteller, Einkommen, etc.)
- **Partner-ID Dropdown**: CZU26 / XET70 → speichert in overrides
- **Finanzierungsdaten-Widget**: Nutzt effectiveView, speichert in overrides
- **Readiness-Anzeige**: Fehlende Felder, Dokumente, Warnungen
- **Aktionen**: Neu analysieren, Scan OneDrive, GDrive verarbeiten, Europace-Export

---

## Bild-Verarbeitung

### EXIF-Auto-Rotation
Kamerafotos haben oft EXIF-Orientation-Tags. `_fix_image_orientation()` nutzt Pillow `ImageOps.exif_transpose()` um Bilder vor der GPT-Analyse korrekt zu drehen.

### Rotation-Retry
Wenn EXIF Orientation=1 (Normal, keine Rotation-Metadaten vorhanden) aber das Bild visuell gedreht ist und GPT "Sonstiges" sagt:
→ Automatischer Retry mit 90°-gedrehtem Bild

### Dateiname-Fallback
Generische Dateinamen (z.B. "21588.jpg") geben GPT keinen Hinweis. Wenn der Dateiname Keywords enthält (z.B. "gehaltsnachweis.pdf"), wird dieser als Fallback-Klassifizierung verwendet.

---

## Deploy

```bash
git push origin main  # → Railway Auto-Deploy (~2-3 Min)
```

- **URL**: https://document-analyzer-service-production.up.railway.app
- **Dashboard**: /app
- **API**: /api/*
- **Health**: /health

### Umgebungsvariablen
- `OPENAI_API_KEY`, `DATABASE_URL`, `JWT_SECRET`, `DASHBOARD_PASSWORD`
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`
- `EMAIL_DRY_RUN=true` → Kein echter Versand
- `BROKER_EMAIL=backoffice@alexander-heil.com`

---

## Changelog

### 2026-03-11
- **Post-GPT Sanitization**: `_sanitize_extracted_data()` Layer nach jeder GPT-Analyse — Typ-Coercion für Zahlenfelder (deutsches Kommaformat, Währungssuffixe), Enum-Validierung (object_type, usage, employment_type), Datumsformat-Normalisierung (DD.MM.YYYY → ISO), Plausibilitäts-Checks mit Warnings
- **Tausender-Heuristik**: "3.500" wird als 3500 erkannt (nicht 3.5) — Pattern `\d{1,3}(\.\d{3})+`
- **Merge-Overwrite-Logging**: `merge_facts()` loggt jede Wertüberschreibung (field, old, new) per Logger + im audit_log des Cases
- **5-Seiten-Limit Safety**: Gescannte PDFs mit >5 Seiten: `months_covered` wird entfernt statt blind zu vertrauen. Dokument zählt als 1, Case bleibt unvollständig → Broker muss hinschauen. `meta.pages_truncated` + `meta.total_pages` für Transparenz
- **Google Drive Links vereinfacht**: Labels "Persönliches"/"Objektunterlagen" → einfach "Google Drive (1)"/"Google Drive (2)"
- **GDrive Import Button**: 2 separate Buttons → 1 Button "Google Drive importieren"

### 2026-03-10
- **OneDrive Attachment Upload**: n8n Mail Trigger Workflow erweitert — E-Mail-Anhänge werden jetzt automatisch in den OneDrive-Ordner des Cases hochgeladen (vorher nur analysiert, nicht gespeichert)
- **Workflow-Fixes**: Merge-Node von passThrough auf append geändert (TRUE-Branch Items gingen vorher verloren), Mark as Read nutzt jetzt `$json._outlook_id` statt fragiler `.first()`-Referenz
- **Neue Workflow-Nodes**: Enrich Response, Enrich Folder Result, Prepare Upload, Upload to OneDrive

### 2026-03-08
- **monthly_rent Extraktion**: Kontoauszüge extrahieren jetzt monatliche Miete automatisch
- **monthly_rent in Europace**: Feld in Ausgaben-Gruppe im Export
- **RECHECK GDrive**: Scannt Google Drive vor Readiness-Check auf neue Dateien
- **Dotted field keys Fix**: Verschachtelte Felder im effective_view korrekt auflösen
- **Doc Type Filter + Editing**: Dashboard zeigt Dokumenttyp-Filter, Typ kann nachträglich geändert werden
- **X-API-Key Header**: Alle n8n Webhook-Aufrufe nutzen jetzt API-Key Auth
- **extracted_data list Fix**: 500-Fehler behoben wenn GPT eine Liste statt Dict zurückgibt

### 2026-03-05
- **Kontoauszug months_covered**: Readiness zählt jetzt `months_covered` statt Datei-Anzahl. Ein PDF über 5 Monate = 5/3 ✓
- **merge_facts → Last-Write-Wins**: Neuere Dokumente überschreiben ältere Werte (vorher fill-empty-only)
- **Objektadresse-Fix**: GPT-verschachteltes Address-Objekt wird jetzt korrekt aufgelöst + Fallback-Suchpfade
- **Partner-E-Mails überarbeitet**: Festes Template mit persönlicher Anrede statt GPT-generiert, kein OpenAI mehr in notify.py
- **partner_name DB-Feld**: Aus from_name bei Case-Erstellung, in effective_view verfügbar
- **Microsoft Bookings Blocklist**: AlexanderHeil1@... + "Neue Buchung von" im NON_FINANCE Filter
- **Junk-Werte erweitert**: "null", "None", "none" zu _JUNK_VALUES hinzugefügt
- **Rotation-Retry**: Wenn GPT "Sonstiges" bei gedrehtem Bild → automatischer 90°-Retry
- **Prompt-Verstärkung**: Gedrehte Personalausweise explizit beschrieben
- **Scan-PDF Limit**: Von 2 auf 5 Seiten erhöht

### 2026-03-04 (vorherige Session)
- **Mieteinnahmen**: monthly_rental_income Feld, Warning, Europace-Export
- **DOC_TYPE_ALIASES**: Zinsbescheinigung, Flurkarte/Lageplan
- **Auszahlungsbetrag > Netto**: Priorität bei Gehaltsnachweis
- **Anrede-Raten**: _guess_salutation() aus Vorname (~300 deutsche Namen)
- **Partner-ID Dropdown**: CZU26 / XET70
- **OpenAI Prompt Caching**: System/User Messages getrennt
- **EXIF-Auto-Rotation**: Pillow ImageOps.exif_transpose()
- **Phantom-Zweitantragsteller**: _detect_is_couple() verhindert OCR-Fehler
- **N/A Junk-Filter**: in merge_facts() + GPT-Prompt
- **Ausweis-Rückseite**: Prompt mit MRZ, Adresse, Bundesdruckerei
- **Finanzierungsdaten-Widget**: effectiveView statt flatFacts
