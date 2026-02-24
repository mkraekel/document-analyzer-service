# Document Analyzer Service

Einfacher Python Service für Dokumentenanalyse mit GPT-4o.

## Was macht der Service?

1. Nimmt PDF oder Bild entgegen
2. Bei PDFs: Extrahiert erst Text (schnell & günstig)
3. Wenn kein Text: Nutzt GPT-4o Vision
4. Gibt strukturierte JSON-Daten zurück

## Lokal starten

```bash
cd document-analyzer-service

# Virtual Environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Dependencies
pip install -r requirements.txt

# API Key in .env Datei setzen (bleibt persistent)
cp .env.example .env
# Dann .env editieren und API Key eintragen

# Starten
python main.py
```

Service läuft auf http://localhost:8000

## Mit Docker

```bash
docker build -t doc-analyzer .
docker run -p 8000:8000 -e OPENAI_API_KEY="sk-..." doc-analyzer
```

## API Endpunkte

### POST /analyze

Analysiert ein Dokument.

```bash
curl -X POST http://localhost:8000/analyze \
  -F "file=@dokument.pdf"
```

Response:
```json
{
  "success": true,
  "filename": "dokument.pdf",
  "doc_type": "Gehaltsnachweis",
  "confidence": "high",
  "meta": {
    "doc_date": "2024-01-15",
    "person_name": "Max Mustermann"
  },
  "extracted_data": {
    "arbeitgeber": "Firma GmbH",
    "brutto": 5000,
    "netto": 3200,
    "monat": "Januar 2024"
  }
}
```

### GET /health

Health Check für Monitoring.

## Deployment

### Railway

1. Repo zu GitHub pushen
2. In Railway: New Project → Deploy from GitHub
3. Environment Variable setzen: `OPENAI_API_KEY`
4. Deploy

### Render

1. New Web Service
2. Connect GitHub Repo
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Environment: `OPENAI_API_KEY`

## n8n Integration

In n8n dann nur noch:

1. **OneDrive Download** - Datei holen
2. **HTTP Request** - POST an /analyze mit Binary
3. **SeaTable** - Ergebnis speichern

Fertig. Keine 50 Nodes mehr.
