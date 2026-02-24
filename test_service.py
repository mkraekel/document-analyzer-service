"""
Test-Script für den Document Analyzer Service
Erstellt ein einfaches Test-PDF und testet den Service
"""

import httpx
import io
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

def create_test_pdf() -> bytes:
    """Erstellt ein Test-PDF mit Beispiel-Gehaltsdaten"""
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)

    # Header
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, 800, "Gehaltsabrechnung")
    c.drawString(50, 780, "Januar 2024")

    # Arbeitgeber
    c.setFont("Helvetica", 12)
    c.drawString(50, 740, "Arbeitgeber: Musterfirma GmbH")
    c.drawString(50, 720, "Personalnummer: 12345")

    # Mitarbeiter
    c.drawString(50, 680, "Name: Max Mustermann")
    c.drawString(50, 660, "Geburtsdatum: 15.03.1985")
    c.drawString(50, 640, "Steuerklasse: 3")

    # Beträge
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, 600, "Bruttolohn:")
    c.drawString(250, 600, "5.500,00 EUR")

    c.setFont("Helvetica", 11)
    c.drawString(50, 580, "Lohnsteuer:")
    c.drawString(250, 580, "- 850,00 EUR")
    c.drawString(50, 560, "Kirchensteuer:")
    c.drawString(250, 560, "- 76,50 EUR")
    c.drawString(50, 540, "Solidaritätszuschlag:")
    c.drawString(250, 540, "- 46,75 EUR")
    c.drawString(50, 520, "Rentenversicherung:")
    c.drawString(250, 520, "- 511,50 EUR")
    c.drawString(50, 500, "Krankenversicherung:")
    c.drawString(250, 500, "- 445,50 EUR")
    c.drawString(50, 480, "Pflegeversicherung:")
    c.drawString(250, 480, "- 93,50 EUR")
    c.drawString(50, 460, "Arbeitslosenversicherung:")
    c.drawString(250, 460, "- 71,50 EUR")

    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, 420, "Nettolohn:")
    c.drawString(250, 420, "3.404,75 EUR")

    # IBAN
    c.setFont("Helvetica", 10)
    c.drawString(50, 380, "Auszahlung auf: DE89 3704 0044 0532 0130 00")

    c.save()
    buffer.seek(0)
    return buffer.read()


def test_service():
    """Testet den Document Analyzer Service"""

    print("1. Erstelle Test-PDF...")
    try:
        pdf_bytes = create_test_pdf()
        print(f"   PDF erstellt: {len(pdf_bytes)} bytes")
    except ImportError:
        print("   reportlab nicht installiert. Teste mit leerem PDF...")
        # Minimales gültiges PDF
        pdf_bytes = b"""%PDF-1.4
1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj
2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj
3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >> endobj
xref
0 4
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
trailer << /Size 4 /Root 1 0 R >>
startxref
196
%%EOF"""

    print("\n2. Sende an http://localhost:8000/analyze...")

    try:
        with httpx.Client(timeout=60.0) as client:
            files = {"file": ("test_gehaltsnachweis.pdf", pdf_bytes, "application/pdf")}
            response = client.post("http://localhost:8000/analyze", files=files)

            print(f"\n3. Response Status: {response.status_code}")
            print("\n4. Ergebnis:")

            import json
            result = response.json()
            print(json.dumps(result, indent=2, ensure_ascii=False))

            if result.get("success"):
                print("\n✓ Service funktioniert!")
                print(f"  Dokumenttyp: {result.get('doc_type')}")
                print(f"  Confidence: {result.get('confidence')}")
            else:
                print(f"\n✗ Fehler: {result.get('error')}")

    except httpx.ConnectError:
        print("\n✗ Verbindung fehlgeschlagen!")
        print("  Ist der Service gestartet?")
        print("  Starte mit: cd document-analyzer-service && python main.py")
    except Exception as e:
        print(f"\n✗ Fehler: {e}")


if __name__ == "__main__":
    test_service()
