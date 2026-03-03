"""
Lokale Tests für den Document Analyzer Service
Läuft ohne gestarteten Server per FastAPI TestClient.

Ausführen:
    source venv/bin/activate
    python test_local.py              # alle Tests
    python test_local.py stateless    # nur ohne SeaTable
    python test_local.py seatable     # nur SeaTable Verbindungstest
"""

import sys
import json
import os
import traceback
from unittest.mock import patch, MagicMock
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# Farben für Ausgabe
# ─────────────────────────────────────────────
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"
BOLD = "\033[1m"


def ok(msg):
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg, detail=None):
    print(f"  {RED}✗{RESET} {msg}")
    if detail:
        print(f"    {RED}{detail}{RESET}")


def warn(msg):
    print(f"  {YELLOW}⚠{RESET} {msg}")


def section(title):
    print(f"\n{BOLD}{BLUE}{'─'*50}{RESET}")
    print(f"{BOLD}{BLUE}  {title}{RESET}")
    print(f"{BOLD}{BLUE}{'─'*50}{RESET}")


# ─────────────────────────────────────────────
# SEATABLE MOCK
# ─────────────────────────────────────────────
MOCK_CASE = {
    "_id": "ABC123",
    "case_id": "CASE-TEST-001",
    "applicant_name": "Max Mustermann",
    "partner_email": "test@example.com",
    "status": "INTAKE",
    "facts_extracted": json.dumps({
        "property_data": {
            "purchase_price": 350000,
            "object_type": "ETW",
            "usage": "Eigennutzung",
        },
        "financing_data": {
            "loan_amount": 280000,
            "equity_to_use": 70000,
        },
        "applicant_data": {
            "vorname": "Max",
            "nachname": "Mustermann",
        }
    }),
    "answers_user": json.dumps({}),
    "manual_overrides": json.dumps({}),
    "derived_values": json.dumps({}),
    "docs_index": json.dumps({}),
    "readiness": json.dumps({}),
    "audit_log": json.dumps([]),
    "actors": json.dumps({}),
    "conversation_ids": json.dumps([]),
    "_facts_extracted": {
        "property_data": {
            "purchase_price": 350000,
            "object_type": "ETW",
            "usage": "Eigennutzung",
        },
        "financing_data": {
            "loan_amount": 280000,
            "equity_to_use": 70000,
        },
        "applicant_data": {
            "vorname": "Max",
            "nachname": "Mustermann",
        }
    },
    "_answers_user": {},
    "_manual_overrides": {},
    "_derived_values": {},
    "_docs_index": {},
    "_readiness": {},
    "_audit_log": [],
    "_actors": {},
    "_conversation_ids": [],
}

MOCK_DOCS = []


def mock_seatable():
    """Gibt Context Manager zurück der SeaTable mockt"""
    import unittest.mock as mock

    patches = []

    def mock_list_rows(table_name, view_name="Default View"):
        if table_name == "fin_cases":
            return [MOCK_CASE]
        return []

    def mock_search_rows(table_name, column, value):
        if table_name == "fin_cases" and column == "case_id" and value == "CASE-TEST-001":
            return [MOCK_CASE]
        if table_name == "fin_cases" and column == "partner_email":
            return [MOCK_CASE]
        if table_name == "fin_documents":
            return MOCK_DOCS
        if table_name == "processed_emails":
            return []
        return []

    def mock_get_row(table_name, row_id):
        if table_name == "fin_cases" and row_id == "ABC123":
            return MOCK_CASE
        return None

    def mock_create_row(table_name, row_data):
        return {"_id": "NEW_ROW_123", **row_data}

    def mock_update_row(table_name, row_id, row_data):
        return {"_id": row_id, **row_data}

    def mock_is_processed(msg_id):
        return msg_id == "ALREADY-PROCESSED-ID"

    def mock_log_email(msg_id, intent, action, case_id=None):
        pass

    patches = [
        mock.patch("seatable.list_rows", side_effect=mock_list_rows),
        mock.patch("seatable.search_rows", side_effect=mock_search_rows),
        mock.patch("seatable.get_row", side_effect=mock_get_row),
        mock.patch("seatable.create_row", side_effect=mock_create_row),
        mock.patch("seatable.update_row", side_effect=mock_update_row),
        mock.patch("seatable.is_email_processed", side_effect=mock_is_processed),
        mock.patch("seatable.log_processed_email", side_effect=mock_log_email),
    ]
    return patches


# ─────────────────────────────────────────────
# TEST RUNNER HELPER
# ─────────────────────────────────────────────
def run_test(name, fn):
    try:
        fn()
        ok(name)
        return True
    except AssertionError as e:
        fail(name, str(e))
        return False
    except Exception as e:
        fail(name, f"{type(e).__name__}: {e}")
        if os.getenv("DEBUG"):
            traceback.print_exc()
        return False


# ─────────────────────────────────────────────
# TESTS: STATELESS ENDPOINTS
# ─────────────────────────────────────────────
def test_stateless():
    section("STATELESS ENDPOINTS (kein SeaTable, kein OpenAI)")

    from fastapi.testclient import TestClient
    import main as app_module
    client = TestClient(app_module.app)

    passed = 0
    total = 0

    # Health
    total += 1
    def t_health():
        r = client.get("/health")
        assert r.status_code == 200, f"Status: {r.status_code}"
        assert r.json()["status"] == "healthy"
    passed += run_test("GET /health → 200 ok", t_health)

    # Validate-Data: valide Daten
    total += 1
    def t_validate_ok():
        r = client.post("/validate-data", json={
            "data": {
                "purchase_price": 350000,
                "loan_amount": 280000,
                "equity": 70000,
                "email": "max.mustermann@test.de",
                "phone": "0151 12345678",
            },
            "schema_type": "full"
        })
        assert r.status_code == 200, f"Status: {r.status_code} Body: {r.text[:200]}"
        d = r.json()
        assert d["valid"] is True, f"Nicht valid: {d.get('errors')}"
    passed += run_test("POST /validate-data → valide Daten", t_validate_ok)

    # Validate-Data: ungültige Daten
    total += 1
    def t_validate_invalid():
        r = client.post("/validate-data", json={
            "data": {
                "purchase_price": 500,       # zu niedrig
                "equity": -1000,             # negativ
                "email": "kein-at-zeichen",  # ungültig
            }
        })
        assert r.status_code == 200
        d = r.json()
        assert d["valid"] is False, "Sollte invalid sein"
        assert len(d["errors"]) >= 2, f"Erwartet mind. 2 Fehler, got: {d['errors']}"
    passed += run_test("POST /validate-data → invalide Daten erkannt", t_validate_invalid)

    # Compose-Notification
    total += 1
    def t_notification():
        r = client.post("/compose-notification", json={
            "case_id": "CASE-123",
            "notification_type": "case_created",
            "applicant_name": "Max Mustermann",
            "applicant_email": "max@test.de",
            "broker_name": "Alexander Heil"
        })
        assert r.status_code == 200
        d = r.json()
        assert d["success"] is True
        assert "Max Mustermann" in d["email_body"]
        assert "CASE-123" in d["email_body"]
    passed += run_test("POST /compose-notification → case_created", t_notification)

    # Compose-Notification: unknown type
    total += 1
    def t_notification_unknown():
        r = client.post("/compose-notification", json={
            "case_id": "CASE-123",
            "notification_type": "unknown_type_xyz"
        })
        assert r.status_code == 200
        d = r.json()
        assert d["success"] is False
    passed += run_test("POST /compose-notification → unknown_type → Fehler", t_notification_unknown)

    # Build Europace Payload (Builder erwartet englische Feldnamen im input)
    total += 1
    def t_europace():
        r = client.post("/build-europace-payload", json={
            "case_id": "CASE-TEST-001",
            "facts_extracted": {
                "applicant_data": {
                    "first_name": "Max",
                    "last_name": "Mustermann",
                    "birth_date": "1985-03-15",
                },
                "property_data": {
                    "purchase_price": 350000,
                    "object_type": "ETW",
                    "usage": "Eigennutzung",
                },
                "financing_data": {
                    "loan_amount": 280000,
                    "equity_to_use": 70000,
                }
            }
        })
        assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text[:200]}"
        d = r.json()
        assert d["case_id"] == "CASE-TEST-001"
        p = d["payload"]
        assert p is not None, "Payload sollte nicht None sein"
        kundendaten = p["kundenangaben"]["haushalte"][0]["kunden"][0]
        assert kundendaten["personendaten"]["vorname"] == "Max", f"Vorname: {kundendaten['personendaten'].get('vorname')}"
        assert kundendaten["personendaten"]["nachname"] == "Mustermann"
        immobilie = p["kundenangaben"]["finanzierungsobjekt"]["immobilie"]
        assert immobilie["kaufpreis"] == 350000
        assert immobilie["objektart"] == "EIGENTUMSWOHNUNG"
    passed += run_test("POST /build-europace-payload → Payload korrekt", t_europace)

    # Check-Readiness: delegiert an readiness.py (braucht DB-Mock)
    total += 1
    def t_readiness_via_db():
        from unittest.mock import patch
        mock_case = {
            "_id": "CASE-TEST-READINESS",
            "applicant_name": "Test User",
            "_facts_extracted": {
                "property_data": {"purchase_price": 350000, "object_type": "ETW", "usage": "Eigennutzung"},
                "financing_data": {"loan_amount": 280000, "equity_to_use": 70000},
            },
            "_answers_user": {},
            "_manual_overrides": {},
            "_derived_values": {},
        }
        with patch("readiness.cases.load_case", return_value=mock_case), \
             patch("readiness.cases.build_docs_index", return_value={}), \
             patch("readiness.cases.update_status"):
            r = client.post("/check-readiness", json={"case_id": "CASE-TEST-READINESS"})
            assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text[:200]}"
            d = r.json()
            assert "new_status" in d
            assert "missing_required" in d
            assert "missing_docs" in d
            assert len(d["missing_docs"]) > 0, "Ohne Dokumente sollten missing_docs > 0 sein"
    passed += run_test("POST /check-readiness → via readiness.py (DB-Mock)", t_readiness_via_db)

    # Generate-Questions: Fallback ohne GPT
    total += 1
    def t_generate_questions_fallback():
        r = client.post("/generate-questions", json={
            "case_id": "CASE-123",
            "missing_fields": ["purchase_price", "loan_amount"],
            "target": "partner"
        })
        assert r.status_code == 200
        d = r.json()
        assert d["success"] is True
        assert len(d["questions"]) == 2
        assert d["email_body"] != ""
    passed += run_test("POST /generate-questions → Fallback ohne GPT", t_generate_questions_fallback)

    print(f"\n  Ergebnis: {passed}/{total} Tests bestanden")
    return passed, total


# ─────────────────────────────────────────────
# TESTS: CASE LOGIC (unit tests mit Mock)
# ─────────────────────────────────────────────
def test_case_logic():
    section("CASE LOGIC (unit tests, kein SeaTable)")

    passed = 0
    total = 0

    # Gatekeeper: Erlaubter Sender
    total += 1
    def t_gate_allowed():
        import case_logic as cases
        result = cases.gatekeeper("l.safi@muniqre.com", "Finanzierungsanfrage ETW München")
        assert result["pass"] is True, f"Sollte erlaubt sein: {result}"
        assert result["actor"] == "partner"
    passed += run_test("Gatekeeper: erlaubter Partner → pass=True", t_gate_allowed)

    # Gatekeeper: Nicht erlaubter Sender
    total += 1
    def t_gate_blocked():
        import case_logic as cases
        result = cases.gatekeeper("spam@unknown.de", "Buy Now!!!")
        assert result["pass"] is False
        assert result["reason"] == "sender_not_allowlisted"
    passed += run_test("Gatekeeper: unbekannter Absender → pass=False", t_gate_blocked)

    # Gatekeeper: Non-Finance Subject
    total += 1
    def t_gate_nonfinance():
        import case_logic as cases
        result = cases.gatekeeper("l.safi@muniqre.com", "Your receipt from Railway")
        assert result["pass"] is False
        assert result["reason"] == "non_finance_subject"
    passed += run_test("Gatekeeper: non-finance Betreff → geblockt", t_gate_nonfinance)

    # Gatekeeper: Interner Broker Reply
    total += 1
    def t_gate_broker():
        import case_logic as cases
        result = cases.gatekeeper("backoffice@alexander-heil.com", "Re: CASE-123 Rückfrage")
        assert result["pass"] is True
        assert result["actor"] == "broker"
        assert result["is_internal_reply"] is True
    passed += run_test("Gatekeeper: interner Broker Reply → pass=True, actor=broker", t_gate_broker)

    # merge_facts: Neue Werte füllen leere Slots
    total += 1
    def t_merge_facts():
        import case_logic as cases
        existing = {"purchase_price": 350000, "object_type": None, "notes": "test"}
        new = {"purchase_price": 400000, "object_type": "ETW", "loan_amount": 280000}
        merged = cases.merge_facts(existing, new)
        assert merged["purchase_price"] == 350000, "Bestehende Werte bleiben"
        assert merged["object_type"] == "ETW", "Leere Slots werden gefüllt"
        assert merged["loan_amount"] == 280000, "Neue Keys werden hinzugefügt"
        assert merged["notes"] == "test", "Andere Keys bleiben"
    passed += run_test("merge_facts: bestehende Werte bleiben, leere werden gefüllt", t_merge_facts)

    # match_case: Referenced Case-ID
    total += 1
    def t_match_by_case_id():
        import case_logic as cases
        with patch("case_logic.get_all_active_cases", return_value=[MOCK_CASE]):
            result = cases.match_case(
                from_email="test@example.com",
                applicant_last_name="Mustermann",
                referenced_case_id="CASE-TEST-001",
                conversation_id=None,
                mail_type="reply",
                actor="partner",
            )
            assert result["action"] == "update"
            assert result["matched_by"] == "referenced_case_id"
    passed += run_test("match_case: Referenzierte CASE-ID → update", t_match_by_case_id)

    # match_case: Neue Anfrage → create
    total += 1
    def t_match_new():
        import case_logic as cases
        with patch("case_logic.get_all_active_cases", return_value=[]):
            result = cases.match_case(
                from_email="neu@example.com",
                applicant_last_name="Neumann",
                referenced_case_id=None,
                conversation_id=None,
                mail_type="new_request",
                actor="partner",
            )
            assert result["action"] == "create"
            assert result["case_id"].startswith("CASE-")
    passed += run_test("match_case: Kein Match + new_request → create", t_match_new)

    print(f"\n  Ergebnis: {passed}/{total} Tests bestanden")
    return passed, total


# ─────────────────────────────────────────────
# TESTS: READINESS LOGIC (unit tests)
# ─────────────────────────────────────────────
def test_readiness_logic():
    section("READINESS LOGIC (unit tests)")

    passed = 0
    total = 0

    # Vollständiger Case → AWAITING_BROKER_CONFIRMATION
    total += 1
    def t_ready_awaiting():
        import readiness as rdns
        with patch("case_logic.load_case", return_value={
            **MOCK_CASE,
            "_facts_extracted": {
                "property_data": {"purchase_price": 350000, "object_type": "ETW", "usage": "Eigennutzung"},
                "financing_data": {"loan_amount": 280000, "equity_to_use": 70000},
            },
            "_answers_user": {},
            "_manual_overrides": {},
            "_derived_values": {},
        }), patch("case_logic.build_docs_index", return_value={}), \
           patch("case_logic.update_status"):
            result = rdns.check_readiness("CASE-TEST-001")
            # Ohne Dokumente → NEEDS_QUESTIONS_PARTNER
            assert result["status"] == "NEEDS_QUESTIONS_PARTNER", f"Got: {result['status']}"
            assert len(result["missing_docs"]) > 0
    passed += run_test("Readiness: Case ohne Docs → NEEDS_QUESTIONS_PARTNER", t_ready_awaiting)

    # APPROVE_IMPORT override → READY_FOR_IMPORT
    total += 1
    def t_approve_import():
        import readiness as rdns
        with patch("case_logic.load_case", return_value={
            **MOCK_CASE,
            "_facts_extracted": {
                "property_data": {"purchase_price": 350000, "object_type": "ETW", "usage": "Eigennutzung"},
                "financing_data": {"loan_amount": 280000, "equity_to_use": 70000},
            },
            "_answers_user": {},
            "_manual_overrides": {"APPROVE_IMPORT": True},
            "_derived_values": {},
        }), patch("case_logic.build_docs_index", return_value={}), \
           patch("case_logic.update_status"):
            result = rdns.check_readiness("CASE-TEST-001")
            assert result["status"] == "READY_FOR_IMPORT", f"Got: {result['status']}"
            assert result["approve_import"] is True
    passed += run_test("Readiness: APPROVE_IMPORT override → READY_FOR_IMPORT", t_approve_import)

    # WAIT_FOR_DOCS override
    total += 1
    def t_wait_for_docs():
        import readiness as rdns
        with patch("case_logic.load_case", return_value={
            **MOCK_CASE,
            "_facts_extracted": {
                "property_data": {"purchase_price": 350000, "object_type": "ETW", "usage": "Eigennutzung"},
                "financing_data": {"loan_amount": 280000, "equity_to_use": 70000},
            },
            "_answers_user": {},
            "_manual_overrides": {"WAIT_FOR_DOCS": True},
            "_derived_values": {},
        }), patch("case_logic.build_docs_index", return_value={}), \
           patch("case_logic.update_status"):
            result = rdns.check_readiness("CASE-TEST-001")
            assert result["status"] == "WAITING_FOR_DOCUMENTS"
    passed += run_test("Readiness: WAIT_FOR_DOCS override → WAITING_FOR_DOCUMENTS", t_wait_for_docs)

    # Fehlende Finanzierungsdaten → NEEDS_QUESTIONS_PARTNER
    total += 1
    def t_missing_financing():
        import readiness as rdns
        with patch("case_logic.load_case", return_value={
            **MOCK_CASE,
            "_facts_extracted": {"property_data": {"object_type": "ETW"}},  # purchase_price fehlt
            "_answers_user": {},
            "_manual_overrides": {},
            "_derived_values": {},
        }), patch("case_logic.build_docs_index", return_value={}), \
           patch("case_logic.update_status"):
            result = rdns.check_readiness("CASE-TEST-001")
            assert "purchase_price" in result["missing_financing"]
            assert "loan_amount" in result["missing_financing"]
    passed += run_test("Readiness: fehlende Finanzierungsdaten → in missing_financing", t_missing_financing)

    print(f"\n  Ergebnis: {passed}/{total} Tests bestanden")
    return passed, total


# ─────────────────────────────────────────────
# TESTS: PIPELINE ENDPOINTS (mit Mock SeaTable)
# ─────────────────────────────────────────────
def test_pipeline_endpoints():
    section("PIPELINE ENDPOINTS (mit gemocktem SeaTable)")

    from fastapi.testclient import TestClient
    import main as app_module

    passed = 0
    total = 0

    patches = mock_seatable()
    for p in patches:
        p.start()

    try:
        client = TestClient(app_module.app, raise_server_exceptions=False)

        # process-email: Skip already processed
        total += 1
        def t_email_already_processed():
            r = client.post("/process-email", json={
                "provider_message_id": "ALREADY-PROCESSED-ID",
                "from_email": "l.safi@muniqre.com",
                "subject": "Finanzierungsanfrage",
                "body_text": "Test"
            })
            assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text[:200]}"
            d = r.json()
            assert d["action"] == "skipped"
            assert d["reason"] == "already_processed"
        passed += run_test("POST /process-email: bereits verarbeitet → skipped", t_email_already_processed)

        # process-email: Gatekeeper blockt unbekannten Sender
        total += 1
        def t_email_blocked():
            r = client.post("/process-email", json={
                "provider_message_id": "MSG-BLOCKED-001",
                "from_email": "spam@unknown.de",
                "subject": "Hallo!",
                "body_text": "Test"
            })
            assert r.status_code == 200
            d = r.json()
            assert d["action"] == "skipped"
            assert d["reason"] == "sender_not_allowlisted"
        passed += run_test("POST /process-email: Gatekeeper blockt unbekannten Sender", t_email_blocked)

        # ingest-answers: Antworten speichern
        total += 1
        def t_ingest_answers():
            with patch("readiness.check_readiness") as mock_check, \
                 patch("notify.dispatch_notifications") as mock_notify:
                mock_check.return_value = {
                    "status": "NEEDS_QUESTIONS_PARTNER",
                    "missing_financing": ["purchase_price"],
                    "missing_docs": [],
                    "stale_docs": [],
                    "warnings": [],
                    "manual_overrides_applied": [],
                    "effective_view": {},
                    "approve_import": False,
                    "is_complete": False,
                }
                r = client.post("/ingest-answers", json={
                    "case_id": "CASE-TEST-001",
                    "actor": "partner",
                    "answers": {"purchase_price": 350000}
                })
                assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text[:200]}"
                d = r.json()
                assert d["success"] is True
                assert d["case_id"] == "CASE-TEST-001"
        passed += run_test("POST /ingest-answers → Antworten gespeichert", t_ingest_answers)

        # update-onedrive-folder
        total += 1
        def t_update_folder():
            with patch("readiness.check_readiness") as mock_check, \
                 patch("notify.dispatch_notifications"):
                mock_check.return_value = {
                    "status": "NEEDS_QUESTIONS_PARTNER",
                    "missing_financing": [],
                    "missing_docs": [],
                    "stale_docs": [],
                    "warnings": [],
                    "manual_overrides_applied": [],
                    "effective_view": {},
                    "approve_import": False,
                    "is_complete": False,
                }
                r = client.post("/update-onedrive-folder", json={
                    "case_id": "CASE-TEST-001",
                    "onedrive_folder_id": "FOLDER-ID-ABC"
                })
                assert r.status_code == 200, f"Status: {r.status_code}, Body: {r.text[:200]}"
                d = r.json()
                assert d["success"] is True
        passed += run_test("POST /update-onedrive-folder → OK", t_update_folder)

        # full-readiness-check
        total += 1
        def t_full_readiness():
            with patch("readiness.check_readiness") as mock_check, \
                 patch("notify.dispatch_notifications"):
                mock_check.return_value = {
                    "status": "AWAITING_BROKER_CONFIRMATION",
                    "missing_financing": [],
                    "missing_docs": [],
                    "stale_docs": [],
                    "warnings": [],
                    "manual_overrides_applied": [],
                    "effective_view": {},
                    "approve_import": False,
                    "is_complete": True,
                }
                r = client.post("/full-readiness-check", json={
                    "case_id": "CASE-TEST-001",
                    "send_notifications": False
                })
                assert r.status_code == 200
                d = r.json()
                assert d["status"] == "AWAITING_BROKER_CONFIRMATION"
        passed += run_test("POST /full-readiness-check → OK", t_full_readiness)

    finally:
        for p in patches:
            p.stop()

    print(f"\n  Ergebnis: {passed}/{total} Tests bestanden")
    return passed, total


# ─────────────────────────────────────────────
# TESTS: SEATABLE VERBINDUNG (braucht echte Credentials)
# ─────────────────────────────────────────────
def test_seatable_connection():
    section("SEATABLE VERBINDUNG (echte Credentials)")

    passed = 0
    total = 0

    token = os.getenv("SEATABLE_API_TOKEN", "")
    uuid = os.getenv("SEATABLE_BASE_UUID", "")
    base_url = os.getenv("SEATABLE_BASE_URL", "https://cloud.seatable.io")

    if not token or not uuid:
        warn("SEATABLE_API_TOKEN und/oder SEATABLE_BASE_UUID nicht in .env gesetzt")
        warn("SeaTable-Tests werden übersprungen")
        warn("Füge diese Werte zur .env Datei hinzu um zu testen")
        return 0, 0

    print(f"  Token: {token[:20]}...")
    print(f"  UUID:  {uuid}")
    print(f"  URL:   {base_url}")

    import seatable as db

    # Auth Test
    total += 1
    def t_auth():
        db.invalidate_token()
        t = db._get_access_token()
        assert t, "Kein Token erhalten"
        print(f"    Token: {t[:30]}...")
        print(f"    UUID: {db._get_uuid()}")
        print(f"    API URL: {db._api('rows/')}")
    passed += run_test("SeaTable Auth (GET /api/v2.1/dtable/app-access-token/)", t_auth)

    # Metadata Test - zeigt welche Tabellen existieren (v2 API)
    total += 1
    def t_metadata():
        import requests as _req
        db.invalidate_token()
        token_val = db._get_access_token()
        metadata_url = db._api("metadata/")

        r = _req.get(metadata_url, headers={"Authorization": f"Bearer {token_val}"}, timeout=10)
        assert r.ok, f"Metadata failed: {r.status_code}: {r.text[:200]}"
        tables = [t["name"] for t in r.json().get("metadata", {}).get("tables", [])]
        print(f"    Tabellen: {tables}")
        required = {"fin_cases", "fin_documents", "processed_emails"}
        missing = required - set(tables)
        assert not missing, f"Tabellen fehlen: {missing}"
    passed += run_test("SeaTable Metadata (v2 API - Tabellen vorhanden)", t_metadata)

    # List rows test
    total += 1
    def t_list_rows():
        db.invalidate_token()
        rows = db.list_rows("fin_cases")
        print(f"    fin_cases: {len(rows)} Zeilen gefunden")
        # Kein Assert - 0 Zeilen ist auch OK wenn DB leer
    passed += run_test("SeaTable list_rows(fin_cases)", t_list_rows)

    print(f"\n  Ergebnis: {passed}/{total} Tests bestanden")
    return passed, total


# ─────────────────────────────────────────────
# TESTS: OPENAI (braucht echten Key)
# ─────────────────────────────────────────────
def test_openai_endpoints():
    section("OPENAI ENDPOINTS (echte API, kostet Token!)")

    if not os.getenv("OPENAI_API_KEY"):
        warn("OPENAI_API_KEY nicht gesetzt - überspringe Tests")
        return 0, 0

    from fastapi.testclient import TestClient
    import main as app_module
    client = TestClient(app_module.app)

    passed = 0
    total = 0

    # Parse Email
    total += 1
    def t_parse_email():
        r = client.post("/parse-email", json={
            "from_address": "l.safi@muniqre.com",
            "from_name": "Lotfi Safi",
            "subject": "Finanzierungsanfrage ETW München Schwabing",
            "body": """Sehr geehrter Herr Heil,

ich möchte eine Finanzierungsanfrage für meinen Kunden Max Mustermann stellen.

Objekt: Eigentumswohnung in München Schwabing, Leopoldstraße 15
Kaufpreis: 450.000 EUR
Gewünschtes Darlehen: 360.000 EUR
Eigenkapital: 90.000 EUR
Nutzungsart: Eigennutzung

Mit freundlichen Grüßen
Lotfi Safi"""
        })
        assert r.status_code == 200, f"Status: {r.status_code}"
        d = r.json()
        assert d["success"] is True, f"Fehler: {d.get('error')}"
        assert d["intent"] in ["new_request", "unknown"], f"Intent: {d['intent']}"
        print(f"    Intent: {d['intent']}, Confidence: {d['confidence']}")
        print(f"    Summary: {d.get('summary', '')[:100]}")
        if d.get("property_data", {}).get("purchase_price"):
            print(f"    Kaufpreis erkannt: {d['property_data']['purchase_price']}")
    passed += run_test("POST /parse-email → GPT-4o-mini Parsing", t_parse_email)

    print(f"\n  Ergebnis: {passed}/{total} Tests bestanden")
    return passed, total


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    print(f"\n{BOLD}=== Document Analyzer Service - Lokale Tests ==={RESET}")
    print(f"Modus: {mode}")
    print(f"CWD: {os.getcwd()}")

    # Sicherstellen dass wir im richtigen Verzeichnis sind
    service_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(service_dir)
    if service_dir not in sys.path:
        sys.path.insert(0, service_dir)

    total_passed = 0
    total_tests = 0

    if mode in ("all", "stateless"):
        p, t = test_stateless()
        total_passed += p
        total_tests += t

    if mode in ("all", "unit", "case"):
        p, t = test_case_logic()
        total_passed += p
        total_tests += t

    if mode in ("all", "unit", "readiness"):
        p, t = test_readiness_logic()
        total_passed += p
        total_tests += t

    if mode in ("all", "pipeline"):
        p, t = test_pipeline_endpoints()
        total_passed += p
        total_tests += t

    if mode in ("all", "seatable"):
        p, t = test_seatable_connection()
        total_passed += p
        total_tests += t

    if mode in ("all", "openai"):
        p, t = test_openai_endpoints()
        total_passed += p
        total_tests += t

    # Gesamtergebnis
    print(f"\n{BOLD}{'═'*50}{RESET}")
    if total_tests > 0:
        pct = round(total_passed / total_tests * 100)
        if total_passed == total_tests:
            print(f"{GREEN}{BOLD}  Alle Tests bestanden: {total_passed}/{total_tests} ({pct}%) ✓{RESET}")
        else:
            failed = total_tests - total_passed
            print(f"{YELLOW}{BOLD}  {total_passed}/{total_tests} bestanden ({failed} fehlgeschlagen){RESET}")
    print(f"{BOLD}{'═'*50}{RESET}\n")

    sys.exit(0 if total_passed == total_tests else 1)
