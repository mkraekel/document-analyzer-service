"""
Import Builder
Baut Europace-Payload, validiert und importiert Cases.
Portiert aus dem n8n Import Builder Workflow.
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Optional

import httpx

import case_logic as cases
import seatable as db
from readiness import _compute_effective_view

logger = logging.getLogger(__name__)

EUROPACE_API_URL = os.getenv("EUROPACE_API_URL", "https://api.europace.de/kundenangaben")
EUROPACE_API_KEY = os.getenv("EUROPACE_API_KEY", "")

# ============================================================
# Europace Enum Mappings
# ============================================================
EUROPACE_ENUMS = {
    "anrede": {"Herr": "HERR", "Frau": "FRAU"},
    "familienstand": {
        "ledig": "LEDIG",
        "verheiratet": "VERHEIRATET",
        "geschieden": "GESCHIEDEN",
        "verwitwet": "VERWITWET",
        "eingetragene Lebenspartnerschaft": "LEBENSPARTNERSCHAFT",
    },
    "objektart": {
        "ETW": "EIGENTUMSWOHNUNG",
        "EFH": "EINFAMILIENHAUS",
        "DHH": "DOPPELHAUSHAELFTE",
        "RH": "REIHENHAUS",
        "MFH": "MEHRFAMILIENHAUS",
        "Eigentumswohnung": "EIGENTUMSWOHNUNG",
        "Einfamilienhaus": "EINFAMILIENHAUS",
    },
    "nutzungsart": {
        "Eigennutzung": "EIGENGENUTZT",
        "Kapitalanlage": "VERMIETET",
        "Teilvermietet": "TEILWEISE_VERMIETET",
    },
    "beschaeftigungsart": {
        "Angestellter": "ANGESTELLTER",
        "Selbstständig": "SELBSTAENDIGER",
        "Beamter": "BEAMTER",
        "Rentner": "RENTNER",
        "Sonstiges": "SONSTIGES",
    },
}


# ============================================================
# Helper functions
# ============================================================

def _get_nested(obj: dict, path: str):
    """Tiefensuche per Punkt-Pfad: 'property_data.purchase_price'"""
    if not obj or not path:
        return None
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _map_enum(value, enum_map: dict):
    """Mappt einen Wert auf den Europace-Enum. Gibt None zurueck bei leerem Input."""
    if not value or not enum_map:
        return None
    return enum_map.get(value, value)


def _clean_payload(obj):
    """Entfernt None/leere Werte aus verschachteltem Dict."""
    if obj is None:
        return None
    if isinstance(obj, list):
        cleaned = [_clean_payload(v) for v in obj]
        cleaned = [v for v in cleaned if v is not None]
        return cleaned if cleaned else None
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            cv = _clean_payload(v)
            if cv is not None and cv != "" and cv != []:
                cleaned[k] = cv
        return cleaned if cleaned else None
    return obj


def _get_value(effective_view: dict, primary: str, *fallbacks):
    """Holt einen Wert aus dem Effective View mit Fallback-Pfaden."""
    val = _get_nested(effective_view, primary)
    if val is not None and val != "":
        return val
    for fb in fallbacks:
        val = _get_nested(effective_view, fb)
        if val is not None and val != "":
            return val
    return None


# ============================================================
# Payload Builder
# ============================================================

def build_europace_payload(case_id: str) -> dict:
    """
    Laedt Case, berechnet effective view, baut Europace-Payload.
    Rueckgabe: {payload: dict, effective_view: dict, case: dict}
    """
    case = cases.load_case(case_id)
    if not case:
        raise ValueError(f"Case nicht gefunden: {case_id}")

    if case.get("status") != "READY_FOR_IMPORT":
        raise ValueError(
            f"Case {case_id} hat Status '{case.get('status')}', erwartet 'READY_FOR_IMPORT'"
        )

    view = _compute_effective_view(case)

    def gv(primary, *fallbacks):
        return _get_value(view, primary, *fallbacks)

    # Kunden-Objekt bauen
    def _build_kunde(prefix: str, id_suffix: str) -> dict:
        p = f"{prefix}." if prefix else "applicant_data."
        p_alt = prefix.replace("applicant_data", "applicant") if prefix else "applicant"

        return {
            "externeKundenId": f"{case_id}{id_suffix}",
            "personendaten": {
                "anrede": _map_enum(
                    gv(f"{p}salutation", f"{p_alt}_salutation"),
                    EUROPACE_ENUMS["anrede"],
                ),
                "titel": gv(f"{p}title", f"{p_alt}_title"),
                "vorname": gv(f"{p}first_name", f"{p_alt}_first_name"),
                "nachname": gv(f"{p}last_name", f"{p_alt}_last_name"),
                "geburtsdatum": gv(f"{p}birth_date", f"{p_alt}_birth_date"),
                "geburtsort": gv(f"{p}birth_place", f"{p_alt}_birth_place"),
                "staatsangehoerigkeit": gv(f"{p}nationality", f"{p_alt}_nationality") or "DE",
                "steuerId": gv(f"{p}tax_id", f"{p_alt}_tax_id"),
            },
            "kontakt": {
                "telefonPrivat": gv(f"{p}phone", f"{p_alt}_phone"),
                "email": gv(f"{p}email", f"{p_alt}_email"),
            },
            "wohnsituation": None if prefix == "applicant_data_2" else {
                "anschrift": {
                    "strasse": gv("address_data.street"),
                    "hausnummer": gv("address_data.house_number"),
                    "plz": gv("address_data.zip"),
                    "ort": gv("address_data.city"),
                },
                "wohnhaftSeit": gv("address_data.resident_since"),
            },
            "familienstand": {
                "familienstand": _map_enum(
                    gv("household_data.marital_status"),
                    EUROPACE_ENUMS["familienstand"],
                ),
            },
            "beschaeftigung": {
                "beschaeftigungsverhaeltnis": {
                    "beschaeftigungsart": _map_enum(
                        gv(f"{p}employment_type", f"{p_alt}_employment_type"),
                        EUROPACE_ENUMS["beschaeftigungsart"],
                    ),
                    "beruf": gv(f"{p}occupation", f"{p_alt}_occupation"),
                    "arbeitgeber": {
                        "name": gv(f"{p}employer", f"{p_alt}_employer"),
                        "inDeutschland": gv(f"{p}employer_in_germany", f"{p_alt}_employer_in_germany") is not False,
                    },
                    "beschaeftigtSeit": gv(f"{p}employed_since", f"{p_alt}_employed_since"),
                    "befristet": gv(f"{p}employment_status", f"{p_alt}_employment_status") == "befristet",
                    "inProbezeit": gv(f"{p}probation", f"{p_alt}_probation") or False,
                },
            },
            "einkommenNetto": {
                "monatlichesNettoEinkommen": gv(
                    f"{p}net_income", f"{p}monthly_income", f"{p_alt}_monthly_income"
                ),
                "anzahlGehaelterProJahr": gv(
                    f"{p}salaries_per_year", f"{p_alt}_salaries_per_year"
                ) or 12,
            },
        }

    # Kunden-Array
    kunde1 = _build_kunde("applicant_data", "_1")
    kunden = [kunde1]

    is_couple = (
        gv("is_couple") is True
        or gv("applicant_2_first_name") is not None
        or gv("applicant_data_2.first_name") is not None
    )
    if is_couple:
        kunde2 = _build_kunde("applicant_data_2", "_2")
        if (kunde2.get("personendaten") or {}).get("vorname") or (
            kunde2.get("personendaten") or {}
        ).get("nachname"):
            kunden.append(kunde2)

    # Werte fuer Finanzierung
    kaufpreis = gv("purchase_price", "property_data.purchase_price", "financing_data.purchase_price")
    loan_amount = gv("loan_amount", "financing_data.loan_amount")
    equity = gv("equity_to_use", "financing_data.equity_to_use", "equity")

    # Payload zusammenbauen
    payload = {
        "kundenangaben": {
            "haushalte": [
                {
                    "kunden": kunden,
                    "finanzielleSituation": {
                        "vermoegen": {
                            "summeBankUndSparguthaben": gv("assets.bank_savings", "bank_savings"),
                            "summeBausparvertraege": gv("assets.bauspar", "bauspar"),
                        }
                    },
                    "finanzbedarf": {
                        "fahrzeuge": {
                            "anzahlPKWGesamt": gv(
                                "household_data.cars_in_household", "cars_in_household"
                            )
                            or 0
                        }
                    },
                }
            ],
            "finanzierungsobjekt": {
                "immobilie": {
                    "objektart": _map_enum(
                        gv("object_type", "property_data.object_type"),
                        EUROPACE_ENUMS["objektart"],
                    ),
                    "nutzungsart": _map_enum(
                        gv("usage", "property_data.usage"),
                        EUROPACE_ENUMS["nutzungsart"],
                    ),
                    "adresse": {
                        "strasse": gv("property_data.street", "object_street"),
                        "hausnummer": gv("property_data.house_number", "object_house_number"),
                        "plz": gv("property_data.zip", "object_zip"),
                        "ort": gv("property_data.city", "object_city"),
                    },
                    "wohnflaeche": gv("property_data.living_space", "living_space"),
                    "baujahr": gv("property_data.year_built", "year_built"),
                    "kaufpreis": kaufpreis,
                    "marktwert": gv("property_data.market_value", "market_value") or kaufpreis,
                }
            },
            "finanzierungswunsch": {
                "darlehenssumme": loan_amount,
                "eigenkapital": equity,
                "zinsbindungInJahren": gv("zinsbindung") or 10,
                "wunschrate": gv("wunschrate"),
            },
        },
        "bearbeiter": {
            "partnerId": gv("partnerId"),
            "tippgeberPartnerId": gv("tippgeberPartnerId"),
        },
    }

    cleaned = _clean_payload(payload)

    return {
        "payload": cleaned,
        "effective_view": view,
        "case": case,
    }


# ============================================================
# Payload Validator
# ============================================================

def validate_payload(payload: dict, effective_view: dict) -> dict:
    """
    Plausibilitaetschecks fuer den Europace-Payload.
    Rueckgabe: {is_valid: bool, errors: list, warnings: list, summary: dict}
    """
    errors = []
    warnings = []
    current_year = datetime.now().year

    kundenangaben = (payload or {}).get("kundenangaben", {})
    haushalte = (kundenangaben.get("haushalte") or [{}])[0] if kundenangaben.get("haushalte") else {}
    kunden_list = haushalte.get("kunden", [{}]) if haushalte else [{}]
    kunde = kunden_list[0] if kunden_list else {}
    immobilie = kundenangaben.get("finanzierungsobjekt", {}).get("immobilie", {})
    finanzierungswunsch = kundenangaben.get("finanzierungswunsch", {})

    # ── 1. Strukturelle Validierung (Pflichtfelder) ──
    if not kundenangaben:
        errors.append("Struktur: kundenangaben fehlt")
    if not haushalte:
        errors.append("Struktur: haushalte[0] fehlt")
    if not kunde:
        errors.append("Struktur: kunden[0] fehlt")

    if not (kunde.get("personendaten") or {}).get("vorname"):
        errors.append("Pflichtfeld: Vorname fehlt")
    if not (kunde.get("personendaten") or {}).get("nachname"):
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

    # ── 2. Wertbereichs-Checks ──
    kaufpreis = immobilie.get("kaufpreis") or immobilie.get("marktwert") or 0
    darlehenssumme = finanzierungswunsch.get("darlehenssumme") or 0
    eigenkapital = finanzierungswunsch.get("eigenkapital") or 0
    wohnflaeche = immobilie.get("wohnflaeche")
    baujahr = immobilie.get("baujahr")

    if kaufpreis > 0:
        if kaufpreis < 30000:
            errors.append(f"Plausibilitaet: Kaufpreis zu niedrig ({kaufpreis:,.0f} EUR) - Minimum 30.000 EUR")
        if kaufpreis > 10_000_000:
            errors.append(f"Plausibilitaet: Kaufpreis zu hoch ({kaufpreis:,.0f} EUR) - Maximum 10.000.000 EUR")

    if darlehenssumme > 0:
        if darlehenssumme < 10000:
            errors.append(f"Plausibilitaet: Darlehenssumme zu niedrig ({darlehenssumme:,.0f} EUR) - Minimum 10.000 EUR")
        if darlehenssumme > 10_000_000:
            errors.append(f"Plausibilitaet: Darlehenssumme zu hoch ({darlehenssumme:,.0f} EUR) - Maximum 10.000.000 EUR")

    if eigenkapital < 0:
        errors.append(f"Plausibilitaet: Eigenkapital kann nicht negativ sein ({eigenkapital:,.0f} EUR)")
    if kaufpreis > 0 and eigenkapital > kaufpreis * 1.5:
        errors.append(
            f"Plausibilitaet: Eigenkapital ({eigenkapital:,.0f} EUR) > 150% des Kaufpreises ({kaufpreis:,.0f} EUR)"
        )

    if wohnflaeche is not None:
        if wohnflaeche < 15:
            warnings.append(f"Plausibilitaet: Wohnflaeche sehr klein ({wohnflaeche} m2) - unter 15 m2 unueblich")
        if wohnflaeche > 2000:
            errors.append(f"Plausibilitaet: Wohnflaeche zu gross ({wohnflaeche} m2) - Maximum 2.000 m2")

    if baujahr is not None:
        if baujahr < 1800:
            errors.append(f"Plausibilitaet: Baujahr zu alt ({baujahr}) - Minimum 1800")
        if baujahr > current_year + 2:
            errors.append(f"Plausibilitaet: Baujahr in der Zukunft ({baujahr})")

    # ── 3. Verhaeltniss-Checks (Financial Logic) ──
    geschaetzte_nebenkosten = kaufpreis * 0.12
    gesamtkosten = kaufpreis + geschaetzte_nebenkosten

    if kaufpreis > 0 and darlehenssumme > 0:
        if darlehenssumme > kaufpreis * 1.2:
            warnings.append(
                f"Plausibilitaet: Darlehenssumme ({darlehenssumme:,.0f} EUR) > 120% des Kaufpreises - Vollfinanzierung plus Nebenkosten?"
            )
        benoetigt = gesamtkosten - eigenkapital
        if darlehenssumme < benoetigt * 0.7 and eigenkapital < kaufpreis * 0.5:
            warnings.append(
                f"Plausibilitaet: Darlehenssumme ({darlehenssumme:,.0f} EUR) koennte zu niedrig sein - geschaetzt benoetigt: {benoetigt:,.0f} EUR"
            )

    if kaufpreis > 0:
        ek_quote = (eigenkapital / kaufpreis) * 100
        if ek_quote < 5 and eigenkapital > 0:
            warnings.append(f"Plausibilitaet: Eigenkapitalquote sehr niedrig ({ek_quote:.1f}%) - unter 5% unueblich")

    # ── 4. Personen-Checks ──
    geburtsdatum = (kunde.get("personendaten") or {}).get("geburtsdatum")
    if geburtsdatum:
        try:
            birth_date = datetime.fromisoformat(str(geburtsdatum).replace("Z", ""))
            today = datetime.now()
            age = (today - birth_date).days // 365
            if age < 18:
                errors.append(f"Plausibilitaet: Antragsteller unter 18 Jahre ({age} Jahre)")
            if age > 99:
                errors.append(f"Plausibilitaet: Geburtsdatum ergibt Alter ueber 99 Jahre ({age} Jahre)")
            if age > 75:
                warnings.append(f"Plausibilitaet: Antragsteller ueber 75 Jahre ({age} Jahre) - Finanzierung ggf. eingeschraenkt")
            if birth_date > today:
                errors.append(f"Plausibilitaet: Geburtsdatum liegt in der Zukunft ({geburtsdatum})")
        except Exception:
            pass

    netto_einkommen = (kunde.get("einkommenNetto") or {}).get("monatlichesNettoEinkommen")
    if netto_einkommen is not None:
        if netto_einkommen < 0:
            errors.append(f"Plausibilitaet: Nettoeinkommen kann nicht negativ sein ({netto_einkommen} EUR)")
        if 0 < netto_einkommen < 500:
            warnings.append(f"Plausibilitaet: Nettoeinkommen sehr niedrig ({netto_einkommen} EUR/Monat)")
        if netto_einkommen > 100000:
            warnings.append(f"Plausibilitaet: Nettoeinkommen sehr hoch ({netto_einkommen:,.0f} EUR/Monat) - bitte pruefen")

    # ── 5. Format-Checks ──
    plz = (immobilie.get("adresse") or {}).get("plz")
    if not plz:
        plz = ((kunde.get("wohnsituation") or {}).get("anschrift") or {}).get("plz")
    if plz:
        plz_str = str(plz)
        if not re.match(r"^\d{5}$", plz_str):
            errors.append(f'Format: PLZ muss 5 Ziffern haben (aktuell: "{plz}")')

    # ── 6. Zweiter Antragsteller ──
    if len(kunden_list) > 1:
        kunde2 = kunden_list[1]
        pd2 = kunde2.get("personendaten") or {}
        if not pd2.get("vorname") and not pd2.get("nachname"):
            warnings.append("Zweiter Antragsteller ohne Namen angegeben")
        geb2 = pd2.get("geburtsdatum")
        if geb2:
            try:
                bd2 = datetime.fromisoformat(str(geb2).replace("Z", ""))
                age2 = (datetime.now() - bd2).days // 365
                if age2 < 18:
                    errors.append(f"Plausibilitaet: Zweiter Antragsteller unter 18 Jahre ({age2} Jahre)")
                if age2 > 99:
                    errors.append(f"Plausibilitaet: Zweiter Antragsteller ueber 99 Jahre ({age2} Jahre)")
            except Exception:
                pass

    summary = {
        "errors_count": len(errors),
        "warnings_count": len(warnings),
        "kaufpreis": kaufpreis,
        "darlehenssumme": darlehenssumme,
        "eigenkapital": eigenkapital,
        "geschaetzte_nebenkosten": round(geschaetzte_nebenkosten),
        "geschaetztes_gesamtvolumen": round(gesamtkosten),
    }

    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "summary": summary,
    }


# ============================================================
# Import Executor
# ============================================================

def execute_import(case_id: str, dry_run: bool = False) -> dict:
    """
    Hauptfunktion: Payload bauen -> validieren -> API aufrufen -> Status updaten.
    dry_run=True: Nur validieren, nicht an API senden.
    Rueckgabe: {success: bool, case_id: str, europace_case_id: str|None,
               errors: list, warnings: list, payload_preview: dict}
    """
    result = {
        "success": False,
        "case_id": case_id,
        "europace_case_id": None,
        "errors": [],
        "warnings": [],
        "payload_preview": None,
        "dry_run": dry_run,
    }

    # 1. Payload bauen
    try:
        build_result = build_europace_payload(case_id)
    except ValueError as e:
        result["errors"].append(str(e))
        return result

    payload = build_result["payload"]
    effective_view = build_result["effective_view"]
    case = build_result["case"]

    result["payload_preview"] = payload

    # 2. Validieren
    validation = validate_payload(payload, effective_view)
    result["errors"].extend(validation["errors"])
    result["warnings"].extend(validation["warnings"])

    if not validation["is_valid"]:
        # Validierungsfehler -> Status ERROR + Fehler speichern
        _update_case_error(case, {
            "validation_errors": validation["errors"],
            "validation_warnings": validation["warnings"],
        })
        logger.warning(f"Import validation failed for {case_id}: {validation['errors']}")
        return result

    # Payload-Preview in DB speichern
    try:
        db.update_row("fin_cases", case["_id"], {
            "final_payload_preview": json.dumps(payload, indent=2, ensure_ascii=False),
        })
    except Exception as e:
        logger.warning(f"Could not store payload preview for {case_id}: {e}")

    # 3. Dry Run -> Nur validieren
    if dry_run:
        result["success"] = True
        logger.info(f"Import dry-run OK for {case_id}: payload valid")
        return result

    # 4. Europace API aufrufen
    if not EUROPACE_API_KEY:
        result["errors"].append("EUROPACE_API_KEY nicht konfiguriert")
        _update_case_error(case, {"error": "EUROPACE_API_KEY nicht konfiguriert"})
        return result

    try:
        api_response = _call_europace_api(payload)
    except Exception as e:
        error_msg = f"Europace API Fehler: {str(e)}"
        result["errors"].append(error_msg)
        _update_case_error(case, {"error": True, "message": error_msg})
        logger.error(f"Europace API call failed for {case_id}: {e}")
        return result

    # 5. API-Antwort auswerten
    status_code = api_response.get("_status_code", 0)

    if status_code < 300:
        # Erfolg
        europace_case_id = api_response.get("vorgangId") or api_response.get("id") or ""
        result["success"] = True
        result["europace_case_id"] = europace_case_id

        now = datetime.utcnow().isoformat()
        audit = case.get("_audit_log", [])
        audit.append({
            "event": "imported_to_europace",
            "ts": now,
            "europace_case_id": europace_case_id,
        })
        audit = audit[-100:]

        db.update_row("fin_cases", case["_id"], {
            "status": "IMPORTED",
            "europace_response": json.dumps(api_response, ensure_ascii=False),
            "europace_case_id": europace_case_id,
            "last_status_change": now,
            "audit_log": json.dumps(audit),
        })
        logger.info(f"Case {case_id} imported to Europace: {europace_case_id}")
    else:
        # API-Fehler
        error_body = json.dumps(api_response, ensure_ascii=False)
        result["errors"].append(f"Europace API HTTP {status_code}: {error_body[:500]}")
        _update_case_error(case, {
            "error": True,
            "statusCode": status_code,
            "body": api_response,
        })
        logger.error(f"Europace API returned {status_code} for {case_id}")

    return result


def _call_europace_api(payload: dict) -> dict:
    """Ruft die Europace API synchron auf. Gibt Response als dict zurueck."""
    headers = {
        "Authorization": f"Bearer {EUROPACE_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(EUROPACE_API_URL, json=payload, headers=headers)

    # Status-Code im Response mitgeben fuer Auswertung
    try:
        body = resp.json()
    except Exception:
        body = {"raw_body": resp.text[:2000]}

    body["_status_code"] = resp.status_code
    return body


def _update_case_error(case: dict, error_data: dict):
    """Setzt Case-Status auf ERROR und speichert Fehlerdetails."""
    now = datetime.utcnow().isoformat()
    audit = case.get("_audit_log", [])
    audit.append({"event": "import_error", "ts": now})
    audit = audit[-100:]

    try:
        db.update_row("fin_cases", case["_id"], {
            "status": "ERROR",
            "europace_response": json.dumps(error_data, ensure_ascii=False),
            "last_status_change": now,
            "audit_log": json.dumps(audit),
        })
    except Exception as e:
        logger.error(f"Failed to update case error status: {e}")
