"""
Document Processor
Consolidates all document processing logic: queue management, GPT analysis orchestration,
person detection, facts mapping, and OneDrive upload.
"""

import base64
import json
import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import case_logic as cases
import db_postgres as db

logger = logging.getLogger(__name__)

# ── MIME Map (single source of truth) ────────────────────────────

MIME_MAP = {
    "pdf": "application/pdf",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "tiff": "image/tiff",
    "tif": "image/tiff",
    "webp": "image/webp",
    "gif": "image/gif",
}


# ── Data classes ─────────────────────────────────────────────────

@dataclass
class FileInput:
    filename: str
    file_bytes: bytes
    mime_type: str
    onedrive_file_id: str | None = None
    gdrive_file_id: str | None = None
    source: str = "unknown"  # "email", "onedrive", "gdrive"


@dataclass
class FileResult:
    filename: str
    success: bool
    doc_type: str | None = None
    error: str | None = None
    person_name: str | None = None


# ── Processing Queue ─────────────────────────────────────────────

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


# ── Person name helpers ──────────────────────────────────────────

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
    Compares first names (fuzzy). If only last name matches but first name
    is clearly different → NOT primary (likely the partner).
    Returns True by default if we can't determine.
    """
    from difflib import SequenceMatcher

    if not person_name or not case_applicant_name:
        return True  # Can't determine → default to primary

    pn = _clean_person_name(person_name).lower()
    cn = _clean_person_name(case_applicant_name).lower()

    # Handle "und"/"&" in names — case_applicant_name may be "Max und Lisa Müller"
    cn_names = cn.replace(" und ", " ").replace(" & ", " ").split()
    pn_parts = pn.split()

    # Remove common filler words
    fillers = {"und", "&", "von", "van", "de", "der", "die", "das"}
    cn_names = [p for p in cn_names if p not in fillers]
    pn_parts = [p for p in pn_parts if p not in fillers]

    if not pn_parts or not cn_names:
        return True

    # Strategy: If we can identify first + last name, compare first names.
    # A last-name-only match with a different first name means NOT primary.
    if len(pn_parts) >= 2 and len(cn_names) >= 2:
        pn_first = pn_parts[0]
        pn_last = " ".join(pn_parts[1:])
        # Case name may contain multiple first names (e.g. "Max Lisa Müller")
        # Try each name in cn_names as potential first name match
        cn_last = cn_names[-1]  # Last part is likely the surname
        cn_firsts = cn_names[:-1]  # Everything else is first name(s)

        last_match = (pn_last == cn_last
                      or SequenceMatcher(None, pn_last, cn_last).ratio() >= 0.8)
        first_match = any(
            pn_first == cf or SequenceMatcher(None, pn_first, cf).ratio() >= 0.8
            for cf in cn_firsts
        )
        if last_match and not first_match:
            # Same family name but different first name → partner, not primary
            return False
        if first_match:
            return True

    # Fallback: any part matches
    for pp in pn_parts:
        for cp in cn_names:
            if pp == cp or (len(pp) > 2 and len(cp) > 2 and SequenceMatcher(None, pp, cp).ratio() >= 0.8):
                return True
    return False


def _extract_names_from_dict(ed: dict) -> list[str]:
    """Extract person names from a single extracted_data dict."""
    names = []
    vorname = ed.get("Vorname") or ed.get("vorname") or ""
    nachname = ed.get("Nachname") or ed.get("nachname") or ""
    if vorname and nachname:
        names.append(f"{vorname} {nachname}")
    ad = ed.get("applicant_data") or {}
    if isinstance(ad, dict):
        fn = ad.get("first_name") or ad.get("vorname") or ""
        ln = ad.get("last_name") or ad.get("nachname") or ""
        if fn and ln:
            names.append(f"{fn} {ln}")
    return names


def _collect_person_names_from_docs(case_id: str) -> list[str]:
    """Sammelt alle Personennamen aus den Dokumenten eines Cases."""
    names = []
    docs = db.search_rows("fin_documents", "caseId", case_id) or []
    for doc in docs:
        ed = doc.get("extracted_data") or {}
        if isinstance(ed, str):
            try:
                ed = json.loads(ed)
            except Exception:
                continue
        # Handle list (e.g. Reisepässe with 2 persons)
        items = ed if isinstance(ed, list) else [ed]
        for item in items:
            if not isinstance(item, dict):
                continue
            for name in _extract_names_from_dict(item):
                if name not in names:
                    names.append(name)
    return names


def _detect_is_couple(applicant_name: str, facts: dict, all_person_names: list[str] | None = None) -> bool:
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
    # 3. Verschiedene Vornamen in bisherigen Dokumenten → Paar
    if all_person_names and applicant_name:
        from difflib import SequenceMatcher
        case_clean = _clean_person_name(applicant_name).lower()
        case_parts = case_clean.split()
        if len(case_parts) >= 2:
            case_first = case_parts[0]
            case_last_parts = case_parts[1:]  # Kann mehrteilig sein (von Müller)
            for pn in all_person_names:
                pn_clean = _clean_person_name(pn).lower()
                pn_parts = pn_clean.split()
                if len(pn_parts) < 2:
                    continue
                pn_first = pn_parts[0]
                pn_last_parts = pn_parts[1:]
                # Vorname muss verschieden sein
                if pn_first == case_first:
                    continue
                # Nachname muss ähnlich sein (exakt ODER fuzzy >= 0.8)
                case_last = " ".join(case_last_parts)
                pn_last = " ".join(pn_last_parts)
                if case_last == pn_last or SequenceMatcher(None, case_last, pn_last).ratio() >= 0.8:
                    logger.info(
                        f"Couple detected: '{applicant_name}' vs person '{pn}' "
                        f"(last name match: '{case_last}' ~ '{pn_last}', different first names)"
                    )
                    return True
    return False


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


# ── Facts Mapping ────────────────────────────────────────────────

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
        _miete = extracted.get("Monatliche_Miete") or extracted.get("Monatliche Miete")
        if _miete:
            facts["monthly_rent"] = _miete

    elif doc_type in ("Exposé",):
        # Property data is shared
        # GPT liefert Adresse manchmal als Objekt {Ort, PLZ, Straße} statt einzeln
        _addr = extracted.get("Adresse") or extracted.get("Address") or {}
        if isinstance(_addr, str):
            _addr = {}  # String-Adresse ignorieren, Einzelfelder bevorzugen
        _street = (extracted.get("Straße") or extracted.get("Strasse") or extracted.get("street")
                   or (isinstance(_addr, dict) and (_addr.get("Straße") or _addr.get("Strasse") or _addr.get("StraßE") or _addr.get("street"))))
        _hnr = (extracted.get("Hausnummer") or extracted.get("house_number")
                or (isinstance(_addr, dict) and (_addr.get("Hausnummer") or _addr.get("house_number"))))
        _plz = (extracted.get("PLZ") or extracted.get("zip")
                or (isinstance(_addr, dict) and (_addr.get("PLZ") or _addr.get("zip"))))
        _city = (extracted.get("Ort") or extracted.get("Stadt") or extracted.get("city")
                 or (isinstance(_addr, dict) and (_addr.get("Ort") or _addr.get("Stadt") or _addr.get("city"))))
        facts["property_data"] = {
            "purchase_price": extracted.get("Kaufpreis") or extracted.get("purchase_price"),
            "street": _street or None,
            "house_number": _hnr or None,
            "zip": _plz or None,
            "plz": _plz or None,
            "city": _city or None,
            "ort": _city or None,
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

    elif doc_type in ("Wohnflächenberechnung",):
        # Wohnflächenberechnung hat höchste Priorität für living_space
        wfl = extracted.get("Wohnfläche") or extracted.get("Wohnflaeche") or extracted.get("Gesamtwohnfläche")
        facts["property_data"] = {
            "living_space": wfl,
            "living_space_wfb": wfl,  # Marker: aus Wohnflächenberechnung (hat Vorrang)
        }

    elif doc_type in ("Baubeschreibung", "Grundriss", "Teilungserklärung", "Modernisierungsaufstellung", "Grundbuch"):
        # Property-Dokumente: Objektadresse extrahieren, aber living_space NICHT überschreiben
        _addr = extracted.get("Adresse") or extracted.get("Address") or {}
        if isinstance(_addr, str):
            _addr = {}
        _street = (extracted.get("Straße") or extracted.get("Strasse") or extracted.get("street")
                   or (isinstance(_addr, dict) and (_addr.get("Straße") or _addr.get("Strasse") or _addr.get("street"))))
        _hnr = (extracted.get("Hausnummer") or extracted.get("house_number")
                or (isinstance(_addr, dict) and (_addr.get("Hausnummer") or _addr.get("house_number"))))
        _plz = (extracted.get("PLZ") or extracted.get("zip")
                or (isinstance(_addr, dict) and (_addr.get("PLZ") or _addr.get("zip"))))
        _city = (extracted.get("Ort") or extracted.get("Stadt") or extracted.get("city")
                 or (isinstance(_addr, dict) and (_addr.get("Ort") or _addr.get("Stadt") or _addr.get("city"))))
        prop = {}
        if _street: prop["street"] = _street
        if _hnr: prop["house_number"] = _hnr
        if _plz: prop["zip"] = _plz; prop["plz"] = _plz
        if _city: prop["city"] = _city; prop["ort"] = _city
        baujahr = extracted.get("Baujahr")
        if baujahr: prop["year_built"] = baujahr
        if prop:
            facts["property_data"] = prop

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


# ── OneDrive Upload ──────────────────────────────────────────────

def _upload_to_onedrive(case_id: str, filename: str, file_bytes: bytes, mime: str, onedrive_folder_id: str):
    """Upload a file to OneDrive via n8n webhook (best-effort, non-blocking)."""
    import httpx

    webhook_url = os.getenv("N8N_ONEDRIVE_UPLOAD_WEBHOOK", "")
    if not webhook_url or not onedrive_folder_id:
        return

    try:
        b64 = base64.b64encode(file_bytes).decode("utf-8")
        api_key = os.getenv("N8N_WEBHOOK_API_KEY", "")
        headers = {"X-API-Key": api_key} if api_key else {}
        resp = httpx.post(
            webhook_url,
            headers=headers,
            json={
                "case_id": case_id,
                "filename": filename,
                "data_base64": b64,
                "mime_type": mime,
                "onedrive_folder_id": onedrive_folder_id,
            },
            timeout=60,
        )
        if resp.status_code == 200:
            logger.info(f"[{case_id}] Uploaded to OneDrive: {filename}")
        else:
            logger.warning(f"[{case_id}] OneDrive upload failed ({resp.status_code}): {filename}")
    except Exception as e:
        logger.warning(f"[{case_id}] OneDrive upload error for {filename}: {e}")


# ── DocumentProcessor class ─────────────────────────────────────

class DocumentProcessor:
    """
    Central document processing orchestrator.
    Handles batch and single-file processing with per-case locking,
    GPT analysis, facts mapping, and OneDrive upload.
    """

    def __init__(self, analyze_fn: Callable[[bytes, str, str], dict]):
        """
        Args:
            analyze_fn: Function matching signature of analyze_with_gpt4o(file_bytes, mime_type, filename) -> dict
        """
        self._analyze_fn = analyze_fn
        self._case_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)

    def _get_lock(self, case_id: str) -> threading.Lock:
        """Get or create a per-case threading.Lock."""
        return self._case_locks[case_id]

    def process_batch(self, case_id: str, files: list[FileInput], upload_to_onedrive_folder: str = "") -> dict:
        """
        Process a batch of files for a case. Sync function (call via asyncio.to_thread).

        Steps:
        1. Add all files to queue
        2. Acquire per-case lock
        3. Pass 1: Analyze each file with GPT (with rate limiting + retry)
        4. Pass 2: Detect couple, map facts for each result
        5. Batch-upsert doc rows
        6. Single save_facts() call
        7. Update applicant name for identity docs
        8. Upload to OneDrive if folder provided
        9. Return results dict
        """
        # 1. Add all files to queue
        for f in files:
            _queue_add(case_id, f.filename)

        results: list[FileResult] = []
        errors: list[str] = []
        doc_rows: list[dict] = []
        all_new_facts: dict = {}
        all_person_names: list[str] = []
        analysis_results: list[dict] = []
        files_processed = 0

        # 2. Acquire per-case lock
        lock = self._get_lock(case_id)
        with lock:
            # 3. Load case for applicant_name
            case = cases.load_case(case_id)
            case_name = case.get("applicant_name") if case else None
            now_ts = datetime.utcnow().isoformat()

            # ── Pass 1: Analyze all documents + collect person names ──
            for i, file_input in enumerate(files):
                fname = file_input.filename
                try:
                    _queue_update(case_id, fname, status="processing", started_at=datetime.utcnow().isoformat())

                    # Throttle: pause between GPT calls to avoid rate limits
                    if i > 0:
                        time.sleep(4)

                    # Analyze with GPT (with retry on 429)
                    result = None
                    for _attempt in range(3):
                        try:
                            result = self._analyze_fn(file_input.file_bytes, file_input.mime_type, fname)
                            break
                        except Exception as _gpt_err:
                            if "429" in str(_gpt_err) or "rate_limit" in str(_gpt_err):
                                wait = 8 * (_attempt + 1)
                                logger.warning(f"[{case_id}] Rate limit hit for {fname}, waiting {wait}s (attempt {_attempt+1}/3)")
                                time.sleep(wait)
                                if _attempt == 2:
                                    raise
                            else:
                                raise

                    extracted_raw = result.get("extracted_data") or {}
                    extracted = extracted_raw
                    if isinstance(extracted, list):
                        # Multi-person doc (e.g. Reisepässe) — collect all names, use first as primary
                        for _item in extracted:
                            if isinstance(_item, dict):
                                for _n in _extract_names_from_dict(_item):
                                    if _n not in all_person_names:
                                        all_person_names.append(_n)
                        extracted = extracted[0] if extracted and isinstance(extracted[0], dict) else {}
                        logger.info(f"[{case_id}] Multi-person doc: {all_person_names}")
                    doc_type = result.get("doc_type", "Sonstiges")
                    _person = (result.get("meta") or {}).get("person_name")

                    # Collect person name
                    if _person and _person not in all_person_names:
                        all_person_names.append(_person)

                    # Build doc row for DB insert (store original list if multi-person)
                    doc_row = {
                        "caseId": case_id,
                        "file_name": fname,
                        "doc_type": doc_type,
                        "extracted_data": json.dumps(extracted_raw),
                        "processing_status": "completed",
                        "processed_at": now_ts,
                    }
                    if file_input.gdrive_file_id:
                        doc_row["gdrive_file_id"] = file_input.gdrive_file_id
                    if file_input.onedrive_file_id:
                        doc_row["onedrive_file_id"] = file_input.onedrive_file_id
                    doc_rows.append(doc_row)

                    # Collect analysis result for Pass 2
                    analysis_results.append({
                        "doc_type": doc_type,
                        "extracted": extracted,
                        "person_name": _person,
                        "filename": fname,
                    })

                    results.append(FileResult(
                        filename=fname,
                        success=True,
                        doc_type=doc_type,
                        person_name=_person,
                    ))
                    files_processed += 1

                    # Upload to OneDrive immediately after analysis (best-effort)
                    if upload_to_onedrive_folder and file_input.file_bytes:
                        _upload_to_onedrive(case_id, fname, file_input.file_bytes, file_input.mime_type, upload_to_onedrive_folder)

                    _queue_update(case_id, fname, status="done", doc_type=doc_type, finished_at=datetime.utcnow().isoformat())
                    logger.info(f"[{case_id}] Analyzed: {fname} -> {doc_type}")

                except Exception as e:
                    err_msg = str(e)
                    logger.error(f"[{case_id}] Failed to process {fname}: {err_msg}")
                    results.append(FileResult(filename=fname, success=False, error=err_msg))
                    errors.append(f"{fname}: {err_msg}")
                    _queue_update(case_id, fname, status="error", error=err_msg, finished_at=datetime.utcnow().isoformat())

            # ── Pass 2: Facts mapping with couple detection ──
            _is_couple = _detect_is_couple(case_name or "", all_new_facts, all_person_names)
            if _is_couple:
                logger.info(f"[{case_id}] Couple detected from batch docs: {all_person_names}")

            for ar in analysis_results:
                new_facts = _map_extracted_to_facts(
                    ar["doc_type"], ar["extracted"],
                    person_name=ar["person_name"],
                    case_applicant_name=case_name,
                    is_couple=_is_couple,
                )
                if new_facts:
                    all_new_facts = cases.merge_facts(all_new_facts, new_facts)

                # Applicant name correction for identity docs
                if ar["doc_type"] in ("Ausweiskopie", "Selbstauskunft") and ar["person_name"]:
                    _maybe_update_applicant_name(case_id, ar["person_name"])

            # 6. Batch-upsert doc rows (match by gdrive_file_id, then onedrive_file_id, then filename)
            if doc_rows:
                try:
                    existing_docs = db.search_rows("fin_documents", "caseId", case_id)
                    existing_by_name = {}
                    existing_by_gdrive_id = {}
                    existing_by_onedrive_id = {}
                    for d in existing_docs:
                        fn = d.get("file_name", "")
                        existing_by_name[fn] = d
                        if fn.startswith("gdrive:"):
                            existing_by_name[fn[7:]] = d
                        gid = d.get("gdrive_file_id", "")
                        if gid:
                            existing_by_gdrive_id[gid] = d
                        oid = d.get("onedrive_file_id", "")
                        if oid:
                            existing_by_onedrive_id[oid] = d

                    rows_to_insert = []
                    for row in doc_rows:
                        fname = row.get("file_name")
                        gid = row.get("gdrive_file_id", "")
                        oid = row.get("onedrive_file_id", "")
                        existing = (
                            existing_by_gdrive_id.get(gid) if gid else None
                        ) or (
                            existing_by_onedrive_id.get(oid) if oid else None
                        ) or existing_by_name.get(fname)

                        if existing:
                            update_data = {
                                "file_name": fname,
                                "doc_type": row["doc_type"],
                                "extracted_data": row["extracted_data"],
                                "processing_status": row["processing_status"],
                                "processed_at": row["processed_at"],
                            }
                            if gid:
                                update_data["gdrive_file_id"] = gid
                            if oid:
                                update_data["onedrive_file_id"] = oid
                            db.update_row("fin_documents", existing["_id"], update_data)
                        else:
                            rows_to_insert.append(row)

                    if rows_to_insert:
                        db.batch_create_rows("fin_documents", rows_to_insert)
                    logger.info(f"[{case_id}] Docs upsert: {len(doc_rows) - len(rows_to_insert)} updated, {len(rows_to_insert)} inserted")
                except Exception as e:
                    logger.error(f"[{case_id}] Docs upsert failed: {e}")
                    errors.append(f"DB save error: {e}")

            # 7. Single save_facts() call
            if all_new_facts:
                try:
                    cases.save_facts(case_id, all_new_facts, source="batch")
                except Exception as e:
                    logger.error(f"[{case_id}] Batch facts merge failed: {e}")
                    errors.append(f"Facts merge error: {e}")

            _queue_cleanup(case_id)

        return {
            "success": files_processed > 0,
            "files_found": len(files),
            "files_processed": files_processed,
            "files_skipped": 0,
            "results": [
                {
                    "filename": r.filename,
                    "success": r.success,
                    "doc_type": r.doc_type,
                    "error": r.error,
                    "person_name": r.person_name,
                }
                for r in results
            ],
            "errors": errors,
        }

    def process_single(self, case_id: str, file: FileInput) -> dict:
        """
        Process a single file for a case. Runs in background thread.

        Steps:
        1. Add to queue
        2. Acquire per-case lock
        3. Analyze with GPT
        4. Collect person names from existing docs + this doc
        5. Detect couple, map facts
        6. Upsert single doc row
        7. Save facts
        8. Readiness check
        9. Update queue
        10. Return result
        """
        _queue_add(case_id, file.filename)

        lock = self._get_lock(case_id)
        with lock:
            _queue_update(case_id, file.filename, status="processing", started_at=datetime.utcnow().isoformat())
            logger.info(f"[{case_id}] Processing {file.filename} (single)")

            # 3. Analyze with GPT
            try:
                result = self._analyze_fn(file.file_bytes, file.mime_type, file.filename)
            except Exception as e:
                logger.error(f"[{case_id}] Document analysis failed for {file.filename}: {e}")
                _queue_update(case_id, file.filename, status="error", error=str(e), finished_at=datetime.utcnow().isoformat())
                db.create_row("fin_documents", {
                    "caseId": case_id,
                    "onedrive_file_id": file.onedrive_file_id or "",
                    "file_name": file.filename,
                    "doc_type": "error",
                    "processing_status": "error",
                    "error_message": str(e),
                    "processed_at": datetime.utcnow().isoformat(),
                })
                _queue_cleanup(case_id)
                return {"success": False, "case_id": case_id, "error": str(e)}

            extracted_raw = result.get("extracted_data") or {}
            extracted = extracted_raw
            if isinstance(extracted, list):
                # Multi-person doc — collect all names for couple detection
                for _item in extracted:
                    if isinstance(_item, dict):
                        for _n in _extract_names_from_dict(_item):
                            logger.info(f"[{case_id}] Multi-person doc name: {_n}")
                extracted = extracted[0] if extracted and isinstance(extracted[0], dict) else {}
            doc_type = result.get("doc_type", "Sonstiges")
            _person = (result.get("meta") or {}).get("person_name")

            # 6. Upsert single doc row (store original list if multi-person)
            doc_data = {
                "doc_type": doc_type,
                "extracted_data": json.dumps(extracted_raw),
                "processing_status": "completed",
                "processed_at": datetime.utcnow().isoformat(),
            }
            existing_doc = None
            existing_docs = db.search_rows("fin_documents", "caseId", case_id)
            for d in existing_docs:
                if file.onedrive_file_id and d.get("onedrive_file_id") == file.onedrive_file_id:
                    existing_doc = d
                    break
                if file.gdrive_file_id and d.get("gdrive_file_id") == file.gdrive_file_id:
                    existing_doc = d
                    break
                if d.get("file_name") == file.filename:
                    existing_doc = d
                    break
            if existing_doc:
                db.update_row("fin_documents", existing_doc["_id"], doc_data)
            else:
                doc_data["caseId"] = case_id
                doc_data["onedrive_file_id"] = file.onedrive_file_id or ""
                doc_data["file_name"] = file.filename
                if file.gdrive_file_id:
                    doc_data["gdrive_file_id"] = file.gdrive_file_id
                db.create_row("fin_documents", doc_data)

            # 5. Collect person names, detect couple, map facts
            try:
                _case = cases.load_case(case_id)
                _case_name = _case.get("applicant_name") if _case else None
                _existing_facts = _case.get("_facts_extracted", {}) if _case else {}

                _existing_person_names = _collect_person_names_from_docs(case_id)
                if _person and _person not in _existing_person_names:
                    _existing_person_names.append(_person)

                _is_couple = _detect_is_couple(_case_name, _existing_facts, _existing_person_names)
                new_facts = _map_extracted_to_facts(
                    doc_type, extracted,
                    person_name=_person,
                    case_applicant_name=_case_name,
                    is_couple=_is_couple,
                )

                # 7. Save facts
                if new_facts:
                    cases.save_facts(case_id, new_facts, source=f"document:{doc_type}")

                if doc_type in ("Ausweiskopie", "Selbstauskunft") and _person:
                    _maybe_update_applicant_name(case_id, _person)
            except Exception as e:
                logger.error(f"[{case_id}] Facts merge failed for {file.filename}: {e}")

            # 8. Readiness check (lazy import to avoid circular deps)
            try:
                import readiness as rdns
                rdns.check_readiness(case_id)
            except Exception as e:
                logger.error(f"[{case_id}] Readiness check failed after {file.filename}: {e}")

            # 9. Update queue
            _queue_update(case_id, file.filename,
                          status="done",
                          doc_type=doc_type,
                          finished_at=datetime.utcnow().isoformat())
            _queue_cleanup(case_id)
            logger.info(f"[{case_id}] Done processing {file.filename} -> {doc_type}")

        # 10. Return result
        return {
            "success": True,
            "case_id": case_id,
            "doc_type": doc_type,
            "person_name": _person,
            "facts_merged": True,
        }

    def get_queue(self, case_id: str) -> dict:
        """Returns queue status for a case. Same format as /api/dashboard/case/{case_id}/queue."""
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
