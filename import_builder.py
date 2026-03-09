"""
Import Builder
Baut Europace-Payload gemaess Kundenangaben API v1.0, validiert und importiert Cases.
Erstellt parallel einen Finlink-Lead via Partner API v2.

Europace API Spec: https://developer.europace.de/api/baufismart-kundenangaben-api
Finlink API Spec:  POST https://api.finlink.de/partner-api/leads
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

EUROPACE_API_URL = os.getenv(
    "EUROPACE_API_URL",
    "https://baufinanzierung.api.europace.de/kundenangaben",
)
EUROPACE_TOKEN_URL = os.getenv(
    "EUROPACE_TOKEN_URL",
    "https://api.europace.de/auth/access-token",
)
EUROPACE_CLIENT_ID = os.getenv("EUROPACE_CLIENT_ID", "")
EUROPACE_CLIENT_SECRET = os.getenv("EUROPACE_CLIENT_SECRET", "")

# Cached Europace OAuth2 token
_europace_token: Optional[str] = None
_europace_token_expires: float = 0

FINLINK_API_URL = os.getenv(
    "FINLINK_API_URL",
    "https://api.finlink.de/partner-api/leads",
)
FINLINK_API_KEY = os.getenv("FINLINK_API_KEY", "")

# ============================================================
# Europace Enum Mappings (for @type discriminator fields)
# ============================================================
FAMILIENSTAND_MAP = {
    "ledig": "LEDIG",
    "verheiratet": "VERHEIRATET",
    "geschieden": "GESCHIEDEN",
    "verwitwet": "VERWITWET",
    "getrennt lebend": "GETRENNT_LEBEND",
    "eingetragene Lebenspartnerschaft": "LEBENSPARTNERSCHAFT",
}

BESCHAEFTIGUNG_MAP = {
    "Angestellter": "ANGESTELLTER",
    "Angestellte": "ANGESTELLTER",
    "Arbeiter": "ARBEITER",
    "Beamter": "BEAMTER",
    "Beamtin": "BEAMTER",
    "Selbstständig": "SELBSTSTAENDIGER",
    "Selbständig": "SELBSTSTAENDIGER",
    "Freiberufler": "FREIBERUFLER",
    "Rentner": "RENTNER",
    "Rentnerin": "RENTNER",
    "Hausfrau": "HAUSHALTENDE_PERSON",
    "Hausmann": "HAUSHALTENDE_PERSON",
    "Arbeitslos": "ARBEITSLOSER",
    "Sonstiges": "ANGESTELLTER",  # Fallback
}

OBJEKTART_MAP = {
    "ETW": "EIGENTUMSWOHNUNG",
    "EFH": "EINFAMILIENHAUS",
    "DHH": "DOPPELHAUSHAELFTE",
    "RH": "REIHENHAUS",
    "MFH": "MEHRFAMILIENHAUS",
    "ZFH": "ZWEIFAMILIENHAUS",
    "Eigentumswohnung": "EIGENTUMSWOHNUNG",
    "Einfamilienhaus": "EINFAMILIENHAUS",
    "Doppelhaushälfte": "DOPPELHAUSHAELFTE",
    "Reihenhaus": "REIHENHAUS",
    "Mehrfamilienhaus": "MEHRFAMILIENHAUS",
    "Zweifamilienhaus": "ZWEIFAMILIENHAUS",
}

NUTZUNGSART_MAP = {
    "Eigennutzung": "EIGENGENUTZT",
    "Eigengenutzt": "EIGENGENUTZT",
    "Kapitalanlage": "VERMIETET",
    "Vermietet": "VERMIETET",
    "Teilvermietet": "TEILVERMIETET",
}

ANREDE_MAP = {"Herr": "HERR", "Frau": "FRAU"}


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
    """Entfernt None/leere Werte aus verschachteltem Dict. Behaelt @type Felder."""
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
            # @type muss immer erhalten bleiben (Europace Discriminator)
            if k == "@type" and isinstance(cv, str):
                cleaned[k] = cv
            elif cv is not None and cv != "" and cv != []:
                cleaned[k] = cv
        return cleaned if cleaned else None
    return obj


def _normalize_effective_view(view: dict) -> dict:
    """
    Uebersetzt deutsche Keys aus GPT-Extraktion auf die englischen Keys,
    die der Payload Builder erwartet. Arbeitet in-place auf einer Kopie.
    Nur fehlende EN-Keys werden befuellt (bestehende haben Vorrang).
    """
    import copy
    v = copy.deepcopy(view)

    # Mapping pro Person-Suffix ("" = primary, "_2" = partner)
    for suffix in ("", "_2"):
        app = f"applicant_data{suffix}"
        inc = f"income_data{suffix}"
        emp = f"employment_data{suffix}"

        app_dict = v.get(app, {})
        inc_dict = v.get(inc, {})
        emp_dict = v.get(emp, {})

        # --- applicant_data: DE -> EN ---
        de_en_map = {
            "vorname": "first_name",
            "nachname": "last_name",
            "geburtsdatum": "birth_date",
            "geburtsort": "birth_place",
            "nationalitaet": "nationality",
            "telefon": "phone",
        }
        for de_key, en_key in de_en_map.items():
            if not app_dict.get(en_key) and app_dict.get(de_key):
                app_dict[en_key] = app_dict[de_key]

        # --- Cross-section: income_data / employment_data -> applicant_data ---
        if not app_dict.get("net_income"):
            net = inc_dict.get("netto") or inc_dict.get("net_income")
            if net:
                app_dict["net_income"] = net

        if not app_dict.get("employer"):
            employer = (
                inc_dict.get("arbeitgeber")
                or emp_dict.get("arbeitgeber")
                or emp_dict.get("employer")
            )
            if employer:
                app_dict["employer"] = employer

        if not app_dict.get("employment_type"):
            etype = emp_dict.get("employment_type")
            if etype:
                app_dict["employment_type"] = etype

        if app_dict:
            v[app] = app_dict

    # --- household_data: familienstand ---
    hh = v.get("household_data", {})
    app_main = v.get("applicant_data", {})
    if not hh.get("marital_status") and app_main.get("familienstand"):
        hh["marital_status"] = app_main["familienstand"]
        v["household_data"] = hh

    # --- property_data: living_area -> living_space ---
    prop = v.get("property_data", {})
    if not prop.get("living_space") and prop.get("living_area"):
        prop["living_space"] = prop["living_area"]
        v["property_data"] = prop

    return v


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


def _safe_float(val) -> Optional[float]:
    """Konvertiert zu float, gibt None bei Fehler."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    """Konvertiert zu int, gibt None bei Fehler."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


# ============================================================
# Payload Builder (Europace Kundenangaben API v1.0)
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
    view = _normalize_effective_view(view)  # DE -> EN key translation

    def gv(primary, *fallbacks):
        return _get_value(view, primary, *fallbacks)

    # ── Kunde bauen (Europace Kunde Schema) ──
    def _build_kunde(prefix: str, ref_id: str) -> dict:
        p = f"{prefix}." if prefix else "applicant_data."
        p_alt = prefix.replace("applicant_data", "applicant") if prefix else "applicant"

        # Personendaten.person (nested!)
        anrede = _map_enum(gv(f"{p}salutation", f"{p_alt}_salutation"), ANREDE_MAP)
        vorname = gv(f"{p}first_name", f"{p_alt}_first_name")
        nachname = gv(f"{p}last_name", f"{p_alt}_last_name")

        person = {
            "anrede": anrede,
            "vorname": vorname,
            "nachname": nachname,
        }

        # Titel als Objekt: {"dr": true, "prof": true}
        titel_raw = gv(f"{p}title", f"{p_alt}_title")
        if titel_raw:
            titel_lower = str(titel_raw).lower()
            person["titel"] = {
                "dr": "dr" in titel_lower,
                "prof": "prof" in titel_lower,
            }

        # Familienstand als polymorphes Objekt mit @type
        fam_raw = gv("household_data.marital_status")
        fam_type = _map_enum(fam_raw, FAMILIENSTAND_MAP) if fam_raw else None
        familienstand = {"@type": fam_type} if fam_type else None

        personendaten = {
            "person": person,
            "geburtsdatum": gv(f"{p}birth_date", f"{p_alt}_birth_date"),
            "geburtsort": gv(f"{p}birth_place", f"{p_alt}_birth_place"),
            "staatsangehoerigkeit": gv(f"{p}nationality", f"{p_alt}_nationality") or "DE",
            "familienstand": familienstand,
        }

        # Kontakt
        kontakt = {
            "email": gv(f"{p}email", f"{p_alt}_email"),
        }
        phone = gv(f"{p}phone", f"{p_alt}_phone")
        if phone:
            kontakt["telefonnummer"] = {"nummer": phone}

        # Wohnsituation (nur fuer Hauptantragsteller)
        wohnsituation = None
        if prefix != "applicant_data_2":
            wohnsituation = {
                "anschrift": {
                    "strasse": gv("address_data.street"),
                    "hausnummer": gv("address_data.house_number"),
                    "plz": gv("address_data.zip"),
                    "ort": gv("address_data.city"),
                },
                "wohnhaftSeit": gv("address_data.resident_since"),
            }

        # Finanzielles (Beschaeftigung + Einkommen)
        beschaeftigung_raw = gv(f"{p}employment_type", f"{p_alt}_employment_type")
        beschaeftigung_type = _map_enum(beschaeftigung_raw, BESCHAEFTIGUNG_MAP)

        beschaeftigung = None
        if beschaeftigung_type:
            beschaeftigung = {"@type": beschaeftigung_type}

            # Angestellter/Beamter: Beschaeftigungsverhaeltnis
            if beschaeftigung_type in ("ANGESTELLTER", "ARBEITER", "BEAMTER"):
                bv = {}
                arbeitgeber_name = gv(f"{p}employer", f"{p_alt}_employer")
                if arbeitgeber_name:
                    bv["arbeitgeber"] = {
                        "name": arbeitgeber_name,
                        "inDeutschland": gv(f"{p}employer_in_germany") is not False,
                    }
                employed_since = gv(f"{p}employed_since", f"{p_alt}_employed_since")
                if employed_since:
                    bv["beschaeftigtSeit"] = employed_since

                emp_status = gv(f"{p}employment_status", f"{p_alt}_employment_status")
                if emp_status == "befristet":
                    bv["beschaeftigungsstatus"] = "BEFRISTET"

                probation = gv(f"{p}probation", f"{p_alt}_probation")
                if probation:
                    bv["probezeit"] = True

                beschaeftigung["beschaeftigungsverhaeltnis"] = bv

                beruf = gv(f"{p}occupation", f"{p_alt}_occupation")
                if beruf:
                    beschaeftigung["beruf"] = beruf

            # Selbststaendiger/Freiberufler: taetigkeit
            elif beschaeftigung_type in ("SELBSTSTAENDIGER", "FREIBERUFLER"):
                taetigkeit = {}
                taetig_seit = gv(f"{p}employed_since", f"{p_alt}_employed_since")
                if taetig_seit:
                    taetigkeit["taetigSeit"] = taetig_seit
                firma = gv(f"{p}employer", f"{p_alt}_employer")
                if firma:
                    taetigkeit["firma"] = firma
                if taetigkeit:
                    beschaeftigung["taetigkeit"] = taetigkeit

                beruf = gv(f"{p}occupation", f"{p_alt}_occupation")
                if beruf:
                    beschaeftigung["beruf"] = beruf

        net_income = _safe_float(
            gv(f"{p}net_income", f"{p}monthly_income", f"{p_alt}_monthly_income")
        )

        finanzielles = {
            "beschaeftigung": beschaeftigung,
            "einkommenNetto": net_income,
            "steuerId": gv(f"{p}tax_id", f"{p_alt}_tax_id"),
        }

        return {
            "referenzId": ref_id,
            "externeKundenId": f"{case_id}{ref_id}",
            "personendaten": personendaten,
            "kontakt": kontakt,
            "wohnsituation": wohnsituation,
            "finanzielles": finanzielles,
        }

    # ── Kunden-Array ──
    kunde1 = _build_kunde("applicant_data", "_1")
    kunden = [kunde1]

    is_couple = (
        gv("is_couple") is True
        or gv("applicant_2_first_name") is not None
        or gv("applicant_data_2.first_name") is not None
    )
    if is_couple:
        kunde2 = _build_kunde("applicant_data_2", "_2")
        pd2 = (kunde2.get("personendaten") or {}).get("person") or {}
        if pd2.get("vorname") or pd2.get("nachname"):
            kunden.append(kunde2)

    # ── Finanzierungswerte ──
    kaufpreis = _safe_float(
        gv("purchase_price", "property_data.purchase_price", "financing_data.purchase_price")
    )
    loan_amount = _safe_float(gv("loan_amount", "financing_data.loan_amount"))
    equity = _safe_float(gv("equity_to_use", "financing_data.equity_to_use", "equity"))

    # ── Immobilie.typ als polymorphes Objekt ──
    objektart_raw = gv("object_type", "property_data.object_type")
    objektart_type = _map_enum(objektart_raw, OBJEKTART_MAP)
    immobilie_typ = {"@type": objektart_type} if objektart_type else None

    # Gebaeude mit Nutzung
    nutzungsart_raw = gv("usage", "property_data.usage")
    nutzungsart_type = _map_enum(nutzungsart_raw, NUTZUNGSART_MAP)

    wohnflaeche = _safe_float(gv("property_data.living_space", "living_space"))
    baujahr = _safe_int(gv("property_data.year_built", "year_built"))

    gebaeude = {}
    if baujahr:
        gebaeude["baujahr"] = baujahr
    if nutzungsart_type or wohnflaeche:
        nutzung_wohnen = {}
        if nutzungsart_type:
            nutzung_wohnen["nutzungsart"] = {"@type": nutzungsart_type}
        if wohnflaeche:
            nutzung_wohnen["gesamtflaeche"] = wohnflaeche
        gebaeude["nutzung"] = {"wohnen": nutzung_wohnen}

    # Immobilie-Objekt
    immobilie = {
        "typ": immobilie_typ,
        "adresse": {
            "strasse": gv("property_data.street", "property_data.strasse", "property_street"),
            "hausnummer": gv("property_data.house_number", "property_house_number"),
            "plz": gv("property_data.zip", "property_data.plz", "property_zip"),
            "ort": gv("property_data.city", "property_data.ort", "property_city"),
        },
    }

    # Gebaeude nur setzen wenn Inhalt vorhanden
    # Immobilientyp-spezifische Felder kommen ins typ-Objekt
    if immobilie_typ and gebaeude:
        immobilie_typ["gebaeude"] = gebaeude

    # ── Finanzierungsbedarf mit Finanzierungszweck ──
    finanzierungszweck = None
    if kaufpreis:
        finanzierungszweck = {
            "@type": "KAUF",
            "kaufpreis": kaufpreis,
        }

    finanzierungsbedarf = {}
    if finanzierungszweck:
        finanzierungsbedarf["finanzierungszweck"] = finanzierungszweck

    # Eigenkapital als Vermoegen im Haushalt
    haushalt_vermoegen = None
    if equity and equity > 0:
        haushalt_vermoegen = {
            "summeBankUndSparguthaben": equity,
        }

    # ── Partner-ID (Betreuung) ──
    partner_id = gv("partnerId")

    # ── Gesamter Payload (ImportKundenangabenRequest) ──
    payload = {
        "importMetadaten": {
            "datenkontext": "ECHT_GESCHAEFT",
            "externeVorgangsId": case_id,
            "importquelle": "Alexander Heil Finanzierung Automation",
        },
        "kundenangaben": {
            "haushalte": [
                {
                    "kunden": kunden,
                    "finanzielleSituation": {
                        "vermoegen": haushalt_vermoegen,
                    } if haushalt_vermoegen else None,
                }
            ],
            "finanzierungsobjekt": {
                "immobilie": immobilie,
            },
            "finanzierungsbedarf": finanzierungsbedarf if finanzierungsbedarf else None,
        },
    }

    # Betreuung: Partner-ID als Kundenbetreuer
    if partner_id:
        payload["importMetadaten"]["betreuung"] = {
            "kundenbetreuer": partner_id,
        }

    # Tippgeber
    tippgeber_id = gv("tippgeberPartnerId")
    if tippgeber_id:
        payload["importMetadaten"]["tippgeber"] = {
            "tippgeberPartnerId": tippgeber_id,
        }

    cleaned = _clean_payload(payload)

    return {
        "payload": cleaned,
        "effective_view": view,
        "case": case,
    }


# ============================================================
# Finlink Lead Builder
# ============================================================

FINLINK_EMPLOYMENT_MAP = {
    "Angestellter": "employed_unlimited",
    "Angestellte": "employed_unlimited",
    "angestellt": "employed_unlimited",
    "befristet": "employed_unlimited",
    "Selbstständig": "self_employed",
    "Selbständig": "self_employed",
    "Freiberufler": "freelancer",
    "Beamter": "civil_servant",
    "Beamtin": "civil_servant",
    "Rentner": "retired",
    "Rentnerin": "retired",
    "Hausfrau": "home_maker",
    "Hausmann": "home_maker",
}

FINLINK_PROPERTY_TYPE_MAP = {
    "ETW": "apartment",
    "Eigentumswohnung": "apartment",
    "EIGENTUMSWOHNUNG": "apartment",
    "EFH": "single_family",
    "Einfamilienhaus": "single_family",
    "EINFAMILIENHAUS": "single_family",
    "DHH": "double_family",
    "Doppelhaushälfte": "double_family",
    "DOPPELHAUSHAELFTE": "double_family",
    "ZFH": "two_family",
    "Zweifamilienhaus": "two_family",
    "ZWEIFAMILIENHAUS": "two_family",
    "RH": "middle_terraced",
    "Reihenhaus": "middle_terraced",
    "REIHENHAUS": "middle_terraced",
    "MFH": "apartment_building",
    "Mehrfamilienhaus": "apartment_building",
    "MEHRFAMILIENHAUS": "apartment_building",
}

FINLINK_PROPERTY_USE_MAP = {
    "Eigennutzung": "primary_residence",
    "Eigengenutzt": "primary_residence",
    "EIGENGENUTZT": "primary_residence",
    "Kapitalanlage": "rental",
    "Vermietet": "rental",
    "VERMIETET": "rental",
    "Teilvermietet": "partial_rental",
    "TEILVERMIETET": "partial_rental",
}

FINLINK_RELATIONSHIP_MAP = {
    "ledig": "single",
    "verheiratet": "married",
    "geschieden": "divorced",
    "verwitwet": "widowed",
    "getrennt lebend": "separated",
    "eingetragene Lebenspartnerschaft": "registered_partnership",
}

FINLINK_GENDER_MAP = {
    "Herr": "male",
    "Frau": "female",
}


def build_finlink_payload(case_id: str, effective_view: dict) -> dict:
    """Baut einen Finlink Lead-Payload aus dem effective_view."""
    v = effective_view

    def gv(primary, *fallbacks):
        return _get_value(v, primary, *fallbacks)

    # Applicant data
    first_name = gv("applicant_data.first_name", "applicant_first_name")
    last_name = gv("applicant_data.last_name", "applicant_last_name")
    email = gv("applicant_data.email", "partner_email")
    phone = gv("applicant_data.phone", "applicant_phone")
    salutation = gv("applicant_data.salutation", "applicant_salutation")
    gender = _map_enum(salutation, FINLINK_GENDER_MAP)

    # Employment
    emp_raw = gv("applicant_data.employment_type", "applicant_employment_type")
    employment_status = _map_enum(emp_raw, FINLINK_EMPLOYMENT_MAP) or "employed_unlimited"

    # Income
    net_income = _safe_float(gv("applicant_data.net_income", "net_income"))

    # Relationship
    marital_raw = gv("household_data.marital_status")
    relationship_status = _map_enum(marital_raw, FINLINK_RELATIONSHIP_MAP)

    # Children
    children = _safe_int(gv("household_data.children", "children"))

    # Property
    property_type_raw = gv("object_type", "property_data.object_type")
    property_type = _map_enum(property_type_raw, FINLINK_PROPERTY_TYPE_MAP)
    usage_raw = gv("usage", "property_data.usage")
    property_use = _map_enum(usage_raw, FINLINK_PROPERTY_USE_MAP)

    # Prices
    purchase_price = _safe_float(gv("purchase_price", "property_data.purchase_price", "financing_data.purchase_price"))
    loan_amount = _safe_float(gv("loan_amount", "financing_data.loan_amount"))
    equity = _safe_float(gv("equity_to_use", "financing_data.equity_to_use"))

    # Is couple?
    is_couple = (
        gv("is_couple") is True
        or gv("applicant_2_first_name") is not None
        or gv("applicant_data_2.first_name") is not None
    )

    payload = {
        "lead": {
            "email": email or gv("partner_email") or "",
            "external_id": case_id,
            "user_meta": {
                "first_name": first_name or "",
                "last_name": last_name or "",
                "email": email or gv("partner_email") or "",
                "phone_number": phone,
                "gender": gender,
                "language_preference": "de",
            },
            "applicant_meta": {
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender or "male",
                "employment_status": employment_status,
                "monthly_net_income": net_income,
                "dob": gv("applicant_data.birth_date", "applicant_birth_date"),
                "birth_city": gv("applicant_data.birth_place", "applicant_birth_place"),
                "nationality": gv("applicant_data.nationality") or "German",
                "german_tax_id": gv("applicant_data.tax_id", "applicant_tax_id"),
                "relationship_status": relationship_status,
                "number_of_dependents": children,
                "phone_number": phone,
                # Address
                "street_address": gv("address_data.street"),
                "house_number": gv("address_data.house_number"),
                "zipcode": gv("address_data.zip"),
                "city": gv("address_data.city"),
                # Equity
                "bank_savings_amount_towards_down_payment": equity,
            },
            "property_meta": {
                "property_type": property_type,
                "property_use": property_use,
                "listed_price": purchase_price,
                "final_sale_price": purchase_price,
                "city_name": gv("property_data.city", "property_city"),
                "city_zipcode": gv("property_data.zip", "property_zip"),
                "german_zipcode_number": gv("property_data.zip", "property_zip"),
                "street_address": gv("property_data.street", "property_street"),
                "house_number": gv("property_data.house_number", "property_house_number"),
                "living_size_sq_meters": _safe_float(gv("property_data.living_space", "living_space")),
                "year_of_construction": _safe_int(gv("property_data.year_built", "year_built")),
                "number_of_rooms": _safe_float(gv("property_data.rooms")),
                "gross_monthly_rent": _safe_float(gv("monthly_rental_income")),
            },
            "loan_application_meta": {
                "applying_alone": "with_others" if is_couple else "on_own",
                "capital_amount_needed": _safe_int(loan_amount) or 0,
                "finance_type": "buy_existing",
                "found_property": "ready_to_buy" if purchase_price else "still_looking",
                "modernization_cost": 0,
                "refinance_amount_needed": 0,
            },
            "extras_meta": {
                "notes": f"Automatisch importiert. Case-ID: {case_id}",
                "external_id": case_id,
                "consent_for_privacy_policy": True,
                "created_by_push_api": True,
            },
        }
    }

    # Second applicant
    if is_couple:
        sal2 = gv("applicant_data_2.salutation", "applicant_2_salutation")
        gender2 = _map_enum(sal2, FINLINK_GENDER_MAP)
        emp2_raw = gv("applicant_data_2.employment_type", "applicant_2_employment_type")
        emp2 = _map_enum(emp2_raw, FINLINK_EMPLOYMENT_MAP) or "employed_unlimited"
        net2 = _safe_float(gv("applicant_data_2.net_income", "applicant_2_monthly_income"))

        second_applicant = {
            "first_name": gv("applicant_data_2.first_name", "applicant_2_first_name"),
            "last_name": gv("applicant_data_2.last_name", "applicant_2_last_name"),
            "gender": gender2 or "male",
            "employment_status": emp2,
            "monthly_net_income": net2,
            "dob": gv("applicant_data_2.birth_date", "applicant_2_birth_date"),
            "nationality": gv("applicant_data_2.nationality") or "German",
            "relationship_status": relationship_status,
            "lives_with_primary": True,
            "street_address": gv("address_data.street"),
            "house_number": gv("address_data.house_number"),
            "zipcode": gv("address_data.zip"),
            "city": gv("address_data.city"),
        }
        payload["lead"]["extras_meta"]["second_applicant"] = second_applicant

    return _clean_payload(payload)


def create_finlink_lead(case_id: str, facts: dict, applicant_name: str = "", partner_email: str = ""):
    """
    Erstellt einen Finlink Lead direkt bei Case-Erstellung.
    Wird als Background-Task aufgerufen, daher eigenes Error-Handling.
    facts = die initialen facts_extracted aus der E-Mail-Analyse.
    """
    if not FINLINK_API_KEY:
        logger.info(f"[Finlink] API Key nicht konfiguriert – Lead fuer {case_id} uebersprungen")
        return

    # Minimalen effective_view aus den initialen Facts bauen
    # (Bei Case-Erstellung haben wir noch keine Dokumente/Overrides)
    view = dict(facts or {})
    if applicant_name:
        view["applicant_name"] = applicant_name
    if partner_email:
        view["partner_email"] = partner_email

    try:
        view = _normalize_effective_view(view)
        payload = build_finlink_payload(case_id, view)
        response = _call_finlink_api(payload)
        finlink_lead_id = response.get("id") or ""
        logger.info(f"[Finlink] Lead erstellt fuer {case_id}: {finlink_lead_id}")

        # finlink_lead_id in DB speichern
        case = cases.load_case(case_id)
        if case:
            db.update_row("fin_cases", case["_id"], {
                "finlink_lead_id": finlink_lead_id,
            })
    except Exception as e:
        logger.error(f"[Finlink] Lead-Erstellung fehlgeschlagen fuer {case_id}: {e}")


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
    import_meta = (payload or {}).get("importMetadaten", {})
    haushalte = (kundenangaben.get("haushalte") or [{}])[0] if kundenangaben.get("haushalte") else {}
    kunden_list = haushalte.get("kunden", [{}]) if haushalte else [{}]
    kunde = kunden_list[0] if kunden_list else {}
    immobilie = kundenangaben.get("finanzierungsobjekt", {}).get("immobilie", {})
    finanzierungsbedarf = kundenangaben.get("finanzierungsbedarf", {})
    finanzierungszweck = finanzierungsbedarf.get("finanzierungszweck", {})

    # ── 1. Strukturelle Validierung (Pflichtfelder) ──
    if not import_meta:
        errors.append("Struktur: importMetadaten fehlt")
    if not import_meta.get("datenkontext"):
        errors.append("Pflichtfeld: importMetadaten.datenkontext fehlt")
    if not kundenangaben:
        errors.append("Struktur: kundenangaben fehlt")
    if not haushalte:
        errors.append("Struktur: haushalte[0] fehlt")
    if not kunde:
        errors.append("Struktur: kunden[0] fehlt")

    # referenzId ist required auf Kunde
    if not kunde.get("referenzId"):
        errors.append("Pflichtfeld: Kunde.referenzId fehlt")

    person = (kunde.get("personendaten") or {}).get("person") or {}
    if not person.get("vorname"):
        errors.append("Pflichtfeld: Vorname fehlt")
    if not person.get("nachname"):
        errors.append("Pflichtfeld: Nachname fehlt")

    imm_typ = immobilie.get("typ") or {}
    if not imm_typ.get("@type"):
        errors.append("Pflichtfeld: Immobilie.typ fehlt (Objektart)")

    kaufpreis = finanzierungszweck.get("kaufpreis") or 0
    if not kaufpreis:
        errors.append("Pflichtfeld: Kaufpreis fehlt")

    # ── 2. Wertbereichs-Checks ──
    if kaufpreis > 0:
        if kaufpreis < 30000:
            errors.append(f"Plausibilitaet: Kaufpreis zu niedrig ({kaufpreis:,.0f} EUR) - Minimum 30.000 EUR")
        if kaufpreis > 10_000_000:
            errors.append(f"Plausibilitaet: Kaufpreis zu hoch ({kaufpreis:,.0f} EUR) - Maximum 10.000.000 EUR")

    eigenkapital = ((haushalte.get("finanzielleSituation") or {}).get("vermoegen") or {}).get("summeBankUndSparguthaben") or 0

    if eigenkapital < 0:
        errors.append(f"Plausibilitaet: Eigenkapital kann nicht negativ sein ({eigenkapital:,.0f} EUR)")
    if kaufpreis > 0 and eigenkapital > kaufpreis * 1.5:
        errors.append(
            f"Plausibilitaet: Eigenkapital ({eigenkapital:,.0f} EUR) > 150% des Kaufpreises ({kaufpreis:,.0f} EUR)"
        )

    # Wohnflaeche/Baujahr aus Immobilientyp.gebaeude
    gebaeude = imm_typ.get("gebaeude") or {}
    wohnflaeche_obj = (gebaeude.get("nutzung") or {}).get("wohnen") or {}
    wohnflaeche = wohnflaeche_obj.get("gesamtflaeche")
    baujahr = gebaeude.get("baujahr")

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
        except (ValueError, TypeError):
            pass

    netto_einkommen = (kunde.get("finanzielles") or {}).get("einkommenNetto")
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
        pd2 = (kunde2.get("personendaten") or {}).get("person") or {}
        if not pd2.get("vorname") and not pd2.get("nachname"):
            warnings.append("Zweiter Antragsteller ohne Namen angegeben")
        geb2 = (kunde2.get("personendaten") or {}).get("geburtsdatum")
        if geb2:
            try:
                bd2 = datetime.fromisoformat(str(geb2).replace("Z", ""))
                age2 = (datetime.now() - bd2).days // 365
                if age2 < 18:
                    errors.append(f"Plausibilitaet: Zweiter Antragsteller unter 18 Jahre ({age2} Jahre)")
                if age2 > 99:
                    errors.append(f"Plausibilitaet: Zweiter Antragsteller ueber 99 Jahre ({age2} Jahre)")
            except (ValueError, TypeError):
                pass

    summary = {
        "errors_count": len(errors),
        "warnings_count": len(warnings),
        "kaufpreis": kaufpreis,
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
        "finlink_lead_id": None,
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

    # Finlink-ID aus DB mitlesen (wurde bei Case-Erstellung gesetzt)
    result["finlink_lead_id"] = case.get("finlink_lead_id") or None

    # 2. Validieren
    validation = validate_payload(payload, effective_view)
    result["errors"].extend(validation["errors"])
    result["warnings"].extend(validation["warnings"])

    if not validation["is_valid"]:
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
    if not EUROPACE_CLIENT_ID or not EUROPACE_CLIENT_SECRET:
        result["errors"].append("EUROPACE_CLIENT_ID / EUROPACE_CLIENT_SECRET nicht konfiguriert")
        _update_case_error(case, {"error": "Europace OAuth2 Credentials nicht konfiguriert"})
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
        europace_case_id = api_response.get("vorgangsnummer") or ""
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
        error_body = json.dumps(api_response, ensure_ascii=False)
        result["errors"].append(f"Europace API HTTP {status_code}: {error_body[:500]}")
        _update_case_error(case, {
            "error": True,
            "statusCode": status_code,
            "body": api_response,
        })
        logger.error(f"Europace API returned {status_code} for {case_id}")

    return result


def _get_europace_token() -> str:
    """Holt einen Europace OAuth2 Token via Client Credentials Flow. Cached bis Ablauf."""
    global _europace_token, _europace_token_expires
    import time as _time

    if _europace_token and _time.time() < _europace_token_expires - 60:
        return _europace_token

    if not EUROPACE_CLIENT_ID or not EUROPACE_CLIENT_SECRET:
        raise RuntimeError("EUROPACE_CLIENT_ID und EUROPACE_CLIENT_SECRET muessen gesetzt sein")

    resp = httpx.post(EUROPACE_TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": EUROPACE_CLIENT_ID,
        "client_secret": EUROPACE_CLIENT_SECRET,
    }, timeout=15.0)

    if resp.status_code != 200:
        raise RuntimeError(f"Europace token request failed ({resp.status_code}): {resp.text[:500]}")

    data = resp.json()
    _europace_token = data["access_token"]
    _europace_token_expires = _time.time() + data.get("expires_in", 3600)
    logger.info("[Europace] Token acquired, expires in %ds", data.get("expires_in", 3600))
    return _europace_token


def _call_europace_api(payload: dict) -> dict:
    """Ruft die Europace API synchron auf. Gibt Response als dict zurueck."""
    token = _get_europace_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json;version=1.0",
        "Accept": "application/json;version=1.0",
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


def _call_finlink_api(payload: dict) -> dict:
    """Ruft die Finlink Partner API synchron auf. Gibt Response als dict zurueck."""
    headers = {
        "x-api-key": FINLINK_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(FINLINK_API_URL, json=payload, headers=headers)

    try:
        body = resp.json()
    except Exception:
        body = {"raw_body": resp.text[:2000]}

    if resp.status_code >= 400:
        raise RuntimeError(f"Finlink API HTTP {resp.status_code}: {json.dumps(body, ensure_ascii=False)[:500]}")

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
