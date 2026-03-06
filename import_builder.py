"""
Import Builder
Baut Europace-Payload gemaess Kundenangaben API v1.0, validiert und importiert Cases.

API Spec: https://developer.europace.de/api/baufismart-kundenangaben-api
Server:   https://baufinanzierung.api.europace.de
Endpoint: POST /kundenangaben
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
EUROPACE_API_KEY = os.getenv("EUROPACE_API_KEY", "")

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
        # Erfolg — Response enthaelt "vorgangsnummer"
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
