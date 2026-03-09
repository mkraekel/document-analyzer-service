"""
Readiness Check Logic
Prüft ob ein Case vollständig ist und bestimmt den nächsten Status.

EINZIGE AUTHORITATIVE IMPLEMENTIERUNG.
main.py's /check-readiness delegiert hierher.
"""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Optional
import case_logic as cases

logger = logging.getLogger(__name__)

# ============================================================
# PFLICHTFELDER (Blockierend) – Finanzierungsdaten
# ============================================================
REQUIRED_FINANCING_KEYS = ["purchase_price", "loan_amount", "equity_to_use", "object_type", "usage"]

# ============================================================
# PFLICHTFELDER (Blockierend) – Antragsteller 1 Stammdaten
# ============================================================
REQUIRED_APPLICANT_KEYS = [
    "applicant_first_name", "applicant_last_name", "applicant_birth_date",
    "employment_type", "net_income",
]

# ============================================================
# PFLICHTFELDER (Blockierend) – Wohnadresse
# ============================================================
REQUIRED_ADDRESS_KEYS = [
    "address_street", "address_house_number", "address_zip", "address_city",
]

# ============================================================
# PFLICHTFELDER (Blockierend) – Zusatz Selbstständige
# ============================================================
REQUIRED_SELF_EMPLOYED_KEYS = ["self_employed_since", "profit_last_year"]

# ============================================================
# EMPFOHLENE FELDER (Nicht blockierend, Warnung)
# ============================================================
RECOMMENDED_KEYS = [
    "salutation", "birth_place", "nationality", "tax_id", "phone",
    "occupation", "employer", "employed_since", "marital_status",
    "children", "property_street", "property_city", "property_zip",
    "living_space", "year_built", "zinsbindung", "partnerId",
]

KEY_SEARCH_PATHS = {
    # Finanzierungsdaten
    "purchase_price": ["purchase_price", "property_data.purchase_price", "financing_data.purchase_price"],
    "loan_amount": ["loan_amount", "financing_data.loan_amount"],
    "equity_to_use": ["equity_to_use", "financing_data.equity_to_use"],
    "object_type": ["object_type", "property_data.object_type"],
    "usage": ["usage", "property_data.usage"],
    # Antragsteller Stammdaten
    "applicant_first_name": ["applicant_data.first_name", "applicant_data.vorname", "first_name", "vorname"],
    "applicant_last_name": ["applicant_data.last_name", "applicant_data.nachname", "last_name", "nachname", "applicant_name"],
    "applicant_birth_date": ["applicant_data.birth_date", "applicant_data.geburtsdatum", "birth_date", "geburtsdatum"],
    "employment_type": ["applicant_data.employment_type", "employment_data.employment_type", "employment_type", "employment_status"],
    "net_income": ["applicant_data.net_income", "income_data.net_income", "income_data.netto", "applicant_data.nettoeinkommen", "net_income", "nettoeinkommen", "monthly_income"],
    # Wohnadresse
    "address_street": ["address_data.street", "address_data.strasse", "street", "strasse"],
    "address_house_number": ["address_data.house_number", "address_data.hausnummer", "house_number", "hausnummer"],
    "address_zip": ["address_data.zip", "address_data.plz", "zip", "plz", "postal_code"],
    "address_city": ["address_data.city", "address_data.ort", "city", "ort"],
    # Selbstständige Zusatz
    "self_employed_since": ["applicant_data.self_employed_since", "employment_data.self_employed_since", "self_employed_since", "selbststaendig_seit"],
    "profit_last_year": ["applicant_data.profit_last_year", "income_data.profit_last_year", "profit_last_year", "gewinn_vorjahr"],
    # Empfohlene Felder
    "salutation": ["applicant_data.salutation", "applicant_data.anrede", "salutation", "anrede"],
    "birth_place": ["applicant_data.birth_place", "birth_place", "geburtsort"],
    "nationality": ["applicant_data.nationality", "nationality", "staatsangehoerigkeit"],
    "tax_id": ["applicant_data.tax_id", "applicant_data.steuer_id", "tax_id", "steuer_id"],
    "phone": ["applicant_data.phone", "phone", "telefon"],
    "occupation": ["applicant_data.occupation", "applicant_data.beruf", "occupation", "beruf"],
    "employer": ["applicant_data.employer", "employment_data.employer", "income_data.employer", "employment_data.arbeitgeber", "income_data.arbeitgeber", "employer", "arbeitgeber"],
    "employed_since": ["applicant_data.employed_since", "employment_data.employed_since", "employed_since", "beschaeftigt_seit"],
    "marital_status": ["household_data.marital_status", "applicant_data.marital_status", "applicant_data.familienstand", "marital_status", "familienstand"],
    "children": ["household_data.children", "applicant_data.children", "applicant_data.kinder", "children", "kinder"],
    "property_street": ["property_data.street", "property_data.strasse", "property_data.address.Straße", "property_data.address.Strasse", "property_data.address.StraßE", "property_street"],
    "property_city": ["property_data.city", "property_data.ort", "property_data.address.Ort", "property_data.address.Stadt", "property_city"],
    "property_zip": ["property_data.zip", "property_data.plz", "property_data.address.PLZ", "property_zip"],
    "living_space": ["property_data.living_space", "property_data.living_area", "living_space", "living_area", "wohnflaeche"],
    "year_built": ["property_data.year_built", "year_built", "baujahr"],
    "zinsbindung": ["zinsbindung", "financing_data.zinsbindung"],
    "partnerId": ["partnerId", "partner_id"],
    "monthly_rental_income": ["monthly_rental_income", "rental_data.cold_rent", "tax_data.income_rental"],
}

# ============================================================
# DOKUMENT-ANFORDERUNGEN
# per_person=True → count wird bei Paaren verdoppelt
# ============================================================
DOCS_REQUIRED_ALWAYS = {
    "Selbstauskunft":       {"count": 1, "max_age_days": None, "per_person": False},
    "Ausweiskopie":         {"count": 1, "max_age_days": None, "per_person": True, "warn_expiry_days": 90},
    "Eigenkapitalnachweis": {"count": 1, "max_age_days": 30,   "per_person": True},
    "Renteninfo":           {"count": 1, "max_age_days": None, "per_person": True},
}

DOCS_REQUIRED_EMPLOYED = {
    "Gehaltsnachweis":          {"count": 3, "max_age_days": 90,   "per_person": True},
    "Kontoauszug":              {"count": 3, "max_age_days": 90,   "per_person": True},
    "Steuerbescheid":           {"count": 1, "max_age_days": None, "per_person": True},
    "Steuererklärung":          {"count": 1, "max_age_days": None, "per_person": True},
    "Lohnsteuerbescheinigung":  {"count": 1, "max_age_days": None, "per_person": True, "alternative": "Gehaltsabrechnung Dezember"},
}

DOCS_REQUIRED_SELF_EMPLOYED = {
    "BWA":                          {"count": 1, "max_age_days": None, "per_person": True},
    "Summen und Saldenliste":       {"count": 1, "max_age_days": None, "per_person": True},
    "Jahresabschluss":              {"count": 3, "max_age_days": None, "per_person": True},
    "Steuerbescheid":               {"count": 2, "max_age_days": None, "per_person": True},
    "Steuererklärung":              {"count": 2, "max_age_days": None, "per_person": True},
    "Kontoauszug":                  {"count": 3, "max_age_days": 90,   "per_person": True},
    "Nachweis Krankenversicherung": {"count": 1, "max_age_days": None, "per_person": True},
}

DOCS_REQUIRED_PROPERTY = {
    "Exposé":                     {"count": 1, "max_age_days": None},
    "Objektbild Innen":           {"count": 1, "max_age_days": None},
    "Objektbild Außen":           {"count": 1, "max_age_days": None},
    "Baubeschreibung":            {"count": 1, "max_age_days": None},
    "Grundbuch":                  {"count": 1, "max_age_days": 90},
    "Teilungserklärung":          {"count": 1, "max_age_days": None, "only_object_type": ["ETW"]},
    "Wohnflächenberechnung":      {"count": 1, "max_age_days": None},
    "Grundriss":                  {"count": 1, "max_age_days": None},
    "Energieausweis":             {"count": 1, "max_age_days": None},
    "Modernisierungsaufstellung": {"count": 1, "max_age_days": None},
}

# ============================================================
# DOC-TYPE ALIASE
# Wenn GPT einen Dokumenttyp anders benennt als erwartet,
# wird hier das Mapping definiert. Key = erwarteter Typ,
# Values = alternative Bezeichnungen die GPT verwenden koennte.
# ============================================================
DOC_TYPE_ALIASES: dict[str, list[str]] = {
    "Gehaltsnachweis": [
        "Gehaltsabrechnung", "Entgeltnachweis", "Entgeltabrechnung",
        "Lohn/Gehaltsabrechnung", "Lohnabrechnung", "Verdienstbescheinigung",
        "Bezügemitteilung", "Lohnausweis", "Brutto-Netto-Abrechnung",
    ],
    "Ausweiskopie": [
        "Reisepass", "Personalausweis", "Ausweis", "Aufenthaltstitel",
        "Identitätsdokument",
    ],
    "Renteninfo": [
        "Renteninformation", "Renteninformation 2025", "Rentenauskunft",
        "Rentenversicherung", "Deutsche Rentenversicherung",
    ],
    "Exposé": [
        "Expose", "Objektexposé", "Immobilienexposé", "Verkaufsexposé",
    ],
    "Grundbuch": [
        "Grundbuchauszug", "Grundbuchblatt", "Grundbucheintrag",
    ],
    "Grundriss": [
        "Wohnungsgrundriss", "Grundrisszeichnung", "Grundrissplan",
        "Flurkarte", "Lageplan", "Katasterkarte", "Liegenschaftskarte",
    ],
    "Eigenkapitalnachweis": [
        "Finanzstatus", "Vermögensaufstellung", "Depotauszug",
        "Sparkontoauszug", "Kontostände", "Vermögensstatus",
        "Kontoübersicht", "Bankkontenübersicht", "Depotnachweis",
        "Zinsbescheinigung",
    ],
    "Steuererklärung": [
        "Einkommensteuererklärung", "Steuererklärung (Anlage)",
    ],
    "Energieausweis": [
        "Energiepass", "Energetischer Ausweis",
        "Energieverbrauchsausweis", "Energiebedarfsausweis",
    ],
    "Baubeschreibung": [
        "Hausbeschreibung", "Objektbeschreibung",
    ],
    "Wohnflächenberechnung": [
        "Flächenberechnung", "DIN277 Berechnung",
    ],
    "Steuerbescheid": [
        "Einkommensteuerbescheid",
    ],
    "Kontoauszug": [
        "Bankkontoauszug", "Girokonto Auszug",
    ],
    "Selbstauskunft": [
        "SCHUFA-Auskunft", "Bonitätsauskunft", "Selbstauskunft SCHUFA",
    ],
    "Lohnsteuerbescheinigung": [
        "Elektronische Lohnsteuerbescheinigung",
    ],
    "Teilungserklärung": [
        "Aufteilungsplan", "Gemeinschaftsordnung",
    ],
    "Gehaltsabrechnung Dezember": [
        "Dezemberabrechnung", "Gehaltsnachweis Dezember",
        "Lohnabrechnung Dezember", "Dezember-Gehaltsabrechnung",
    ],
    "Modernisierungsaufstellung": [
        "Modernisierungsliste", "Sanierungsaufstellung",
        "Renovierungsaufstellung", "Modernisierungsnachweis",
    ],
}


def _get_nested(obj: dict, path: str):
    """Tiefensuche per Punkt-Pfad: 'property_data.purchase_price'"""
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _compute_effective_view(case: dict) -> dict:
    """
    Baut effektive Sicht: manual_overrides > answers_user > facts_extracted > derived_values
    Flacht answers_user (partner + broker) in eine Ebene.
    """
    # Statistische Defaults (häufigste Werte) – niedrigste Priorität
    defaults = {
        "object_type": "ETW",
        "usage": "Vermietet",
    }
    derived = case.get("_derived_values", {})
    facts = case.get("_facts_extracted", {})
    answers_raw = case.get("_answers_user", {})
    overrides = case.get("_manual_overrides", {})

    # answers_user: partner dann broker (broker hat Priorität)
    answers_flat = {}
    for actor in ["partner", "broker"]:
        actor_data = answers_raw.get(actor, {})
        if isinstance(actor_data, dict):
            answers_flat.update(actor_data)
    # Flat-Keys direkt (Legacy-Format)
    for k, v in answers_raw.items():
        if k not in ("partner", "broker", "_meta") and not k.startswith("_"):
            answers_flat[k] = v

    view = {}
    for src in [defaults, derived, facts, answers_flat, overrides]:
        if isinstance(src, dict):
            view.update({k: v for k, v in src.items() if v is not None and v != ""})

    # Top-level Felder aus Case uebernehmen (nicht in JSON-Blobs gespeichert)
    for top_key in ["applicant_name", "partner_email", "partner_name"]:
        if not view.get(top_key) and case.get(top_key):
            view[top_key] = case[top_key]

    # Anrede aus Vorname ableiten wenn nicht vorhanden
    if not view.get("salutation"):
        first_name = (
            view.get("applicant_first_name")
            or _get_nested(view, "applicant_data.first_name")
            or _get_nested(view, "applicant_data.vorname")
            or ""
        ).strip()
        if first_name:
            guessed = _guess_salutation(first_name)
            if guessed:
                view["salutation"] = guessed

    return view


# Häufige deutsche Vornamen für Anrede-Erkennung
_FEMALE_NAMES = {
    "anna", "andrea", "angelika", "anja", "annette", "antje", "astrid",
    "barbara", "beate", "birgit", "brigitte", "britta",
    "carmen", "carola", "caroline", "charlotte", "christa", "christiane", "christina", "claudia", "cornelia",
    "daniela", "diana", "doris", "dorothea",
    "elena", "elisa", "elisabeth", "ella", "elke", "emma", "erika", "eva",
    "franziska", "frieda", "friederike",
    "gabriele", "gerda", "gisela", "gudrun",
    "hannah", "heide", "heidi", "heike", "helga", "henriette", "hildegard",
    "ilona", "ines", "ingrid", "irene", "iris",
    "jana", "janina", "jasmin", "jennifer", "jessica", "johanna", "judith", "julia", "juliane",
    "karin", "karla", "katharina", "kathrin", "katja", "katrin", "kerstin", "kirsten", "klara", "kristin",
    "larissa", "laura", "lea", "lena", "linda", "lisa", "lotte", "louisa", "lucia", "luise",
    "manuela", "margarete", "margit", "maria", "marie", "marina", "marlene", "martina", "melanie", "michaela", "miriam", "monika",
    "nadine", "natascha", "nicole", "nina", "nora",
    "olga",
    "patricia", "paula", "petra", "pia",
    "regina", "renate", "rita", "rosa", "rosemarie", "ruth",
    "sabine", "sabrina", "sandra", "sara", "sarah", "silke", "silvia", "simone", "sofia", "sophie", "stefanie", "stephanie", "susanne", "svenja",
    "tamara", "tanja", "tatjana", "teresa", "theresa", "tina",
    "ulrike", "ursula", "uta",
    "vanessa", "vera", "veronika",
    "waltraud", "wiebke",
    "yvonne",
}

_MALE_NAMES = {
    "achim", "adam", "adrian", "albert", "alexander", "alfred", "andreas", "anton", "armin", "arno", "arthur", "axel",
    "bastian", "benjamin", "bernd", "bernhard", "björn", "boris", "bruno",
    "carl", "carsten", "christian", "christoph", "claus", "clemens",
    "daniel", "david", "dennis", "detlef", "dieter", "dietmar", "dirk", "dominik",
    "eckhard", "edgar", "edmund", "eduard", "egon", "erich", "ernst", "erwin", "eugen",
    "fabian", "felix", "ferdinand", "florian", "frank", "franz", "frederik", "friedhelm", "friedrich", "fritz",
    "georg", "gerald", "gerd", "gerhard", "gregor", "guenter", "günter", "günther", "gustav",
    "hans", "harald", "hartmut", "heinrich", "helmut", "hendrik", "henning", "henry", "herbert", "hermann", "horst", "hubert", "hugo",
    "ingo",
    "jan", "jens", "joachim", "jochen", "johann", "johannes", "jonas", "jonathan", "joerg", "jörg", "josef", "juergen", "jürgen", "julian", "julius",
    "kai", "karl", "karsten", "klaus", "konrad", "kurt",
    "lars", "leon", "leonhard", "lorenz", "lothar", "ludwig", "lukas", "lutz",
    "manfred", "marc", "marcel", "marco", "marcus", "mario", "markus", "martin", "mathias", "matthias", "max", "maximilian", "michael", "moritz",
    "nico", "nicolas", "niklas", "nikolaus", "norbert",
    "olaf", "oliver", "oskar", "otto",
    "pascal", "patrick", "paul", "peter", "philipp",
    "rainer", "ralf", "ralph", "reinhard", "reinhold", "richard", "robert", "robin", "roland", "rolf", "roman", "rudi", "rudolf", "ruediger", "rüdiger",
    "sascha", "sebastian", "siegfried", "simon", "stefan", "steffen", "stephan", "sven",
    "theo", "theodor", "thomas", "thorsten", "till", "timo", "tobias", "tom", "torsten",
    "uwe", "udo", "ulrich",
    "valentin", "viktor", "volker",
    "walter", "werner", "wilhelm", "willi", "wolfgang",
}


def _guess_salutation(first_name: str) -> Optional[str]:
    """Versucht die Anrede anhand des Vornamens zu erraten."""
    name = first_name.lower().strip()
    # Bei zusammengesetzten Vornamen den ersten Teil nehmen
    name = name.split()[0] if " " in name else name
    name = name.split("-")[0] if "-" in name else name
    if name in _FEMALE_NAMES:
        return "Frau"
    if name in _MALE_NAMES:
        return "Herr"
    # Heuristik: Namen auf -a, -e (nicht -ke, -se, -te) sind oft weiblich
    if name.endswith("a") and len(name) > 2:
        return "Frau"
    return None


def _doc_age_ok(doc: dict, max_age_days: Optional[int]) -> bool:
    """Prüft ob Dokument nicht zu alt ist"""
    if not max_age_days:
        return True
    analyzed = doc.get("analyzed_at") or (doc.get("meta") or {}).get("doc_date")
    if not analyzed:
        return True  # Kein Datum → nicht prüfbar → akzeptieren
    try:
        dt = datetime.fromisoformat(analyzed.replace("Z", "+00:00"))
        return (datetime.now(dt.tzinfo) - dt).days <= max_age_days
    except Exception:
        return True


def _doc_expiry_warn(doc: dict, warn_days: Optional[int]) -> bool:
    """Prüft ob Dokument bald abläuft (z.B. Ausweis)"""
    if not warn_days:
        return False
    extracted = doc.get("extracted") or {}
    expiry_str = extracted.get("Gültig bis") or extracted.get("expiry_date")
    if not expiry_str:
        return False
    try:
        # Deutsches Format: DD.MM.YYYY
        if re.match(r"\d{2}\.\d{2}\.\d{4}", expiry_str):
            parts = expiry_str.split(".")
            expiry = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
        else:
            expiry = datetime.fromisoformat(expiry_str)
        return (expiry - datetime.now()).days <= warn_days
    except Exception:
        return False


def _count_docs_with_aliases(docs_index: dict, doc_type: str) -> list:
    """Sammelt alle Dokumente fuer einen Typ inkl. aller Aliase."""
    docs = list(docs_index.get(doc_type, []))
    for alias in DOC_TYPE_ALIASES.get(doc_type, []):
        docs.extend(docs_index.get(alias, []))
    return docs


def _find_value(view: dict, key: str):
    """Sucht einen Wert über alle konfigurierten Suchpfade. Ignoriert dict/list-Werte."""
    paths = KEY_SEARCH_PATHS.get(key, [key])
    for path in paths:
        value = _get_nested(view, path)
        if value is not None and value != "" and not isinstance(value, (dict, list)):
            return value
    return None


def check_readiness(case_id: str) -> dict:
    """
    Vollständige Readiness-Prüfung für einen Case.
    EINZIGE autoritative Implementierung.

    Rückgabe:
    {
        status: str,
        missing_financing: list,
        missing_applicant_data: list,
        missing_docs: list,
        stale_docs: list,
        warnings: list,
        recommended_missing: list,
        manual_overrides_applied: list,
        effective_view: dict,
    }
    """
    case = cases.load_case(case_id)
    if not case:
        raise ValueError(f"Case nicht gefunden: {case_id}")

    view = _compute_effective_view(case)
    overrides = case.get("_manual_overrides", {})
    docs_index = cases.build_docs_index(case_id)

    # Ergebnisse
    missing_financing = []
    missing_applicant_data = []
    missing_docs = []
    stale_docs = []
    warnings = []
    recommended_missing = []
    overrides_applied = []
    total_doc_checks = 0

    # ──────────────────────────────────────────
    # 1. Pflichtfelder Finanzierung
    # ──────────────────────────────────────────
    for key in REQUIRED_FINANCING_KEYS:
        if _find_value(view, key) is None:
            missing_financing.append(key)

    # ──────────────────────────────────────────
    # 2. Pflichtfelder Antragsteller + Adresse
    # ──────────────────────────────────────────
    for key in REQUIRED_APPLICANT_KEYS + REQUIRED_ADDRESS_KEYS:
        if _find_value(view, key) is None:
            missing_applicant_data.append(key)

    # ──────────────────────────────────────────
    # 3. Employment-Typ bestimmen (für Selbstständige-Checks + Doku)
    # ──────────────────────────────────────────
    employment = str(
        _find_value(view, "employment_type")
        or view.get("employment_status")
        or "Angestellter"
    )
    is_self_employed = "selbst" in employment.lower() or "freiberuf" in employment.lower()

    # Selbstständige: Zusätzliche Pflichtfelder
    if is_self_employed:
        for key in REQUIRED_SELF_EMPLOYED_KEYS:
            if _find_value(view, key) is None:
                missing_applicant_data.append(key)

    # ──────────────────────────────────────────
    # 5. Empfohlene Felder (nicht blockierend → Warnungen)
    # ──────────────────────────────────────────
    for key in RECOMMENDED_KEYS:
        if _find_value(view, key) is None:
            recommended_missing.append(key)

    # ──────────────────────────────────────────
    # 6. Dokument-Checks
    # ──────────────────────────────────────────
    is_couple = bool(
        view.get("is_couple")
        or _get_nested(view, "applicant_data_2.vorname")
        or _get_nested(view, "applicant_data_2.nachname")
    )
    has_joint_account = bool(overrides.get("has_joint_account") or view.get("has_joint_account"))

    # Objekttyp für bedingte Dokumente (Teilungserklärung nur bei ETW)
    object_type = str(
        view.get("object_type")
        or _get_nested(view, "property_data.object_type")
        or ""
    ).upper().strip()

    person_count = 2 if is_couple else 1

    def check_doc(doc_type: str, req: dict):
        nonlocal total_doc_checks

        # Bedingt: nur bei bestimmtem Objekttyp erforderlich
        only_obj_types = req.get("only_object_type")
        if only_obj_types:
            if object_type not in [t.upper() for t in only_obj_types]:
                return  # Nicht erforderlich für diesen Objekttyp

        total_doc_checks += 1

        # Alle Dokumente inkl. Aliase sammeln
        docs = _count_docs_with_aliases(docs_index, doc_type)

        alternative = req.get("alternative")

        # Alternative prüfen wenn Haupttyp leer
        if not docs and alternative:
            alt_docs = _count_docs_with_aliases(docs_index, alternative)
            if alt_docs:
                docs = alt_docs

        # Override: Accept missing?
        if overrides.get(f"accept_missing_{doc_type.lower().replace(' ', '_')}"):
            overrides_applied.append(f"accept_missing:{doc_type}")
            return

        # Required count berechnen: bei per_person=True verdoppeln fuer Paare
        base_count = req.get("count", 1)
        is_per_person = req.get("per_person", False)

        if is_per_person and is_couple:
            # Sonderfall Kontoauszug bei Gemeinschaftskonto
            if doc_type == "Kontoauszug" and has_joint_account:
                required_count = base_count  # Nicht verdoppeln
            else:
                required_count = base_count * person_count
        else:
            required_count = base_count

        max_age = req.get("max_age_days")
        warn_days = req.get("warn_expiry_days")

        # Override: Accept stale?
        accept_stale = bool(overrides.get(f"accept_stale_{doc_type.lower().replace(' ', '_')}"))

        fresh_docs = [d for d in docs if accept_stale or _doc_age_ok(d, max_age)]

        # Kontoauszug + Gehaltsnachweis: months_covered zählen statt Datei-Anzahl
        # Ein PDF kann mehrere Monate abdecken (z.B. 3 Gehaltsabrechnungen in einer PDF)
        if doc_type in ("Kontoauszug", "Gehaltsnachweis"):
            effective_count = 0
            for doc in fresh_docs:
                ext = doc.get("extracted") or {}
                months = ext.get("months_covered")
                if months and isinstance(months, (int, float)) and months > 0:
                    effective_count += int(months)
                else:
                    effective_count += 1
            effective_count_all = 0
            for doc in docs:
                ext = doc.get("extracted") or {}
                months = ext.get("months_covered")
                if months and isinstance(months, (int, float)) and months > 0:
                    effective_count_all += int(months)
                else:
                    effective_count_all += 1
        else:
            effective_count = len(fresh_docs)
            effective_count_all = len(docs)

        if effective_count < required_count:
            if effective_count_all >= required_count and not accept_stale:
                stale_docs.append({
                    "type": doc_type,
                    "required": required_count,
                    "found": effective_count_all,
                    "fresh": effective_count,
                })
            else:
                missing_docs.append({
                    "type": doc_type,
                    "required": required_count,
                    "found": effective_count,
                })
        else:
            # Ablaufdatum-Warnung
            if warn_days:
                for doc in fresh_docs:
                    if _doc_expiry_warn(doc, warn_days):
                        warnings.append(f"{doc_type} läuft bald ab")

    # Immer erforderlich (KEIN Loop ueber person_count - stattdessen per_person flag)
    for doc_type, req in DOCS_REQUIRED_ALWAYS.items():
        check_doc(doc_type, req)

    # Angestellte oder Selbstständige
    if is_self_employed:
        for doc_type, req in DOCS_REQUIRED_SELF_EMPLOYED.items():
            check_doc(doc_type, req)
    else:
        for doc_type, req in DOCS_REQUIRED_EMPLOYED.items():
            check_doc(doc_type, req)

    # Objektdokumente (nie per_person)
    for doc_type, req in DOCS_REQUIRED_PROPERTY.items():
        check_doc(doc_type, req)

    # ──────────────────────────────────────────
    # 6b. Kontoauszüge als Eigenkapitalnachweis werten
    # ──────────────────────────────────────────
    ek_missing = any(d["type"] == "Eigenkapitalnachweis" for d in missing_docs)
    if ek_missing:
        kontoauszug_docs = _count_docs_with_aliases(docs_index, "Kontoauszug")
        # Effektive Monate zählen: ein Dokument kann mehrere Monate abdecken
        effective_months = 0
        for doc in kontoauszug_docs:
            ext = doc.get("extracted") or {}
            months = ext.get("months_covered")
            if months and isinstance(months, (int, float)) and months > 0:
                effective_months += int(months)
            else:
                effective_months += 1  # Default: 1 Monat pro Dokument
        if effective_months >= 3:
            missing_docs = [d for d in missing_docs if d["type"] != "Eigenkapitalnachweis"]
            logger.info(f"[{case_id}] Eigenkapitalnachweis durch {len(kontoauszug_docs)} Kontoauszüge ({effective_months} Monate) abgedeckt")

    # ──────────────────────────────────────────
    # 6c. Mieteinnahmen-Hinweis
    # ──────────────────────────────────────────
    rental_income_1 = _get_nested(view, "tax_data.income_rental")
    rental_income_2 = _get_nested(view, "tax_data_2.income_rental")
    try:
        has_rental_income = (
            (rental_income_1 and float(rental_income_1) > 0)
            or (rental_income_2 and float(rental_income_2) > 0)
        )
    except (ValueError, TypeError):
        has_rental_income = False
    if has_rental_income and not _find_value(view, "monthly_rental_income"):
        warnings.append(
            "Mieteinnahmen im Steuerbescheid erkannt – bitte monatliche Mieteinnahmen eintragen"
        )

    # ──────────────────────────────────────────
    # 7. Manual Overrides prüfen
    # ──────────────────────────────────────────
    approve_import = bool(overrides.get("APPROVE_IMPORT"))
    wait_for_docs = bool(overrides.get("WAIT_FOR_DOCS"))

    # ──────────────────────────────────────────
    # 8. Status bestimmen
    # Priorität: APPROVE_IMPORT > WAIT_FOR_DOCS > sonstige Blocker
    # ──────────────────────────────────────────
    if approve_import:
        status = "READY_FOR_IMPORT"

    elif wait_for_docs:
        status = "WAITING_FOR_DOCUMENTS"

    elif stale_docs and not any(overrides.get(f"accept_stale_{d['type'].lower().replace(' ', '_')}") for d in stale_docs):
        status = "NEEDS_MANUAL_REVIEW_BROKER"

    elif missing_financing or missing_applicant_data:
        status = "NEEDS_QUESTIONS_PARTNER"

    elif missing_docs:
        status = "NEEDS_QUESTIONS_PARTNER"

    else:
        status = "AWAITING_BROKER_CONFIRMATION"

    # Tatsächlich vollständig = keine offenen Punkte
    actually_complete = (
        not missing_financing
        and not missing_applicant_data
        and not missing_docs
        and not stale_docs
    )
    is_complete = status in ("READY_FOR_IMPORT", "AWAITING_BROKER_CONFIRMATION")

    # Completeness Prozent (fuer Dashboard)
    total_checks = (
        len(REQUIRED_FINANCING_KEYS)
        + len(REQUIRED_APPLICANT_KEYS)
        + len(REQUIRED_ADDRESS_KEYS)
        + total_doc_checks
    )
    if is_self_employed:
        total_checks += len(REQUIRED_SELF_EMPLOYED_KEYS)
    failed_checks = len(missing_financing) + len(missing_applicant_data) + len(missing_docs) + len(stale_docs)
    passed_checks = total_checks - failed_checks
    completeness_percent = max(0, min(100, round((passed_checks / max(total_checks, 1)) * 100)))

    result = {
        "status": status,
        "missing_financing": missing_financing,
        "missing_applicant_data": missing_applicant_data,
        "missing_docs": missing_docs,
        "stale_docs": stale_docs,
        "warnings": warnings,
        "recommended_missing": recommended_missing,
        "manual_overrides_applied": overrides_applied,
        "effective_view": view,
        "approve_import": approve_import,
        "is_complete": is_complete,
        "actually_complete": actually_complete,
        "forced_approval": approve_import and not actually_complete,
        "completeness_percent": completeness_percent,
        "is_couple": is_couple,
        "employment_type": employment,
    }

    # In DB speichern (cached case durchreichen → spart 1 DB call)
    cases.update_status(case_id, status, result, _cached_case=case)
    logger.info(
        f"Readiness check for {case_id}: {status} | "
        f"missing_fin={missing_financing} | missing_applicant={missing_applicant_data} | "
        f"missing_docs={len(missing_docs)} | is_couple={is_couple}"
    )

    return result
