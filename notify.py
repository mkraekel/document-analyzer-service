"""
Notification Sender
Versendet E-Mails per SMTP basierend auf Case-Status.
Portiert aus Readiness Router (n8n SMTP Nodes + GPT-4o Question Generator).

DRY-RUN MODUS:
  EMAIL_DRY_RUN=true  → Kein echter Versand, alle E-Mails landen in der
                         SeaTable-Tabelle "email_test_log" (sichtbar im Browser).
"""

import os
import logging
import smtplib
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
logger = logging.getLogger(__name__)

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)

BROKER_EMAIL = os.getenv("BROKER_EMAIL", "backoffice@alexander-heil.com")

# Interne Domains: E-Mails an diese Adressen werden NICHT als Partner-Rueckfragen verschickt
INTERNAL_DOMAINS = {"alexander-heil.com"}

# Dry-Run: EMAIL_DRY_RUN=true → kein echter Versand, Eintrag in SeaTable
EMAIL_DRY_RUN = os.getenv("EMAIL_DRY_RUN", "false").lower() in ("true", "1", "yes")

# Notification Cooldown: verhindert doppelten Versand derselben Notification
# Key = (case_id, status), Value = timestamp des letzten Versands
NOTIFICATION_COOLDOWN_SECONDS = int(os.getenv("NOTIFICATION_COOLDOWN_SECONDS", "600"))  # 10 min
_notification_cooldown: dict[tuple[str, str], float] = {}

def _log_to_seatable(to: str, subject: str, html_body: str, text_body: str = None):
    """Schreibt E-Mail in SeaTable-Tabelle 'email_test_log' statt zu senden."""
    try:
        import seatable as db
        db.create_row("email_test_log", {
            "to": to,
            "subject": subject,
            "body_text": (text_body or "")[:2000],
            "body_html": html_body[:5000],
            "logged_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "dry_run": True,
        })
        logger.info(f"[DRY-RUN] E-Mail nach SeaTable email_test_log: To={to} | {subject}")
    except Exception as e:
        # Fallback auf Log-Datei wenn SeaTable nicht erreichbar
        logger.warning(f"[DRY-RUN] SeaTable-Log fehlgeschlagen ({e}), schreibe in dry_run_emails.log")
        _log_to_file(to, subject, html_body, text_body)


def _log_to_file(to: str, subject: str, html_body: str, text_body: str = None):
    """Fallback: schreibt E-Mail in lokale Datei dry_run_emails.log."""
    try:
        log_path = os.path.join(os.path.dirname(__file__), "dry_run_emails.log")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"[{datetime.utcnow().isoformat()}] DRY-RUN E-MAIL\n")
            f.write(f"To:      {to}\n")
            f.write(f"Subject: {subject}\n")
            f.write(f"Body:\n{text_body or html_body}\n")
    except Exception as e:
        logger.error(f"[DRY-RUN] Log-Datei konnte nicht geschrieben werden: {e}")


def _send_email(to: str, subject: str, html_body: str, text_body: str = None):
    """E-Mail versenden – oder im Dry-Run-Modus nach SeaTable loggen."""

    # Dry-Run: kein echter Versand
    if EMAIL_DRY_RUN:
        _log_to_seatable(to, subject, html_body, text_body)
        return

    # SMTP nicht konfiguriert
    if not SMTP_HOST or not SMTP_USER:
        logger.warning(f"SMTP nicht konfiguriert – E-Mail an {to} nicht gesendet")
        logger.info(f"E-Mail Inhalt:\nTo: {to}\nSubject: {subject}\n{text_body or html_body}")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to

    if text_body:
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, to, msg.as_string())
        logger.info(f"E-Mail gesendet an {to}: {subject}")
    except Exception as e:
        logger.error(f"SMTP Fehler beim Senden an {to}: {e}")
        raise


def _get_partner_first_name(effective_view: dict) -> str:
    """Extrahiert den Vornamen des Vertriebspartners."""
    full = effective_view.get("partner_name", "")
    if full:
        return full.strip().split()[0]
    return ""


# Labels fuer technische Keys
_DATA_LABELS = {
    "purchase_price": "Kaufpreis",
    "loan_amount": "gewünschte Darlehenssumme",
    "equity_to_use": "einzusetzendes Eigenkapital",
    "object_type": "Objektart (z.B. ETW, Haus)",
    "usage": "Nutzungsart (Eigennutzung/Kapitalanlage)",
    "employment_type": "Beschäftigungsart",
    "applicant_first_name": "Vorname des Antragstellers",
    "applicant_last_name": "Nachname des Antragstellers",
    "applicant_birth_date": "Geburtsdatum",
    "net_income": "Nettoeinkommen (monatlich)",
    "address_street": "Straße (Wohnadresse)",
    "address_house_number": "Hausnummer",
    "address_zip": "PLZ (Wohnadresse)",
    "address_city": "Wohnort",
    "self_employed_since": "Selbstständig seit (Datum)",
    "profit_last_year": "Gewinn Vorjahr",
    "marital_status": "Familienstand",
    "children": "Anzahl Kinder",
    "property_street": "Objektadresse (Straße)",
    "property_zip": "Objektadresse (PLZ)",
    "property_city": "Objektadresse (Ort)",
}


def _build_partner_email_body(
    effective_view: dict,
    missing_financing: list,
    missing_applicant_data: list,
    missing_docs: list,
    stale_docs: list,
) -> str:
    """Baut eine strukturierte E-Mail an den Vertriebspartner."""
    partner_first = _get_partner_first_name(effective_view)
    applicant_name = effective_view.get("applicant_name", "")

    greeting = f"Hallo {partner_first}," if partner_first else "Hallo,"
    intro = f"für die Finanzierungsanfrage von {applicant_name}" if applicant_name else "für die vorliegende Finanzierungsanfrage"

    items = []

    # Fehlende Daten
    all_missing_data = list(missing_financing or []) + list(missing_applicant_data or [])
    if all_missing_data:
        items.append("Folgende Angaben:")
        for key in all_missing_data:
            items.append(f"  - {_DATA_LABELS.get(key, key)}")

    # Fehlende Dokumente
    if missing_docs:
        items.append("Folgende Dokumente:")
        for d in missing_docs:
            count_info = f" ({d['required']}x benötigt)" if d.get("required", 1) > 1 else ""
            items.append(f"  - {d['type']}{count_info}")

    # Veraltete Dokumente
    if stale_docs:
        items.append("Folgende Dokumente in aktueller Fassung:")
        for d in stale_docs:
            items.append(f"  - {d['type']} (vorliegende Version ist leider zu alt)")

    if not items:
        return ""

    body = f"""{greeting}

{intro} bräuchte ich noch folgende Informationen, um die Anfrage bei der Bank einreichen zu können:

{chr(10).join(items)}

Könnten Sie mir diese Unterlagen/Informationen zukommen lassen?"""

    return body


def _build_broker_email_body(
    effective_view: dict,
    missing_financing: list,
    missing_applicant_data: list,
    missing_docs: list,
    stale_docs: list,
) -> str:
    """Baut eine interne Info-E-Mail an das Backoffice."""
    applicant_name = effective_view.get("applicant_name", "")
    display = applicant_name or "Unbekannter Antragsteller"

    items = []
    all_missing_data = list(missing_financing or []) + list(missing_applicant_data or [])
    if all_missing_data:
        for key in all_missing_data:
            items.append(f"- {_DATA_LABELS.get(key, key)}")
    if missing_docs:
        for d in missing_docs:
            items.append(f"- {d['type']} (fehlt, {d['required']}x benötigt)")
    if stale_docs:
        for d in stale_docs:
            items.append(f"- {d['type']} (veraltet)")

    if not items:
        return ""

    return f"""Hallo,

bei {display} fehlen noch:

{chr(10).join(items)}"""


def _is_internal_email(email: str) -> bool:
    """Prueft ob eine E-Mail-Adresse zu einer internen Domain gehoert."""
    if not email or "@" not in email:
        return True
    domain = email.rsplit("@", 1)[1].lower()
    return domain in INTERNAL_DOMAINS


def send_partner_questions(case_id: str, partner_email: str, readiness_result: dict, effective_view: dict):
    """Sendet Rückfrage-E-Mail an Partner"""
    if _is_internal_email(partner_email):
        logger.warning(f"[{case_id}] Partner-E-Mail ist intern ({partner_email}) – übersprungen")
        return

    applicant_name = effective_view.get("applicant_name", "")
    body = _build_partner_email_body(
        effective_view=effective_view,
        missing_financing=readiness_result.get("missing_financing", []),
        missing_applicant_data=readiness_result.get("missing_applicant_data", []),
        missing_docs=readiness_result.get("missing_docs", []),
        stale_docs=readiness_result.get("stale_docs", []),
    )

    if not body:
        return

    html_body = f"""<html><body>
<p>{body.replace(chr(10), '<br>')}</p>
<br>
<p>Mit freundlichen Grüßen<br>Alexander Heil Finanzierung</p>
</body></html>"""

    subject_name = applicant_name or "Ihre Anfrage"
    _send_email(
        to=partner_email,
        subject=f"Fehlende Unterlagen – Finanzierungsanfrage {subject_name}",
        html_body=html_body,
        text_body=body + "\n\nMit freundlichen Grüßen\nAlexander Heil Finanzierung",
    )


def send_broker_confirmation(case_id: str, effective_view: dict):
    """Sendet Freigabe-Anfrage an Broker wenn alles vollständig"""
    name = effective_view.get("applicant_name", "")
    display_name = name or case_id
    price = effective_view.get("purchase_price") or _get_nested(effective_view, "property_data.purchase_price") or "k.A."
    loan = effective_view.get("loan_amount") or _get_nested(effective_view, "financing_data.loan_amount") or "k.A."
    equity = effective_view.get("equity_to_use") or _get_nested(effective_view, "financing_data.equity_to_use") or "k.A."

    html_body = f"""<html><body>
<h3>Finanzierungsanfrage bereit: {display_name}</h3>
<p>Antragsteller: <strong>{name}</strong></p>
<ul>
  <li>Kaufpreis: {price} €</li>
  <li>Darlehensbetrag: {loan} €</li>
  <li>Eigenkapital: {equity} €</li>
</ul>
<p><strong>Bitte antworten Sie mit "FREIGABE" oder "GENEHMIGT" um den Import zu starten.</strong></p>
</body></html>"""

    _send_email(
        to=BROKER_EMAIL,
        subject=f"[FREIGABE] {display_name} - Bereit fuer Import",
        html_body=html_body,
    )


def send_manual_review(case_id: str, readiness_result: dict, effective_view: dict):
    """Sendet Review-Benachrichtigung bei veralteten Dokumenten"""
    applicant_name = effective_view.get("applicant_name", "")
    display_name = applicant_name or case_id
    stale = readiness_result.get("stale_docs", [])
    stale_list = "\n".join(f"- {d.get('type', d.get('doc_type', ''))}" for d in stale)

    stale_commands = "\n".join(
        f"<li><code>ACCEPT_STALE {d.get('type', '')}</code> – veraltetes Dokument akzeptieren</li>"
        for d in stale
    )

    html_body = f"""<html><body>
<h3>Manuelle Pruefung erforderlich: {display_name}</h3>
<p>Folgende Dokumente sind veraltet oder abgelaufen:</p>
<ul>{"".join(f"<li>{d.get('type', d.get('doc_type', ''))} (vorhanden: {d.get('found',0)}x, benötigt: {d.get('required',1)}x frisch)</li>" for d in stale)}</ul>
<p>Bitte antworten Sie auf diese E-Mail mit einem oder mehreren der folgenden Kommandos:</p>
<ul>
{stale_commands}
<li><code>ACCEPT_MISSING [Dokumenttyp]</code> – fehlendes Dokument überspringen</li>
<li><code>WAIT_FOR_DOCS</code> – auf neue Dokumente warten (kein Import)</li>
<li><code>FREIGABE</code> – alle Checks überspringen und direkt importieren</li>
</ul>
<p><small>Mehrere Kommandos können in einer Antwort stehen.</small></p>
</body></html>"""

    _send_email(
        to=BROKER_EMAIL,
        subject=f"[REVIEW] Veraltete Dokumente - {display_name}",
        html_body=html_body,
    )


def _get_nested(obj: dict, path: str):
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _check_cooldown(case_id: str, status: str) -> bool:
    """
    Prüft ob für (case_id, status) kürzlich schon eine Notification gesendet wurde.
    Gibt True zurück wenn die Notification gesendet werden darf (kein Cooldown aktiv).
    Räumt nebenbei abgelaufene Einträge auf.
    """
    now = time.time()
    key = (case_id, status)

    # Abgelaufene Einträge aufräumen (max alle 100 Einträge)
    if len(_notification_cooldown) > 100:
        expired = [k for k, ts in _notification_cooldown.items() if now - ts > NOTIFICATION_COOLDOWN_SECONDS]
        for k in expired:
            del _notification_cooldown[k]

    last_sent = _notification_cooldown.get(key)
    if last_sent and (now - last_sent) < NOTIFICATION_COOLDOWN_SECONDS:
        elapsed = int(now - last_sent)
        logger.info(f"Notification cooldown aktiv für {case_id}/{status} "
                     f"(vor {elapsed}s gesendet, Cooldown {NOTIFICATION_COOLDOWN_SECONDS}s)")
        return False

    return True


def _record_cooldown(case_id: str, status: str):
    """Speichert den Zeitpunkt des Versands für Cooldown-Tracking."""
    _notification_cooldown[(case_id, status)] = time.time()


def send_reminder(case_id: str, readiness_result: dict, reminder_count: int, target: str = "partner"):
    """
    Sendet eine Erinnerungs-Benachrichtigung (gleicher Inhalt wie Original,
    aber mit 'Erinnerung' Prefix im Betreff).

    target: "partner" oder "broker"
    """
    import case_logic as cases

    view = readiness_result.get("effective_view", {})
    case = cases.load_case(case_id)
    partner_email = case.get("partner_email", "") if case else ""

    ordinal = f"{reminder_count}." if reminder_count > 1 else ""
    prefix = f"[{ordinal} Erinnerung] " if ordinal else "[Erinnerung] "

    if target == "partner" and partner_email:
        if _is_internal_email(partner_email):
            logger.warning(f"Reminder fuer {case_id}: partner_email ist intern ({partner_email}), ueberspringe")
            return

        applicant_name = view.get("applicant_name", "")
        body = _build_partner_email_body(
            effective_view=view,
            missing_financing=readiness_result.get("missing_financing", []),
            missing_applicant_data=readiness_result.get("missing_applicant_data", []),
            missing_docs=readiness_result.get("missing_docs", []),
            stale_docs=readiness_result.get("stale_docs", []),
        )
        if not body:
            logger.info(f"Reminder für {case_id}: kein Body generiert, überspringe")
            return

        reminder_note = (
            f"\n\nDies ist eine freundliche Erinnerung (Nr. {reminder_count}). "
            f"Wir haben noch keine Rückmeldung zu unserer vorherigen Anfrage erhalten."
        )
        body_with_note = body + reminder_note

        html_body = f"""<html><body>
<p>{body.replace(chr(10), '<br>')}</p>
<br>
<p style="color: #666; font-size: 0.9em;">
<em>Dies ist eine freundliche Erinnerung (Nr. {reminder_count}).
Wir haben noch keine Rückmeldung zu unserer vorherigen Anfrage erhalten.</em></p>
<br>
<p>Mit freundlichen Grüßen<br>Alexander Heil Finanzierung</p>
</body></html>"""

        subject_name = applicant_name or "Ihre Anfrage"
        _send_email(
            to=partner_email,
            subject=f"{prefix}Fehlende Unterlagen – Finanzierungsanfrage {subject_name}",
            html_body=html_body,
            text_body=body_with_note + "\n\nMit freundlichen Grüßen\nAlexander Heil Finanzierung",
        )
        logger.info(f"Reminder #{reminder_count} gesendet an Partner {partner_email} für Case {case_id}")

    else:
        logger.warning(f"Reminder für {case_id}: target={target}, partner_email={partner_email} – übersprungen")


def dispatch_notifications(case_id: str, readiness_result: dict, force: bool = False, dry_run_override: bool = False):
    """
    Sendet die richtige Benachrichtigung basierend auf Status.
    Wird nach jedem Readiness Check aufgerufen.

    force=True überspringt den Cooldown (z.B. bei manuellem Recheck aus Dashboard).
    dry_run_override=True erzwingt eine Partner-Mail unabhängig vom Status (für Test-Mails).
    """
    status = readiness_result.get("status")
    view = readiness_result.get("effective_view", {})

    # Cooldown Check: verhindert doppelten Versand bei schnell aufeinanderfolgenden Mails
    if not force and not dry_run_override and not _check_cooldown(case_id, status):
        return

    import case_logic as cases
    case = cases.load_case(case_id)
    partner_email = case.get("partner_email", "") if case else ""

    if dry_run_override:
        # Test-Mail: Partner-Mail generieren unabhängig vom Status
        # Verwende test@example.com um _is_internal_email Check zu umgehen
        test_to = partner_email if partner_email and not _is_internal_email(partner_email) else "test@example.com"
        send_partner_questions(case_id, test_to, readiness_result, view)
        return

    if status == "NEEDS_QUESTIONS_PARTNER":
        send_partner_questions(case_id, partner_email, readiness_result, view)
        _record_cooldown(case_id, status)

    elif status == "NEEDS_MANUAL_REVIEW_BROKER":
        send_manual_review(case_id, readiness_result, view)
        _record_cooldown(case_id, status)

    elif status == "AWAITING_BROKER_CONFIRMATION":
        send_broker_confirmation(case_id, view)
        _record_cooldown(case_id, status)

    elif status == "READY_FOR_IMPORT":
        logger.info(f"Case {case_id} ready for import – warte auf manuelle Freigabe im Dashboard (Import-Button)")

    else:
        logger.info(f"Status {status} – keine Benachrichtigung nötig")
