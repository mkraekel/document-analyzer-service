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
from openai import OpenAI

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

_openai = None


def _get_openai() -> OpenAI:
    global _openai
    if not _openai:
        _openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai


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


def _generate_questions_with_ai(
    case_id: str,
    missing_financing: list,
    missing_docs: list,
    stale_docs: list,
    effective_view: dict,
    recipient: str,  # "partner" oder "broker"
    language: str = "de",
) -> str:
    """Generiert natürlichsprachliche Fragen per GPT-4o-mini"""

    applicant_name = effective_view.get("applicant_name") or effective_view.get("applicant_first_name", "")
    case_summary = []

    # Fehlende Finanzierungsdaten: technische Keys in deutsche Labels uebersetzen
    FINANCING_LABELS = {
        "purchase_price": "Kaufpreis",
        "loan_amount": "Darlehenssumme",
        "equity_to_use": "einzusetzendes Eigenkapital",
        "object_type": "Objektart (z.B. ETW, Haus)",
        "usage": "Nutzungsart (Eigennutzung/Kapitalanlage)",
        "employment_type": "Beschäftigungsart",
    }

    if missing_financing:
        translated = [FINANCING_LABELS.get(f, f) for f in missing_financing]
        case_summary.append(f"Fehlende Finanzierungsdaten: {', '.join(translated)}")
    if missing_docs:
        doc_list = ", ".join(f"{d['type']} ({d['required']}x benötigt, {d['found']}x vorhanden)" for d in missing_docs)
        case_summary.append(f"Fehlende Dokumente: {doc_list}")
    if stale_docs:
        stale_list = ", ".join(f"{d['type']} (zu alt)" for d in stale_docs)
        case_summary.append(f"Veraltete Dokumente: {stale_list}")

    if not case_summary:
        return ""

    if recipient == "partner":
        system = """Du bist ein freundlicher Assistent für Baufinanzierungsanfragen bei Alexander Heil Consulting.
Schreibe eine höfliche E-Mail an den Vertriebspartner um fehlende Informationen/Dokumente zu erfragen.
WICHTIG:
- Verwende KEINE Case-IDs oder Referenznummern
- Nenne den Antragsteller beim Namen
- Sei konkret aber nicht technisch
- Deutsch, professionell, freundlich
- KEINE Signatur, KEIN "Mit freundlichen Grüßen", KEIN Name am Ende (wird automatisch ergaenzt)
- Schreibe NUR den E-Mail-Body, NICHTS anderes"""
        prompt = f"""Antragsteller: {applicant_name}

Bitte erfrage folgende fehlenden Informationen/Dokumente:
{chr(10).join(case_summary)}

Schreibe eine komplette E-Mail (nur Body, ohne Betreff, ohne Signatur). Beginne mit einer freundlichen Begrüßung.
Beziehe dich auf den Antragsteller "{applicant_name}" beim Namen, NICHT auf eine Case-ID.
WICHTIG: Beende den Text VOR der Grußformel. Schreibe NICHT "Mit freundlichen Grüßen" oder einen Namen/Position."""
    else:
        system = """Du bist ein internes Backoffice-System für Baufinanzierung bei Alexander Heil Consulting.
Schreibe eine kurze, sachliche interne Info-E-Mail an das Backoffice-Team.
WICHTIG:
- Verwende KEINE Case-IDs oder Referenznummern im Text
- Nenne den Antragsteller beim Namen
- Liste fehlende Punkte als Aufzählung auf
- KEINE Signatur, KEIN "[Dein Name]", KEINE Platzhalter
- KEINE Handlungsanweisungen wie "Bitte kümmern Sie sich" – einfach nur auflisten was fehlt
- Kurz und sachlich, max 10 Zeilen"""
        prompt = f"""Antragsteller: {applicant_name}

Folgende Punkte sind noch offen:
{chr(10).join(case_summary)}

Schreibe eine kurze interne Info (nur Body, ohne Betreff, ohne Signatur).
Beginne mit "Hallo," und liste danach nur auf was fehlt.
WICHTIG: KEINE Signatur, KEIN Name, KEINE Platzhalter wie [Dein Name] am Ende."""

    try:
        resp = _get_openai().chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            max_tokens=800,
        )
        return resp.choices[0].message.content or ""
    except Exception as e:
        logger.error(f"Question generation failed: {e}")
        # Fallback: manuelle Liste
        return f"Folgende Informationen werden benötigt:\n" + "\n".join(f"- {s}" for s in case_summary)


def _is_internal_email(email: str) -> bool:
    """Prueft ob eine E-Mail-Adresse zu einer internen Domain gehoert."""
    if not email or "@" not in email:
        return True
    domain = email.rsplit("@", 1)[1].lower()
    return domain in INTERNAL_DOMAINS


def send_partner_questions(case_id: str, partner_email: str, readiness_result: dict, effective_view: dict):
    """Sendet Rückfrage-E-Mail an Partner"""
    if _is_internal_email(partner_email):
        logger.warning(f"[{case_id}] Partner-E-Mail ist intern ({partner_email}) – "
                       f"keine Rueckfrage an Partner, leite an Broker weiter")
        send_broker_questions(case_id, readiness_result, effective_view)
        return

    applicant_name = effective_view.get("applicant_name", "")
    body = _generate_questions_with_ai(
        case_id=case_id,
        missing_financing=readiness_result.get("missing_financing", []),
        missing_docs=readiness_result.get("missing_docs", []),
        stale_docs=readiness_result.get("stale_docs", []),
        effective_view=effective_view,
        recipient="partner",
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
        text_body=body,
    )


def send_broker_questions(case_id: str, readiness_result: dict, effective_view: dict):
    """Sendet interne Info an Broker/Backoffice über fehlende Unterlagen"""
    applicant_name = effective_view.get("applicant_name", "")

    # Strukturierte Liste statt GPT-Freitext für Broker
    missing_fin = readiness_result.get("missing_financing", [])
    missing_docs = readiness_result.get("missing_docs", [])
    stale_docs = readiness_result.get("stale_docs", [])

    FINANCING_LABELS = {
        "purchase_price": "Kaufpreis",
        "loan_amount": "Darlehenssumme",
        "equity_to_use": "Eigenkapital",
        "object_type": "Objektart",
        "usage": "Nutzungsart",
    }

    display_name = applicant_name or "Unbekannt"

    lines_html = []
    if missing_fin:
        labels = [FINANCING_LABELS.get(f, f) for f in missing_fin]
        lines_html.append("<strong>Fehlende Finanzierungsdaten:</strong><ul>")
        lines_html.extend(f"<li>{l}</li>" for l in labels)
        lines_html.append("</ul>")

    if missing_docs:
        lines_html.append("<strong>Fehlende Dokumente:</strong><ul>")
        for d in missing_docs:
            if isinstance(d, dict):
                dtype = d.get("type", "?")
                req = d.get("required", 1)
                found = d.get("found", 0)
                lines_html.append(f"<li>{dtype} ({found}/{req})</li>")
            else:
                lines_html.append(f"<li>{d}</li>")
        lines_html.append("</ul>")

    if stale_docs:
        lines_html.append("<strong>Veraltete Dokumente:</strong><ul>")
        for d in stale_docs:
            dtype = d.get("type", "?") if isinstance(d, dict) else str(d)
            lines_html.append(f"<li>{dtype}</li>")
        lines_html.append("</ul>")

    if not lines_html:
        return

    html_body = f"""<html><body>
<p>Hallo,</p>
<p>für den Antrag von <strong>{display_name}</strong> fehlen noch folgende Unterlagen:</p>
{"".join(lines_html)}
</body></html>"""

    text_lines = [f"Antrag: {display_name}\n"]
    if missing_fin:
        text_lines.append("Fehlende Daten: " + ", ".join(FINANCING_LABELS.get(f, f) for f in missing_fin))
    if missing_docs:
        for d in missing_docs:
            if isinstance(d, dict):
                text_lines.append(f"- {d.get('type', '?')} ({d.get('found',0)}/{d.get('required',1)})")
            else:
                text_lines.append(f"- {d}")

    _send_email(
        to=BROKER_EMAIL,
        subject=f"Offene Unterlagen – {display_name}",
        html_body=html_body,
        text_body="\n".join(text_lines),
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
        body = _generate_questions_with_ai(
            case_id=case_id,
            missing_financing=readiness_result.get("missing_financing", []),
            missing_docs=readiness_result.get("missing_docs", []),
            stale_docs=readiness_result.get("stale_docs", []),
            effective_view=view,
            recipient="partner",
        )
        if not body:
            logger.info(f"Reminder für {case_id}: kein Body generiert, überspringe")
            return

        reminder_note = (
            f"\n\n--- Dies ist eine automatische Erinnerung (Nr. {reminder_count}). "
            f"Wir haben noch keine Rückmeldung zu unserer vorherigen Anfrage erhalten. ---"
        )
        body_with_note = body + reminder_note

        html_body = f"""<html><body>
<p>{body.replace(chr(10), '<br>')}</p>
<br>
<p style="color: #666; font-size: 0.9em;">
<em>Dies ist eine automatische Erinnerung (Nr. {reminder_count}).
Wir haben noch keine Rückmeldung zu unserer vorherigen Anfrage erhalten.</em></p>
<br>
<p>Mit freundlichen Grüßen<br>Alexander Heil Finanzierung</p>
</body></html>"""

        subject_name = applicant_name or "Ihre Anfrage"
        _send_email(
            to=partner_email,
            subject=f"{prefix}Fehlende Unterlagen – Finanzierungsanfrage {subject_name}",
            html_body=html_body,
            text_body=body_with_note,
        )
        logger.info(f"Reminder #{reminder_count} gesendet an Partner {partner_email} für Case {case_id}")

    elif target == "broker":
        # Broker-Reminder: gleiche strukturierte Liste wie send_broker_questions
        display_name = view.get("applicant_name") or "Unbekannt"
        # Reuse send_broker_questions logic but with reminder prefix
        send_broker_questions(case_id, readiness_result, view)
        logger.info(f"Reminder #{reminder_count} gesendet an Broker für Case {case_id}")

    else:
        logger.warning(f"Reminder für {case_id}: target={target}, partner_email={partner_email} – übersprungen")


def dispatch_notifications(case_id: str, readiness_result: dict, force: bool = False):
    """
    Sendet die richtige Benachrichtigung basierend auf Status.
    Wird nach jedem Readiness Check aufgerufen.

    force=True überspringt den Cooldown (z.B. bei manuellem Recheck aus Dashboard).
    """
    status = readiness_result.get("status")
    view = readiness_result.get("effective_view", {})

    # Cooldown Check: verhindert doppelten Versand bei schnell aufeinanderfolgenden Mails
    if not force and not _check_cooldown(case_id, status):
        return

    import case_logic as cases
    case = cases.load_case(case_id)
    partner_email = case.get("partner_email", "") if case else ""

    if status == "NEEDS_QUESTIONS_PARTNER":
        send_partner_questions(case_id, partner_email, readiness_result, view)
        _record_cooldown(case_id, status)

    elif status == "NEEDS_QUESTIONS_BROKER":
        send_broker_questions(case_id, readiness_result, view)
        _record_cooldown(case_id, status)

    elif status == "NEEDS_MANUAL_REVIEW_BROKER":
        send_manual_review(case_id, readiness_result, view)
        _record_cooldown(case_id, status)

    elif status == "AWAITING_BROKER_CONFIRMATION":
        send_broker_confirmation(case_id, view)
        _record_cooldown(case_id, status)

    elif status == "READY_FOR_IMPORT":
        logger.info(f"Case {case_id} ready for import – warte auf manuelle Freigabe im Dashboard (Import-Button)")
        # Kein automatischer Import-Trigger. Der Broker gibt den Import
        # bewusst ueber den Dashboard-Button frei.

    else:
        logger.info(f"Status {status} – keine Benachrichtigung nötig")
