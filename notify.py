"""
Notification Sender
Versendet E-Mails per SMTP basierend auf Case-Status.
Portiert aus Readiness Router (n8n SMTP Nodes + GPT-4o Question Generator).
"""

import os
import logging
import smtplib
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

_openai = None


def _get_openai() -> OpenAI:
    global _openai
    if not _openai:
        _openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai


def _send_email(to: str, subject: str, html_body: str, text_body: str = None):
    """SMTP E-Mail versenden"""
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

    if missing_financing:
        case_summary.append(f"Fehlende Finanzierungsdaten: {', '.join(missing_financing)}")
    if missing_docs:
        doc_list = ", ".join(f"{d['type']} ({d['required']}x benötigt, {d['found']}x vorhanden)" for d in missing_docs)
        case_summary.append(f"Fehlende Dokumente: {doc_list}")
    if stale_docs:
        stale_list = ", ".join(f"{d['type']} (zu alt)" for d in stale_docs)
        case_summary.append(f"Veraltete Dokumente: {stale_list}")

    if not case_summary:
        return ""

    if recipient == "partner":
        system = """Du bist ein freundlicher Assistent für Baufinanzierungsanfragen.
Schreibe eine höfliche E-Mail an den Kunden/Partner um fehlende Informationen zu erfragen.
Sei konkret aber nicht technisch. Deutsch, professionell, freundlich."""
        prompt = f"""Case: {case_id}
Antragsteller: {applicant_name}

Bitte erfrage folgende fehlenden Informationen/Dokumente:
{chr(10).join(case_summary)}

Schreibe eine komplette E-Mail (nur Body, ohne Betreff). Beginne mit einer freundlichen Begrüßung."""
    else:
        system = """Du bist ein internes System für Baufinanzierung.
Schreibe eine prägnante interne Nachricht an den Backoffice-Mitarbeiter."""
        prompt = f"""Case: {case_id}
Antragsteller: {applicant_name}

Folgende Punkte benötigen Ihre Aufmerksamkeit:
{chr(10).join(case_summary)}

Schreibe eine kurze interne Nachricht mit konkreten Handlungsempfehlungen."""

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


def send_partner_questions(case_id: str, partner_email: str, readiness_result: dict, effective_view: dict):
    """Sendet Rückfrage-E-Mail an Partner"""
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
<p>Case-Referenz: <strong>{case_id}</strong></p>
<p>Mit freundlichen Grüßen<br>Alexander Heil Finanzierung</p>
</body></html>"""

    _send_email(
        to=partner_email,
        subject=f"Rückfrage zu Ihrer Finanzierungsanfrage [{case_id}]",
        html_body=html_body,
        text_body=body,
    )


def send_broker_questions(case_id: str, readiness_result: dict, effective_view: dict):
    """Sendet interne Rückfrage an Broker/Backoffice"""
    body = _generate_questions_with_ai(
        case_id=case_id,
        missing_financing=readiness_result.get("missing_financing", []),
        missing_docs=readiness_result.get("missing_docs", []),
        stale_docs=readiness_result.get("stale_docs", []),
        effective_view=effective_view,
        recipient="broker",
    )

    if not body:
        return

    html_body = f"""<html><body>
<h3>Interne Rückfrage: {case_id}</h3>
<p>{body.replace(chr(10), '<br>')}</p>
</body></html>"""

    _send_email(
        to=BROKER_EMAIL,
        subject=f"[INTERN] Rückfrage Case {case_id}",
        html_body=html_body,
        text_body=body,
    )


def send_broker_confirmation(case_id: str, effective_view: dict):
    """Sendet Freigabe-Anfrage an Broker wenn alles vollständig"""
    name = effective_view.get("applicant_name", "")
    price = effective_view.get("purchase_price") or _get_nested(effective_view, "property_data.purchase_price") or "k.A."
    loan = effective_view.get("loan_amount") or _get_nested(effective_view, "financing_data.loan_amount") or "k.A."
    equity = effective_view.get("equity_to_use") or _get_nested(effective_view, "financing_data.equity_to_use") or "k.A."

    html_body = f"""<html><body>
<h3>✅ Case bereit für Import: {case_id}</h3>
<p>Antragsteller: <strong>{name}</strong></p>
<ul>
  <li>Kaufpreis: {price} €</li>
  <li>Darlehensbetrag: {loan} €</li>
  <li>Eigenkapital: {equity} €</li>
</ul>
<p><strong>Bitte antworten Sie mit "FREIGABE" oder "GENEHMIGT" um den Import zu starten.</strong></p>
<p>Case-ID: {case_id}</p>
</body></html>"""

    _send_email(
        to=BROKER_EMAIL,
        subject=f"[FREIGABE] Case {case_id} - {name}",
        html_body=html_body,
    )


def send_manual_review(case_id: str, readiness_result: dict, effective_view: dict):
    """Sendet Review-Benachrichtigung bei veralteten Dokumenten"""
    stale = readiness_result.get("stale_docs", [])
    stale_list = "\n".join(f"- {d.get('type', d.get('doc_type', ''))}" for d in stale)

    html_body = f"""<html><body>
<h3>⚠️ Manuelle Prüfung erforderlich: {case_id}</h3>
<p>Folgende Dokumente sind veraltet oder abgelaufen:</p>
<ul>{"".join(f"<li>{d.get('type', d.get('doc_type', ''))}</li>" for d in stale)}</ul>
<p>Bitte prüfen Sie ob diese Dokumente akzeptiert werden können.</p>
<p>Antworten Sie mit:<br>
<code>ACCEPT_STALE [Dokumenttyp]</code> um ein veraltetes Dokument zu akzeptieren<br>
<code>WAIT_FOR_DOCS</code> um auf neue Dokumente zu warten</p>
</body></html>"""

    _send_email(
        to=BROKER_EMAIL,
        subject=f"[REVIEW] Veraltete Dokumente - Case {case_id}",
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


def dispatch_notifications(case_id: str, readiness_result: dict):
    """
    Sendet die richtige Benachrichtigung basierend auf Status.
    Wird nach jedem Readiness Check aufgerufen.
    """
    status = readiness_result.get("status")
    view = readiness_result.get("effective_view", {})

    import case_logic as cases
    case = cases.load_case(case_id)
    partner_email = case.get("partner_email", "") if case else ""

    if status == "NEEDS_QUESTIONS_PARTNER":
        send_partner_questions(case_id, partner_email, readiness_result, view)

    elif status == "NEEDS_QUESTIONS_BROKER":
        send_broker_questions(case_id, readiness_result, view)

    elif status == "NEEDS_MANUAL_REVIEW_BROKER":
        send_manual_review(case_id, readiness_result, view)

    elif status == "AWAITING_BROKER_CONFIRMATION":
        send_broker_confirmation(case_id, view)

    elif status == "READY_FOR_IMPORT":
        logger.info(f"Case {case_id} ready for import – triggering import builder")
        # Import wird durch API Endpoint ausgelöst (return status to n8n)

    else:
        logger.info(f"Status {status} – keine Benachrichtigung nötig")
