"""
email_sender.py — Outbound email for LeadGen outreach

Delivery methods (first match wins):
  1) Resend HTTPS API (port 443) — best on Render; SMTP ports are often blocked or slow.
     Set RESEND_API_KEY=re_xxx  OR  RESEND_USE_HTTP=true with SMTP_HOST=smtp.resend.com
     and SMTP_PASSWORD=your Resend API key.
  2) Classic SMTP (Gmail, SendGrid SMTP, Resend SMTP, etc.)

Config via .env:
  SMTP_FROM_EMAIL, SMTP_FROM_NAME (required for any method)

  Resend HTTP (recommended on PaaS):
    RESEND_API_KEY=re_xxx
    — or —
    RESEND_USE_HTTP=true
    SMTP_HOST=smtp.resend.com
    SMTP_PASSWORD=re_xxx   # same as Resend API key

  SMTP:
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD
    Optional: SMTP_TIMEOUT (default 25), SMTP_USE_SSL=true (port 465),
    SMTP_DEBUG=true (verbose SMTP logs)

  Optional: RESEND_HTTP_TIMEOUT (default 30 seconds)
"""

from __future__ import annotations

import os
import logging
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import httpx
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = (os.getenv("SMTP_HOST", "smtp.gmail.com") or "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587") or "587")
SMTP_USER = (os.getenv("SMTP_USER") or "").strip()
SMTP_PASSWORD = (os.getenv("SMTP_PASSWORD") or "").strip()
SMTP_FROM_EMAIL = (os.getenv("SMTP_FROM_EMAIL") or "").strip()
SMTP_FROM_NAME = (os.getenv("SMTP_FROM_NAME") or "Nirmallya").strip()
SMTP_TIMEOUT = int(os.getenv("SMTP_TIMEOUT", "25") or "25")
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "").lower() in ("1", "true", "yes")
SMTP_DEBUG = os.getenv("SMTP_DEBUG", "").lower() in ("1", "true", "yes")

RESEND_API_KEY = (os.getenv("RESEND_API_KEY") or "").strip()
RESEND_USE_HTTP = os.getenv("RESEND_USE_HTTP", "").lower() in ("1", "true", "yes")
RESEND_HTTP_TIMEOUT = float(os.getenv("RESEND_HTTP_TIMEOUT", "30") or "30")

RESEND_API_URL = "https://api.resend.com/emails"

log = logging.getLogger("leadgen.smtp")


def _resend_api_key_effective() -> str:
    if RESEND_API_KEY:
        return RESEND_API_KEY
    return SMTP_PASSWORD


def use_resend_http() -> bool:
    """True when we should call Resend REST API instead of SMTP."""
    if RESEND_API_KEY:
        return True
    if RESEND_USE_HTTP and "resend" in SMTP_HOST.lower() and SMTP_PASSWORD:
        return True
    return False


def is_configured() -> bool:
    """Enough config to send (Resend HTTP or SMTP)."""
    if not SMTP_FROM_EMAIL:
        return False
    if use_resend_http():
        return bool(_resend_api_key_effective())
    return bool(SMTP_USER and SMTP_PASSWORD)


def _html_part(body_plain: str) -> str:
    html_body = body_plain.replace("\n", "<br>")
    return f"""\
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
              color: #1a1a1a; font-size: 14px; line-height: 1.6;">
    <p>{html_body}</p>
</body>
</html>"""


def _send_resend_http(
    to_email: str,
    subject: str,
    body_plain: str,
    from_name: str,
    from_email: str,
) -> dict:
    api_key = _resend_api_key_effective()
    t0 = time.perf_counter()
    payload = {
        "from": f"{from_name} <{from_email}>",
        "to": [to_email],
        "subject": subject,
        "text": body_plain,
        "html": _html_part(body_plain),
        "reply_to": from_email,
    }
    log.info(
        "resend_http_begin | to=%s subject_len=%s timeout_s=%s",
        to_email,
        len(subject or ""),
        RESEND_HTTP_TIMEOUT,
    )
    try:
        with httpx.Client(timeout=RESEND_HTTP_TIMEOUT) as client:
            resp = client.post(
                RESEND_API_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if resp.status_code in (200, 201):
            try:
                data = resp.json()
            except Exception:
                data = {}
            rid = data.get("id") or ""
            log.info(
                "resend_http_ok | to=%s elapsed_ms=%.0f resend_id=%s",
                to_email,
                elapsed_ms,
                rid or "?",
            )
            msg = f"Sent to {to_email}"
            if rid:
                msg += f" (resend_id={rid})"
            return {"success": True, "message": msg}

        try:
            err = resp.json()
            detail = err.get("message") or err.get("name") or str(err)
        except Exception:
            detail = (resp.text or "")[:500]
        log.warning(
            "resend_http_fail | to=%s status=%s elapsed_ms=%.0f detail=%s",
            to_email,
            resp.status_code,
            elapsed_ms,
            detail,
        )
        return {
            "success": False,
            "message": f"Resend API HTTP {resp.status_code}: {detail}",
        }
    except httpx.TimeoutException as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        log.warning(
            "resend_http_timeout | to=%s elapsed_ms=%.0f detail=%s",
            to_email,
            elapsed_ms,
            str(e),
        )
        return {"success": False, "message": f"Resend API timed out: {e}"}
    except Exception as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        log.warning(
            "resend_http_error | to=%s elapsed_ms=%.0f error_type=%s detail=%s",
            to_email,
            elapsed_ms,
            type(e).__name__,
            str(e),
        )
        return {"success": False, "message": f"Resend API error: {e}"}


def send_email(
    to_email: str,
    subject: str,
    body: str,
    from_name: str | None = None,
    from_email: str | None = None,
) -> dict:
    """
    Send a single email via Resend HTTP (preferred) or SMTP.

    Returns dict with:
      - success: bool
      - message: str (error or confirmation)
    """
    sender_name = from_name or SMTP_FROM_NAME
    sender_email = from_email or SMTP_FROM_EMAIL

    if not is_configured():
        log.warning("email_send_skipped | reason=not_configured")
        return {
            "success": False,
            "message": (
                "Email not configured. Set SMTP_FROM_EMAIL and either "
                "RESEND_API_KEY (or RESEND_USE_HTTP=true + Resend SMTP_PASSWORD), "
                "or full SMTP credentials."
            ),
        }

    if not to_email:
        log.warning("email_send_skipped | reason=no_recipient")
        return {"success": False, "message": "No recipient email provided"}

    if use_resend_http():
        return _send_resend_http(to_email, subject, body, sender_name, sender_email)

    return _send_smtp(to_email, subject, body, sender_name, sender_email)


def _send_smtp(
    to_email: str,
    subject: str,
    body: str,
    sender_name: str,
    sender_email: str,
) -> dict:
    use_ssl = SMTP_USE_SSL or SMTP_PORT == 465
    t0 = time.perf_counter()
    log.info(
        "smtp_begin | host=%s port=%s ssl=%s timeout_s=%s to=%s subject_len=%s",
        SMTP_HOST,
        SMTP_PORT,
        use_ssl,
        SMTP_TIMEOUT,
        to_email,
        len(subject or ""),
    )

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = sender_email

        msg.attach(MIMEText(body, "plain", "utf-8"))
        msg.attach(MIMEText(_html_part(body), "html", "utf-8"))

        if SMTP_USE_SSL or SMTP_PORT == 465:
            server_ctx = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT)
        else:
            server_ctx = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT)

        log.info(
            "smtp_tcp_ehlo_ok | host=%s port=%s to=%s elapsed_ms=%.0f",
            SMTP_HOST,
            SMTP_PORT,
            to_email,
            (time.perf_counter() - t0) * 1000,
        )

        with server_ctx as server:
            if SMTP_DEBUG:
                server.set_debuglevel(1)
            if not (SMTP_USE_SSL or SMTP_PORT == 465):
                server.starttls()
                log.info("smtp_starttls_ok | to=%s", to_email)
            server.login(SMTP_USER, SMTP_PASSWORD)
            log.info("smtp_login_ok | to=%s", to_email)
            server.send_message(msg)
            log.info("smtp_message_accepted | to=%s", to_email)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        log.info(
            "smtp_send_ok | to=%s elapsed_ms=%.0f from=%s",
            to_email,
            elapsed_ms,
            sender_email,
        )
        return {
            "success": True,
            "message": f"Sent to {to_email}",
        }

    except smtplib.SMTPAuthenticationError as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        log.warning(
            "smtp_auth_failed | to=%s elapsed_ms=%.0f detail=%s",
            to_email,
            elapsed_ms,
            str(e),
        )
        return {
            "success": False,
            "message": "SMTP authentication failed. Check SMTP_USER and SMTP_PASSWORD.",
        }
    except smtplib.SMTPRecipientsRefused as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        log.warning(
            "smtp_recipient_refused | to=%s elapsed_ms=%.0f detail=%s",
            to_email,
            elapsed_ms,
            str(e),
        )
        return {
            "success": False,
            "message": f"Recipient refused: {to_email}",
        }
    except Exception as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        log.warning(
            "smtp_send_error | to=%s elapsed_ms=%.0f error_type=%s detail=%s",
            to_email,
            elapsed_ms,
            type(e).__name__,
            str(e),
        )
        return {
            "success": False,
            "message": f"Send failed: {str(e)}",
        }


def send_batch(emails: list[dict]) -> list[dict]:
    """
    Send a batch of approved emails.

    Args:
        emails: list of dicts with to_email, subject, body

    Returns list of result dicts with email_id, success, message.
    """
    results = []
    total = len(emails)
    success_count = 0

    log.info("email_batch.start | count=%s transport=%s", total, "resend_http" if use_resend_http() else "smtp")

    for i, email in enumerate(emails):
        email_id = email.get("id", "")
        to = email.get("to_email", "")
        subject = email.get("subject", "")
        body = email.get("body", "")

        log.info("email_batch.item | index=%s/%s email_id=%s to=%s", i + 1, total, email_id, to)

        result = send_email(to, subject, body)
        result["email_id"] = email_id
        results.append(result)

        if result["success"]:
            success_count += 1
        else:
            log.warning(
                "email_batch.item_fail | index=%s/%s email_id=%s detail=%s",
                i + 1,
                total,
                email_id,
                result.get("message", ""),
            )

    log.info("email_batch.complete | ok=%s fail=%s total=%s", success_count, total - success_count, total)
    return results
