"""
email_sender.py — SMTP email sending for LeadGen outreach
Sends approved email drafts via SMTP and logs results.

Supports:
  - Gmail (with App Password)
  - Any SMTP server (Outlook, SendGrid SMTP relay, etc.)

Config via .env:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
  SMTP_FROM_EMAIL, SMTP_FROM_NAME
  Optional: SMTP_TIMEOUT (default 25), SMTP_USE_SSL=true (port 465),
  SMTP_DEBUG=true (verbose SMTP logs)
"""

import os
import logging
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Nirmallya")
# Socket timeout (connect + low-level ops). Cloud SMTP can be slow; Render HTTP limit is ~30s for sync requests.
SMTP_TIMEOUT = int(os.getenv("SMTP_TIMEOUT", "25"))
# Use SMTP_SSL on 465; otherwise STARTTLS on 587 (default).
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "").lower() in ("1", "true", "yes")
SMTP_DEBUG = os.getenv("SMTP_DEBUG", "").lower() in ("1", "true", "yes")

log = logging.getLogger("leadgen.smtp")


def is_configured() -> bool:
    """Check if SMTP credentials are configured."""
    return bool(SMTP_USER and SMTP_PASSWORD and SMTP_FROM_EMAIL)


def send_email(to_email: str, subject: str, body: str,
               from_name: str = None, from_email: str = None) -> dict:
    """
    Send a single email via SMTP.
    
    Returns dict with:
      - success: bool
      - message: str (error or confirmation)
    """
    sender_name = from_name or SMTP_FROM_NAME
    sender_email = from_email or SMTP_FROM_EMAIL

    if not is_configured():
        log.warning("smtp_send_skipped | reason=not_configured")
        return {
            "success": False,
            "message": "SMTP not configured. Set SMTP_USER, SMTP_PASSWORD, SMTP_FROM_EMAIL in .env"
        }

    if not to_email:
        log.warning("smtp_send_skipped | reason=no_recipient")
        return {"success": False, "message": "No recipient email provided"}

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

        # Plain text body
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # Also create a simple HTML version (same content, with line breaks)
        html_body = body.replace("\n", "<br>")
        html_content = f"""
        <html>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                      color: #1a1a1a; font-size: 14px; line-height: 1.6;">
            <p>{html_body}</p>
        </body>
        </html>
        """
        msg.attach(MIMEText(html_content, "html", "utf-8"))

        # Connect and send (SSL for 465, STARTTLS for 587)
        # TCP + SMTP EHLO happen inside the constructor — may block up to SMTP_TIMEOUT seconds.
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
            "message": f"Sent to {to_email}"
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
            "message": "SMTP authentication failed. Check SMTP_USER and SMTP_PASSWORD."
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
            "message": f"Recipient refused: {to_email}"
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
            "message": f"Send failed: {str(e)}"
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
    
    log.info("smtp_batch.start | count=%s", total)

    for i, email in enumerate(emails):
        email_id = email.get("id", "")
        to = email.get("to_email", "")
        subject = email.get("subject", "")
        body = email.get("body", "")

        log.info("smtp_batch.item | index=%s/%s email_id=%s to=%s", i + 1, total, email_id, to)

        result = send_email(to, subject, body)
        result["email_id"] = email_id
        results.append(result)

        if result["success"]:
            success_count += 1
        else:
            log.warning(
                "smtp_batch.item_fail | index=%s/%s email_id=%s detail=%s",
                i + 1,
                total,
                email_id,
                result.get("message", ""),
            )

    log.info("smtp_batch.complete | ok=%s fail=%s total=%s", success_count, total - success_count, total)
    return results
