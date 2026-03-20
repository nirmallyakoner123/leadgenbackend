"""
email_sender.py — SMTP email sending for LeadGen outreach
Sends approved email drafts via SMTP and logs results.

Supports:
  - Gmail (with App Password)
  - Any SMTP server (Outlook, SendGrid SMTP relay, etc.)

Config via .env:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
  SMTP_FROM_EMAIL, SMTP_FROM_NAME
"""

import os
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
        return {
            "success": False,
            "message": "SMTP not configured. Set SMTP_USER, SMTP_PASSWORD, SMTP_FROM_EMAIL in .env"
        }

    if not to_email:
        return {"success": False, "message": "No recipient email provided"}

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

        # Connect and send
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        return {
            "success": True,
            "message": f"Sent to {to_email}"
        }

    except smtplib.SMTPAuthenticationError:
        return {
            "success": False,
            "message": "SMTP authentication failed. Check SMTP_USER and SMTP_PASSWORD."
        }
    except smtplib.SMTPRecipientsRefused:
        return {
            "success": False,
            "message": f"Recipient refused: {to_email}"
        }
    except Exception as e:
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
    
    print(f"\n[Email Sender] Sending {total} emails...\n")
    
    for i, email in enumerate(emails):
        email_id = email.get("id", "")
        to = email.get("to_email", "")
        subject = email.get("subject", "")
        body = email.get("body", "")
        
        print(f"  [{i+1}/{total}] Sending to: {to}")
        
        result = send_email(to, subject, body)
        result["email_id"] = email_id
        results.append(result)
        
        if result["success"]:
            success_count += 1
            print(f"    ✅ Sent successfully")
        else:
            print(f"    ❌ Failed: {result['message']}")
    
    print(f"\n[Email Sender] Complete. {success_count}/{total} sent successfully.\n")
    return results
