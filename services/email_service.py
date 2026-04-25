"""
Email service for Opsyn Assistant.

Supports SendGrid (preferred) and SMTP fallback.
If neither is configured, returns needs_integration: true — never crashes.

Environment variables:
  SENDGRID_API_KEY   — SendGrid API key (preferred)
  FROM_EMAIL         — Sender address for SendGrid
  SMTP_HOST          — SMTP server hostname
  SMTP_PORT          — SMTP server port (default: 587)
  SMTP_USERNAME      — SMTP auth username
  SMTP_PASSWORD      — SMTP auth password
  SMTP_FROM_EMAIL    — Sender address for SMTP

Security rules:
  - Never log full PINs
  - Never log full email addresses (only domain)
  - Never raise — always return structured JSON
"""

import logging
import os
from typing import Any

logger = logging.getLogger("email_service")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _mask_email(email: str) -> str:
    """Return a masked email for safe logging (e.g. j***@example.com)."""
    try:
        local, domain = email.split("@", 1)
        return f"{local[0]}***@{domain}"
    except Exception:
        return "***@***"


def _is_sendgrid_configured() -> bool:
    return bool(os.getenv("SENDGRID_API_KEY"))


def _is_smtp_configured() -> bool:
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USERNAME") and os.getenv("SMTP_PASSWORD"))


# ---------------------------------------------------------------------------
# SendGrid sender
# ---------------------------------------------------------------------------


async def _send_via_sendgrid(
    driver_email: str,
    driver_name: str,
    org_code: str,
    pin: str,
) -> dict[str, Any]:
    """Send a PIN email via SendGrid."""
    try:
        import sendgrid  # type: ignore
        from sendgrid.helpers.mail import Mail  # type: ignore

        api_key = os.getenv("SENDGRID_API_KEY", "")
        from_email = os.getenv("FROM_EMAIL", "noreply@opsyn.app")

        message = Mail(
            from_email=from_email,
            to_emails=driver_email,
            subject=f"Your Opsyn Driver PIN — {org_code}",
            html_content=(
                f"<p>Hi {driver_name},</p>"
                f"<p>Your Opsyn driver PIN is: <strong>{pin}</strong></p>"
                f"<p>Keep this PIN secure. Do not share it with anyone.</p>"
                f"<p>— Opsyn Operations</p>"
            ),
        )

        sg = sendgrid.SendGridAPIClient(api_key=api_key)
        response = sg.send(message)

        status_code = response.status_code
        logger.info(
            "email_service: SendGrid sent to %s status=%s",
            _mask_email(driver_email),
            status_code,
        )

        if status_code in (200, 202):
            return {"ok": True, "message": "Email sent via SendGrid", "provider": "sendgrid"}
        else:
            logger.error("email_service: SendGrid returned status=%s", status_code)
            return {
                "ok": False,
                "error": f"SendGrid returned status {status_code}",
                "provider": "sendgrid",
            }

    except ImportError:
        logger.warning("email_service: sendgrid package not installed")
        return {
            "ok": False,
            "error": "sendgrid package not installed",
            "needs_integration": True,
            "integration": "email",
        }
    except Exception as exc:
        logger.error("email_service: SendGrid send failed: %s", exc)
        return {"ok": False, "error": str(exc), "provider": "sendgrid"}


# ---------------------------------------------------------------------------
# SMTP sender
# ---------------------------------------------------------------------------


async def _send_via_smtp(
    driver_email: str,
    driver_name: str,
    org_code: str,
    pin: str,
) -> dict[str, Any]:
    """Send a PIN email via SMTP."""
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        smtp_host = os.getenv("SMTP_HOST", "")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USERNAME", "")
        smtp_pass = os.getenv("SMTP_PASSWORD", "")
        from_email = os.getenv("SMTP_FROM_EMAIL", smtp_user)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Your Opsyn Driver PIN — {org_code}"
        msg["From"] = from_email
        msg["To"] = driver_email

        html_body = (
            f"<p>Hi {driver_name},</p>"
            f"<p>Your Opsyn driver PIN is: <strong>{pin}</strong></p>"
            f"<p>Keep this PIN secure. Do not share it with anyone.</p>"
            f"<p>— Opsyn Operations</p>"
        )
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, driver_email, msg.as_string())

        logger.info(
            "email_service: SMTP sent to %s via %s",
            _mask_email(driver_email),
            smtp_host,
        )
        return {"ok": True, "message": "Email sent via SMTP", "provider": "smtp"}

    except Exception as exc:
        logger.error("email_service: SMTP send failed: %s", exc)
        return {"ok": False, "error": str(exc), "provider": "smtp"}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def send_driver_pin_email(
    driver_email: str,
    driver_name: str,
    org_code: str,
    pin: str,
) -> dict[str, Any]:
    """
    Send a driver PIN email.

    Tries SendGrid first, then SMTP. If neither is configured, returns
    needs_integration: true without crashing.

    Parameters
    ----------
    driver_email : str
        Recipient email address.
    driver_name : str
        Driver's display name (used in greeting).
    org_code : str
        Organisation identifier (used in subject line).
    pin : str
        The 4-digit PIN to send. Never logged in full.

    Returns
    -------
    dict with ok: bool and message or error fields.
    """
    logger.info(
        "send_driver_pin_email: org_code=%s recipient=%s",
        org_code,
        _mask_email(driver_email),
    )

    if _is_sendgrid_configured():
        return await _send_via_sendgrid(driver_email, driver_name, org_code, pin)

    if _is_smtp_configured():
        return await _send_via_smtp(driver_email, driver_name, org_code, pin)

    logger.warning(
        "send_driver_pin_email: no email provider configured for org_code=%s", org_code
    )
    return {
        "ok": False,
        "needs_integration": True,
        "integration": "email",
        "message": (
            "Email service is not configured. "
            "Set SENDGRID_API_KEY or SMTP_HOST/SMTP_USERNAME/SMTP_PASSWORD to enable."
        ),
    }
