"""
SMS service for Opsyn Assistant.

Supports Twilio. If not configured, returns needs_integration: true — never crashes.

Environment variables:
  TWILIO_ACCOUNT_SID  — Twilio account SID
  TWILIO_AUTH_TOKEN   — Twilio auth token
  TWILIO_FROM_NUMBER  — Twilio sender phone number (E.164 format, e.g. +15551234567)

Security rules:
  - Never log full phone numbers (only last 4 digits)
  - Never log message content if it may contain PINs
  - Never raise — always return structured JSON
"""

import logging
import os
from typing import Any

logger = logging.getLogger("sms_service")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _mask_phone(phone: str) -> str:
    """Return a masked phone number for safe logging (e.g. ***1234)."""
    if not phone:
        return "***"
    cleaned = phone.replace("+", "").replace("-", "").replace(" ", "")
    return f"***{cleaned[-4:]}" if len(cleaned) >= 4 else "***"


def _is_twilio_configured() -> bool:
    return bool(
        os.getenv("TWILIO_ACCOUNT_SID")
        and os.getenv("TWILIO_AUTH_TOKEN")
        and os.getenv("TWILIO_FROM_NUMBER")
    )


def _message_may_contain_pin(message: str) -> bool:
    """Heuristic: if message contains a 4-digit sequence, treat as sensitive."""
    import re
    return bool(re.search(r"\b\d{4}\b", message))


# ---------------------------------------------------------------------------
# Twilio sender
# ---------------------------------------------------------------------------


async def _send_via_twilio(phone: str, message: str) -> dict[str, Any]:
    """Send an SMS via Twilio."""
    try:
        from twilio.rest import Client  # type: ignore

        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_number = os.getenv("TWILIO_FROM_NUMBER", "")

        client = Client(account_sid, auth_token)
        msg = client.messages.create(
            body=message,
            from_=from_number,
            to=phone,
        )

        sensitive = _message_may_contain_pin(message)
        logger.info(
            "sms_service: Twilio sent to %s sid=%s content_logged=%s",
            _mask_phone(phone),
            msg.sid,
            not sensitive,
        )

        return {
            "ok": True,
            "message": "SMS sent via Twilio",
            "provider": "twilio",
            "sid": msg.sid,
        }

    except ImportError:
        logger.warning("sms_service: twilio package not installed")
        return {
            "ok": False,
            "error": "twilio package not installed",
            "needs_integration": True,
            "integration": "sms",
        }
    except Exception as exc:
        logger.error("sms_service: Twilio send failed to %s: %s", _mask_phone(phone), exc)
        return {"ok": False, "error": str(exc), "provider": "twilio"}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def send_driver_sms(phone: str, message: str) -> dict[str, Any]:
    """
    Send an SMS to a driver.

    If Twilio is not configured, returns needs_integration: true without crashing.

    Parameters
    ----------
    phone : str
        Recipient phone number in E.164 format (e.g. +15551234567).
    message : str
        Message body. If it appears to contain a PIN, the content is not logged.

    Returns
    -------
    dict with ok: bool and message or error fields.
    """
    logger.info("send_driver_sms: to=%s", _mask_phone(phone))

    if _is_twilio_configured():
        return await _send_via_twilio(phone, message)

    logger.warning("send_driver_sms: no SMS provider configured")
    return {
        "ok": False,
        "needs_integration": True,
        "integration": "sms",
        "message": (
            "SMS service is not configured. "
            "Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_FROM_NUMBER to enable."
        ),
    }
