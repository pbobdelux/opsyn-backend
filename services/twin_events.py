"""
Twin event hook system.

Fires fire-and-forget POST events to the Twin ingest endpoint whenever
significant driver lifecycle events occur (driver created, PIN reset).
Errors are logged but never re-raised so callers are never disrupted.
"""

import logging
import os
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("twin_events")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TWIN_INGEST_URL = os.getenv(
    "TWIN_INGEST_URL",
    "https://twin.opsyn.ai/ingest/driver-events",
)
TWIN_INGEST_SECRET = os.getenv("TWIN_INGEST_SECRET", "")

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def send_driver_event(
    event_type: str,
    org_id: str,
    org_name: str,
    org_code: str,
    driver_id: int,
    driver_name: str,
    driver_email: str | None,
    driver_phone: str | None,
    driver_pin: str | None,
) -> bool:
    """
    POST a driver lifecycle event to the Twin ingest endpoint.

    Parameters
    ----------
    event_type:   "driver_created" | "driver_pin_reset"
    org_id:       Organisation identifier (e.g. "org_onboarding")
    org_name:     Human-readable org name
    org_code:     Short org code / slug
    driver_id:    Primary key of the Driver row
    driver_name:  Full name of the driver
    driver_email: Driver email address (may be None)
    driver_phone: Driver phone number (may be None)
    driver_pin:   Driver PIN (may be None)

    Returns True on success, False on any failure.
    Never raises an exception.
    """
    payload: dict = {
        "event_type": event_type,
        "org_id": org_id,
        "org_name": org_name,
        "org_code": org_code,
        "driver_id": driver_id,
        "driver_name": driver_name,
        "driver_email": driver_email or "",
        "driver_phone": driver_phone or "",
        "driver_pin": driver_pin or "",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    headers: dict = {"Content-Type": "application/json"}
    if TWIN_INGEST_SECRET:
        headers["X-Twin-Secret"] = TWIN_INGEST_SECRET

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.post(TWIN_INGEST_URL, json=payload, headers=headers)

        if response.is_success:
            logger.info(
                "twin_events: sent %s for driver_id=%s org_id=%s → HTTP %s",
                event_type,
                driver_id,
                org_id,
                response.status_code,
            )
            return True

        logger.warning(
            "twin_events: non-success response for %s driver_id=%s → HTTP %s body=%s",
            event_type,
            driver_id,
            response.status_code,
            response.text[:200],
        )
        return False

    except Exception as exc:  # noqa: BLE001
        logger.error(
            "twin_events: failed to send %s for driver_id=%s org_id=%s: %s",
            event_type,
            driver_id,
            org_id,
            exc,
        )
        return False
