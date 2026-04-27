"""
Opsyn Watchdog outbound client.

Emits fire-and-forget POST events to the configured Watchdog webhook URL
using Bearer token authentication.  The secret is read exclusively from the
OPSYN_WATCHDOG_SECRET environment variable and is never written to logs or
included in response payloads.

Usage
-----
    from services.watchdog_client import emit_watchdog_event

    await emit_watchdog_event(
        event_type="sync_started",
        brand_id="noble-nectar",
        sync_metadata={"total_fetched_from_leaflink": 0, "total_in_database": 0},
        webhook_url="https://watchdog.opsyn.ai/hooks/ingest",
    )
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger("watchdog_client")


def _get_secret() -> str:
    """Return the watchdog secret from the environment.

    Never logs or returns the raw value to callers outside of auth headers.
    """
    return os.getenv("OPSYN_WATCHDOG_SECRET", "")


async def emit_watchdog_event(
    event_type: str,
    brand_id: str,
    sync_metadata: Optional[dict[str, Any]] = None,
    webhook_url: Optional[str] = None,
) -> bool:
    """POST a sync lifecycle event to the Watchdog webhook endpoint.

    Parameters
    ----------
    event_type:    One of: sync_started, sync_progress, sync_completed, sync_failed, sync_stalled
    brand_id:      Brand slug / ID being synced
    sync_metadata: Dict with sync progress fields (counts, dates, errors, etc.)
    webhook_url:   Destination URL.  If None or empty the call is skipped gracefully.

    Returns True on success, False on any failure.
    Never raises an exception — callers must not be disrupted by watchdog failures.
    """
    if not webhook_url:
        logger.info(
            "[Watchdog] outbound_event skipped — no webhook_url configured type=%s brand=%s",
            event_type,
            brand_id,
        )
        return False

    secret = _get_secret()
    if not secret:
        logger.warning(
            "[Watchdog] outbound_event skipped — OPSYN_WATCHDOG_SECRET not set type=%s brand=%s",
            event_type,
            brand_id,
        )
        return False

    payload: dict[str, Any] = {
        "event_type": event_type,
        "brand_id": brand_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sync_metadata": sync_metadata or {},
    }

    # Secret is placed only in the Authorization header — never in the payload or logs.
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {secret}",
    }

    logger.info(
        "[Watchdog] outbound_event type=%s brand=%s url=%s",
        event_type,
        brand_id,
        webhook_url,
    )

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.post(webhook_url, json=payload, headers=headers)

        if response.is_success:
            logger.info(
                "[Watchdog] outbound_event_sent type=%s brand=%s status=%s",
                event_type,
                brand_id,
                response.status_code,
            )
            return True

        logger.warning(
            "[Watchdog] outbound_event_non_success type=%s brand=%s status=%s body=%s",
            event_type,
            brand_id,
            response.status_code,
            response.text[:200],
        )
        return False

    except Exception as exc:  # noqa: BLE001
        logger.error(
            "[Watchdog] outbound_event_failed type=%s brand=%s error=%s",
            event_type,
            brand_id,
            exc,
        )
        return False
