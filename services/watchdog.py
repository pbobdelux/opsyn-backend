"""
Opsyn Watchdog client — outbound webhook delivery to Twin.

Emits structured sync lifecycle events (started, progress, completed,
failed, stalled) via authenticated POST requests.  The shared secret is
read from the OPSYN_WATCHDOG_SECRET environment variable and is NEVER
written to logs or response payloads.
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("watchdog")

# Maximum number of delivery attempts per event.
_MAX_RETRIES = 3
# Seconds to wait between retries (simple linear back-off: 1 s, 2 s, 3 s).
_RETRY_DELAY_BASE = 1.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class WatchdogClient:
    """Sends sync lifecycle events to the Twin webhook URL."""

    def __init__(self) -> None:
        self._secret: str = os.getenv("OPSYN_WATCHDOG_SECRET", "")
        self._webhook_url: str = os.getenv("TWIN_WEBHOOK_URL", "")

    # ------------------------------------------------------------------
    # Public event helpers
    # ------------------------------------------------------------------

    def sync_started(self, brand_id: str, **meta: Any) -> None:
        self.emit_event("sync_started", brand_id, **meta)

    def sync_progress(
        self,
        brand_id: str,
        *,
        total_fetched: int = 0,
        total_in_database: int = 0,
        percent_complete: int = 0,
        latest_order_date: str | None = None,
        oldest_order_date: str | None = None,
        **extra: Any,
    ) -> None:
        self.emit_event(
            "sync_progress",
            brand_id,
            total_fetched_from_leaflink=total_fetched,
            total_in_database=total_in_database,
            percent_complete=percent_complete,
            latest_order_date=latest_order_date,
            oldest_order_date=oldest_order_date,
            **extra,
        )

    def sync_completed(
        self,
        brand_id: str,
        *,
        total_fetched: int = 0,
        total_in_database: int = 0,
        latest_order_date: str | None = None,
        oldest_order_date: str | None = None,
        **extra: Any,
    ) -> None:
        self.emit_event(
            "sync_completed",
            brand_id,
            total_fetched_from_leaflink=total_fetched,
            total_in_database=total_in_database,
            percent_complete=100,
            latest_order_date=latest_order_date,
            oldest_order_date=oldest_order_date,
            **extra,
        )

    def sync_failed(
        self,
        brand_id: str,
        *,
        error: str | None = None,
        total_fetched: int = 0,
        total_in_database: int = 0,
        **extra: Any,
    ) -> None:
        self.emit_event(
            "sync_failed",
            brand_id,
            total_fetched_from_leaflink=total_fetched,
            total_in_database=total_in_database,
            sync_error=error,
            **extra,
        )

    def sync_stalled(
        self,
        brand_id: str,
        *,
        total_fetched: int = 0,
        total_in_database: int = 0,
        **extra: Any,
    ) -> None:
        self.emit_event(
            "sync_stalled",
            brand_id,
            total_fetched_from_leaflink=total_fetched,
            total_in_database=total_in_database,
            **extra,
        )

    # ------------------------------------------------------------------
    # Core delivery
    # ------------------------------------------------------------------

    def emit_event(self, event_type: str, brand_id: str, **meta: Any) -> None:
        """POST a sync event to the Twin webhook URL.

        Silently skips delivery when TWIN_WEBHOOK_URL is not configured
        (logs a warning instead of raising).  Retries up to _MAX_RETRIES
        times on transient failures.
        """
        if not self._webhook_url:
            logger.warning(
                "[Watchdog] outbound_event skipped — TWIN_WEBHOOK_URL not configured "
                "type=%s brand=%s",
                event_type,
                brand_id,
            )
            return

        if not self._secret:
            logger.warning(
                "[Watchdog] outbound_event skipped — OPSYN_WATCHDOG_SECRET not configured "
                "type=%s brand=%s",
                event_type,
                brand_id,
            )
            return

        payload: dict[str, Any] = {
            "event_type": event_type,
            "brand_id": brand_id,
            "timestamp": _utc_now_iso(),
            "sync_metadata": {
                "total_fetched_from_leaflink": meta.get("total_fetched_from_leaflink", 0),
                "total_in_database": meta.get("total_in_database", 0),
                "percent_complete": meta.get("percent_complete", 0),
                "latest_order_date": meta.get("latest_order_date"),
                "oldest_order_date": meta.get("oldest_order_date"),
                "sync_error": meta.get("sync_error"),
            },
        }

        headers = {
            "Authorization": f"Bearer {self._secret}",
            "Content-Type": "application/json",
        }

        percent = meta.get("percent_complete")
        total_fetched = meta.get("total_fetched_from_leaflink")

        # Log without the secret.
        if event_type == "sync_progress" and percent is not None:
            logger.info(
                "[Watchdog] outbound_event type=%s brand=%s percent_complete=%s",
                event_type,
                brand_id,
                percent,
            )
        elif event_type == "sync_completed" and total_fetched is not None:
            logger.info(
                "[Watchdog] outbound_event type=%s brand=%s total_fetched=%s",
                event_type,
                brand_id,
                total_fetched,
            )
        else:
            logger.info(
                "[Watchdog] outbound_event type=%s brand=%s",
                event_type,
                brand_id,
            )

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                with httpx.Client(timeout=10.0) as client:
                    response = client.post(
                        self._webhook_url,
                        json=payload,
                        headers=headers,
                    )
                    response.raise_for_status()
                    logger.info(
                        "[Watchdog] outbound_delivery_ok type=%s brand=%s status=%s attempt=%s",
                        event_type,
                        brand_id,
                        response.status_code,
                        attempt,
                    )
                    return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "[Watchdog] outbound_delivery_failed type=%s brand=%s attempt=%s/%s error=%s",
                    event_type,
                    brand_id,
                    attempt,
                    _MAX_RETRIES,
                    exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY_BASE * attempt)

        logger.error(
            "[Watchdog] outbound_delivery_exhausted type=%s brand=%s error=%s",
            event_type,
            brand_id,
            last_exc,
        )
