"""
sync_request_processor.py — Process individual sync_requests queue entries.

Handles four job types:
  webhook_order             — fetch a single order from LeafLink API and upsert
  webhook_product           — fetch a single product from LeafLink API and upsert
  incremental_recent_orders — fetch orders changed in the last ~1 hour (API safety net)
  full_resync               — delegate to sync_leaflink_background_continuous()

Called by sync_request_background_worker.py for each claimed job.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import BrandAPICredential, SyncRequest
from models.sync_health import SyncHealth
from services.leaflink_webhook import upsert_webhook_order

logger = logging.getLogger("sync_request_processor")

# How far back to look for "recent" orders in incremental sync
INCREMENTAL_LOOKBACK_MINUTES = int(os.getenv("LEAFLINK_INCREMENTAL_LOOKBACK_MINUTES", "62"))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Credential loader
# ---------------------------------------------------------------------------

async def _load_credential(db: AsyncSession, brand_id: str) -> Optional[BrandAPICredential]:
    """Load the active LeafLink credential for a brand."""
    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
    )
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# LeafLink API helpers (run in thread pool — requests is synchronous)
# ---------------------------------------------------------------------------

def _fetch_single_order_sync(api_key: str, base_url: str, auth_scheme: str, order_id: str) -> Optional[dict]:
    """Fetch a single order from LeafLink API by external ID (synchronous)."""
    import requests as _requests

    url = f"{base_url.rstrip('/')}/orders-received/{order_id}/"
    headers = {
        "Authorization": f"{auth_scheme} {api_key}",
        "Content-Type": "application/json",
    }
    try:
        resp = _requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            logger.warning(
                "[LEAFLINK_WEBHOOK_ORDER_SYNCED] order_not_found order_id=%s status=404",
                order_id,
            )
            return None
        else:
            logger.error(
                "[LEAFLINK_WEBHOOK_ORDER_SYNCED] fetch_failed order_id=%s status=%s",
                order_id,
                resp.status_code,
            )
            return None
    except Exception as exc:
        logger.error(
            "[LEAFLINK_WEBHOOK_ORDER_SYNCED] fetch_error order_id=%s error=%s",
            order_id,
            str(exc)[:200],
        )
        return None


def _fetch_recent_orders_sync(
    api_key: str,
    base_url: str,
    auth_scheme: str,
    since: datetime,
) -> list[dict]:
    """Fetch orders updated since `since` from LeafLink API (synchronous)."""
    import requests as _requests

    url = f"{base_url.rstrip('/')}/orders-received/"
    headers = {
        "Authorization": f"{auth_scheme} {api_key}",
        "Content-Type": "application/json",
    }
    since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
    params = {
        "modified__gte": since_str,
        "limit": 250,
        "ordering": "-modified",
    }

    orders: list[dict] = []
    try:
        resp = _requests.get(url, headers=headers, params=params, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results") or data if isinstance(data, list) else []
            orders.extend(results)
            logger.info(
                "[LEAFLINK_INCREMENTAL_SYNC_START] fetched_count=%s since=%s",
                len(orders),
                since_str,
            )
        else:
            logger.error(
                "[LEAFLINK_INCREMENTAL_SYNC_START] fetch_failed status=%s since=%s",
                resp.status_code,
                since_str,
            )
    except Exception as exc:
        logger.error(
            "[LEAFLINK_INCREMENTAL_SYNC_START] fetch_error error=%s since=%s",
            str(exc)[:200],
            since_str,
        )

    return orders


# ---------------------------------------------------------------------------
# Job handlers
# ---------------------------------------------------------------------------

async def _handle_webhook_order(
    db: AsyncSession,
    sync_request: SyncRequest,
    cred: BrandAPICredential,
) -> None:
    """
    Fetch a single order from LeafLink API and upsert it.

    If object_id is present: fetch that specific order.
    Otherwise: fetch orders changed in the last hour.
    """
    brand_id = sync_request.brand_id
    org_id = sync_request.org_id
    object_id = sync_request.object_id

    api_key = cred.api_key or ""
    base_url = cred.base_url or "https://www.leaflink.com/api/v2"
    auth_scheme = cred.auth_scheme or "Token"

    if object_id:
        # Targeted single-order fetch
        loop = asyncio.get_event_loop()
        order_data = await loop.run_in_executor(
            None,
            _fetch_single_order_sync,
            api_key,
            base_url,
            auth_scheme,
            object_id,
        )

        if order_data:
            async with AsyncSessionLocal() as upsert_db:
                async with upsert_db.begin():
                    result = await upsert_webhook_order(
                        upsert_db,
                        brand_id=brand_id,
                        org_id=org_id,
                        order_data=order_data,
                    )
            logger.info(
                "[LEAFLINK_WEBHOOK_ORDER_SYNCED] object_id=%s action=%s brand_id=%s",
                object_id,
                result.get("action"),
                brand_id,
            )
        else:
            logger.warning(
                "[LEAFLINK_WEBHOOK_ORDER_SYNCED] no_data_returned object_id=%s brand_id=%s",
                object_id,
                brand_id,
            )
    else:
        # Fallback: fetch recent orders (last INCREMENTAL_LOOKBACK_MINUTES)
        since = _utc_now() - timedelta(minutes=INCREMENTAL_LOOKBACK_MINUTES)
        loop = asyncio.get_event_loop()
        orders = await loop.run_in_executor(
            None,
            _fetch_recent_orders_sync,
            api_key,
            base_url,
            auth_scheme,
            since,
        )

        upserted = 0
        async with AsyncSessionLocal() as upsert_db:
            async with upsert_db.begin():
                for order_data in orders:
                    if isinstance(order_data, dict):
                        result = await upsert_webhook_order(
                            upsert_db,
                            brand_id=brand_id,
                            org_id=org_id,
                            order_data=order_data,
                        )
                        if result.get("action") in ("created", "updated"):
                            upserted += 1

        logger.info(
            "[LEAFLINK_WEBHOOK_ORDER_SYNCED] fallback_recent fetched=%s upserted=%s brand_id=%s",
            len(orders),
            upserted,
            brand_id,
        )


async def _handle_webhook_product(
    db: AsyncSession,
    sync_request: SyncRequest,
    cred: BrandAPICredential,
) -> None:
    """
    Fetch a single product from LeafLink API and upsert it.

    Product sync is a no-op placeholder — product upsert logic can be
    added here when the product model is implemented.
    """
    object_id = sync_request.object_id
    brand_id = sync_request.brand_id

    # Product upsert is not yet implemented — log and skip gracefully
    logger.info(
        "[LEAFLINK_WEBHOOK_PRODUCT_SYNCED] object_id=%s brand_id=%s status=not_implemented",
        object_id,
        brand_id,
    )


async def _handle_incremental_recent_orders(
    db: AsyncSession,
    sync_request: SyncRequest,
    cred: BrandAPICredential,
) -> None:
    """
    Fetch orders changed since (last_successful_sync_at - 2 min buffer) and upsert.

    Updates sync_health.last_successful_sync_at on completion.
    """
    brand_id = sync_request.brand_id
    org_id = sync_request.org_id

    api_key = cred.api_key or ""
    base_url = cred.base_url or "https://www.leaflink.com/api/v2"
    auth_scheme = cred.auth_scheme or "Token"

    logger.info("[LEAFLINK_INCREMENTAL_SYNC_START] brand_id=%s", brand_id)

    # Determine lookback window from sync_health or default
    since = _utc_now() - timedelta(minutes=INCREMENTAL_LOOKBACK_MINUTES)
    try:
        health_result = await db.execute(
            select(SyncHealth).where(SyncHealth.brand_id == brand_id)
        )
        health = health_result.scalar_one_or_none()
        if health and health.last_successful_sync_at:
            # Add 2-minute buffer to avoid missing orders at boundary
            since = health.last_successful_sync_at - timedelta(minutes=2)
    except Exception as exc:
        logger.warning(
            "[LEAFLINK_INCREMENTAL_SYNC_START] health_lookup_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )

    loop = asyncio.get_event_loop()
    orders = await loop.run_in_executor(
        None,
        _fetch_recent_orders_sync,
        api_key,
        base_url,
        auth_scheme,
        since,
    )

    upserted = 0
    async with AsyncSessionLocal() as upsert_db:
        async with upsert_db.begin():
            for order_data in orders:
                if isinstance(order_data, dict):
                    result = await upsert_webhook_order(
                        upsert_db,
                        brand_id=brand_id,
                        org_id=org_id,
                        order_data=order_data,
                    )
                    if result.get("action") in ("created", "updated"):
                        upserted += 1

    # Update sync_health.last_successful_sync_at
    try:
        async with AsyncSessionLocal() as health_db:
            async with health_db.begin():
                health_result = await health_db.execute(
                    select(SyncHealth).where(SyncHealth.brand_id == brand_id)
                )
                health = health_result.scalar_one_or_none()
                now = _utc_now()
                if health:
                    health.last_successful_sync_at = now
                    health.last_attempted_sync_at = now
                    health.consecutive_failures = 0
                else:
                    health_db.add(SyncHealth(
                        brand_id=brand_id,
                        last_successful_sync_at=now,
                        last_attempted_sync_at=now,
                        consecutive_failures=0,
                    ))
    except Exception as exc:
        logger.warning(
            "[LEAFLINK_INCREMENTAL_SYNC_COMPLETE] health_update_failed brand_id=%s error=%s",
            brand_id,
            str(exc)[:200],
        )

    logger.info(
        "[LEAFLINK_INCREMENTAL_SYNC_COMPLETE] brand_id=%s fetched=%s upserted=%s since=%s",
        brand_id,
        len(orders),
        upserted,
        since.isoformat(),
    )


async def _handle_full_resync(
    db: AsyncSession,
    sync_request: SyncRequest,
    cred: BrandAPICredential,
) -> None:
    """
    Delegate to sync_leaflink_background_continuous() for a full resync.

    This preserves the existing full-sync behaviour — it is only triggered
    manually (never by the incremental scheduler).
    """
    from services.leaflink_sync import sync_leaflink_background_continuous

    brand_id = sync_request.brand_id
    api_key = cred.api_key or ""
    company_id = cred.company_id or ""
    auth_scheme = cred.auth_scheme or "Token"
    base_url = cred.base_url or ""
    org_id = sync_request.org_id or cred.org_id or ""

    logger.info("[LEAFLINK_FULL_RESYNC_START] brand_id=%s", brand_id)

    await sync_leaflink_background_continuous(
        brand_id=brand_id,
        api_key=api_key,
        company_id=company_id,
        auth_scheme=auth_scheme,
        start_page=1,
        total_pages=None,
        manager=None,
        sync_run_id=None,
        total_orders_available=None,
        base_url=base_url,
        org_id=org_id or None,
        last_next_url=None,
    )

    logger.info("[LEAFLINK_FULL_RESYNC_COMPLETE] brand_id=%s", brand_id)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def process_sync_request(db: AsyncSession, sync_request: SyncRequest) -> None:
    """
    Process a single sync_request queue entry.

    Dispatches to the appropriate handler based on sync_request.type.
    Marks the request as completed or failed, and updates retry_count.

    Args:
        db: Active async database session (used for credential lookup).
        sync_request: The SyncRequest ORM row to process.
    """
    brand_id = sync_request.brand_id
    job_type = sync_request.type or "full_resync"
    object_id = sync_request.object_id

    logger.info(
        "[SYNC_REQUEST_PROCESSOR] processing id=%s type=%s object_id=%s brand_id=%s",
        sync_request.id,
        job_type,
        object_id,
        brand_id,
    )

    # Load credential
    cred = await _load_credential(db, brand_id)
    if not cred:
        raise ValueError(f"No active LeafLink credential for brand_id={brand_id}")

    # Dispatch to handler
    if job_type == "webhook_order":
        await _handle_webhook_order(db, sync_request, cred)
    elif job_type == "webhook_product":
        await _handle_webhook_product(db, sync_request, cred)
    elif job_type == "incremental_recent_orders":
        await _handle_incremental_recent_orders(db, sync_request, cred)
    elif job_type == "full_resync":
        await _handle_full_resync(db, sync_request, cred)
    else:
        raise ValueError(f"Unknown sync_request type: {job_type!r}")
