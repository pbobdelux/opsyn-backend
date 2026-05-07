"""
LeafLink diagnostic endpoints — raw API inspection and dead-letter reprocessing.

GET  /diagnostics/leaflink/orders-raw?brand_id=<brand_id>
POST /diagnostics/leaflink/reprocess-dead-letters?brand_id=<brand_id>
POST /diagnostics/leaflink/reprocess-order/{order_id}?brand_id=<brand_id>
POST /diagnostics/leaflink/backfill-normalized-dates?brand_id=<brand_id>

Use orders-raw to inspect exactly what LeafLink returns for a given brand's credentials,
including the final URL, HTTP status, query params, pagination fields, and raw body.
No data is upserted or transformed — purely observational.

Use reprocess-dead-letters to retry all unresolved dead-letter records for a brand.
Use reprocess-order to retry a specific order by its LeafLink external_id.
Use backfill-normalized-dates to re-parse LeafLink dates in existing orders.
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential
from services.leaflink_client import LeafLinkClient

logger = logging.getLogger("diagnostics")

router = APIRouter(prefix="/diagnostics", tags=["diagnostics"])

# Thread pool for running synchronous LeafLink HTTP calls off the event loop
_diag_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="leaflink-diag")


def _call_list_orders_raw(client: LeafLinkClient, limit: int = 10) -> dict[str, Any]:
    """
    Call LeafLinkClient.list_orders() with no date filters and return a structured
    diagnostic dict containing the raw response, status, params, and pagination info.

    This runs synchronously inside a thread pool executor so it doesn't block the
    async event loop.
    """
    # Build the final URL the same way list_orders() does, for reporting
    _orders_path = "orders-received/"
    _final_url = f"{client.base_url.rstrip('/')}/{_orders_path}"
    _params_sent = {"limit": limit, "offset": 0}

    logger.info(
        "[LEAFLINK_DIAGNOSTIC] calling_list_orders brand_id=%s final_url=%s params=%s",
        client.brand_id,
        _final_url,
        _params_sent,
    )

    try:
        # Call with skip_date_filters=True so we get the raw unfiltered response
        data = client.list_orders(
            limit=limit,
            offset=0,
            skip_date_filters=True,
        )
    except RuntimeError as exc:
        # list_orders raises RuntimeError on non-200 responses
        return {
            "ok": False,
            "final_url": _final_url,
            "params": _params_sent,
            "error": str(exc)[:500],
            "response_body": None,
        }

    # Determine response type and extract pagination fields
    if isinstance(data, list):
        response_type = "list"
        parsed_count = len(data)
        pagination = {"count": None, "next": None, "previous": None}
        response_body = data
    elif isinstance(data, dict):
        response_type = "dict"
        results = (
            data.get("results")
            or data.get("data")
            or data.get("orders")
            or []
        )
        parsed_count = len(results) if isinstance(results, list) else 0
        pagination = {
            "count": data.get("count"),
            "next": data.get("next"),
            "previous": data.get("previous"),
        }
        response_body = data
    else:
        response_type = "other"
        parsed_count = 0
        pagination = {"count": None, "next": None, "previous": None}
        response_body = str(data)

    # Build a text preview of the response body (first 1000 chars of JSON repr)
    import json as _json
    try:
        _preview = _json.dumps(response_body, default=str)[:1000]
    except Exception:
        _preview = str(response_body)[:1000]

    return {
        "ok": True,
        "final_url": _final_url,
        "params": _params_sent,
        "response_type": response_type,
        "response_preview": _preview,
        "parsed_count": parsed_count,
        "pagination": pagination,
        "response_body": response_body,
    }


@router.get("/leaflink/orders-raw")
async def diagnostic_leaflink_orders_raw(
    brand_id: str = Query(..., description="Brand ID to load LeafLink credentials for"),
    limit: int = Query(10, ge=1, le=100, description="Max orders to fetch (default 10)"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Fetch raw LeafLink orders for a brand WITHOUT any transformation or DB upsert.

    Returns the exact response from LeafLink including:
    - The final URL called
    - HTTP status code (logged; errors surface as ok=false)
    - Query parameters sent
    - Response type (dict | list | other)
    - First 1000 chars of the response body
    - Pagination fields (count, next, previous)
    - Full response body

    Date filters are intentionally omitted so the call tests the broadest possible
    query — if this returns orders but the sync returns zero, date filters are the
    likely culprit.
    """
    logger.info(
        "[LEAFLINK_DIAGNOSTIC] request brand_id=%s limit=%s",
        brand_id,
        limit,
    )

    # Load the active LeafLink credential for this brand
    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
    )
    cred = result.scalar_one_or_none()

    if cred is None:
        logger.warning(
            "[LEAFLINK_DIAGNOSTIC] no_credential_found brand_id=%s",
            brand_id,
        )
        raise HTTPException(
            status_code=404,
            detail=f"No active LeafLink credential found for brand_id={brand_id}",
        )

    logger.info(
        "[LEAFLINK_DIAGNOSTIC] credential_found brand_id=%s company_id=%s"
        " base_url=%s auth_scheme=%s",
        brand_id,
        cred.company_id or "none",
        cred.base_url or "none",
        cred.auth_scheme or "Token (default)",
    )

    # Instantiate the client — this validates the credential and logs base_url/auth_scheme
    try:
        client = LeafLinkClient(
            api_key=cred.api_key,
            company_id=cred.company_id,
            brand_id=brand_id,
            base_url=cred.base_url,
            auth_scheme=cred.auth_scheme or "Token",
        )
    except Exception as client_exc:
        logger.error(
            "[LEAFLINK_DIAGNOSTIC] client_init_failed brand_id=%s error=%s",
            brand_id,
            str(client_exc)[:300],
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialise LeafLink client: {str(client_exc)[:300]}",
        )

    # Run the synchronous HTTP call in a thread pool so we don't block the event loop
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        diag_result = await loop.run_in_executor(
            _diag_executor,
            lambda: _call_list_orders_raw(client, limit=limit),
        )
    except Exception as exc:
        logger.error(
            "[LEAFLINK_DIAGNOSTIC] executor_error brand_id=%s error=%s",
            brand_id,
            str(exc)[:300],
        )
        raise HTTPException(
            status_code=500,
            detail=f"Diagnostic call failed: {str(exc)[:300]}",
        )

    _endpoint_called = diag_result.get("final_url", "unknown")
    _status = "ok" if diag_result.get("ok") else "error"

    logger.info(
        "[LEAFLINK_DIAGNOSTIC] brand_id=%s endpoint_called=%s status=%s"
        " parsed_count=%s response_type=%s",
        brand_id,
        _endpoint_called,
        _status,
        diag_result.get("parsed_count", 0),
        diag_result.get("response_type", "unknown"),
    )

    return {
        "ok": diag_result.get("ok", False),
        "brand_id": brand_id,
        "company_id": cred.company_id,
        "final_url": diag_result.get("final_url"),
        "params": diag_result.get("params"),
        "response_type": diag_result.get("response_type"),
        "response_preview": diag_result.get("response_preview"),
        "parsed_count": diag_result.get("parsed_count", 0),
        "pagination": diag_result.get("pagination"),
        "response_body": diag_result.get("response_body"),
        "error": diag_result.get("error"),
    }


# ---------------------------------------------------------------------------
# Dead-letter reprocessing endpoints
# ---------------------------------------------------------------------------


@router.post("/leaflink/reprocess-dead-letters")
async def reprocess_dead_letters(
    brand_id: str = Query(..., description="Brand ID to reprocess dead letters for"),
    limit: int = Query(100, ge=1, le=1000, description="Max records to reprocess per call"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Retry all unresolved dead-letter records for a brand.

    Reads raw_payload from sync_dead_letters, attempts to re-insert each order
    via sync_leaflink_orders(), and marks successfully reprocessed records as
    resolved (resolved_at = NOW()).

    Returns counts of attempted, succeeded, and failed reprocessing attempts.

    Logs:
      [REPROCESS_STARTED]  — when reprocessing begins
      [REPROCESS_COMPLETE] — when all records have been attempted
    """
    from database import AsyncSessionLocal
    from services.leaflink_sync import sync_leaflink_orders

    logger.info(
        "[REPROCESS_STARTED] brand_id=%s limit=%s source=reprocess-dead-letters",
        brand_id,
        limit,
    )

    now = datetime.now(timezone.utc)

    # Fetch unresolved dead-letter records for this brand
    try:
        result = await db.execute(
            text("""
                SELECT id, external_id, order_number, raw_payload, error_stage, retry_count
                FROM sync_dead_letters
                WHERE brand_id = CAST(:brand_id AS uuid)
                  AND resolved_at IS NULL
                ORDER BY created_at ASC
                LIMIT :limit
            """),
            {"brand_id": brand_id, "limit": limit},
        )
        rows = result.fetchall()
    except Exception as exc:
        logger.error(
            "[REPROCESS_ERROR] brand_id=%s error=%s",
            brand_id,
            str(exc)[:300],
        )
        raise HTTPException(status_code=500, detail=f"Failed to fetch dead letters: {str(exc)[:300]}")

    if not rows:
        logger.info("[REPROCESS_COMPLETE] brand_id=%s attempted=0 succeeded=0 failed=0 reason=no_unresolved_records", brand_id)
        return {
            "ok": True,
            "brand_id": brand_id,
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "message": "No unresolved dead-letter records found",
        }

    attempted = 0
    succeeded = 0
    failed = 0
    failed_ids: list[int] = []

    for row in rows:
        record_id = row[0]
        external_id = row[1]
        raw_payload = row[3]
        retry_count = row[5]
        attempted += 1

        try:
            # Attempt to re-insert the order using the resilient sync function
            async with AsyncSessionLocal() as _reprocess_db:
                async with _reprocess_db.begin():
                    reprocess_result = await sync_leaflink_orders(
                        _reprocess_db,
                        brand_id=brand_id,
                        orders=[raw_payload] if isinstance(raw_payload, dict) else [],
                        pages_fetched=0,
                    )

            inserted = reprocess_result.get("inserted_count", 0)
            updated = reprocess_result.get("updated_count", 0)

            if inserted > 0 or updated > 0:
                # Mark as resolved
                await db.execute(
                    text("""
                        UPDATE sync_dead_letters
                        SET resolved_at = :now,
                            retry_count = retry_count + 1
                        WHERE id = :id
                    """),
                    {"now": now, "id": record_id},
                )
                await db.commit()
                succeeded += 1
                logger.info(
                    "[REPROCESS_ORDER_SUCCESS] brand_id=%s external_id=%s record_id=%s",
                    brand_id,
                    external_id,
                    record_id,
                )
            else:
                # Increment retry count but leave unresolved
                await db.execute(
                    text("""
                        UPDATE sync_dead_letters
                        SET retry_count = retry_count + 1
                        WHERE id = :id
                    """),
                    {"id": record_id},
                )
                await db.commit()
                failed += 1
                failed_ids.append(record_id)
                logger.warning(
                    "[REPROCESS_ORDER_FAILED] brand_id=%s external_id=%s record_id=%s reason=no_rows_written",
                    brand_id,
                    external_id,
                    record_id,
                )

        except Exception as exc:
            failed += 1
            failed_ids.append(record_id)
            logger.error(
                "[REPROCESS_ORDER_ERROR] brand_id=%s external_id=%s record_id=%s error=%s",
                brand_id,
                external_id,
                record_id,
                str(exc)[:300],
            )
            try:
                await db.rollback()
            except Exception:
                pass

    logger.info(
        "[REPROCESS_COMPLETE] brand_id=%s attempted=%s succeeded=%s failed=%s",
        brand_id,
        attempted,
        succeeded,
        failed,
    )

    return {
        "ok": True,
        "warning": failed > 0,
        "brand_id": brand_id,
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "failed_record_ids": failed_ids[:20],
        "message": f"Reprocessed {attempted} dead-letter records: {succeeded} succeeded, {failed} failed",
    }


@router.post("/leaflink/reprocess-order/{order_id}")
async def reprocess_order(
    order_id: str,
    brand_id: str = Query(..., description="Brand ID that owns the order"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Retry a specific order by its LeafLink external_id (order_id path param).

    Looks up the most recent unresolved dead-letter record for this external_id,
    attempts to re-insert it, and marks it resolved on success.

    Logs:
      [REPROCESS_STARTED]  — when reprocessing begins
      [REPROCESS_COMPLETE] — when the attempt is done
    """
    from database import AsyncSessionLocal
    from services.leaflink_sync import sync_leaflink_orders

    logger.info(
        "[REPROCESS_STARTED] brand_id=%s external_id=%s source=reprocess-order",
        brand_id,
        order_id,
    )

    now = datetime.now(timezone.utc)

    # Fetch the most recent unresolved dead-letter record for this order
    try:
        result = await db.execute(
            text("""
                SELECT id, external_id, order_number, raw_payload, error_stage, retry_count
                FROM sync_dead_letters
                WHERE brand_id = CAST(:brand_id AS uuid)
                  AND external_id = :external_id
                  AND resolved_at IS NULL
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"brand_id": brand_id, "external_id": order_id},
        )
        row = result.fetchone()
    except Exception as exc:
        logger.error(
            "[REPROCESS_ERROR] brand_id=%s external_id=%s error=%s",
            brand_id,
            order_id,
            str(exc)[:300],
        )
        raise HTTPException(status_code=500, detail=f"Failed to fetch dead letter: {str(exc)[:300]}")

    if not row:
        logger.info(
            "[REPROCESS_COMPLETE] brand_id=%s external_id=%s attempted=0 reason=no_unresolved_record",
            brand_id,
            order_id,
        )
        return {
            "ok": True,
            "brand_id": brand_id,
            "external_id": order_id,
            "attempted": False,
            "message": f"No unresolved dead-letter record found for external_id={order_id}",
        }

    record_id = row[0]
    raw_payload = row[3]

    try:
        async with AsyncSessionLocal() as _reprocess_db:
            async with _reprocess_db.begin():
                reprocess_result = await sync_leaflink_orders(
                    _reprocess_db,
                    brand_id=brand_id,
                    orders=[raw_payload] if isinstance(raw_payload, dict) else [],
                    pages_fetched=0,
                )

        inserted = reprocess_result.get("inserted_count", 0)
        updated = reprocess_result.get("updated_count", 0)

        if inserted > 0 or updated > 0:
            await db.execute(
                text("""
                    UPDATE sync_dead_letters
                    SET resolved_at = :now,
                        retry_count = retry_count + 1
                    WHERE id = :id
                """),
                {"now": now, "id": record_id},
            )
            await db.commit()
            logger.info(
                "[REPROCESS_COMPLETE] brand_id=%s external_id=%s record_id=%s succeeded=true",
                brand_id,
                order_id,
                record_id,
            )
            return {
                "ok": True,
                "brand_id": brand_id,
                "external_id": order_id,
                "record_id": record_id,
                "attempted": True,
                "succeeded": True,
                "inserted": inserted,
                "updated": updated,
                "message": f"Order {order_id} reprocessed successfully",
            }
        else:
            await db.execute(
                text("UPDATE sync_dead_letters SET retry_count = retry_count + 1 WHERE id = :id"),
                {"id": record_id},
            )
            await db.commit()
            logger.warning(
                "[REPROCESS_COMPLETE] brand_id=%s external_id=%s record_id=%s succeeded=false reason=no_rows_written",
                brand_id,
                order_id,
                record_id,
            )
            return {
                "ok": False,
                "brand_id": brand_id,
                "external_id": order_id,
                "record_id": record_id,
                "attempted": True,
                "succeeded": False,
                "message": f"Order {order_id} reprocessing attempted but no rows were written",
                "sync_result": reprocess_result,
            }

    except Exception as exc:
        err_msg = str(exc)[:300]
        logger.error(
            "[REPROCESS_COMPLETE] brand_id=%s external_id=%s record_id=%s succeeded=false error=%s",
            brand_id,
            order_id,
            record_id,
            err_msg,
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "brand_id": brand_id,
            "external_id": order_id,
            "record_id": record_id,
            "attempted": True,
            "succeeded": False,
            "error": err_msg,
            "message": f"Order {order_id} reprocessing failed: {err_msg}",
        }


@router.post("/leaflink/backfill-normalized-dates")
async def backfill_normalized_dates(
    brand_id: str = Query(..., description="Brand ID to backfill dates for"),
    limit: int = Query(500, ge=1, le=5000, description="Max orders to process per call"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Re-parse LeafLink dates in existing orders that have NULL external timestamps.

    Reads raw_payload from orders where external_created_at or external_updated_at
    is NULL, attempts to parse the LeafLink date fields, and updates the order row
    if valid dates are found.

    This is a backfill operation for orders that were ingested before date parsing
    was implemented, or where date parsing previously failed.

    Logs:
      [REPROCESS_STARTED]  — when backfill begins
      [REPROCESS_COMPLETE] — when all records have been attempted
    """
    from services.leaflink_sync import normalize_datetime, ensure_utc

    logger.info(
        "[REPROCESS_STARTED] brand_id=%s limit=%s source=backfill-normalized-dates",
        brand_id,
        limit,
    )

    now = datetime.now(timezone.utc)

    # Fetch orders with NULL external timestamps that have raw_payload
    try:
        result = await db.execute(
            text("""
                SELECT id, external_order_id, raw_payload
                FROM orders
                WHERE brand_id = CAST(:brand_id AS uuid)
                  AND raw_payload IS NOT NULL
                  AND (external_created_at IS NULL OR external_updated_at IS NULL)
                ORDER BY id ASC
                LIMIT :limit
            """),
            {"brand_id": brand_id, "limit": limit},
        )
        rows = result.fetchall()
    except Exception as exc:
        logger.error(
            "[REPROCESS_ERROR] brand_id=%s source=backfill-normalized-dates error=%s",
            brand_id,
            str(exc)[:300],
        )
        raise HTTPException(status_code=500, detail=f"Failed to fetch orders: {str(exc)[:300]}")

    if not rows:
        logger.info(
            "[REPROCESS_COMPLETE] brand_id=%s attempted=0 succeeded=0 source=backfill-normalized-dates reason=no_orders_with_null_dates",
            brand_id,
        )
        return {
            "ok": True,
            "brand_id": brand_id,
            "attempted": 0,
            "succeeded": 0,
            "skipped": 0,
            "message": "No orders with NULL external timestamps found",
        }

    attempted = 0
    succeeded = 0
    skipped = 0

    for row in rows:
        order_id = row[0]
        external_order_id = row[1]
        raw_payload = row[2]
        attempted += 1

        if not isinstance(raw_payload, dict):
            skipped += 1
            continue

        # Try to parse LeafLink date fields from raw_payload
        created_raw = (
            raw_payload.get("created")
            or raw_payload.get("created_at")
            or raw_payload.get("external_created_at")
        )
        modified_raw = (
            raw_payload.get("modified")
            or raw_payload.get("updated")
            or raw_payload.get("updated_at")
            or raw_payload.get("external_updated_at")
        )

        ext_created = ensure_utc(normalize_datetime(created_raw))
        ext_updated = ensure_utc(normalize_datetime(modified_raw))

        if ext_created is None and ext_updated is None:
            skipped += 1
            continue

        try:
            await db.execute(
                text("""
                    UPDATE orders
                    SET external_created_at = COALESCE(external_created_at, :ext_created),
                        external_updated_at = COALESCE(external_updated_at, :ext_updated),
                        updated_at = :now
                    WHERE id = :order_id
                """),
                {
                    "ext_created": ext_created,
                    "ext_updated": ext_updated,
                    "now": now,
                    "order_id": order_id,
                },
            )
            await db.commit()
            succeeded += 1
        except Exception as exc:
            logger.error(
                "[REPROCESS_DATE_ERROR] brand_id=%s order_id=%s external_order_id=%s error=%s",
                brand_id,
                order_id,
                external_order_id,
                str(exc)[:200],
            )
            try:
                await db.rollback()
            except Exception:
                pass
            skipped += 1

    logger.info(
        "[REPROCESS_COMPLETE] brand_id=%s attempted=%s succeeded=%s skipped=%s source=backfill-normalized-dates",
        brand_id,
        attempted,
        succeeded,
        skipped,
    )

    return {
        "ok": True,
        "brand_id": brand_id,
        "attempted": attempted,
        "succeeded": succeeded,
        "skipped": skipped,
        "message": f"Backfilled dates for {succeeded}/{attempted} orders ({skipped} skipped — no parseable dates in raw_payload)",
    }
