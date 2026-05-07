"""
Admin recovery endpoints for LeafLink sync operations.

Provides:
  - POST /orders/sync/leaflink          — manually trigger a sync for a brand
  - GET  /orders/sync/leaflink/status   — get sync health state for a brand
  - GET  /orders/sync/leaflink/dead-letter — list dead-lettered line items
  - POST /orders/sync/leaflink/replay-dead-letter — replay a dead-lettered item
  - POST /orders/sync/recover           — retry failed/retryable sync runs
"""

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, has_column
from models import Order, SyncRequest, SyncRun
from models.sync_health import DeadLetterLineItem, SyncHealth
from services.leaflink_sync import (
    ensure_utc,
    normalize_datetime_fields,
    normalize_uuid_fields,
    safe_uuid_for_db,
    safe_uuid_mapped_product,
    sanitize_sql_params,
)
from utils.json_utils import make_json_safe

logger = logging.getLogger("sync_routes")

router = APIRouter(prefix="/orders/sync", tags=["sync"])


@router.get("/leaflink/status")
async def get_sync_status(
    brand_id: str = Query(..., description="Brand ID to query sync health for"),
    db: AsyncSession = Depends(get_db),
):
    """Get sync health status for a brand.

    Returns the last successful sync time, consecutive failure count, and
    running totals for orders and line items synced.
    """
    result = await db.execute(
        select(SyncHealth).where(SyncHealth.brand_id == brand_id)
    )
    health = result.scalar_one_or_none()

    if health is None:
        return {
            "ok": True,
            "brand_id": brand_id,
            "error": "No sync history for this brand",
            "health": None,
        }

    return {
        "ok": True,
        "brand_id": brand_id,
        "health": {
            "id": health.id,
            "brand_id": health.brand_id,
            "last_successful_sync_at": health.last_successful_sync_at.isoformat() if health.last_successful_sync_at else None,
            "last_attempted_sync_at": health.last_attempted_sync_at.isoformat() if health.last_attempted_sync_at else None,
            "last_error": health.last_error,
            "consecutive_failures": health.consecutive_failures,
            "last_page_synced": health.last_page_synced,
            "total_orders_synced": health.total_orders_synced,
            "total_line_items_synced": health.total_line_items_synced,
            "created_at": health.created_at.isoformat() if health.created_at else None,
            "updated_at": health.updated_at.isoformat() if health.updated_at else None,
        },
    }


@router.get("/leaflink/dead-letter")
async def get_dead_letter_items(
    brand_id: str = Query(..., description="Brand ID to query dead-letter items for"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of items to return"),
    db: AsyncSession = Depends(get_db),
):
    """Get dead-lettered line items for a brand.

    Returns line items that failed insertion after max retries, ordered by
    most recently failed first.
    """
    result = await db.execute(
        select(DeadLetterLineItem)
        .where(DeadLetterLineItem.brand_id == brand_id)
        .order_by(DeadLetterLineItem.last_failed_at.desc())
        .limit(limit)
    )
    items = result.scalars().all()

    return {
        "ok": True,
        "brand_id": brand_id,
        "count": len(items),
        "items": [
            {
                "id": item.id,
                "brand_id": item.brand_id,
                "external_order_id": item.external_order_id,
                "order_id": item.order_id,
                "sku": item.sku,
                "product_name": item.product_name,
                "failure_reason": item.failure_reason,
                "failure_count": item.failure_count,
                "last_failed_at": item.last_failed_at.isoformat() if item.last_failed_at else None,
                "created_at": item.created_at.isoformat() if item.created_at else None,
            }
            for item in items
        ],
    }


@router.post("/leaflink/replay-dead-letter")
async def replay_dead_letter_item(
    item_id: int = Query(..., description="ID of the dead-letter item to replay"),
    db: AsyncSession = Depends(get_db),
):
    """Attempt to re-insert a dead-lettered line item.

    Looks up the dead-letter record, tries to insert the line item into
    order_lines using the idempotent upsert, and removes the dead-letter
    record on success.
    """
    import json
    from datetime import datetime, timezone

    result = await db.execute(
        select(DeadLetterLineItem).where(DeadLetterLineItem.id == item_id)
    )
    item = result.scalar_one_or_none()

    if item is None:
        raise HTTPException(status_code=404, detail=f"Dead-letter item {item_id} not found")

    if item.order_id is None:
        return {
            "ok": False,
            "error": "Cannot replay: parent order_id is NULL (order was never found in DB)",
            "item_id": item_id,
        }

    # Build idempotent upsert for this single item
    optional_columns = ["packed_qty", "unit_price_cents", "total_price_cents"]
    enabled_columns = {col: has_column("order_lines", col) for col in optional_columns}

    insert_columns = [
        "order_id", "sku", "product_name", "quantity",
        "unit_price", "total_price", "mapped_product_id",
        "mapping_status", "mapping_issue", "raw_payload",
        "created_at", "updated_at",
    ]
    for col in optional_columns:
        if enabled_columns.get(col, False):
            insert_columns.append(col)

    # UUID columns that require explicit CAST in the SQL VALUES clause so
    # PostgreSQL never infers the parameter type as character varying.
    _uuid_columns = {"mapped_product_id"}

    columns_str = ", ".join(insert_columns)
    placeholders = ", ".join(
        f"CAST(:{col} AS UUID)" if col in _uuid_columns else f":{col}"
        for col in insert_columns
    )
    logger.info(
        "[UUID_SQL_CAST_APPLIED] statement=replay_dead_letter_item columns=%s",
        ",".join(col for col in insert_columns if col in _uuid_columns),
    )

    update_set_clauses = [
        "quantity = EXCLUDED.quantity",
        "unit_price = EXCLUDED.unit_price",
        "total_price = EXCLUDED.total_price",
        "mapped_product_id = EXCLUDED.mapped_product_id",
        "mapping_status = EXCLUDED.mapping_status",
        "mapping_issue = EXCLUDED.mapping_issue",
        "raw_payload = EXCLUDED.raw_payload",
        "updated_at = EXCLUDED.updated_at",
    ]
    if enabled_columns.get("unit_price_cents", False):
        update_set_clauses.append("unit_price_cents = EXCLUDED.unit_price_cents")
    if enabled_columns.get("total_price_cents", False):
        update_set_clauses.append("total_price_cents = EXCLUDED.total_price_cents")

    update_set_str = ",\n        ".join(update_set_clauses)

    upsert_stmt = f"""
        INSERT INTO public.order_lines ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (order_id, sku, product_name)
        WHERE sku IS NOT NULL AND product_name IS NOT NULL
        DO UPDATE SET
        {update_set_str}
    """

    now = ensure_utc(datetime.now(timezone.utc), "created_at")

    # Extract values from raw_payload if available
    raw = item.raw_payload or {}

    # Coerce org_id and brand_id from raw payload to valid UUIDs or None.
    # These may be present in the raw payload and must not be passed as arbitrary
    # strings to UUID columns, which would cause PostgreSQL type errors.
    org_id = safe_uuid_for_db(raw.get("org_id"), "org_id")
    brand_id = safe_uuid_for_db(raw.get("brand_id") or item.brand_id, "brand_id")
    logger.info(
        "[REPLAY_UUID_COERCE] item_id=%s org_id=%s brand_id=%s",
        item_id,
        org_id,
        brand_id,
    )

    # Re-coerce immediately before SQL params — belt-and-suspenders guard
    _sql_org_id = safe_uuid_for_db(org_id, "org_id")
    _sql_brand_id = safe_uuid_for_db(brand_id, "brand_id")
    logger.info(
        "[ORG_ID_BEFORE_SQL] field=org_id value=%s item_id=%s function=replay_dead_letter_item",
        _sql_org_id,
        item_id,
    )
    logger.info(
        "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s item_id=%s function=replay_dead_letter_item",
        _sql_brand_id,
        item_id,
    )

    _mapped_product_id_raw = raw.get("mapped_product_id")
    logger.error(
        "[MAPPED_PRODUCT_ID_BEFORE_SQL] value=%s type=%s",
        _mapped_product_id_raw,
        type(_mapped_product_id_raw),
    )
    params: dict = {
        "order_id": item.order_id,
        "sku": item.sku or "unknown",
        "product_name": item.product_name,
        "quantity": raw.get("quantity", 0),
        "unit_price": raw.get("unit_price"),
        "total_price": raw.get("total_price"),
        "mapped_product_id": safe_uuid_for_db(_mapped_product_id_raw, "mapped_product_id"),
        "mapping_status": raw.get("mapping_status", "unknown"),
        "mapping_issue": raw.get("mapping_issue"),
        "raw_payload": json.dumps(make_json_safe(raw)) if raw else None,
        "created_at": now,
        "updated_at": now,
    }
    if enabled_columns.get("packed_qty", False):
        params["packed_qty"] = 0
    if enabled_columns.get("unit_price_cents", False):
        params["unit_price_cents"] = raw.get("unit_price_cents")
    if enabled_columns.get("total_price_cents", False):
        params["total_price_cents"] = raw.get("total_price_cents")

    try:
        # FINAL validation — enforce coercion directly into params dict
        # immediately before execute() so no mutation after coercion can slip through.
        params["org_id"] = safe_uuid_for_db(params.get("org_id"), "org_id")
        params["brand_id"] = safe_uuid_for_db(params.get("brand_id"), "brand_id")
        params["mapped_product_id"] = safe_uuid_mapped_product(params.get("mapped_product_id"))
        assert (
            params.get("mapped_product_id") is None
            or isinstance(params.get("mapped_product_id"), (str, uuid.UUID))
        ), (
            f"mapped_product_id must be None, str, or UUID, "
            f"got {type(params.get('mapped_product_id'))}"
        )
        logger.error(
            "[FINAL_SQL_PARAMS] org_id=%s org_type=%s brand_id=%s brand_type=%s"
            " mapped_product_id=%s mapped_type=%s item_id=%s function=replay_dead_letter_item",
            params.get("org_id"),
            type(params.get("org_id")).__name__,
            params.get("brand_id"),
            type(params.get("brand_id")).__name__,
            params.get("mapped_product_id"),
            type(params.get("mapped_product_id")).__name__,
            item_id,
        )
        logger.error(
            "[FINAL_SQL_PARAMS_AUDIT] mapped_product_id=%s type=%s is_none=%s is_str=%s is_uuid=%s"
            " function=replay_dead_letter_item",
            params.get("mapped_product_id"),
            type(params.get("mapped_product_id")).__name__,
            params.get("mapped_product_id") is None,
            isinstance(params.get("mapped_product_id"), str),
            isinstance(params.get("mapped_product_id"), uuid.UUID),
        )
        logger.info("[ORG_ID_BEFORE_SQL] org_id=%s", org_id)
        logger.info("[BRAND_ID_BEFORE_SQL] brand_id=%s", brand_id)
        logger.info(
            "[REPLAY_UPSERT_ATTEMPT] item_id=%s order_id=%s sku=%s product_name=%s",
            item_id,
            params.get("order_id"),
            params.get("sku"),
            params.get("product_name"),
        )
        # Sanitize SQL params: remove provider dates, keep only operational timestamps
        params = sanitize_sql_params(params)
        params = normalize_uuid_fields(params)
        params = normalize_datetime_fields(params)
        await db.execute(text(upsert_stmt), params)
        await db.commit()
        logger.info(
            "[REPLAY_UPSERT_SUCCESS] item_id=%s order_id=%s sku=%s",
            item_id,
            params.get("order_id"),
            params.get("sku"),
        )

        # Remove from dead letter on success
        await db.execute(
            delete(DeadLetterLineItem).where(DeadLetterLineItem.id == item_id)
        )
        await db.commit()

        logger.info(
            "[DEAD_LETTER_REPLAYED] item_id=%s brand_id=%s external_order_id=%s sku=%s",
            item_id,
            item.brand_id,
            item.external_order_id,
            item.sku,
        )

        return {
            "ok": True,
            "message": "Item replayed successfully",
            "item_id": item_id,
            "brand_id": item.brand_id,
            "external_order_id": item.external_order_id,
            "sku": item.sku,
        }

    except Exception as exc:
        await db.rollback()
        exc_str = str(exc)
        is_duplicate = "duplicate key" in exc_str.lower() or "uq_order_line_identity" in exc_str.lower()
        if is_duplicate:
            logger.error(
                "[DEAD_LETTER_REPLAY_DUPLICATE_KEY] item_id=%s order_id=%s sku=%s"
                " product_name=%s constraint=uq_order_line_identity error=%s",
                item_id,
                params.get("order_id"),
                params.get("sku"),
                params.get("product_name"),
                exc_str[:500],
            )
        else:
            logger.error(
                "[DEAD_LETTER_REPLAY_FAILED] item_id=%s error=%s",
                item_id,
                exc_str[:300],
            )
        return {
            "ok": False,
            "error": exc_str[:500],
            "item_id": item_id,
            "duplicate_key_violation": is_duplicate,
        }


@router.post("/recover")
async def recover_sync(
    brand_id: str = Query(..., description="Brand ID to recover sync for"),
    db: AsyncSession = Depends(get_db),
):
    """Retry failed/retryable sync runs and orphaned line items.

    Looks up the sync health record for the brand. If consecutive_failures > 0,
    enqueues a new pending SyncRequest so the worker will retry on next poll.
    Returns immediately — the sync runs in the background.
    """
    result = await db.execute(
        select(SyncHealth).where(
            SyncHealth.brand_id == brand_id,
            SyncHealth.consecutive_failures > 0,
        )
    )
    health = result.scalar_one_or_none()

    if not health:
        return {"ok": True, "message": "No retryable syncs found", "brand_id": brand_id}

    sync_req = SyncRequest(
        brand_id=brand_id,
        status="pending",
        start_page=1,
    )
    db.add(sync_req)
    await db.commit()

    logger.info(
        "[SYNC_RECOVER] brand_id=%s consecutive_failures=%s sync_request_id=%s",
        brand_id,
        health.consecutive_failures,
        sync_req.id,
    )

    return {
        "ok": True,
        "message": f"Recovery sync enqueued for {brand_id}",
        "sync_request_id": sync_req.id,
        "brand_id": brand_id,
        "consecutive_failures_before_recovery": health.consecutive_failures,
    }


@router.post("/leaflink/trigger")
async def trigger_leaflink_sync(
    brand_id: str = Query(..., description="Brand ID to trigger sync for"),
    db: AsyncSession = Depends(get_db),
):
    """Manually trigger a LeafLink sync for a brand.

    Looks up the brand's credentials and enqueues a sync request.
    Returns immediately — the sync runs in the background.
    """
    from models import BrandAPICredential

    # Look up credentials
    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
    )
    cred = result.scalar_one_or_none()

    if cred is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active LeafLink credential found for brand_id={brand_id}",
        )

    # Enqueue a sync request
    sync_req = SyncRequest(
        brand_id=brand_id,
        status="pending",
        start_page=1,
    )
    db.add(sync_req)
    await db.commit()

    logger.info(
        "[SYNC_TRIGGER] brand_id=%s sync_request_id=%s",
        brand_id,
        sync_req.id,
    )

    return {
        "ok": True,
        "message": f"Sync enqueued for brand_id={brand_id}",
        "sync_request_id": sync_req.id,
        "brand_id": brand_id,
    }
