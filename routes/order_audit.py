"""
Order Total Audit & Line Items Backfill endpoints.

Endpoints:
  GET  /api/orders/audit/totals          — Audit order totals vs derived line-item sums
  GET  /api/orders/audit/totals/{order_id} — Audit a single order's totals
  POST /api/orders/backfill/line-items   — Backfill missing line items from LeafLink API

These endpoints address two related data-quality issues:
  1. Stored order totals (amount / total_cents) don't match the sum of line items.
  2. ~4,703 orders have no order_lines rows, breaking repeat-order and prediction logic.

Audit logic:
  - derived_lines_total = SUM(quantity * unit_price) from order_lines
  - stored_total = order.amount (or total_cents / 100 as fallback)
  - display_total = derived_lines_total if stored_total is missing or < 50% of derived
  - total_source = "stored" | "derived_lines" | "unknown"

Backfill logic:
  - Finds all orders with no order_lines rows
  - Fetches line items from LeafLink API using the order's external_order_id
  - Inserts/updates order_lines safely (idempotent ON CONFLICT upsert)
  - Logs progress every 100 orders
  - Resumable: accepts last_processed_order_id to skip already-processed orders
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from utils.json_utils import make_json_safe

logger = logging.getLogger("order_audit")

router = APIRouter(prefix="/api/orders", tags=["order-audit"])

# Thread pool for synchronous LeafLink HTTP calls
_audit_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="order-audit")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compute_display_total(
    stored_total: Optional[float],
    derived_lines_total: float,
) -> tuple[float, str]:
    """Compute display_total and total_source.

    Rules:
    - If stored_total is None or 0: use derived_lines_total → source = "derived_lines"
    - If derived_lines_total > 0 and stored_total < 50% of derived: use derived → source = "derived_lines"
    - Otherwise: use stored_total → source = "stored"
    - If both are 0/None: source = "unknown"
    """
    has_stored = stored_total is not None and stored_total > 0
    has_derived = derived_lines_total > 0

    if not has_stored and not has_derived:
        return 0.0, "unknown"

    if not has_stored:
        return round(derived_lines_total, 2), "derived_lines"

    if has_derived and stored_total < (derived_lines_total * 0.5):
        # Stored total is less than 50% of derived — likely stale or wrong field
        return round(derived_lines_total, 2), "derived_lines"

    return round(stored_total, 2), "stored"


# ---------------------------------------------------------------------------
# GET /api/orders/audit/totals
# ---------------------------------------------------------------------------

@router.get("/audit/totals")
async def audit_order_totals(
    brand_id: str = Query(..., description="Brand ID to audit"),
    limit: int = Query(100, ge=1, le=1000, description="Max orders to audit per call"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    only_mismatched: bool = Query(False, description="Only return orders where stored != derived"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Audit order totals vs derived line-item sums for a brand.

    For each order, computes:
      - stored_total: value in orders.amount (or total_cents/100)
      - derived_lines_total: SUM(quantity * unit_price) from order_lines
      - delta: stored_total - derived_lines_total
      - display_total: which total to show in the UI
      - total_source: "stored" | "derived_lines" | "unknown"

    Logs [ORDER_TOTAL_AUDIT] for every order with a significant delta (>$1).

    Returns a summary with mismatch_count, missing_lines_count, and per-order details.
    """
    t_start = time.monotonic()

    logger.info(
        "[ORDER_TOTAL_AUDIT_START] brand_id=%s limit=%d offset=%d only_mismatched=%s",
        brand_id, limit, offset, only_mismatched,
    )

    try:
        # Fetch orders with their derived line-item totals in one query
        result = await db.execute(
            text("""
                SELECT
                    o.id,
                    o.external_order_id,
                    o.order_number,
                    o.customer_name,
                    o.status,
                    o.amount,
                    o.total_cents,
                    COALESCE(
                        SUM(ol.quantity * ol.unit_price),
                        0
                    ) AS derived_lines_total,
                    COUNT(ol.id) AS line_item_count
                FROM orders o
                LEFT JOIN order_lines ol ON ol.order_id = o.id
                WHERE o.brand_id = :brand_id
                GROUP BY
                    o.id, o.external_order_id, o.order_number,
                    o.customer_name, o.status, o.amount, o.total_cents
                ORDER BY o.external_created_at DESC NULLS LAST, o.created_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"brand_id": brand_id, "limit": limit, "offset": offset},
        )
        rows = result.fetchall()
    except Exception as exc:
        logger.error(
            "[ORDER_TOTAL_AUDIT_ERROR] brand_id=%s error=%s",
            brand_id, str(exc)[:300],
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "brand_id": brand_id,
            "error": str(exc)[:300],
            "timestamp": _utc_now_iso(),
        }

    # Count totals for summary
    try:
        count_result = await db.execute(
            text("SELECT COUNT(*) FROM orders WHERE brand_id = :brand_id"),
            {"brand_id": brand_id},
        )
        total_orders = count_result.scalar() or 0
    except Exception:
        total_orders = len(rows)

    try:
        missing_lines_result = await db.execute(
            text("""
                SELECT COUNT(*) FROM orders o
                WHERE o.brand_id = :brand_id
                  AND NOT EXISTS (
                      SELECT 1 FROM order_lines ol WHERE ol.order_id = o.id
                  )
            """),
            {"brand_id": brand_id},
        )
        orders_without_lines_count = missing_lines_result.scalar() or 0
    except Exception:
        orders_without_lines_count = 0

    items = []
    mismatch_count = 0
    missing_lines_in_page = 0

    for row in rows:
        (
            order_id, external_order_id, order_number, customer_name,
            status, amount, total_cents, derived_lines_total, line_item_count,
        ) = row

        # Resolve stored_total: prefer amount, fall back to total_cents/100
        stored_total: Optional[float] = None
        if amount is not None:
            try:
                stored_total = float(amount)
            except (TypeError, ValueError):
                stored_total = None
        if stored_total is None and total_cents is not None:
            try:
                stored_total = round(int(total_cents) / 100.0, 2)
            except (TypeError, ValueError):
                stored_total = None

        derived = float(derived_lines_total or 0)
        display_total, total_source = _compute_display_total(stored_total, derived)

        delta = round((stored_total or 0) - derived, 2)
        is_mismatch = abs(delta) > 1.0  # >$1 difference is significant
        has_no_lines = int(line_item_count or 0) == 0

        if is_mismatch:
            mismatch_count += 1
            logger.info(
                "[ORDER_TOTAL_AUDIT] order_id=%s external_order_id=%s "
                "stored_total=%s derived_lines_total=%s delta=%s "
                "total_source=%s line_item_count=%s",
                order_id,
                external_order_id,
                stored_total,
                round(derived, 2),
                delta,
                total_source,
                line_item_count,
            )

        if has_no_lines:
            missing_lines_in_page += 1

        if only_mismatched and not is_mismatch:
            continue

        items.append({
            "order_id": order_id,
            "external_order_id": external_order_id,
            "order_number": order_number,
            "customer_name": customer_name,
            "status": status,
            "stored_total": stored_total,
            "derived_lines_total": round(derived, 2),
            "display_total": display_total,
            "total_source": total_source,
            "delta": delta,
            "is_mismatch": is_mismatch,
            "line_item_count": int(line_item_count or 0),
            "has_no_lines": has_no_lines,
        })

    elapsed_ms = round((time.monotonic() - t_start) * 1000)

    logger.info(
        "[ORDER_TOTAL_AUDIT_COMPLETE] brand_id=%s audited=%d mismatch_count=%d "
        "missing_lines_in_page=%d orders_without_lines_total=%d elapsed_ms=%d",
        brand_id,
        len(rows),
        mismatch_count,
        missing_lines_in_page,
        orders_without_lines_count,
        elapsed_ms,
    )

    return make_json_safe({
        "ok": True,
        "brand_id": brand_id,
        "total_orders": total_orders,
        "orders_without_lines_count": orders_without_lines_count,
        "audited_count": len(rows),
        "mismatch_count": mismatch_count,
        "missing_lines_in_page": missing_lines_in_page,
        "limit": limit,
        "offset": offset,
        "items": items,
        "elapsed_ms": elapsed_ms,
        "timestamp": _utc_now_iso(),
    })


# ---------------------------------------------------------------------------
# GET /api/orders/audit/totals/{order_id}
# ---------------------------------------------------------------------------

@router.get("/audit/totals/{order_id}")
async def audit_single_order_total(
    order_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Audit totals for a single order by internal order_id.

    Returns stored_total, derived_lines_total, display_total, total_source,
    and per-line-item breakdown.

    Logs [ORDER_TOTAL_AUDIT] with full diagnostic info.
    """
    t_start = time.monotonic()

    logger.info("[ORDER_TOTAL_AUDIT_SINGLE] order_id=%s", order_id)

    try:
        order_result = await db.execute(
            text("""
                SELECT
                    o.id,
                    o.external_order_id,
                    o.order_number,
                    o.customer_name,
                    o.status,
                    o.amount,
                    o.total_cents,
                    o.brand_id
                FROM orders o
                WHERE o.id = :order_id
            """),
            {"order_id": order_id},
        )
        order_row = order_result.fetchone()
    except Exception as exc:
        logger.error("[ORDER_TOTAL_AUDIT_ERROR] order_id=%s error=%s", order_id, str(exc)[:300])
        try:
            await db.rollback()
        except Exception:
            pass
        return {"ok": False, "order_id": order_id, "error": str(exc)[:300]}

    if not order_row:
        return {"ok": False, "order_id": order_id, "error": "Order not found"}

    (
        db_order_id, external_order_id, order_number, customer_name,
        status, amount, total_cents, brand_id,
    ) = order_row

    # Fetch line items
    try:
        lines_result = await db.execute(
            text("""
                SELECT id, sku, product_name, quantity, unit_price, total_price
                FROM order_lines
                WHERE order_id = :order_id
                ORDER BY id ASC
            """),
            {"order_id": order_id},
        )
        line_rows = lines_result.fetchall()
    except Exception as exc:
        logger.error("[ORDER_TOTAL_AUDIT_ERROR] order_id=%s lines_error=%s", order_id, str(exc)[:300])
        line_rows = []

    # Compute totals
    stored_total: Optional[float] = None
    if amount is not None:
        try:
            stored_total = float(amount)
        except (TypeError, ValueError):
            stored_total = None
    if stored_total is None and total_cents is not None:
        try:
            stored_total = round(int(total_cents) / 100.0, 2)
        except (TypeError, ValueError):
            stored_total = None

    line_items = []
    derived_lines_total = 0.0
    for line_row in line_rows:
        line_id, sku, product_name, quantity, unit_price, total_price = line_row
        qty = int(quantity or 0)
        up = float(unit_price or 0)
        tp = float(total_price or 0) if total_price else round(qty * up, 2)
        derived_lines_total += tp
        line_items.append({
            "id": line_id,
            "sku": sku,
            "product_name": product_name,
            "quantity": qty,
            "unit_price": up,
            "total_price": tp,
            "line_contribution": tp,
        })

    derived_lines_total = round(derived_lines_total, 2)
    display_total, total_source = _compute_display_total(stored_total, derived_lines_total)
    delta = round((stored_total or 0) - derived_lines_total, 2)

    elapsed_ms = round((time.monotonic() - t_start) * 1000)

    logger.info(
        "[ORDER_TOTAL_AUDIT] order_id=%s external_order_id=%s "
        "stored_total=%s derived_lines_total=%s delta=%s "
        "total_source=%s line_item_count=%d",
        order_id,
        external_order_id,
        stored_total,
        derived_lines_total,
        delta,
        total_source,
        len(line_items),
    )

    return make_json_safe({
        "ok": True,
        "order_id": order_id,
        "external_order_id": external_order_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "status": status,
        "brand_id": brand_id,
        "stored_total": stored_total,
        "derived_lines_total": derived_lines_total,
        "display_total": display_total,
        "total_source": total_source,
        "delta": delta,
        "is_mismatch": abs(delta) > 1.0,
        "line_item_count": len(line_items),
        "line_items": line_items,
        "elapsed_ms": elapsed_ms,
        "timestamp": _utc_now_iso(),
    })


# ---------------------------------------------------------------------------
# POST /api/orders/backfill/line-items
# ---------------------------------------------------------------------------

# In-memory state for the running backfill (single-instance guard)
_backfill_running: bool = False
_backfill_last_result: Optional[dict] = None


@router.post("/backfill/line-items")
async def backfill_line_items(
    background_tasks: BackgroundTasks,
    brand_id: str = Query(..., description="Brand ID to backfill line items for"),
    limit: int = Query(500, ge=1, le=5000, description="Max orders to process per call"),
    last_processed_order_id: Optional[str] = Query(
        None,
        description="Resume from this order_id (skip orders processed before this one)",
    ),
    dry_run: bool = Query(False, description="If true, fetch from LeafLink but do not write to DB"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Backfill missing line items for orders that have no order_lines rows.

    Finds all orders for the brand where no order_lines exist, then fetches
    line items from the LeafLink API for each order and inserts them.

    Features:
    - Resumable: pass last_processed_order_id to skip already-processed orders
    - Idempotent: uses ON CONFLICT upsert — safe to run multiple times
    - Progress logging every 100 orders
    - Dry-run mode: fetches from LeafLink but does not write to DB

    Logs:
      [LINE_ITEMS_BACKFILL_START]
      [LINE_ITEMS_BACKFILL_PROGRESS] processed=... remaining=... inserted=... skipped=... failed=...
      [LINE_ITEMS_BACKFILL_ORDER] order_id=... external_order_id=... lines=...
      [LINE_ITEMS_BACKFILL_FAILED] order_id=... error=...
      [LINE_ITEMS_BACKFILL_COMPLETE]
    """
    global _backfill_running, _backfill_last_result

    if _backfill_running:
        return {
            "ok": False,
            "brand_id": brand_id,
            "error": "Backfill already running — wait for it to complete or check /api/orders/backfill/line-items/status",
            "last_result": _backfill_last_result,
            "timestamp": _utc_now_iso(),
        }

    # Load LeafLink credential for this brand
    try:
        cred_result = await db.execute(
            text("""
                SELECT api_key, company_id, base_url, auth_scheme
                FROM brand_api_credentials
                WHERE TRIM(LOWER(brand_id::text)) = :brand_id
                  AND TRIM(LOWER(integration_name)) = 'leaflink'
                  AND is_active = true
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
            """),
            {"brand_id": brand_id.strip().lower()},
        )
        cred_row = cred_result.fetchone()
    except Exception as exc:
        logger.error(
            "[LINE_ITEMS_BACKFILL_ERROR] brand_id=%s credential_lookup_failed error=%s",
            brand_id, str(exc)[:300],
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "brand_id": brand_id,
            "error": f"Credential lookup failed: {str(exc)[:300]}",
            "timestamp": _utc_now_iso(),
        }

    if not cred_row:
        return {
            "ok": False,
            "brand_id": brand_id,
            "error": "No active LeafLink credential found for this brand",
            "timestamp": _utc_now_iso(),
        }

    api_key, company_id, base_url, auth_scheme = cred_row

    # Find orders without line items
    try:
        query_params: dict = {"brand_id": brand_id, "limit": limit}
        resume_clause = ""
        if last_processed_order_id:
            resume_clause = "AND o.id > :last_processed_order_id"
            query_params["last_processed_order_id"] = last_processed_order_id

        orders_result = await db.execute(
            text(f"""
                SELECT o.id, o.external_order_id, o.order_number, o.customer_name
                FROM orders o
                WHERE o.brand_id = :brand_id
                  {resume_clause}
                  AND NOT EXISTS (
                      SELECT 1 FROM order_lines ol WHERE ol.order_id = o.id
                  )
                ORDER BY o.id ASC
                LIMIT :limit
            """),
            query_params,
        )
        orders_to_backfill = orders_result.fetchall()
    except Exception as exc:
        logger.error(
            "[LINE_ITEMS_BACKFILL_ERROR] brand_id=%s orders_query_failed error=%s",
            brand_id, str(exc)[:300],
        )
        try:
            await db.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "brand_id": brand_id,
            "error": f"Orders query failed: {str(exc)[:300]}",
            "timestamp": _utc_now_iso(),
        }

    if not orders_to_backfill:
        logger.info(
            "[LINE_ITEMS_BACKFILL_COMPLETE] brand_id=%s reason=no_orders_without_lines",
            brand_id,
        )
        return {
            "ok": True,
            "brand_id": brand_id,
            "orders_to_backfill": 0,
            "message": "No orders without line items found",
            "timestamp": _utc_now_iso(),
        }

    # Count total orders without lines for reporting
    try:
        total_missing_result = await db.execute(
            text("""
                SELECT COUNT(*) FROM orders o
                WHERE o.brand_id = :brand_id
                  AND NOT EXISTS (
                      SELECT 1 FROM order_lines ol WHERE ol.order_id = o.id
                  )
            """),
            {"brand_id": brand_id},
        )
        total_orders_without_lines = total_missing_result.scalar() or 0
    except Exception:
        total_orders_without_lines = len(orders_to_backfill)

    logger.info(
        "[LINE_ITEMS_BACKFILL_START] brand_id=%s orders_to_process=%d "
        "total_without_lines=%d dry_run=%s resume_from=%s",
        brand_id,
        len(orders_to_backfill),
        total_orders_without_lines,
        dry_run,
        last_processed_order_id or "beginning",
    )

    # Run backfill in background
    _backfill_running = True

    background_tasks.add_task(
        _run_backfill_task,
        brand_id=brand_id,
        orders=[(row[0], row[1], row[2], row[3]) for row in orders_to_backfill],
        api_key=api_key,
        company_id=company_id,
        base_url=base_url,
        auth_scheme=auth_scheme or "Token",
        dry_run=dry_run,
        total_orders_without_lines=total_orders_without_lines,
    )

    return {
        "ok": True,
        "brand_id": brand_id,
        "orders_to_process": len(orders_to_backfill),
        "total_orders_without_lines": total_orders_without_lines,
        "dry_run": dry_run,
        "resume_from": last_processed_order_id,
        "message": f"Backfill started for {len(orders_to_backfill)} orders in background",
        "status_endpoint": "/api/orders/backfill/line-items/status",
        "timestamp": _utc_now_iso(),
    }


@router.get("/backfill/line-items/status")
async def backfill_line_items_status() -> dict[str, Any]:
    """Get the status of the last (or currently running) line items backfill."""
    return {
        "ok": True,
        "running": _backfill_running,
        "last_result": _backfill_last_result,
        "timestamp": _utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# Background backfill task
# ---------------------------------------------------------------------------

async def _run_backfill_task(
    brand_id: str,
    orders: list[tuple],  # (order_id, external_order_id, order_number, customer_name)
    api_key: str,
    company_id: str,
    base_url: Optional[str],
    auth_scheme: str,
    dry_run: bool,
    total_orders_without_lines: int,
) -> None:
    """Background task: fetch line items from LeafLink and insert into order_lines."""
    global _backfill_running, _backfill_last_result

    from database import get_async_session_local
    from services.leaflink_client import LeafLinkClient
    from services.leaflink_sync import (
        normalize_line_items,
        safe_uuid_for_db,
        ensure_utc,
        utc_now,
    )
    from database import has_column
    import json

    t_start = time.monotonic()
    processed = 0
    inserted = 0
    skipped = 0
    failed = 0
    last_processed_id: Optional[str] = None

    try:
        client = LeafLinkClient(
            api_key=api_key,
            company_id=company_id,
            brand_id=brand_id,
            base_url=base_url,
            auth_scheme=auth_scheme,
        )
    except Exception as exc:
        logger.error(
            "[LINE_ITEMS_BACKFILL_FAILED] brand_id=%s reason=client_init_failed error=%s",
            brand_id, str(exc)[:300],
        )
        _backfill_running = False
        _backfill_last_result = {
            "ok": False,
            "brand_id": brand_id,
            "error": f"LeafLink client init failed: {str(exc)[:300]}",
            "processed": 0,
            "inserted": 0,
            "skipped": 0,
            "failed": 0,
            "completed_at": _utc_now_iso(),
        }
        return

    # Inspect schema once
    optional_columns = [
        "packed_qty", "unit_price_cents", "total_price_cents",
        "mapped_product_id", "mapping_status", "mapping_issue",
        "raw_payload", "created_at", "updated_at",
    ]
    enabled_columns: dict[str, bool] = {
        col: has_column("order_lines", col) for col in optional_columns
    }

    insert_columns = ["order_id", "sku", "product_name", "quantity", "unit_price", "total_price"]
    for col in optional_columns:
        if enabled_columns.get(col, False):
            insert_columns.append(col)

    _uuid_columns = {"mapped_product_id"}
    _timestamp_columns = {"created_at", "updated_at"}

    columns_str = ", ".join(insert_columns)
    placeholders = ", ".join(
        f"CAST(:{col} AS UUID)" if col in _uuid_columns
        else f"CAST(:{col} AS TIMESTAMP)" if col in _timestamp_columns
        else f":{col}"
        for col in insert_columns
    )

    update_set_clauses = [
        "quantity = EXCLUDED.quantity",
        "unit_price = EXCLUDED.unit_price",
        "total_price = EXCLUDED.total_price",
    ]
    if enabled_columns.get("packed_qty"):
        update_set_clauses.append("packed_qty = EXCLUDED.packed_qty")
    if enabled_columns.get("unit_price_cents"):
        update_set_clauses.append("unit_price_cents = EXCLUDED.unit_price_cents")
    if enabled_columns.get("total_price_cents"):
        update_set_clauses.append("total_price_cents = EXCLUDED.total_price_cents")
    if enabled_columns.get("mapped_product_id"):
        update_set_clauses.append("mapped_product_id = EXCLUDED.mapped_product_id")
    if enabled_columns.get("mapping_status"):
        update_set_clauses.append("mapping_status = EXCLUDED.mapping_status")
    if enabled_columns.get("mapping_issue"):
        update_set_clauses.append("mapping_issue = EXCLUDED.mapping_issue")
    if enabled_columns.get("raw_payload"):
        update_set_clauses.append("raw_payload = EXCLUDED.raw_payload")
    if enabled_columns.get("updated_at"):
        update_set_clauses.append("updated_at = EXCLUDED.updated_at")

    update_set_str = ",\n        ".join(update_set_clauses)

    line_upsert_stmt = (
        f"INSERT INTO public.order_lines ({columns_str}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (order_id, sku, product_name) "
        f"WHERE sku IS NOT NULL AND product_name IS NOT NULL "
        f"DO UPDATE SET {update_set_str}"
    )

    loop = asyncio.get_event_loop()
    _AsyncSessionLocal = get_async_session_local()

    for order_id, external_order_id, order_number, customer_name in orders:
        last_processed_id = order_id
        processed += 1

        # Log progress every 100 orders
        if processed % 100 == 0:
            remaining = len(orders) - processed
            elapsed_s = time.monotonic() - t_start
            logger.info(
                "[LINE_ITEMS_BACKFILL_PROGRESS] brand_id=%s processed=%d remaining=%d "
                "inserted=%d skipped=%d failed=%d elapsed_s=%.1f",
                brand_id, processed, remaining, inserted, skipped, failed, elapsed_s,
            )

        try:
            # Fetch the order from LeafLink API in a thread pool
            def _fetch_order(ext_id: str) -> Optional[dict]:
                try:
                    resp = client._get_raw(
                        "orders-received/",
                        params={"id": ext_id, "limit": 1},
                    )
                    if not resp.ok:
                        return None
                    data = resp.json()
                    if isinstance(data, dict):
                        results = data.get("results") or data.get("data") or []
                        if isinstance(results, list) and results:
                            return results[0]
                    elif isinstance(data, list) and data:
                        return data[0]
                    return None
                except Exception as _fetch_exc:
                    logger.warning(
                        "[LINE_ITEMS_BACKFILL_FETCH_WARN] order_id=%s external_order_id=%s error=%s",
                        order_id, ext_id, str(_fetch_exc)[:200],
                    )
                    return None

            raw_order = await loop.run_in_executor(
                _audit_executor,
                lambda: _fetch_order(external_order_id),
            )

            if raw_order is None:
                logger.warning(
                    "[LINE_ITEMS_BACKFILL_FAILED] order_id=%s external_order_id=%s "
                    "error=order_not_found_in_leaflink",
                    order_id, external_order_id,
                )
                failed += 1
                continue

            # Extract and normalize line items
            raw_line_items = (
                raw_order.get("line_items")
                or raw_order.get("items")
                or raw_order.get("order_items")
                or raw_order.get("products")
                or raw_order.get("ordered_items")
                or raw_order.get("line_item_list")
                or raw_order.get("order_lines")
                or []
            )

            normalized = normalize_line_items(raw_line_items)

            logger.info(
                "[LINE_ITEMS_BACKFILL_ORDER] order_id=%s external_order_id=%s "
                "order_number=%s customer=%s lines=%d dry_run=%s",
                order_id, external_order_id, order_number,
                customer_name, len(normalized), dry_run,
            )

            if not normalized:
                logger.warning(
                    "[LINE_ITEMS_BACKFILL_FAILED] order_id=%s external_order_id=%s "
                    "error=no_line_items_in_leaflink_response",
                    order_id, external_order_id,
                )
                skipped += 1
                continue

            if dry_run:
                inserted += len(normalized)
                continue

            # Write line items to DB
            async with _AsyncSessionLocal() as db_write:
                try:
                    _now = ensure_utc(utc_now(), "backfill_now")
                    lines_written = 0

                    for item in normalized:
                        sku = item.get("sku")
                        if not sku:
                            continue

                        insert_params: dict[str, Any] = {
                            "order_id": order_id,
                            "sku": sku,
                            "product_name": item.get("product_name"),
                            "quantity": item.get("quantity"),
                            "unit_price": float(item["unit_price"]) if item.get("unit_price") is not None else None,
                            "total_price": float(item["total_price"]) if item.get("total_price") is not None else None,
                        }

                        if enabled_columns.get("packed_qty"):
                            insert_params["packed_qty"] = 0
                        if enabled_columns.get("unit_price_cents"):
                            insert_params["unit_price_cents"] = item.get("unit_price_cents")
                        if enabled_columns.get("total_price_cents"):
                            insert_params["total_price_cents"] = item.get("total_price_cents")
                        if enabled_columns.get("mapped_product_id"):
                            insert_params["mapped_product_id"] = safe_uuid_for_db(
                                item.get("mapped_product_id"), "mapped_product_id"
                            )
                        if enabled_columns.get("mapping_status"):
                            insert_params["mapping_status"] = item.get("mapping_status", "unknown")
                        if enabled_columns.get("mapping_issue"):
                            insert_params["mapping_issue"] = item.get("mapping_issue")
                        if enabled_columns.get("raw_payload"):
                            raw_p = item.get("raw_payload")
                            insert_params["raw_payload"] = (
                                json.dumps(make_json_safe(raw_p)) if raw_p is not None else None
                            )
                        if enabled_columns.get("created_at"):
                            insert_params["created_at"] = _now
                        if enabled_columns.get("updated_at"):
                            insert_params["updated_at"] = _now

                        await db_write.execute(text(line_upsert_stmt), insert_params)
                        lines_written += 1

                    await db_write.commit()
                    inserted += lines_written

                except Exception as write_exc:
                    logger.error(
                        "[LINE_ITEMS_BACKFILL_FAILED] order_id=%s external_order_id=%s "
                        "error=%s",
                        order_id, external_order_id, str(write_exc)[:300],
                    )
                    try:
                        await db_write.rollback()
                    except Exception:
                        pass
                    failed += 1

        except Exception as exc:
            logger.error(
                "[LINE_ITEMS_BACKFILL_FAILED] order_id=%s external_order_id=%s error=%s",
                order_id, external_order_id, str(exc)[:300],
            )
            failed += 1

    elapsed_s = round(time.monotonic() - t_start, 1)

    logger.info(
        "[LINE_ITEMS_BACKFILL_COMPLETE] brand_id=%s processed=%d inserted=%d "
        "skipped=%d failed=%d elapsed_s=%s dry_run=%s last_processed_id=%s",
        brand_id, processed, inserted, skipped, failed,
        elapsed_s, dry_run, last_processed_id,
    )

    _backfill_running = False
    _backfill_last_result = {
        "ok": True,
        "brand_id": brand_id,
        "processed": processed,
        "inserted": inserted,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "elapsed_s": elapsed_s,
        "last_processed_order_id": last_processed_id,
        "completed_at": _utc_now_iso(),
    }
