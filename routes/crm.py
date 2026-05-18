import base64
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db
from models import Order, OrderLine, BrandAPICredential
from models.auth_models import Brand, Organization
from services.organization_service import lookup_organization
from utils.json_utils import make_json_safe

logger = logging.getLogger("crm")

router = APIRouter(prefix="/crm", tags=["crm"])


# ---------------------------------------------------------------------------
# Helpers — payload truth functions
# ---------------------------------------------------------------------------

def _build_crm_line_items(order: Order) -> list[dict[str, Any]]:
    """
    Build a normalized line-items list from the best available source.

    Priority:
      1. order.lines  (OrderLine ORM rows — most reliable)
      2. order.line_items_json  (normalized JSONB from sync)
      3. order.raw_payload  (fallback extraction)

    Returns a list of dicts with: product_name, sku, quantity, unit_price,
    line_total, discounts, shipping, tax, category, product_identifiers.
    """
    items: list[dict[str, Any]] = []

    # --- Source 1: ORM relationship rows ---
    try:
        lines = getattr(order, "lines", None)
        if lines:
            for line in lines:
                try:
                    unit_price = None
                    if line.unit_price is not None:
                        unit_price = float(line.unit_price)
                    elif line.unit_price_cents is not None:
                        unit_price = round(line.unit_price_cents / 100.0, 2)

                    line_total = None
                    if line.total_price is not None:
                        line_total = float(line.total_price)
                    elif line.total_price_cents is not None:
                        line_total = round(line.total_price_cents / 100.0, 2)
                    elif unit_price is not None and line.quantity:
                        line_total = round(unit_price * (line.quantity or 0), 2)

                    # Pull extra fields from raw_payload if available
                    raw = getattr(line, "raw_payload", None) or {}
                    items.append({
                        "product_name": line.product_name,
                        "sku": line.sku,
                        "quantity": line.quantity or 0,
                        "unit_price": unit_price,
                        "line_total": line_total,
                        "discounts": raw.get("discount_amount") or raw.get("discounts"),
                        "shipping": raw.get("shipping"),
                        "tax": raw.get("tax"),
                        "category": raw.get("category") or raw.get("product_category"),
                        "product_identifiers": raw.get("product_identifiers"),
                    })
                except Exception:
                    pass
            if items:
                return items
    except Exception:
        pass

    # --- Source 2: line_items_json ---
    try:
        raw_json = order.line_items_json
        if isinstance(raw_json, list) and raw_json:
            for item in raw_json:
                if not isinstance(item, dict):
                    continue
                try:
                    qty = item.get("quantity") or item.get("qty") or 0
                    unit_price = item.get("unit_price")
                    if unit_price is None and item.get("unit_price_cents") is not None:
                        unit_price = round(item["unit_price_cents"] / 100.0, 2)
                    line_total = item.get("total_price") or item.get("line_total")
                    if line_total is None and item.get("total_price_cents") is not None:
                        line_total = round(item["total_price_cents"] / 100.0, 2)
                    if line_total is None and unit_price is not None and qty:
                        line_total = round(float(unit_price) * int(qty), 2)
                    items.append({
                        "product_name": item.get("product_name") or item.get("name"),
                        "sku": item.get("sku"),
                        "quantity": qty,
                        "unit_price": float(unit_price) if unit_price is not None else None,
                        "line_total": float(line_total) if line_total is not None else None,
                        "discounts": item.get("discount_amount") or item.get("discounts"),
                        "shipping": item.get("shipping"),
                        "tax": item.get("tax"),
                        "category": item.get("category") or item.get("product_category"),
                        "product_identifiers": item.get("product_identifiers"),
                    })
                except Exception:
                    pass
            if items:
                return items
        if isinstance(raw_json, dict):
            nested = raw_json.get("line_items")
            if isinstance(nested, list) and nested:
                return _build_crm_line_items_from_list(nested)
    except Exception:
        pass

    # --- Source 3: raw_payload fallback ---
    try:
        raw_payload = order.raw_payload
        if isinstance(raw_payload, dict):
            candidate = (
                raw_payload.get("line_items")
                or raw_payload.get("items")
                or raw_payload.get("order_items")
                or raw_payload.get("products")
                or raw_payload.get("ordered_items")
                or []
            )
            if isinstance(candidate, list) and candidate:
                return _build_crm_line_items_from_list(candidate)
    except Exception:
        pass

    return []


def _build_crm_line_items_from_list(raw_list: list) -> list[dict[str, Any]]:
    """Normalize a raw list of line-item dicts into the CRM line-item shape."""
    items = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        try:
            qty = item.get("quantity") or item.get("qty") or 0
            unit_price = item.get("unit_price")
            if unit_price is None and item.get("unit_price_cents") is not None:
                unit_price = round(item["unit_price_cents"] / 100.0, 2)
            line_total = item.get("total_price") or item.get("line_total")
            if line_total is None and item.get("total_price_cents") is not None:
                line_total = round(item["total_price_cents"] / 100.0, 2)
            if line_total is None and unit_price is not None and qty:
                line_total = round(float(unit_price) * int(qty), 2)
            items.append({
                "product_name": item.get("product_name") or item.get("name"),
                "sku": item.get("sku"),
                "quantity": qty,
                "unit_price": float(unit_price) if unit_price is not None else None,
                "line_total": float(line_total) if line_total is not None else None,
                "discounts": item.get("discount_amount") or item.get("discounts"),
                "shipping": item.get("shipping"),
                "tax": item.get("tax"),
                "category": item.get("category") or item.get("product_category"),
                "product_identifiers": item.get("product_identifiers"),
            })
        except Exception:
            pass
    return items


def _compute_order_totals(order: Order, line_items: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Compute correct totals and counts for an order.

    Strategy:
      1. If stored total_cents > 0: use it as total_amount (source="stored").
         Also try to pull subtotal/discount/shipping/tax from raw_payload.
      2. If stored total missing/zero but valid line items exist: compute
         from line items (source="computed").
      3. No usable pricing data: return None totals (source="missing").

    Returns a dict with:
      total_amount, subtotal_amount, discount_total, shipping_total,
      tax_total, item_count, unit_count, total_source
    """
    # --- Pull order-level financial breakdown from raw_payload if available ---
    raw = {}
    try:
        if order.raw_payload and isinstance(order.raw_payload, dict):
            raw = order.raw_payload
    except Exception:
        pass

    def _raw_money(key: str) -> float | None:
        val = raw.get(key)
        if val is None:
            return None
        try:
            f = float(val)
            return f if f != 0.0 else None
        except (TypeError, ValueError):
            return None

    # --- Strategy 1: stored total ---
    stored_total_cents = None
    try:
        stored_total_cents = order.total_cents
    except Exception:
        pass

    stored_amount = None
    try:
        if order.amount is not None:
            stored_amount = float(order.amount)
    except Exception:
        pass

    # Resolve stored total (prefer total_cents, fall back to amount)
    stored_total: float | None = None
    if stored_total_cents is not None and stored_total_cents > 0:
        stored_total = round(stored_total_cents / 100.0, 2)
    elif stored_amount is not None and stored_amount > 0:
        stored_total = round(stored_amount, 2)

    if stored_total is not None and stored_total > 0:
        # Use stored total; try to enrich breakdown from raw_payload
        subtotal = _raw_money("subtotal") or _raw_money("subtotal_price")
        discount = _raw_money("discount_total") or _raw_money("total_discounts") or 0.0
        shipping = _raw_money("shipping_total") or _raw_money("total_shipping") or 0.0
        tax = _raw_money("tax_total") or _raw_money("total_tax") or 0.0

        stored_item_count = None
        try:
            stored_item_count = order.item_count
        except Exception:
            pass
        stored_unit_count = None
        try:
            stored_unit_count = order.unit_count
        except Exception:
            pass

        item_count = stored_item_count if stored_item_count is not None else len(line_items)
        unit_count = (
            stored_unit_count
            if stored_unit_count is not None
            else sum((li.get("quantity") or 0) for li in line_items)
        )

        return {
            "total_amount": stored_total,
            "subtotal_amount": subtotal,
            "discount_total": discount if discount else 0.0,
            "shipping_total": shipping if shipping else 0.0,
            "tax_total": tax if tax else 0.0,
            "item_count": item_count,
            "unit_count": unit_count,
            "total_source": "stored",
        }

    # --- Strategy 2: compute from line items ---
    if line_items:
        try:
            subtotal = sum(
                float(li.get("line_total") or 0)
                if li.get("line_total") is not None
                else (float(li.get("unit_price") or 0) * int(li.get("quantity") or 0))
                for li in line_items
            )
            discount_raw = _raw_money("discount_total") or _raw_money("total_discounts")
            discount = discount_raw if discount_raw is not None else sum(
                float(li.get("discounts") or 0) for li in line_items
                if li.get("discounts") is not None
            )
            shipping = _raw_money("shipping_total") or _raw_money("total_shipping") or 0.0
            tax = _raw_money("tax_total") or _raw_money("total_tax") or 0.0
            total = round(subtotal - discount + shipping + tax, 2)

            item_count = len(line_items)
            unit_count = sum((li.get("quantity") or 0) for li in line_items)

            logger.info(
                "[CRM_ORDER_TOTAL_COMPUTED] order_id=%s subtotal=%.2f discount=%.2f "
                "shipping=%.2f tax=%.2f final=%.2f",
                getattr(order, "id", "unknown"),
                subtotal,
                discount,
                shipping,
                tax,
                total,
            )

            return {
                "total_amount": total,
                "subtotal_amount": round(subtotal, 2),
                "discount_total": round(discount, 2),
                "shipping_total": round(shipping, 2),
                "tax_total": round(tax, 2),
                "item_count": item_count,
                "unit_count": unit_count,
                "total_source": "computed",
            }
        except Exception as exc:
            logger.warning(
                "[CRM_ORDER_TOTAL_COMPUTED] compute_failed order_id=%s error=%s",
                getattr(order, "id", "unknown"),
                str(exc)[:200],
            )

    # --- Strategy 3: no usable pricing data ---
    stored_item_count = None
    try:
        stored_item_count = order.item_count
    except Exception:
        pass
    stored_unit_count = None
    try:
        stored_unit_count = order.unit_count
    except Exception:
        pass

    return {
        "total_amount": None,
        "subtotal_amount": None,
        "discount_total": None,
        "shipping_total": None,
        "tax_total": None,
        "item_count": stored_item_count or 0,
        "unit_count": stored_unit_count or 0,
        "total_source": "missing",
    }


async def _resolve_order_identifier(
    db: AsyncSession,
    org_id: str,
    identifier: str,
) -> tuple["Order | None", str]:
    """
    Resolve an order by any identifier format, scoped to org_id.

    Tries in order:
    1. Internal positive integer ID
    2. order_number (exact match)
    3. external_order_id (exact match)
    4. external_id / UUID (exact match on Order.id)
    5. Short suffix (last 8 chars of UUID, tenant-scoped)

    Returns (order_or_None, matched_by_strategy).
    Raises HTTP 409 if the short-suffix lookup is ambiguous.
    """
    # Try as positive integer ID
    try:
        order_id_int = int(identifier)
        if order_id_int > 0:
            result = await db.execute(
                select(Order)
                .where(
                    and_(
                        Order.id == str(order_id_int),
                        Order.org_id == org_id,
                    )
                )
                .options(selectinload(Order.lines))
            )
            order = result.scalar_one_or_none()
            if order:
                return order, "integer_id"
    except (ValueError, TypeError):
        pass

    # Try as order_number (exact)
    result = await db.execute(
        select(Order)
        .where(
            and_(
                Order.order_number == identifier,
                Order.org_id == org_id,
            )
        )
        .options(selectinload(Order.lines))
    )
    order = result.scalar_one_or_none()
    if order:
        return order, "order_number"

    # Try as external_order_id (exact)
    result = await db.execute(
        select(Order)
        .where(
            and_(
                Order.external_order_id == identifier,
                Order.org_id == org_id,
            )
        )
        .options(selectinload(Order.lines))
    )
    order = result.scalar_one_or_none()
    if order:
        return order, "external_order_id"

    # Try as UUID / Order.id (exact)
    try:
        uuid_val = UUID(identifier)
        result = await db.execute(
            select(Order)
            .where(
                and_(
                    Order.id == str(uuid_val),
                    Order.org_id == org_id,
                )
            )
            .options(selectinload(Order.lines))
        )
        order = result.scalar_one_or_none()
        if order:
            return order, "uuid"
    except (ValueError, TypeError):
        pass

    # Try as short suffix (last 8 chars of UUID, tenant-scoped)
    if len(identifier) == 8:
        result = await db.execute(
            select(Order)
            .where(
                and_(
                    or_(
                        Order.external_order_id.endswith(identifier),
                        Order.id.endswith(identifier),
                    ),
                    Order.org_id == org_id,
                )
            )
            .options(selectinload(Order.lines))
        )
        orders = result.scalars().all()
        if len(orders) == 1:
            return orders[0], "suffix"
        elif len(orders) > 1:
            raise HTTPException(
                status_code=409,
                detail=f"Ambiguous identifier: {len(orders)} orders match suffix '{identifier}'",
            )

    return None, "not_found"


def _build_crm_line_items_detail(order: "Order") -> list[dict[str, Any]]:
    """
    Build a normalized line-items list for the detail endpoint.

    Extends _build_crm_line_items() with additional fields:
    - product_id  (from line.mapped_product_id or raw_payload)
    - brand_id    (from raw_payload)
    - category    (from raw_payload)
    """
    items: list[dict[str, Any]] = []

    # --- Source 1: ORM relationship rows ---
    try:
        lines = getattr(order, "lines", None)
        if lines:
            for line in lines:
                try:
                    unit_price = None
                    if line.unit_price is not None:
                        unit_price = float(line.unit_price)
                    elif line.unit_price_cents is not None:
                        unit_price = round(line.unit_price_cents / 100.0, 2)

                    line_total = None
                    if line.total_price is not None:
                        line_total = float(line.total_price)
                    elif line.total_price_cents is not None:
                        line_total = round(line.total_price_cents / 100.0, 2)
                    elif unit_price is not None and line.quantity:
                        line_total = round(unit_price * (line.quantity or 0), 2)

                    raw = getattr(line, "raw_payload", None) or {}
                    items.append({
                        "product_name": line.product_name,
                        "sku": line.sku,
                        "quantity": line.quantity or 0,
                        "unit_price": unit_price,
                        "line_total": line_total,
                        "product_id": (
                            line.mapped_product_id
                            or raw.get("product_id")
                            or raw.get("product")
                        ),
                        "brand_id": raw.get("brand_id") or raw.get("brand"),
                        "category": raw.get("category") or raw.get("product_category"),
                        "discounts": raw.get("discount_amount") or raw.get("discounts"),
                        "shipping": raw.get("shipping"),
                        "tax": raw.get("tax"),
                        "product_identifiers": raw.get("product_identifiers"),
                    })
                except Exception:
                    pass
            if items:
                return items
    except Exception:
        pass

    # --- Fallback: delegate to the standard builder and inject None for new fields ---
    base_items = _build_crm_line_items(order)
    for item in base_items:
        item.setdefault("product_id", None)
        item.setdefault("brand_id", None)
        item.setdefault("category", item.get("category"))
    return base_items


async def validate_crm_access(
    db: AsyncSession,
    org_id: str,
    brand_id: str,
) -> dict:
    """
    Validate that org_id and brand_id exist and that brand belongs to org.

    Returns:
        {ok: bool, org_id: str, brand_id: str, error: str or None}
    """
    if not org_id:
        return {"ok": False, "error": "org_id is required (parameter or X-OPSYN-ORG header)"}

    if not brand_id:
        return {"ok": False, "error": "brand_id is required"}

    # Validate org exists (supports UUID or org_code via lookup_organization)
    org_lookup = await lookup_organization(db, org_id)
    if not org_lookup.get("ok"):
        logger.warning("[CRM] org_not_found org_id=%s", org_id)
        return {"ok": False, "error": org_lookup.get("error")}

    resolved_org = org_lookup.get("organization")
    resolved_org_id = str(resolved_org.id)

    # Validate brand exists and belongs to org
    brand_result = await db.execute(
        select(Brand).where(
            and_(Brand.id == brand_id, Brand.org_id == resolved_org_id)
        )
    )
    brand = brand_result.scalar_one_or_none()

    if not brand:
        logger.warning("[CRM] brand_not_found brand_id=%s org_id=%s", brand_id, resolved_org_id)
        return {"ok": False, "error": "Brand not found or does not belong to organization"}

    logger.info("[CRM] access_validated org_id=%s brand_id=%s", resolved_org_id, brand_id)

    return {"ok": True, "org_id": resolved_org_id, "brand_id": brand_id, "error": None}


@router.get("/diagnostic")
async def crm_diagnostic(
    brand: str = Query(None, description="Optional brand filter"),
    db: AsyncSession = Depends(get_db),
):
    """
    Read-only diagnostic for CRM account/customer persistence and API freshness.
    
    Verifies:
    - Database customer count
    - API returned customer count
    - Latest customer update timestamp
    - Account ID stability
    - Whether new accounts are persisted
    """
    try:
        logger.info("[CRMDiagnostic] request brand=%s", brand)
        
        # 1. Count distinct customers in database
        customer_count_result = await db.execute(
            select(func.count(func.distinct(Order.customer_name)))
            .where(Order.customer_name != None)
        )
        db_customer_count = customer_count_result.scalar() or 0
        
        # 2. Get list of all customers with metadata
        customers_result = await db.execute(
            select(
                Order.customer_name,
                func.count(Order.id).label("order_count"),
                func.max(Order.external_updated_at).label("latest_order_updated_at"),
                func.max(Order.updated_at).label("latest_db_updated_at"),
            )
            .where(Order.customer_name != None)
            .group_by(Order.customer_name)
            .order_by(func.max(Order.updated_at).desc())
        )
        
        customers = []
        for row in customers_result:
            customers.append({
                "id": row.customer_name,  # Stable ID = customer_name
                "name": row.customer_name,
                "order_count": row.order_count or 0,
                "latest_order_updated_at": row.latest_order_updated_at.isoformat() if row.latest_order_updated_at else None,
                "latest_db_updated_at": row.latest_db_updated_at.isoformat() if row.latest_db_updated_at else None,
            })
        
        # 3. Get newest customer (most recently updated)
        newest_customer = customers[0] if customers else None
        
        # 4. Get oldest customer (least recently updated)
        oldest_customer = customers[-1] if customers else None
        
        # 5. Count orders by customer (to verify persistence)
        orders_by_customer_result = await db.execute(
            select(
                Order.customer_name,
                func.count(Order.id).label("order_count"),
            )
            .where(Order.customer_name != None)
            .group_by(Order.customer_name)
            .order_by(func.count(Order.id).desc())
            .limit(5)
        )
        
        top_customers = [
            {
                "name": row.customer_name,
                "order_count": row.order_count or 0,
            }
            for row in orders_by_customer_result
        ]
        
        # 6. Check for NULL customer_name orders (data quality issue)
        null_customer_result = await db.execute(
            select(func.count(Order.id))
            .where(Order.customer_name == None)
        )
        null_customer_count = null_customer_result.scalar() or 0
        
        # 7. Get date range of customers
        date_range_result = await db.execute(
            select(
                func.min(Order.updated_at),
                func.max(Order.updated_at),
            )
            .where(Order.customer_name != None)
        )
        oldest_update, newest_update = date_range_result.fetchone()
        
        logger.info(
            "[CRMDiagnostic] complete db_count=%s api_count=%s newest=%s",
            db_customer_count,
            len(customers),
            newest_customer["name"] if newest_customer else None,
        )
        
        return {
            "ok": True,
            "diagnostic": {
                "database": {
                    "total_customers": db_customer_count,
                    "customers_with_orders": len(customers),
                    "orders_with_null_customer": null_customer_count,
                    "date_range": {
                        "oldest_update": oldest_update.isoformat() if oldest_update else None,
                        "newest_update": newest_update.isoformat() if newest_update else None,
                    },
                },
                "api": {
                    "returned_count": len(customers),
                    "customers": customers,
                },
                "persistence": {
                    "newest_customer": newest_customer,
                    "oldest_customer": oldest_customer,
                    "top_5_by_order_count": top_customers,
                },
                "id_stability": {
                    "id_type": "customer_name (string)",
                    "id_is_unique": True,
                    "id_is_stable": "Only if customer name never changes in LeafLink",
                    "sample_ids": [c["id"] for c in customers[:3]],
                },
                "freshness": {
                    "newest_customer_name": newest_customer["name"] if newest_customer else None,
                    "newest_customer_latest_update": newest_customer["latest_db_updated_at"] if newest_customer else None,
                    "oldest_customer_name": oldest_customer["name"] if oldest_customer else None,
                    "oldest_customer_latest_update": oldest_customer["latest_db_updated_at"] if oldest_customer else None,
                },
            },
        }
    
    except Exception as exc:
        logger.error("[CRMDiagnostic] error brand=%s error=%s", brand, str(exc)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(exc)[:500],
        }


@router.get("/health")
async def crm_health():
    """Health check for CRM endpoints."""
    return {
        "ok": True,
        "service": "crm",
        "version": "1.0",
    }


@router.get("/dashboard")
async def crm_dashboard(
    org_id: str = Query(..., description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    x_opsyn_org: str | None = Header(None, description="Optional org_id from header"),
    db: AsyncSession = Depends(get_db),
):
    """
    CRM dashboard scoped to org_id and brand_id.

    Requires:
    - org_id: Organization ID (UUID or org_code like "noble")
    - brand_id: Brand ID (UUID)

    Optional:
    - X-OPSYN-ORG header: Alternative way to pass org_id

    Returns:
    - total_orders: Total orders for brand
    - total_revenue: Total revenue (dollars)
    - unique_customers: Count of unique customers
    - top_customers: Top 10 customers by spend
    - recent_orders: Last 50 orders
    - synced_at: Timestamp

    Performance: <500ms
    """
    try:
        resolved_identifier = x_opsyn_org or org_id
        logger.info("[CRM] dashboard_requested org_id=%s brand_id=%s", resolved_identifier, brand_id)

        # Validate org and brand access
        access_result = await validate_crm_access(db, resolved_identifier, brand_id)
        if not access_result.get("ok"):
            logger.warning("[CRM] dashboard_access_denied error=%s", access_result.get("error"))
            return {"ok": False, "error": access_result.get("error")}

        resolved_org_id = access_result["org_id"]

        # 1. Get aggregate stats filtered by brand_id
        stats_result = await db.execute(
            select(
                func.count(func.distinct(Order.customer_name)).label("unique_customers"),
                func.count(Order.id).label("total_orders"),
                func.sum(Order.amount).label("total_revenue_cents"),
            )
            .where(
                and_(
                    Order.customer_name != None,
                    Order.brand_id == brand_id,
                )
            )
        )
        stats_row = stats_result.fetchone()

        unique_customers = stats_row.unique_customers or 0
        total_orders = stats_row.total_orders or 0
        total_revenue = float(stats_row.total_revenue_cents or 0) / 100.0

        # 2. Get top 10 customers by spend filtered by brand_id
        top_customers_result = await db.execute(
            select(
                Order.customer_name,
                func.count(Order.id).label("order_count"),
                func.sum(Order.amount).label("total_spend_cents"),
                func.max(Order.external_updated_at).label("last_order_at"),
            )
            .where(
                and_(
                    Order.customer_name != None,
                    Order.brand_id == brand_id,
                )
            )
            .group_by(Order.customer_name)
            .order_by(func.sum(Order.amount).desc())
            .limit(10)
        )

        top_customers = [
            {
                "name": row.customer_name,
                "order_count": row.order_count or 0,
                "total_spend": float(row.total_spend_cents or 0) / 100.0,
                "last_order_at": row.last_order_at.isoformat() if row.last_order_at else None,
            }
            for row in top_customers_result
        ]

        # 3. Get recent 50 orders filtered by brand_id
        recent_orders_result = await db.execute(
            select(Order)
            .where(Order.brand_id == brand_id)
            .order_by(Order.external_updated_at.desc())
            .limit(50)
        )

        recent_orders = [
            {
                "id": order.id,
                "external_id": order.external_order_id,
                "customer_name": order.customer_name,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0) / 100.0,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            }
            for order in recent_orders_result.scalars().all()
        ]

        synced_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "[CRM] dashboard_complete org_id=%s brand_id=%s customers=%s orders=%s revenue=%s",
            resolved_org_id,
            brand_id,
            unique_customers,
            total_orders,
            total_revenue,
        )

        return make_json_safe({
            "ok": True,
            "org_id": resolved_org_id,
            "brand_id": brand_id,
            "stats": {
                "total_orders": total_orders,
                "total_revenue": total_revenue,
                "unique_customers": unique_customers,
            },
            "top_customers": top_customers,
            "recent_orders": recent_orders,
            "synced_at": synced_at,
        })

    except Exception as e:
        logger.error("[CRM] dashboard_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "stats": {
                "total_orders": 0,
                "total_revenue": 0.0,
                "unique_customers": 0,
            },
            "top_customers": [],
            "recent_orders": [],
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/orders")
async def crm_orders(
    org_id: str = Query(..., description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    x_opsyn_org: str | None = Header(None, description="Optional org_id from header"),
    limit: int = Query(50, ge=10, le=500, description="Records per page (default 50, max 500)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    customer_name: Optional[str] = Query(None, description="Filter by customer name"),
    status: Optional[str] = Query(None, description="Filter by order status"),
    date_from: Optional[str] = Query(None, description="Filter orders from date (ISO format)"),
    date_to: Optional[str] = Query(None, description="Filter orders to date (ISO format)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated orders scoped to org_id and brand_id.

    Requires:
    - org_id: Organization ID (UUID or org_code)
    - brand_id: Brand ID (UUID)

    Supports:
    - Cursor-based pagination
    - Filter by customer_name
    - Filter by status
    - Filter by date range

    Returns: orders array + next_cursor (if more results).
    Each order row includes corrected totals, item/unit counts, and
    a total_source field ("stored" | "computed" | "missing").
    """
    try:
        resolved_identifier = x_opsyn_org or org_id
        logger.info(
            "[CRM] orders_requested org_id=%s brand_id=%s limit=%s customer=%s status=%s",
            resolved_identifier,
            brand_id,
            limit,
            customer_name,
            status,
        )

        # Validate org and brand access
        access_result = await validate_crm_access(db, resolved_identifier, brand_id)
        if not access_result.get("ok"):
            logger.warning("[CRM] orders_access_denied error=%s", access_result.get("error"))
            return {"ok": False, "error": access_result.get("error")}

        resolved_org_id = access_result["org_id"]

        # Build query with filters scoped to brand_id.
        # Eagerly load order lines so _build_crm_line_items can use them
        # without triggering lazy-load errors in async context.
        query = (
            select(Order)
            .options(selectinload(Order.lines))
            .where(Order.brand_id == brand_id)
        )

        if customer_name:
            query = query.where(Order.customer_name.ilike(f"%{customer_name}%"))

        if status:
            query = query.where(Order.status == status)

        if date_from:
            try:
                from_dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
                query = query.where(Order.external_created_at >= from_dt)
            except Exception:
                pass

        if date_to:
            try:
                to_dt = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
                query = query.where(Order.external_created_at <= to_dt)
            except Exception:
                pass

        # Cursor-based pagination — stable sort: external_created_at DESC, id DESC
        if cursor:
            try:
                cursor_id = base64.b64decode(cursor).decode()
                query = query.where(Order.id > cursor_id)
            except Exception:
                pass

        # Fetch limit + 1 to detect if there are more results
        query = query.order_by(Order.id).limit(limit + 1)

        result = await db.execute(query)
        orders_list = result.scalars().all()

        has_more = len(orders_list) > limit
        if has_more:
            orders_list = orders_list[:limit]

        orders = []
        total_source_counts: dict[str, int] = {"stored": 0, "computed": 0, "missing": 0}

        for order in orders_list:
            # Build line items (lightweight — no full serialization needed for list)
            line_items = _build_crm_line_items(order)

            # Compute corrected totals
            totals = _compute_order_totals(order, line_items)
            total_source_counts[totals["total_source"]] = (
                total_source_counts.get(totals["total_source"], 0) + 1
            )

            orders.append({
                "id": order.id,
                "order_number": order.order_number,
                "external_order_id": order.external_order_id,
                "external_id": order.external_order_id,  # backward-compat alias
                "customer_name": order.customer_name,
                "customer_id": order.customer_name,  # customer_name is the stable ID
                "status": order.status,
                "external_created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "order_date": order.external_created_at.isoformat() if order.external_created_at else None,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
                # Corrected financial fields
                "total_amount": totals["total_amount"],
                "subtotal_amount": totals["subtotal_amount"],
                "discount_total": totals["discount_total"],
                "shipping_total": totals["shipping_total"],
                "tax_total": totals["tax_total"],
                # Corrected counts
                "item_count": totals["item_count"],
                "unit_count": totals["unit_count"],
                # Payload truth metadata
                "has_line_items": len(line_items) > 0,
                "total_source": totals["total_source"],
            })

        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1]["id"]).encode()).decode()

        logger.info(
            "[CRM_ORDER_PAYLOAD_AUDIT] org_id=%s brand_id=%s count=%s has_more=%s "
            "total_source_stored=%s total_source_computed=%s total_source_missing=%s",
            resolved_org_id,
            brand_id,
            len(orders),
            has_more,
            total_source_counts.get("stored", 0),
            total_source_counts.get("computed", 0),
            total_source_counts.get("missing", 0),
        )

        return make_json_safe({
            "ok": True,
            "org_id": resolved_org_id,
            "brand_id": brand_id,
            "orders": orders,
            "count": len(orders),
            "has_more": has_more,
            "next_cursor": next_cursor,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        logger.error("[CRM] orders_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "orders": [],
            "count": 0,
            "has_more": False,
            "next_cursor": None,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/orders/{order_identifier}/debug")
async def crm_order_debug(
    order_identifier: str,
    org_id: str = Query(..., description="Organization ID (UUID or org_code)"),
    x_opsyn_org: str | None = Header(None, description="Optional org_id from header"),
    db: AsyncSession = Depends(get_db),
):
    """
    Debug endpoint — resolves an order by any identifier and returns raw DB
    state, all lookup candidates, computed totals, and the final API payload.

    Accepts org_id only (no brand_id required). Resolves by:
    integer id, order_number, external_order_id, UUID, or 8-char suffix.

    Use for diagnosis only. Do NOT expose in production UI.
    """
    try:
        resolved_identifier = x_opsyn_org or org_id

        # Resolve org
        org_lookup = await lookup_organization(db, resolved_identifier)
        if not org_lookup.get("ok"):
            return {"ok": False, "error": org_lookup.get("error")}
        resolved_org = org_lookup["organization"]
        resolved_org_id = str(resolved_org.id)

        # Run all lookup strategies and record candidates
        lookup_candidates = []

        # Strategy: integer_id
        try:
            order_id_int = int(order_identifier)
            if order_id_int > 0:
                r = await db.execute(
                    select(Order)
                    .where(and_(Order.id == str(order_id_int), Order.org_id == resolved_org_id))
                    .options(selectinload(Order.lines))
                )
                o = r.scalar_one_or_none()
                lookup_candidates.append({
                    "strategy": "integer_id",
                    "matched": o is not None,
                    "order_id": o.id if o else None,
                    "order_number": o.order_number if o else None,
                    "external_order_id": o.external_order_id if o else None,
                })
        except (ValueError, TypeError):
            pass

        # Strategy: order_number
        r = await db.execute(
            select(Order)
            .where(and_(Order.order_number == order_identifier, Order.org_id == resolved_org_id))
            .options(selectinload(Order.lines))
        )
        o = r.scalar_one_or_none()
        lookup_candidates.append({
            "strategy": "order_number",
            "matched": o is not None,
            "order_id": o.id if o else None,
            "order_number": o.order_number if o else None,
            "external_order_id": o.external_order_id if o else None,
        })

        # Strategy: external_order_id
        r = await db.execute(
            select(Order)
            .where(and_(Order.external_order_id == order_identifier, Order.org_id == resolved_org_id))
            .options(selectinload(Order.lines))
        )
        o = r.scalar_one_or_none()
        lookup_candidates.append({
            "strategy": "external_order_id",
            "matched": o is not None,
            "order_id": o.id if o else None,
            "order_number": o.order_number if o else None,
            "external_order_id": o.external_order_id if o else None,
        })

        # Strategy: uuid
        try:
            uuid_val = UUID(order_identifier)
            r = await db.execute(
                select(Order)
                .where(and_(Order.id == str(uuid_val), Order.org_id == resolved_org_id))
                .options(selectinload(Order.lines))
            )
            o = r.scalar_one_or_none()
            lookup_candidates.append({
                "strategy": "uuid",
                "matched": o is not None,
                "order_id": o.id if o else None,
                "order_number": o.order_number if o else None,
                "external_order_id": o.external_order_id if o else None,
            })
        except (ValueError, TypeError):
            pass

        # Strategy: suffix
        if len(order_identifier) == 8:
            r = await db.execute(
                select(Order)
                .where(
                    and_(
                        or_(
                            Order.external_order_id.endswith(order_identifier),
                            Order.id.endswith(order_identifier),
                        ),
                        Order.org_id == resolved_org_id,
                    )
                )
                .options(selectinload(Order.lines))
            )
            suffix_orders = r.scalars().all()
            for so in suffix_orders:
                lookup_candidates.append({
                    "strategy": "suffix",
                    "matched": True,
                    "order_id": so.id,
                    "order_number": so.order_number,
                    "external_order_id": so.external_order_id,
                })
            if not suffix_orders:
                lookup_candidates.append({
                    "strategy": "suffix",
                    "matched": False,
                    "order_id": None,
                    "order_number": None,
                    "external_order_id": None,
                })

        # Resolve the best match
        order, matched_by = await _resolve_order_identifier(db, resolved_org_id, order_identifier)

        if not order:
            logger.warning(
                "[CRM_ORDER_DETAIL_404] org_id=%s identifier=%s reason=not_found",
                resolved_org_id,
                order_identifier,
            )
            return make_json_safe({
                "ok": False,
                "identifier": order_identifier,
                "lookup_candidates": lookup_candidates,
                "matched_order": None,
                "raw_order_fields": None,
                "raw_order_lines": [],
                "final_api_payload": None,
                "error": f"Order not found for identifier '{order_identifier}'",
            })

        logger.info(
            "[CRM_ORDER_DETAIL_LOOKUP] org_id=%s identifier=%s matched_by=%s",
            resolved_org_id,
            order_identifier,
            matched_by,
        )

        line_items = _build_crm_line_items_detail(order)
        totals = _compute_order_totals(order, line_items)

        raw_order_fields = {
            "id": order.id,
            "order_number": order.order_number,
            "external_order_id": order.external_order_id,
            "total_cents": order.total_cents,
            "amount": float(order.amount) if order.amount is not None else None,
            "item_count": order.item_count,
            "unit_count": order.unit_count,
            "has_line_items_json": order.line_items_json is not None,
            "has_raw_payload": order.raw_payload is not None,
            "line_items_json_count": (
                len(order.line_items_json)
                if isinstance(order.line_items_json, list)
                else None
            ),
            "orm_lines_count": len(order.lines) if order.lines else 0,
        }

        raw_order_lines = []
        if order.lines:
            for line in order.lines:
                raw_order_lines.append({
                    "id": line.id,
                    "sku": line.sku,
                    "product_name": line.product_name,
                    "quantity": line.quantity,
                    "unit_price": float(line.unit_price) if line.unit_price is not None else None,
                    "unit_price_cents": line.unit_price_cents,
                    "total_price": float(line.total_price) if line.total_price is not None else None,
                    "total_price_cents": line.total_price_cents,
                })

        final_api_payload = {
            "ok": True,
            "id": order.id,
            "order_number": order.order_number,
            "external_order_id": order.external_order_id,
            "external_id": order.external_order_id,
            "customer_name": order.customer_name,
            "customer_id": order.customer_name,
            "status": order.status,
            "external_created_at": order.external_created_at.isoformat() if order.external_created_at else None,
            "line_items": line_items,
            **totals,
        }

        return make_json_safe({
            "ok": True,
            "identifier": order_identifier,
            "lookup_candidates": lookup_candidates,
            "matched_order": {
                "id": order.id,
                "order_number": order.order_number,
                "external_order_id": order.external_order_id,
                "customer_name": order.customer_name,
            },
            "raw_order_fields": raw_order_fields,
            "raw_order_lines": raw_order_lines,
            "final_api_payload": final_api_payload,
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "[CRM] order_debug_failed identifier=%s error=%s",
            order_identifier,
            str(e)[:500],
            exc_info=True,
        )
        return {"ok": False, "error": str(e)[:500]}


@router.get("/orders/{order_identifier}")
async def crm_order_detail(
    order_identifier: str,
    org_id: str = Query(..., description="Organization ID (UUID or org_code)"),
    brand_id: Optional[str] = Query(None, description="Brand ID (UUID) — optional, used for access validation only"),
    x_opsyn_org: str | None = Header(None, description="Optional org_id from header"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get full detail for a single order, including corrected totals and line items.

    Accepts org_id (required) and an optional brand_id for access validation.
    Resolves the order by any of: internal id, order_number, external_order_id,
    UUID, or 8-character display suffix (e.g. ``28b6e3a4``).

    Returns all order fields plus:
    - line_items[]: full line item array with product_name, sku, quantity,
      unit_price, line_total, product_id, brand_id, category
    - total_amount, subtotal_amount, discount_total, shipping_total, tax_total
    - item_count, unit_count (corrected)
    - total_source: "stored" | "computed" | "missing"
    """
    try:
        resolved_identifier = x_opsyn_org or org_id
        logger.info(
            "[CRM] order_detail_requested identifier=%s org_id=%s brand_id=%s",
            order_identifier,
            resolved_identifier,
            brand_id,
        )

        # Resolve org (brand_id is optional — org_id is the primary tenant scope)
        if brand_id:
            access_result = await validate_crm_access(db, resolved_identifier, brand_id)
            if not access_result.get("ok"):
                logger.warning("[CRM] order_detail_access_denied error=%s", access_result.get("error"))
                return {"ok": False, "error": access_result.get("error")}
            resolved_org_id = access_result["org_id"]
        else:
            org_lookup = await lookup_organization(db, resolved_identifier)
            if not org_lookup.get("ok"):
                logger.warning("[CRM] order_detail_org_not_found org_id=%s", resolved_identifier)
                return {"ok": False, "error": org_lookup.get("error")}
            resolved_org_id = str(org_lookup["organization"].id)

        # Multi-identifier lookup — resolves by any supported format
        order, matched_by = await _resolve_order_identifier(db, resolved_org_id, order_identifier)

        if not order:
            logger.warning(
                "[CRM_ORDER_DETAIL_404] org_id=%s identifier=%s reason=not_found",
                resolved_org_id,
                order_identifier,
            )
            return {
                "ok": False,
                "error": f"Order not found for identifier '{order_identifier}'",
                "identifier": order_identifier,
                "org_id": resolved_org_id,
                "synced_at": datetime.now(timezone.utc).isoformat(),
            }

        logger.info(
            "[CRM_ORDER_DETAIL_LOOKUP] org_id=%s identifier=%s matched_by=%s",
            resolved_org_id,
            order_identifier,
            matched_by,
        )

        # Build line items with extended fields (product_id, brand_id, category)
        line_items = _build_crm_line_items_detail(order)

        # Compute corrected totals
        totals = _compute_order_totals(order, line_items)

        first_sku = line_items[0].get("sku") if line_items else None
        first_line_total = line_items[0].get("line_total") if line_items else None

        logger.info(
            "[CRM_ORDER_LINE_ITEMS_AUDIT] order_id=%s line_count=%s first_sku=%s first_line_total=%s",
            order.id,
            len(line_items),
            first_sku,
            f"{first_line_total:.2f}" if first_line_total is not None else None,
        )

        return make_json_safe({
            "ok": True,
            "org_id": resolved_org_id,
            "brand_id": order.brand_id,
            # Identity
            "id": order.id,
            "order_number": order.order_number,
            "external_order_id": order.external_order_id,
            "external_id": order.external_order_id,  # backward-compat alias
            "customer_name": order.customer_name,
            "customer_id": order.customer_name,  # customer_name is the stable ID
            "status": order.status,
            "review_status": order.review_status,
            # Timestamps
            "external_created_at": order.external_created_at.isoformat() if order.external_created_at else None,
            "order_date": order.external_created_at.isoformat() if order.external_created_at else None,
            "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
            "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            # Corrected financial fields
            "total_amount": totals["total_amount"],
            "subtotal_amount": totals["subtotal_amount"],
            "discount_total": totals["discount_total"],
            "shipping_total": totals["shipping_total"],
            "tax_total": totals["tax_total"],
            # Corrected counts
            "item_count": totals["item_count"],
            "unit_count": totals["unit_count"],
            # Payload truth metadata
            "has_line_items": len(line_items) > 0,
            "total_source": totals["total_source"],
            # Full line items array (with product_id, brand_id, category)
            "line_items": line_items,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "[CRM] order_detail_failed identifier=%s error=%s",
            order_identifier,
            str(e)[:500],
            exc_info=True,
        )
        return {
            "ok": False,
            "error": str(e)[:500],
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/full-data")
async def crm_full_data(
    limit: int = Query(20000, ge=100, le=50000, description="Max records (default 20k, max 50k)"),
    db: AsyncSession = Depends(get_db),
):
    """
    DEBUG/INTERNAL ONLY - Get complete CRM dataset.

    Not for production frontend use. Use /crm/dashboard for summaries
    and /crm/orders for paginated data instead.

    Useful for:
    - Data exports
    - Analytics
    - Internal debugging
    - Batch operations
    """
    try:
        logger.info("[CRM] full_data_requested limit=%s", limit)

        result = await db.execute(
            select(Order)
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
        )
        orders_list = result.scalars().all()

        orders = []
        for order in orders_list:
            orders.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "customer_name": order.customer_name,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0) / 100.0,
                "item_count": order.item_count or 0,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        logger.info("[CRM] full_data_returned count=%s", len(orders))

        return make_json_safe({
            "ok": True,
            "orders": orders,
            "count": len(orders),
            "limit": limit,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        logger.error("[CRM] full_data_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "orders": [],
            "count": 0,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/customers")
async def crm_customers(
    org_id: str = Query(..., description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    x_opsyn_org: str | None = Header(None, description="Optional org_id from header"),
    limit: int = Query(50, ge=10, le=500, description="Records per page (default 50, max 500)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor"),
    search: Optional[str] = Query(None, description="Search customer name"),
    sort_by: Optional[str] = Query("spend", description="Sort by: spend, orders, recent"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated customers with aggregated stats, scoped to org_id and brand_id.

    Requires:
    - org_id: Organization ID (UUID or org_code)
    - brand_id: Brand ID (UUID)

    Returns per-customer:
    - name
    - order_count
    - total_spend (dollars)
    - last_order_at

    Supports:
    - Cursor pagination
    - Name search
    - Sort by spend, order count, or recency
    """
    try:
        resolved_identifier = x_opsyn_org or org_id
        logger.info(
            "[CRM] customers_requested org_id=%s brand_id=%s limit=%s search=%s sort=%s",
            resolved_identifier,
            brand_id,
            limit,
            search,
            sort_by,
        )

        # Validate org and brand access
        access_result = await validate_crm_access(db, resolved_identifier, brand_id)
        if not access_result.get("ok"):
            logger.warning("[CRM] customers_access_denied error=%s", access_result.get("error"))
            return {"ok": False, "error": access_result.get("error")}

        resolved_org_id = access_result["org_id"]

        # Build query scoped to brand_id
        query = select(
            Order.customer_name,
            func.count(Order.id).label("order_count"),
            func.sum(Order.amount).label("total_spend_cents"),
            func.max(Order.external_updated_at).label("last_order_at"),
        ).where(
            and_(Order.customer_name != None, Order.brand_id == brand_id)
        ).group_by(Order.customer_name)

        if search:
            query = query.where(Order.customer_name.ilike(f"%{search}%"))

        # Sort
        if sort_by == "orders":
            query = query.order_by(func.count(Order.id).desc())
        elif sort_by == "recent":
            query = query.order_by(func.max(Order.external_updated_at).desc())
        else:  # spend (default)
            query = query.order_by(func.sum(Order.amount).desc())

        # Cursor pagination
        if cursor:
            try:
                cursor_name = base64.b64decode(cursor).decode()
                query = query.where(Order.customer_name > cursor_name)
            except Exception:
                pass

        query = query.limit(limit + 1)

        result = await db.execute(query)
        customers_list = result.fetchall()

        has_more = len(customers_list) > limit
        if has_more:
            customers_list = customers_list[:limit]

        customers = []
        for row in customers_list:
            customers.append({
                "name": row.customer_name,
                "order_count": row.order_count or 0,
                "total_spend": float(row.total_spend_cents or 0) / 100.0,
                "last_order_at": row.last_order_at.isoformat() if row.last_order_at else None,
            })

        next_cursor = None
        if has_more and customers:
            next_cursor = base64.b64encode(customers[-1]["name"].encode()).decode()

        logger.info(
            "[CRM] customers_returned org_id=%s brand_id=%s count=%s has_more=%s",
            resolved_org_id,
            brand_id,
            len(customers),
            has_more,
        )

        return make_json_safe({
            "ok": True,
            "org_id": resolved_org_id,
            "brand_id": brand_id,
            "customers": customers,
            "count": len(customers),
            "has_more": has_more,
            "next_cursor": next_cursor,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        logger.error("[CRM] customers_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "customers": [],
            "count": 0,
            "has_more": False,
            "next_cursor": None,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/customers/{customer_id}")
async def crm_customer_detail(
    customer_id: str,
    org_id: str = Query(..., description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    x_opsyn_org: str | None = Header(None, description="Optional org_id from header"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed customer information scoped to org_id and brand_id.

    Requires:
    - org_id: Organization ID (UUID or org_code)
    - brand_id: Brand ID (UUID)

    Returns: name, order_count, total_spend, last_order_at, contact info.
    """
    try:
        resolved_identifier = x_opsyn_org or org_id
        logger.info(
            "[CRM] customer_detail_requested customer_id=%s org_id=%s brand_id=%s",
            customer_id,
            resolved_identifier,
            brand_id,
        )

        # Validate org and brand access
        access_result = await validate_crm_access(db, resolved_identifier, brand_id)
        if not access_result.get("ok"):
            logger.warning("[CRM] customer_detail_access_denied error=%s", access_result.get("error"))
            return {"ok": False, "error": access_result.get("error")}

        resolved_org_id = access_result["org_id"]

        # Get customer orders scoped to brand_id
        orders_result = await db.execute(
            select(Order)
            .where(
                and_(
                    Order.customer_name == customer_id,
                    Order.brand_id == brand_id,
                )
            )
            .order_by(Order.external_updated_at.desc())
        )
        orders = orders_result.scalars().all()

        if not orders:
            logger.warning(
                "[CRM] customer_not_found customer_id=%s org_id=%s brand_id=%s",
                customer_id,
                resolved_org_id,
                brand_id,
            )
            return {
                "ok": False,
                "error": "Customer not found",
                "source": "empty",
                "data_source": "empty",
                "synced_at": datetime.now(timezone.utc).isoformat(),
            }

        # Aggregate customer data
        order_count = len(orders)
        total_spend = sum(float(o.amount or 0) for o in orders)
        last_order_at = max((o.external_updated_at for o in orders if o.external_updated_at), default=None)

        logger.info(
            "[CRM] customer_detail_complete customer_id=%s org_id=%s brand_id=%s orders=%s total_spend=%s",
            customer_id,
            resolved_org_id,
            brand_id,
            order_count,
            total_spend,
        )

        synced_at = datetime.now(timezone.utc).isoformat()

        return make_json_safe({
            "ok": True,
            "org_id": resolved_org_id,
            "brand_id": brand_id,
            "source": "live",
            "data_source": "live",
            "synced_at": synced_at,
            "id": customer_id,
            "name": customer_id,
            "order_count": order_count,
            "total_spend": total_spend,
            "last_order_at": last_order_at.isoformat() if last_order_at else None,
            "contact_name": customer_id,
            "email": None,
            "phone": None,
            "address": None,
        })

    except Exception as e:
        logger.error("[CRM] customer_detail_failed customer_id=%s error=%s", customer_id, str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/customers/{customer_id}/orders")
async def crm_customer_orders(
    customer_id: str,
    org_id: str = Query(..., description="Organization ID (UUID or org_code)"),
    brand_id: str = Query(..., description="Brand ID (UUID)"),
    x_opsyn_org: str | None = Header(None, description="Optional org_id from header"),
    limit: int = Query(50, ge=1, le=500, description="Records per page (default 50, max 500)"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get orders for a specific customer scoped to org_id and brand_id.

    Requires:
    - org_id: Organization ID (UUID or org_code)
    - brand_id: Brand ID (UUID)

    Returns: order_id, order_number, status, amount, item_count, created_at.
    """
    try:
        resolved_identifier = x_opsyn_org or org_id
        logger.info(
            "[CRM] customer_orders_requested customer_id=%s org_id=%s brand_id=%s limit=%s offset=%s",
            customer_id,
            resolved_identifier,
            brand_id,
            limit,
            offset,
        )

        # Validate org and brand access
        access_result = await validate_crm_access(db, resolved_identifier, brand_id)
        if not access_result.get("ok"):
            logger.warning("[CRM] customer_orders_access_denied error=%s", access_result.get("error"))
            return {"ok": False, "error": access_result.get("error")}

        resolved_org_id = access_result["org_id"]

        # Get customer orders scoped to brand_id
        orders_result = await db.execute(
            select(Order)
            .where(
                and_(
                    Order.customer_name == customer_id,
                    Order.brand_id == brand_id,
                )
            )
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
            .offset(offset)
        )
        orders = orders_result.scalars().all()

        orders_data = []
        for order in orders:
            orders_data.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0),
                "item_count": order.item_count or 0,
                "unit_count": order.unit_count or 0,
                "review_status": order.review_status,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if orders_data else "empty"

        logger.info(
            "[CRM] customer_orders_returned customer_id=%s org_id=%s brand_id=%s count=%s",
            customer_id,
            resolved_org_id,
            brand_id,
            len(orders_data),
        )

        return make_json_safe({
            "ok": True,
            "org_id": resolved_org_id,
            "brand_id": brand_id,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "customer_id": customer_id,
            "orders": orders_data,
            "count": len(orders_data),
        })

    except Exception as e:
        logger.error("[CRM] customer_orders_failed customer_id=%s error=%s", customer_id, str(e)[:500], exc_info=True)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e)[:500],
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "customer_id": customer_id,
            "orders": [],
            "count": 0,
        }


@router.get("/customers/{customer_id}/activity")
async def crm_customer_activity(
    customer_id: str,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Get recent activity for a customer (orders, updates).
    Returns: activity_type, timestamp, details.
    """
    try:
        logger.info("crm: customer_activity_requested customer_id=%s limit=%s", customer_id, limit)

        # Get recent orders as activity
        orders_result = await db.execute(
            select(Order)
            .where(Order.customer_name == customer_id)
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
        )
        orders = orders_result.scalars().all()

        activity = []
        for order in orders:
            activity.append({
                "type": "order",
                "timestamp": order.external_updated_at.isoformat() if order.external_updated_at else None,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0),
            })

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if activity else "empty"

        logger.info("crm: customer_activity_complete customer_id=%s count=%s", customer_id, len(activity))

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "customer_id": customer_id,
            "activity": activity,
            "count": len(activity),
        })

    except Exception as e:
        logger.error("crm: customer_activity_failed customer_id=%s error=%s", customer_id, e)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "customer_id": customer_id,
            "activity": [],
            "count": 0,
        }


@router.get("/recent-orders")
async def crm_recent_orders(
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Get most recent orders across all customers.
    Returns: order_id, customer_name, order_number, status, amount, created_at.
    """
    try:
        logger.info("crm: recent_orders_requested limit=%s", limit)

        # Get recent orders
        orders_result = await db.execute(
            select(Order)
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
        )
        orders = orders_result.scalars().all()

        orders_data = []
        for order in orders:
            orders_data.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "customer_name": order.customer_name,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0),
                "item_count": order.item_count or 0,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if orders_data else "empty"

        logger.info("[CRM] orders_query count=%s source=%s", len(orders_data), source)

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "orders": orders_data,
            "count": len(orders_data),
        })

    except Exception as e:
        logger.error("crm: recent_orders_failed error=%s", e)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "orders": [],
            "count": 0,
        }


@router.get("/sync-status")
async def crm_sync_status(db: AsyncSession = Depends(get_db)):
    """
    Get sync status for all brands.
    Returns: brand_id, sync_status, last_sync_at, last_error.
    """
    try:
        logger.info("crm: sync_status_requested")

        # Get sync status for all brands
        creds_result = await db.execute(
            select(BrandAPICredential)
            .where(BrandAPICredential.is_active == True)
            .order_by(BrandAPICredential.last_sync_at.desc())
        )
        creds = creds_result.scalars().all()

        brands = []
        for cred in creds:
            brands.append({
                "brand_id": cred.brand_id,
                "integration": cred.integration_name,
                "sync_status": cred.sync_status,
                "last_sync_at": cred.last_sync_at.isoformat() if cred.last_sync_at else None,
                "last_error": cred.last_error,
            })

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if brands else "empty"

        logger.info("crm: sync_status_complete brands=%s", len(brands))

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "brands": brands,
            "count": len(brands),
        })

    except Exception as e:
        logger.error("crm: sync_status_failed error=%s", e)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "brands": [],
            "count": 0,
        }


@router.get("/sync")
async def crm_sync(
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return records updated after this time"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of records to return (default 500, max 1000)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all CRM data (customers and recent orders) for incremental sync.

    Combines customer and order data in a single sync call for efficiency.

    Query params:
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z")
    - cursor: Pagination cursor from previous response
    - limit: Number of records (1-1000, default 500)

    Response includes:
    - customers: Array of customer objects (derived from orders)
    - orders: Array of order objects
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more records exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    try:
        logger.info(
            "crm: sync_requested updated_after=%s cursor=%s limit=%s",
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
        )

        # Parse updated_after if provided
        updated_after_dt = None
        if updated_after:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
                logger.info("crm: sync_filter updated_after=%s", updated_after_dt.isoformat())
            except ValueError as e:
                logger.error("crm: sync_invalid_timestamp error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Decode cursor if provided
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
                logger.info("crm: sync_cursor_decoded cursor_id=%s", cursor_id)
            except Exception as e:
                logger.error("crm: sync_invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Fetch orders (orders are the source of truth for both customers and orders)
        order_query = select(Order)
        if updated_after_dt:
            order_query = order_query.where(Order.updated_at >= updated_after_dt)
        if cursor_id is not None:
            order_query = order_query.where(Order.id > cursor_id)
        order_query = order_query.order_by(Order.updated_at.asc(), Order.id.asc())
        order_query = order_query.limit(limit + 1)

        order_result = await db.execute(order_query)
        orders = order_result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # Build order response
        orders_data = []
        for order in orders:
            order_dict = {
                "id": order.id,
                "external_id": order.external_order_id,
                "order_number": order.order_number,
                "customer_name": order.customer_name,
                "amount": float(order.amount) if order.amount else 0,
                "item_count": order.item_count,
                "unit_count": order.unit_count,
                "status": order.status,
                "review_status": order.review_status,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
            }
            orders_data.append(order_dict)

        # Derive unique customers from the fetched orders
        seen_customers: dict[str, dict] = {}
        for order in orders:
            name = order.customer_name
            if not name or name in seen_customers:
                continue
            seen_customers[name] = {
                "id": name,
                "name": name,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
            }
        customers_data = list(seen_customers.values())

        # Determine next cursor
        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1].id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        source = "live" if (orders_data or customers_data) else "empty"

        logger.info(
            "[CRM] dashboard_complete customers=%s orders=%s source=%s",
            len(customers_data),
            len(orders_data),
            source,
        )

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": server_time,
            "customers": customers_data,
            "orders": orders_data,
            "count": len(orders_data),
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("crm: sync_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "customers": [],
            "orders": [],
            "count": 0,
        }


@router.get("/customers/sync")
async def crm_customers_sync(
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return customers updated after this time"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of customers to return (default 500, max 1000)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get customers for incremental sync with cursor-based pagination.

    Customers are derived from orders since there is no separate customer table.

    Query params:
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z")
    - cursor: Pagination cursor from previous response
    - limit: Number of customers (1-1000, default 500)

    Response includes:
    - customers: Array of customer objects
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more customers exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    try:
        logger.info(
            "crm: customers_sync_requested updated_after=%s cursor=%s limit=%s",
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
        )

        # Parse updated_after if provided
        updated_after_dt = None
        if updated_after:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
            except ValueError as e:
                logger.error("crm: customers_sync_invalid_timestamp error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Decode cursor if provided
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
            except Exception as e:
                logger.error("crm: customers_sync_invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Query orders to derive customer data
        query = select(Order)
        if updated_after_dt:
            query = query.where(Order.updated_at >= updated_after_dt)
        if cursor_id is not None:
            query = query.where(Order.id > cursor_id)
        query = query.order_by(Order.updated_at.asc(), Order.id.asc())
        query = query.limit(limit + 1)

        result = await db.execute(query)
        orders = result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # Derive unique customers with aggregated stats
        customers_data = []
        seen_names: set[str] = set()
        for order in orders:
            name = order.customer_name
            if not name or name in seen_names:
                continue
            seen_names.add(name)

            order_count_result = await db.execute(
                select(func.count(Order.id)).where(Order.customer_name == name)
            )
            order_count = order_count_result.scalar() or 0

            total_spend_result = await db.execute(
                select(func.sum(Order.amount)).where(Order.customer_name == name)
            )
            total_spend = float(total_spend_result.scalar() or 0)

            last_order_result = await db.execute(
                select(Order.external_created_at)
                .where(Order.customer_name == name)
                .order_by(Order.external_created_at.desc())
                .limit(1)
            )
            last_order_at = last_order_result.scalar()

            customers_data.append({
                "id": name,
                "name": name,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
                "order_count": order_count,
                "total_spend": total_spend,
                "last_order_at": last_order_at.isoformat() if last_order_at else None,
            })

        # Determine next cursor
        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1].id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        source = "live" if customers_data else "empty"

        logger.info(
            "[CRM] customers_query count=%s has_more=%s source=%s",
            len(customers_data),
            has_more,
            source,
        )

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": server_time,
            "count": len(customers_data),
            "customers": customers_data,
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("crm: customers_sync_failed error=%s", e, exc_info=True)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "count": 0,
            "customers": [],
        }


@router.get("/recent-orders/sync")
async def crm_recent_orders_sync(
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return orders updated after this time"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of orders to return (default 500, max 1000)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get recent orders for incremental sync with cursor-based pagination.

    Query params:
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z")
    - cursor: Pagination cursor from previous response
    - limit: Number of orders (1-1000, default 500)

    Response includes:
    - orders: Array of order objects
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more orders exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    try:
        logger.info(
            "crm: recent_orders_sync_requested updated_after=%s cursor=%s limit=%s",
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
        )

        # Parse updated_after if provided
        updated_after_dt = None
        if updated_after:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
            except ValueError as e:
                logger.error("crm: recent_orders_sync_invalid_timestamp error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Decode cursor if provided
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
            except Exception as e:
                logger.error("crm: recent_orders_sync_invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Build query
        query = select(Order)
        if updated_after_dt:
            query = query.where(Order.updated_at >= updated_after_dt)
        if cursor_id is not None:
            query = query.where(Order.id > cursor_id)
        query = query.order_by(Order.updated_at.asc(), Order.id.asc())
        query = query.limit(limit + 1)

        result = await db.execute(query)
        orders = result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # Build response
        orders_data = []
        for order in orders:
            order_dict = {
                "id": order.id,
                "external_id": order.external_order_id,
                "order_number": order.order_number,
                "customer_name": order.customer_name,
                "amount": float(order.amount) if order.amount else 0,
                "item_count": order.item_count,
                "unit_count": order.unit_count,
                "status": order.status,
                "review_status": order.review_status,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
            }
            orders_data.append(order_dict)

        # Determine next cursor
        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1].id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        source = "live" if orders_data else "empty"

        logger.info(
            "[CRM] orders_query count=%s has_more=%s source=%s",
            len(orders_data),
            has_more,
            source,
        )

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": server_time,
            "count": len(orders_data),
            "orders": orders_data,
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("crm: recent_orders_sync_failed error=%s", e, exc_info=True)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "count": 0,
            "orders": [],
        }
