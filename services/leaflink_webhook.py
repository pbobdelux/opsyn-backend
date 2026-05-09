"""
LeafLink webhook processing service.

Handles inbound webhook payloads from LeafLink:
- Signature verification (HMAC-SHA256 via LL-Signature header)
- Tenant resolution from company_id in payload
- Idempotent order upsert by (brand_id, external_order_id)
- Safe logging (never logs api_key or webhook_key)
"""

import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import BrandAPICredential, Order, LeafLinkWebhookEvent, SyncRequest
from services.leaflink_sync import safe_uuid_for_db, safe_uuid_mapped_product
from utils.json_utils import make_json_safe

# Feature flag: require signature verification (default True in production)
LEAFLINK_WEBHOOK_REQUIRE_SIGNATURE = os.getenv("LEAFLINK_WEBHOOK_REQUIRE_SIGNATURE", "true").lower() == "true"

logger = logging.getLogger("leaflink_webhook")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------

def verify_ll_signature(payload_bytes: bytes, signature_header: str, webhook_key: str) -> bool:
    """
    Verify the LL-Signature header using HMAC-SHA256.

    LeafLink signs the raw request body with the webhook_key secret.
    The signature is a hex-encoded HMAC-SHA256 digest.

    Args:
        payload_bytes: Raw request body bytes.
        signature_header: Value of the LL-Signature header.
        webhook_key: Webhook secret from brand_api_credentials.

    Returns:
        True if the signature is valid, False otherwise.
    """
    if not signature_header or not webhook_key:
        return False

    try:
        expected = hmac.new(
            webhook_key.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature_header.lower())
    except Exception as exc:
        logger.error("[LeafLinkWebhook] signature_verification_error error=%s", exc)
        return False


# ---------------------------------------------------------------------------
# Tenant resolution
# ---------------------------------------------------------------------------

async def resolve_tenant_from_company_id(
    db: AsyncSession,
    company_id: str,
) -> dict[str, Any]:
    """
    Resolve brand and org from a LeafLink company_id.

    Looks up brand_api_credentials WHERE company_id = :company_id
    AND integration_name = 'leaflink' AND is_active = true.

    Returns:
        {
            "ok": bool,
            "brand_id": str | None,
            "org_id": str | None,
            "credential": BrandAPICredential | None,
            "error": str | None,
            "status_code": int,   # 200, 400, or 409
        }
    """
    if not company_id:
        return {
            "ok": False,
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "error": "company_id is required",
            "status_code": 400,
        }

    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.company_id == company_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        rows = result.scalars().all()
    except Exception as exc:
        logger.error(
            "[LeafLinkWebhook] tenant_lookup_error company_id=%s error=%s",
            company_id,
            exc,
        )
        return {
            "ok": False,
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "error": "Database error during tenant lookup",
            "status_code": 500,
        }

    if not rows:
        logger.warning(
            "[LeafLinkWebhook] tenant_not_found company_id=%s",
            company_id,
        )
        return {
            "ok": False,
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "error": f"Unknown company_id: {company_id}",
            "status_code": 400,
        }

    if len(rows) > 1:
        brand_ids = [r.brand_id for r in rows]
        logger.warning(
            "[LeafLinkWebhook] ambiguous_company_id company_id=%s brand_ids=%s",
            company_id,
            brand_ids,
        )
        return {
            "ok": False,
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "error": f"Ambiguous company_id {company_id} maps to multiple brands: {brand_ids}",
            "status_code": 409,
        }

    cred = rows[0]
    brand_id = cred.brand_id
    org_id = cred.org_id  # May be None if not yet populated

    logger.info(
        "[LeafLinkWebhook] resolved org_id=%s brand_id=%s",
        org_id,
        brand_id,
    )

    return {
        "ok": True,
        "brand_id": brand_id,
        "org_id": org_id,
        "credential": cred,
        "error": None,
        "status_code": 200,
    }


# ---------------------------------------------------------------------------
# Order field extraction helpers
# ---------------------------------------------------------------------------

def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_val = str(value).strip()
    return text_val if text_val else None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_decimal(value: Any) -> Optional[Decimal]:
    try:
        if value is None or value == "":
            return None
        return Decimal(str(value))
    except Exception:
        return None


def _parse_dt(val: Any) -> Optional[datetime]:
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(val, datetime):
        return val
    return None


def _decimal_to_cents(value: Optional[Decimal]) -> Optional[int]:
    if value is None:
        return None
    return int((value * 100).quantize(Decimal("1")))


def _normalize_line_items(raw_line_items: Any) -> list[dict[str, Any]]:
    """Normalize raw line items from a webhook order payload."""
    if not raw_line_items:
        return []

    if isinstance(raw_line_items, dict):
        nested = raw_line_items.get("line_items")
        if isinstance(nested, list):
            raw_line_items = nested
        else:
            return []

    if not isinstance(raw_line_items, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in raw_line_items:
        if not isinstance(item, dict):
            continue

        sku = _safe_str(item.get("sku") or item.get("product_sku") or item.get("external_sku"))
        product_name = _safe_str(item.get("product_name") or item.get("name") or item.get("product"))
        quantity = _safe_int(item.get("quantity") or item.get("qty") or item.get("units"), default=0)

        unit_price = _safe_decimal(item.get("unit_price"))
        if unit_price is None and item.get("unit_price_cents") is not None:
            cents = _safe_int(item.get("unit_price_cents"), default=0)
            unit_price = Decimal(cents) / Decimal("100")

        total_price = _safe_decimal(item.get("total_price"))
        if total_price is None and item.get("total_price_cents") is not None:
            cents = _safe_int(item.get("total_price_cents"), default=0)
            total_price = Decimal(cents) / Decimal("100")

        if total_price is None and unit_price is not None and quantity:
            total_price = unit_price * Decimal(quantity)

        mapping_status = _safe_str(item.get("mapping_status")) or ("unknown" if not sku else "unmapped")
        mapping_issue = _safe_str(item.get("mapping_issue"))
        if not sku and not mapping_issue:
            mapping_issue = "Unknown SKU"

        # Coerce mapped_product_id to a valid UUID or None.
        # safe_uuid_mapped_product() logs [PRODUCT_MAPPING_INVALID_UUID] if the
        # raw value is present but not a valid UUID, preventing
        # "column mapped_product_id is of type uuid but expression is of type
        # character varying" errors from reaching the database.
        _raw_mapped_id = item.get("mapped_product_id")
        mapped_product_id_coerced = safe_uuid_mapped_product(_raw_mapped_id)
        if _raw_mapped_id is not None and _raw_mapped_id != "" and mapped_product_id_coerced is None:
            logger.warning(
                "[PRODUCT_MAPPING_FALLBACK] sku=%s original_value=%s reason=invalid_uuid",
                sku,
                str(_raw_mapped_id)[:100],
            )
            logger.warning(
                "[PRODUCT_MAPPING_NULL_APPLIED] field=mapped_product_id sku=%s",
                sku,
            )

        normalized.append({
            "sku": sku,
            "product_name": product_name,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price,
            "unit_price_cents": _decimal_to_cents(unit_price),
            "total_price_cents": _decimal_to_cents(total_price),
            "mapped_product_id": mapped_product_id_coerced,
            "mapping_status": mapping_status,
            "mapping_issue": mapping_issue,
            "raw_payload": item,
        })

    return normalized


def _derive_review_status(line_items: list[dict[str, Any]]) -> str:
    if not line_items:
        return "needs_review"
    for item in line_items:
        if (
            not item.get("sku")
            or item.get("mapping_status") in {"unknown", "unmapped", None}
            or item.get("mapping_issue")
        ):
            return "blocked"
    return "ready"


# ---------------------------------------------------------------------------
# Order upsert
# ---------------------------------------------------------------------------

async def upsert_webhook_order(
    db: AsyncSession,
    brand_id: str,
    org_id: Optional[str],
    order_data: dict[str, Any],
) -> dict[str, Any]:
    """
    Upsert a single order from a webhook payload.

    Idempotent by (brand_id, external_order_id).
    Writes org_id + brand_id on every upsert.

    Args:
        db: Active async database session (caller owns transaction).
        brand_id: Brand UUID.
        org_id: Organization UUID (required for multi-tenant isolation).
        order_data: Raw order dict from the webhook payload.

    Returns:
        {"action": "created" | "updated" | "skipped", "external_order_id": str, "error": str | None}
    """
    # Coerce org_id and brand_id to valid UUIDs or None before any DB write.
    # This prevents "column is of type uuid but expression is of type character varying"
    # errors from sending webhook orders to the dead-letter queue.
    brand_id = safe_uuid_for_db(brand_id, "brand_id") or brand_id  # keep original if invalid so logging still works
    org_id = safe_uuid_for_db(org_id, "org_id")

    external_id = _safe_str(
        order_data.get("id")
        or order_data.get("external_id")
        or order_data.get("external_order_id")
    )
    if not external_id:
        return {"action": "skipped", "external_order_id": None, "error": "missing external_order_id"}

    try:
        customer_name = _safe_str(order_data.get("customer_name")) or "Unknown Customer"
        status = (_safe_str(order_data.get("status")) or "submitted").lower()
        order_number = _safe_str(order_data.get("order_number"))

        amount_decimal = _safe_decimal(
            order_data.get("total_amount")
            or order_data.get("amount")
            or order_data.get("total")
            or order_data.get("subtotal")
            or order_data.get("price")
        )
        total_cents = _decimal_to_cents(amount_decimal) or 0

        item_count = _safe_int(order_data.get("item_count"), default=0)
        unit_count = _safe_int(order_data.get("unit_count"), default=0)

        raw_line_items = order_data.get("line_items", [])
        normalized_line_items = _normalize_line_items(raw_line_items)

        if item_count == 0:
            item_count = len(normalized_line_items)
        if unit_count == 0:
            unit_count = sum(item.get("quantity", 0) or 0 for item in normalized_line_items)

        review_status = _derive_review_status(normalized_line_items)
        now = utc_now()

        raw_payload = (
            order_data.get("raw_payload")
            if isinstance(order_data.get("raw_payload"), dict)
            else order_data
        )

        external_created_at = _parse_dt(order_data.get("created_at"))
        external_updated_at = _parse_dt(order_data.get("updated_at"))

        existing_result = await db.execute(
            select(Order).where(
                Order.brand_id == brand_id,
                Order.external_order_id == external_id,
            )
        )
        existing = existing_result.scalar_one_or_none()

        # Re-coerce immediately before every DB write — belt-and-suspenders guard
        _write_org_id = safe_uuid_for_db(org_id, "org_id")
        _write_brand_id = safe_uuid_for_db(brand_id, "brand_id") or brand_id
        logger.info(
            "[ORG_ID_BEFORE_SQL] field=org_id value=%s external_id=%s function=upsert_webhook_order",
            _write_org_id,
            external_id,
        )
        logger.info(
            "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s external_id=%s function=upsert_webhook_order",
            _write_brand_id,
            external_id,
        )

        if existing:
            existing.customer_name = customer_name
            existing.status = status
            existing.order_number = order_number
            existing.total_cents = total_cents
            existing.amount = amount_decimal
            existing.item_count = item_count
            existing.unit_count = unit_count
            existing.line_items_json = make_json_safe(normalized_line_items)
            existing.raw_payload = make_json_safe(raw_payload)
            existing.review_status = review_status
            existing.sync_status = "ok"
            existing.synced_at = now
            existing.last_synced_at = now
            existing.external_created_at = external_created_at
            existing.external_updated_at = external_updated_at
            # FINAL coercion immediately before ORM write — belt-and-suspenders
            _write_org_id = safe_uuid_for_db(_write_org_id, "org_id")
            _write_brand_id = safe_uuid_for_db(_write_brand_id, "brand_id") or _write_brand_id
            logger.error(
                "[FINAL_SQL_PARAMS] org_id=%s org_type=%s brand_id=%s brand_type=%s"
                " external_id=%s action=update function=upsert_webhook_order",
                _write_org_id,
                type(_write_org_id).__name__,
                _write_brand_id,
                type(_write_brand_id).__name__,
                external_id,
            )
            if _write_org_id:
                existing.org_id = _write_org_id
            order_row = existing
            action = "updated"
        else:
            # FINAL coercion immediately before ORM write — belt-and-suspenders
            _write_org_id = safe_uuid_for_db(_write_org_id, "org_id")
            _write_brand_id = safe_uuid_for_db(_write_brand_id, "brand_id") or _write_brand_id
            logger.error(
                "[FINAL_SQL_PARAMS] org_id=%s org_type=%s brand_id=%s brand_type=%s"
                " external_id=%s action=insert function=upsert_webhook_order",
                _write_org_id,
                type(_write_org_id).__name__,
                _write_brand_id,
                type(_write_brand_id).__name__,
                external_id,
            )
            order_row = Order(
                org_id=_write_org_id,
                brand_id=_write_brand_id,
                external_order_id=external_id,
                order_number=order_number,
                customer_name=customer_name,
                status=status,
                total_cents=total_cents,
                amount=amount_decimal,
                item_count=item_count,
                unit_count=unit_count,
                line_items_json=make_json_safe(normalized_line_items),
                raw_payload=make_json_safe(raw_payload),
                source="leaflink_webhook",
                review_status=review_status,
                sync_status="ok",
                synced_at=now,
                last_synced_at=now,
                external_created_at=external_created_at,
                external_updated_at=external_updated_at,
            )
            db.add(order_row)
            await db.flush()
            action = "created"

        logger.info(
            "[LeafLinkWebhook] upsert_order external_order_id=%s action=%s",
            external_id,
            action,
        )

        return {"action": action, "external_order_id": external_id, "error": None}

    except Exception as exc:
        err_msg = str(exc)[:300]
        logger.error(
            "[LeafLinkWebhook] upsert_order_error external_order_id=%s error=%s",
            external_id,
            err_msg,
            exc_info=True,
        )
        return {"action": "skipped", "external_order_id": external_id, "error": err_msg}


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Idempotency key generation
# ---------------------------------------------------------------------------

def _build_idempotency_key(
    payload: dict[str, Any],
    event_type: str,
    object_id: Optional[str],
) -> str:
    """
    Build a deterministic idempotency key for a webhook event.

    Uses event_id from payload if present; otherwise derives a key from
    source + event_type + object_id + updated_at to ensure uniqueness
    across retries while deduplicating identical deliveries.
    """
    # Prefer an explicit event_id from the payload
    event_id = _safe_str(payload.get("event_id") or payload.get("id"))
    if event_id:
        return f"leaflink:{event_id}"

    # Derive deterministic key from payload fields
    source = _safe_str(payload.get("source")) or "leaflink"
    updated_at = _safe_str(
        payload.get("updated_at")
        or payload.get("modified")
        or payload.get("timestamp")
    ) or ""
    raw = f"{source}:{event_type}:{object_id or ''}:{updated_at}"
    return f"leaflink:{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


# ---------------------------------------------------------------------------
# Main webhook processor
# ---------------------------------------------------------------------------

async def process_leaflink_webhook(
    payload: dict[str, Any],
    payload_bytes: bytes,
    signature_header: Optional[str],
    db: AsyncSession,
) -> dict[str, Any]:
    """
    Process an inbound LeafLink webhook payload end-to-end.

    Webhook-first architecture:
    1. Extract company_id and event from payload
    2. Resolve tenant (brand_id, org_id, credential) from company_id
    3. Verify LL-Signature using credential.webhook_key
    4. Idempotency check — skip if already seen (by idempotency_key)
    5. Store event in leaflink_webhook_events (status=pending)
    6. Enqueue sync_request(s) for each order/product in payload
    7. Return 200 immediately — no inline upsert

    Always returns a dict — never raises. Caller returns HTTP 200.

    Returns:
        {
            "ok": bool,
            "received_count": int,
            "enqueued_count": int,
            "errors": list[str],
            "error": str | None,
            "status_code": int,
        }
    """
    start = time.monotonic()

    company_id = _safe_str(payload.get("company_id"))
    event = _safe_str(payload.get("event")) or "unknown"

    logger.info(
        "[LEAFLINK_WEBHOOK_RECEIVED] company_id=%s event=%s",
        company_id,
        event,
    )

    # ------------------------------------------------------------------ #
    # Step 1: Resolve tenant from company_id                              #
    # ------------------------------------------------------------------ #
    tenant = await resolve_tenant_from_company_id(db, company_id or "")
    if not tenant["ok"]:
        return {
            "ok": False,
            "received_count": 0,
            "enqueued_count": 0,
            "upserted_count": 0,
            "errors": [tenant["error"]],
            "error": tenant["error"],
            "status_code": tenant["status_code"],
        }

    brand_id: str = tenant["brand_id"]
    org_id: Optional[str] = tenant["org_id"]
    cred: BrandAPICredential = tenant["credential"]

    # Coerce org_id and brand_id to valid UUIDs or None before any DB write.
    brand_id = safe_uuid_for_db(brand_id, "brand_id") or brand_id
    org_id = safe_uuid_for_db(org_id, "org_id")

    logger.info(
        "[ORG_ID_BEFORE_SQL] field=org_id value=%s function=process_leaflink_webhook",
        org_id,
    )
    logger.info(
        "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s function=process_leaflink_webhook",
        brand_id,
    )

    # ------------------------------------------------------------------ #
    # Step 2: Verify signature                                            #
    # ------------------------------------------------------------------ #
    webhook_key = cred.webhook_key
    require_sig = LEAFLINK_WEBHOOK_REQUIRE_SIGNATURE

    if webhook_key:
        if not signature_header:
            logger.warning(
                "[LeafLinkWebhook] missing_signature brand_id=%s",
                brand_id,
            )
            if require_sig:
                return {
                    "ok": False,
                    "received_count": 0,
                    "enqueued_count": 0,
                    "upserted_count": 0,
                    "errors": ["Missing LL-Signature header"],
                    "error": "Missing LL-Signature header",
                    "status_code": 400,
                }

        elif not verify_ll_signature(payload_bytes, signature_header, webhook_key):
            logger.warning(
                "[LeafLinkWebhook] invalid_signature brand_id=%s",
                brand_id,
            )
            if require_sig:
                return {
                    "ok": False,
                    "received_count": 0,
                    "enqueued_count": 0,
                    "upserted_count": 0,
                    "errors": ["Invalid signature"],
                    "error": "Invalid signature",
                    "status_code": 400,
                }
        else:
            logger.info("[LEAFLINK_WEBHOOK_VERIFIED] brand_id=%s", brand_id)
    else:
        logger.warning(
            "[LeafLinkWebhook] signature_check_skipped brand_id=%s reason=no_webhook_key_configured",
            brand_id,
        )

    # ------------------------------------------------------------------ #
    # Step 3: Extract orders and products from payload                    #
    # ------------------------------------------------------------------ #
    data = payload.get("data") or {}
    orders_raw: list[dict] = []
    products_raw: list[dict] = []

    if isinstance(data, dict):
        orders_raw = data.get("orders") or []
        products_raw = data.get("products") or []
    elif isinstance(data, list):
        orders_raw = data

    # Flat payload fallbacks
    if not orders_raw and isinstance(payload.get("orders"), list):
        orders_raw = payload["orders"]
    if not products_raw and isinstance(payload.get("products"), list):
        products_raw = payload["products"]

    received_count = len(orders_raw) + len(products_raw)

    logger.info(
        "[LeafLinkWebhook] objects_extracted brand_id=%s orders=%s products=%s event=%s",
        brand_id,
        len(orders_raw),
        len(products_raw),
        event,
    )

    if received_count == 0:
        logger.info(
            "[LeafLinkWebhook] no_objects_in_payload brand_id=%s event=%s",
            brand_id,
            event,
        )
        return {
            "ok": True,
            "received_count": 0,
            "enqueued_count": 0,
            "upserted_count": 0,
            "errors": [],
            "error": None,
            "status_code": 200,
        }

    # ------------------------------------------------------------------ #
    # Step 4: Idempotency check + event storage + job enqueue             #
    # ------------------------------------------------------------------ #
    enqueued_count = 0
    errors: list[str] = []

    # Determine event_type from the event field
    event_lower = event.lower()
    if "product" in event_lower:
        if "creat" in event_lower:
            event_type = "product_created"
        else:
            event_type = "product_updated"
        object_type = "product"
    else:
        if "creat" in event_lower:
            event_type = "order_created"
        else:
            event_type = "order_updated"
        object_type = "order"

    try:
        async with AsyncSessionLocal() as enqueue_db:
            async with enqueue_db.begin():
                # Process orders
                for order_data in orders_raw:
                    if not isinstance(order_data, dict):
                        errors.append("Skipped non-dict order entry")
                        continue

                    external_id = _safe_str(
                        order_data.get("id")
                        or order_data.get("external_id")
                        or order_data.get("external_order_id")
                    )

                    idem_key = _build_idempotency_key(order_data, event_type, external_id)

                    # Idempotency check
                    existing = await enqueue_db.execute(
                        select(LeafLinkWebhookEvent).where(
                            LeafLinkWebhookEvent.idempotency_key == idem_key
                        )
                    )
                    if existing.scalar_one_or_none():
                        logger.info(
                            "[LEAFLINK_WEBHOOK_DUPLICATE_IGNORED] idempotency_key=%s brand_id=%s",
                            idem_key,
                            brand_id,
                        )
                        continue

                    # Store webhook event
                    webhook_event = LeafLinkWebhookEvent(
                        brand_id=brand_id,
                        org_id=org_id,
                        event_type=event_type,
                        object_type="order",
                        object_id=external_id,
                        payload_json=make_json_safe(order_data),
                        idempotency_key=idem_key,
                        status="pending",
                    )
                    enqueue_db.add(webhook_event)

                    # Enqueue sync_request for this order
                    sync_req = SyncRequest(
                        brand_id=brand_id,
                        org_id=org_id,
                        type="webhook_order",
                        object_id=external_id,
                        status="queued",
                        retry_count=0,
                        max_retries=3,
                    )
                    enqueue_db.add(sync_req)
                    enqueued_count += 1

                # Process products
                for product_data in products_raw:
                    if not isinstance(product_data, dict):
                        errors.append("Skipped non-dict product entry")
                        continue

                    external_id = _safe_str(
                        product_data.get("id")
                        or product_data.get("external_id")
                        or product_data.get("external_product_id")
                    )

                    prod_event_type = (
                        "product_created" if "creat" in event_lower else "product_updated"
                    )
                    idem_key = _build_idempotency_key(product_data, prod_event_type, external_id)

                    # Idempotency check
                    existing = await enqueue_db.execute(
                        select(LeafLinkWebhookEvent).where(
                            LeafLinkWebhookEvent.idempotency_key == idem_key
                        )
                    )
                    if existing.scalar_one_or_none():
                        logger.info(
                            "[LEAFLINK_WEBHOOK_DUPLICATE_IGNORED] idempotency_key=%s brand_id=%s",
                            idem_key,
                            brand_id,
                        )
                        continue

                    # Store webhook event
                    webhook_event = LeafLinkWebhookEvent(
                        brand_id=brand_id,
                        org_id=org_id,
                        event_type=prod_event_type,
                        object_type="product",
                        object_id=external_id,
                        payload_json=make_json_safe(product_data),
                        idempotency_key=idem_key,
                        status="pending",
                    )
                    enqueue_db.add(webhook_event)

                    # Enqueue sync_request for this product
                    sync_req = SyncRequest(
                        brand_id=brand_id,
                        org_id=org_id,
                        type="webhook_product",
                        object_id=external_id,
                        status="queued",
                        retry_count=0,
                        max_retries=3,
                    )
                    enqueue_db.add(sync_req)
                    enqueued_count += 1

    except Exception as exc:
        err_msg = str(exc)[:300]
        logger.error(
            "[LEAFLINK_WEBHOOK_PROCESSING_FAILED] brand_id=%s error=%s",
            brand_id,
            err_msg,
            exc_info=True,
        )
        errors.append(f"Enqueue error: {err_msg}")

    duration = round(time.monotonic() - start, 3)
    logger.info(
        "[LEAFLINK_WEBHOOK_ENQUEUED] received=%s enqueued=%s errors=%s duration=%ss brand_id=%s",
        received_count,
        enqueued_count,
        len(errors),
        duration,
        brand_id,
    )

    return {
        "ok": enqueued_count > 0 or (received_count > 0 and len(errors) == 0),
        "received_count": received_count,
        "enqueued_count": enqueued_count,
        "upserted_count": enqueued_count,  # backward compat alias
        "errors": errors,
        "error": errors[0] if errors and enqueued_count == 0 else None,
        "status_code": 200,
    }
