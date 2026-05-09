import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, get_schema_column_types, has_column
from models import Order, OrderLine
from models.sync_health import DeadLetterLineItem, SyncHealth
from utils.json_utils import make_json_safe

if TYPE_CHECKING:
    from services.background_sync_manager import BackgroundSyncManager

logger = logging.getLogger("leaflink_sync")


def ensure_utc_datetime(value: Any) -> Optional[datetime]:
    """Ensure a datetime value is timezone-aware UTC for DB binding.

    Handles all input types and always returns a UTC-aware datetime or None.
    This is the ONLY function that should normalize datetimes before DB persistence.

    Args:
        value: datetime, date, ISO string, or None

    Returns:
        UTC-aware datetime object, or None

    Raises:
        ValueError if value cannot be normalized
    """
    if value is None:
        return None

    # ISO string → parse to datetime
    if isinstance(value, str):
        try:
            # Handle both "Z" and "+00:00" formats
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Cannot parse ISO string: {value}") from e

    # date object → UTC datetime midnight
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

    # datetime object → ensure UTC-aware
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Naive datetime — assume UTC
            return value.replace(tzinfo=timezone.utc)
        # Already aware — convert to UTC
        return value.astimezone(timezone.utc)

    raise ValueError(f"Cannot normalize datetime: {type(value).__name__}")


def _cursor_hash(cursor: Optional[str]) -> Optional[str]:
    """Return a short hash of a cursor for safe logging."""
    if not cursor:
        return None
    return hashlib.sha256(cursor.encode()).hexdigest()[:12]

# Thread pool for running synchronous LeafLink HTTP calls without blocking the event loop
_leaflink_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="leaflink-bg-sync")

# Number of order headers committed per batch in Phase 1
HEADER_BATCH_SIZE = 25

# Maximum retries before a line item is moved to the dead-letter table
MAX_LINE_ITEM_RETRIES = 3


# ---------------------------------------------------------------------------
# Failure categorization — classify sync errors for structured reporting
# ---------------------------------------------------------------------------

# Specific failure categories (replaces generic 'malformed').
# These map directly to the failure_category column in sync_dead_letters.
FAILURE_CATEGORIES = {
    "missing_customer",
    "missing_order_number",
    "missing_external_order_id",
    "invalid_money",
    "invalid_timestamp",
    "invalid_status",
    "malformed_line_items",
    "orphan_line_items",
    "duplicate_external_id",
    "invalid_json_payload",
    "serializer_error",
    "db_type_error",
    "unknown_transform_error",
    # Legacy / infrastructure categories
    "duplicate_key",
    "missing_field",
    "line_item_issue",
    "fk_issue",
    "timeout",
    "rate_limit",
    "validation",
    "unknown",
    # org_id propagation failures
    "missing_org_context",
}

# Ordered list of (category, predicates) — first match wins.
_FAILURE_CATEGORY_RULES: list[tuple[str, list[str]]] = [
    # Specific domain categories — checked first
    ("duplicate_external_id",   ["duplicate key", "uq_brand_external_order", "uq_"]),
    ("db_type_error",           ["invalid input syntax for type uuid", "invalid input syntax for type", "type mismatch"]),
    ("invalid_json_payload",    ["invalid input syntax for type json", "json", "jsonb"]),
    ("serializer_error",        ["not null", "null value in column", "required"]),
    ("malformed_line_items",    ["order_lines", "line_item"]),
    ("fk_issue",                ["foreign key", "does not exist", "violates foreign key"]),
    ("timeout",                 ["timeout", "timed out"]),
    ("rate_limit",              ["429", "rate limit", "too many requests"]),
    ("validation",              ["check constraint", "validation", "violates check"]),
]


def categorize_sync_failure(exception: Exception, context: Optional[dict] = None) -> str:
    """Classify a sync exception into a specific named failure category.

    Categories (first match wins):
      duplicate_external_id   — unique constraint on (brand_id, external_order_id)
      db_type_error           — PostgreSQL type mismatch (UUID vs varchar, etc.)
      invalid_json_payload    — invalid JSON / JSONB input
      serializer_error        — NOT NULL / required field absent
      malformed_line_items    — error in order_lines table or line item processing
      fk_issue                — foreign key violation or column does not exist
      timeout                 — request or query timed out
      rate_limit              — HTTP 429 / rate limit exceeded
      validation              — check constraint / validation error
      unknown_transform_error — default when no rule matches

    Context keys that trigger specific categories (checked before rule matching):
      failure_category: pre-classified category (returned as-is if valid)
      missing_field:    field name → missing_customer / missing_order_number / etc.

    Args:
        exception: The caught exception.
        context:   Optional dict with extra context (e.g. stage, external_id,
                   failure_category, missing_field).

    Returns:
        Category string.
    """
    # If caller already classified the failure, trust it
    if context:
        pre_classified = context.get("failure_category")
        if pre_classified and pre_classified in FAILURE_CATEGORIES:
            return pre_classified

        # Map missing_field context to specific categories
        missing_field = context.get("missing_field")
        if missing_field == "customer_name":
            return "missing_customer"
        if missing_field == "order_number":
            return "missing_order_number"
        if missing_field == "external_order_id":
            return "missing_external_order_id"
        if missing_field in ("amount", "total_amount", "total"):
            return "invalid_money"
        if missing_field in ("created_at", "updated_at", "external_created_at"):
            return "invalid_timestamp"
        if missing_field == "status":
            return "invalid_status"
        if missing_field == "line_items":
            return "malformed_line_items"

    err_lower = str(exception).lower()
    exc_type = type(exception).__name__

    # Exception-type shortcuts
    if exc_type == "ValueError" and "could not convert" in err_lower:
        return "invalid_money"
    if exc_type in ("KeyError",):
        return "unknown_transform_error"

    for category, keywords in _FAILURE_CATEGORY_RULES:
        if any(kw in err_lower for kw in keywords):
            return category
    return "unknown_transform_error"


def log_sync_order_failed(
    external_id: Optional[str],
    order_number: Optional[str],
    failure_stage: str,
    exception: Exception,
    page_number: Optional[int] = None,
    cursor: Optional[str] = None,
    payload_size: Optional[int] = None,
    customer_name: Optional[str] = None,
    failure_category: Optional[str] = None,
    problematic_field: Optional[str] = None,
    context: Optional[dict] = None,
) -> tuple[str, str, str]:
    """Emit a structured [SYNC_ORDER_FAILED] log line and return failure metadata.

    Args:
        external_id:       LeafLink order PK.
        order_number:      Human-readable order number.
        failure_stage:     Where the failure occurred (fetch, transform, upsert,
                           line_item, customer_map, status_map, sync_health).
        exception:         The caught exception.
        page_number:       API page number (if available).
        cursor:            Cursor hash (if cursor-based pagination).
        payload_size:      Raw payload size in bytes (if available).
        customer_name:     Customer name from the payload (if available).
        failure_category:  Pre-classified failure category (overrides auto-detection).
        problematic_field: The specific field that caused the failure.
        context:           Optional dict with extra context for categorization.

    Returns:
        Tuple of (failure_category, exception_type, traceback_summary).
    """
    import traceback as _tb

    _ctx = context or {}
    if failure_category:
        _ctx["failure_category"] = failure_category
    if problematic_field:
        _ctx["missing_field"] = problematic_field

    category = categorize_sync_failure(exception, _ctx)
    exc_type = type(exception).__name__
    exc_msg = str(exception)[:500]

    # Capture last 3 stack frames as a compact summary
    tb_lines = _tb.format_tb(exception.__traceback__) if exception.__traceback__ else []
    tb_summary = " | ".join(line.strip().replace("\n", " ") for line in tb_lines[-3:])
    tb_summary_short = tb_summary[:500] if tb_summary else "none"

    logger.error(
        "[SYNC_ORDER_FAILED] external_id=%s order_number=%s stage=%s category=%s"
        " exception_type=%s message=%s page=%s payload_size=%s traceback=%s",
        external_id or "unknown",
        order_number or "unknown",
        failure_stage,
        category,
        exc_type,
        exc_msg,
        page_number or "unknown",
        payload_size or "unknown",
        tb_summary_short,
    )

    # Detailed dead-letter log for structured analysis
    logger.error(
        "[DEAD_LETTER_DETAILED] external_order_id=%s order_number=%s customer=%s "
        "failure_stage=%s failure_category=%s exception_type=%s problematic_field=%s",
        external_id or "unknown",
        order_number or "unknown",
        customer_name or "unknown",
        failure_stage,
        category,
        exc_type,
        problematic_field or "unknown",
    )

    return category, exc_type, tb_summary_short


# ---------------------------------------------------------------------------
# Advisory lock helpers — prevent overlapping syncs for the same brand
# ---------------------------------------------------------------------------

def _brand_lock_id(brand_id: str) -> int:
    """Convert brand_id to a stable 32-bit integer for pg_advisory_lock."""
    return int(hashlib.md5(brand_id.encode()).hexdigest()[:8], 16) & 0x7FFFFFFF


async def acquire_sync_lock(db: AsyncSession, brand_id: str) -> bool:
    """Try to acquire an exclusive advisory lock for this brand's sync.

    Returns True if the lock was acquired, False if another sync is already
    running for this brand.
    """
    lock_id = _brand_lock_id(brand_id)
    result = await db.execute(
        text("SELECT pg_try_advisory_lock(:lock_id)"),
        {"lock_id": lock_id},
    )
    return result.scalar() is True


async def release_sync_lock(db: AsyncSession, brand_id: str) -> None:
    """Release the advisory lock for this brand's sync."""
    lock_id = _brand_lock_id(brand_id)
    await db.execute(
        text("SELECT pg_advisory_unlock(:lock_id)"),
        {"lock_id": lock_id},
    )


# ---------------------------------------------------------------------------
# Sync health state helpers
# ---------------------------------------------------------------------------

async def _update_sync_health_phase1(
    brand_id: str,
    orders_count: int,
) -> None:
    """Record that Phase 1 started/completed for this brand."""
    try:
        async with AsyncSessionLocal() as db:
            _phase1_params = sanitize_sql_params(
                {"brand_id": brand_id, "now": ensure_utc(utc_now(), "now"), "count": (orders_count or 0)},
                statement="_update_sync_health_phase1",
            )
            _phase1_params = normalize_uuid_fields(_phase1_params)
            _phase1_params = normalize_datetime_fields(_phase1_params)
            _phase1_params = apply_uuid_str_to_params(_phase1_params)
            # Final guard: scan params for raw naive datetimes before execute
            for _param_key, _param_val in _phase1_params.items():
                if isinstance(_param_val, datetime):
                    if _param_val.tzinfo is None:
                        logger.error("[RAW_DATETIME_DETECTED] field=%s value=%s", _param_key, _param_val.isoformat())
                        _phase1_params[_param_key] = _param_val.replace(tzinfo=timezone.utc)
            await db.execute(
                text("""
                    INSERT INTO sync_health (brand_id, last_attempted_sync_at, total_orders_synced, consecutive_failures, total_line_items_synced, orders_fetched_last_run, updated_at)
                    VALUES (CAST(:brand_id AS UUID), :now, :count, 0, 0, :count, :now)
                    ON CONFLICT (brand_id) DO UPDATE SET
                        last_attempted_sync_at = :now,
                        total_orders_synced = sync_health.total_orders_synced + :count,
                        orders_fetched_last_run = :count,
                        updated_at = :now
                """),
                _phase1_params,
            )
            await db.commit()
            logger.info(
                "[SYNC_HEALTH_UPDATE] brand_id=%s phase=1 orders_delta=%s",
                brand_id,
                orders_count,
            )
    except Exception as exc:
        logger.error(
            "[SYNC_HEALTH_UPDATE_ERROR] brand_id=%s phase=1 error=%s",
            brand_id,
            str(exc)[:300],
        )


async def _update_sync_health_phase2(
    brand_id: str,
    line_items_count: int,
) -> None:
    """Record that Phase 2 completed successfully for this brand."""
    try:
        async with AsyncSessionLocal() as db:
            _phase2_params = sanitize_sql_params(
                {"brand_id": brand_id, "now": ensure_utc(utc_now(), "now"), "count": line_items_count},
                statement="_update_sync_health_phase2",
            )
            _phase2_params = normalize_uuid_fields(_phase2_params)
            _phase2_params = normalize_datetime_fields(_phase2_params)
            _phase2_params = apply_uuid_str_to_params(_phase2_params)
            # Final guard: scan params for raw naive datetimes before execute
            for _param_key, _param_val in _phase2_params.items():
                if isinstance(_param_val, datetime):
                    if _param_val.tzinfo is None:
                        logger.error("[RAW_DATETIME_DETECTED] field=%s value=%s", _param_key, _param_val.isoformat())
                        _phase2_params[_param_key] = _param_val.replace(tzinfo=timezone.utc)
            await db.execute(
                text("""
                    UPDATE sync_health SET
                        last_successful_sync_at = :now,
                        consecutive_failures = 0,
                        total_line_items_synced = total_line_items_synced + :count,
                        orders_written_last_run = :count,
                        last_error = NULL,
                        updated_at = :now
                    WHERE brand_id = CAST(:brand_id AS UUID)
                """),
                _phase2_params,
            )
            await db.commit()
            logger.info(
                "[SYNC_HEALTH_UPDATE] brand_id=%s phase=2 line_items_delta=%s",
                brand_id,
                line_items_count,
            )
    except Exception as exc:
        logger.error(
            "[SYNC_HEALTH_UPDATE_ERROR] brand_id=%s phase=2 error=%s",
            brand_id,
            str(exc)[:300],
        )


async def _record_sync_error(brand_id: str, error: Exception) -> None:
    """Increment consecutive_failures and record the last error message."""
    try:
        async with AsyncSessionLocal() as db:
            _sync_error_params = sanitize_sql_params(
                {"brand_id": brand_id, "error": str(error)[:500], "now": ensure_utc(utc_now(), "now")},
                statement="_record_sync_error",
            )
            _sync_error_params = normalize_uuid_fields(_sync_error_params)
            _sync_error_params = normalize_datetime_fields(_sync_error_params)
            _sync_error_params = apply_uuid_str_to_params(_sync_error_params)
            # Final guard: scan params for raw naive datetimes before execute
            for _param_key, _param_val in _sync_error_params.items():
                if isinstance(_param_val, datetime):
                    if _param_val.tzinfo is None:
                        logger.error("[RAW_DATETIME_DETECTED] field=%s value=%s", _param_key, _param_val.isoformat())
                        _sync_error_params[_param_key] = _param_val.replace(tzinfo=timezone.utc)
            await db.execute(
                text("""
                    INSERT INTO sync_health (brand_id, last_error, consecutive_failures, last_error_at, updated_at)
                    VALUES (CAST(:brand_id AS UUID), :error, 1, :now, :now)
                    ON CONFLICT (brand_id) DO UPDATE SET
                        last_error = :error,
                        consecutive_failures = sync_health.consecutive_failures + 1,
                        last_error_at = :now,
                        updated_at = :now
                """),
                _sync_error_params,
            )
            await db.commit()

    except Exception as exc:
        logger.error(
            "[SYNC_HEALTH_UPDATE_ERROR] brand_id=%s record_error error=%s",
            brand_id,
            str(exc)[:300],
        )


async def _record_retryable_error(brand_id: str, error_msg: str) -> None:
    """Record a retryable (transient) sync error in sync_health without incrementing consecutive_failures."""
    try:
        async with AsyncSessionLocal() as db:
            _retryable_params = sanitize_sql_params(
                {"brand_id": brand_id, "error": error_msg[:500], "now": ensure_utc(utc_now(), "now")},
                statement="_record_retryable_error",
            )
            _retryable_params = normalize_uuid_fields(_retryable_params)
            _retryable_params = normalize_datetime_fields(_retryable_params)
            _retryable_params = apply_uuid_str_to_params(_retryable_params)
            # Final guard: scan params for raw naive datetimes before execute
            for _param_key, _param_val in _retryable_params.items():
                if isinstance(_param_val, datetime):
                    if _param_val.tzinfo is None:
                        logger.error("[RAW_DATETIME_DETECTED] field=%s value=%s", _param_key, _param_val.isoformat())
                        _retryable_params[_param_key] = _param_val.replace(tzinfo=timezone.utc)
            await db.execute(
                text("""
                    INSERT INTO sync_health (brand_id, last_error, consecutive_failures, updated_at)
                    VALUES (CAST(:brand_id AS UUID), :error, 0, :now)
                    ON CONFLICT (brand_id) DO UPDATE SET
                        last_error = :error,
                        updated_at = :now
                """),
                _retryable_params,
            )
            await db.commit()
    except Exception as exc:
        logger.error(
            "[SYNC_HEALTH_UPDATE_ERROR] brand_id=%s record_retryable_error error=%s",
            brand_id,
            str(exc)[:300],
        )



async def _dead_letter_line_item(
    brand_id: str,
    external_order_id: str,
    order_id: Optional[int],
    sku: Optional[str],
    product_name: Optional[str],
    raw_payload: Any,
    failure_reason: str,
    failure_count: int,
) -> None:
    """Insert or update a dead-letter record for a permanently failed line item."""
    try:
        async with AsyncSessionLocal() as db:
            raw_payload_str = (
                json.dumps(make_json_safe(raw_payload))
                if raw_payload is not None
                else None
            )
            _dead_letter_params = sanitize_sql_params({
                "brand_id": brand_id,
                "external_order_id": external_order_id,
                "order_id": order_id,
                "sku": sku or "unknown",
                "product_name": product_name,
                "raw_payload": raw_payload_str,
                "reason": failure_reason[:500],
                "count": failure_count,
                "now": ensure_utc(utc_now(), "now"),
            }, statement="_dead_letter_line_item")
            _dead_letter_params = normalize_uuid_fields(_dead_letter_params)
            _dead_letter_params = normalize_datetime_fields(_dead_letter_params)
            _dead_letter_params = apply_uuid_str_to_params(_dead_letter_params)
            # Final guard: scan params for raw naive datetimes before execute
            for _param_key, _param_val in list(_dead_letter_params.items()):
                if isinstance(_param_val, datetime):
                    if _param_val.tzinfo is None:
                        logger.error("[RAW_DATETIME_DETECTED] field=%s value=%s", _param_key, _param_val.isoformat())
                        _dead_letter_params[_param_key] = _param_val.replace(tzinfo=timezone.utc)
                    else:
                        _dead_letter_params[_param_key] = _param_val.astimezone(timezone.utc)
            await db.execute(
                text("""
                    INSERT INTO dead_letter_line_items
                        (brand_id, external_order_id, order_id, sku, product_name,
                         raw_payload, failure_reason, failure_count, last_failed_at, created_at)
                    VALUES
                        (CAST(:brand_id AS UUID), :external_order_id, :order_id, :sku, :product_name,
                         CAST(:raw_payload AS jsonb), :reason, :count, :now, :now)
                    ON CONFLICT (brand_id, external_order_id, sku) DO UPDATE SET
                        failure_count = dead_letter_line_items.failure_count + 1,
                        failure_reason = :reason,
                        last_failed_at = :now,
                        raw_payload = CAST(:raw_payload AS jsonb)
                """),
                _dead_letter_params,
            )
            await db.commit()
        logger.warning(
            "[LINE_ITEM_DEAD_LETTERED] brand_id=%s external_order_id=%s sku=%s failure_count=%s",
            brand_id,
            external_order_id,
            sku,
            failure_count,
        )
    except Exception as exc:
        logger.error(
            "[DEAD_LETTER_ERROR] brand_id=%s external_order_id=%s sku=%s error=%s",
            brand_id,
            external_order_id,
            sku,
            str(exc)[:300],
        )


async def _write_sync_dead_letter(
    brand_id: str,
    raw_payload: Any,
    error_stage: str,
    error_message: str,
    org_id: Optional[str] = None,
    external_id: Optional[str] = None,
    order_number: Optional[str] = None,
    source: str = "leaflink",
    # Detailed diagnostic fields (added 2026_05_23_01)
    failure_stage: Optional[str] = None,
    failure_category: Optional[str] = None,
    exception_type: Optional[str] = None,
    exception_message: Optional[str] = None,
    traceback_summary: Optional[str] = None,
    problematic_field: Optional[str] = None,
    problematic_value_preview: Optional[str] = None,
    customer_name: Optional[str] = None,
) -> None:
    """Write a dead-letter record to sync_dead_letters for an order that could not be processed.

    This is the order-level dead-letter queue (distinct from dead_letter_line_items which
    tracks individual line item failures). Used when the entire order header insert fails
    or the payload is unprocessable.

    Args:
        brand_id:                  Brand UUID.
        raw_payload:               Full raw LeafLink order payload (stored as JSONB).
        error_stage:               Where the failure occurred (legacy field, e.g. 'header_insert').
        error_message:             Exception or error description.
        org_id:                    Org UUID (nullable).
        external_id:               LeafLink order PK (nullable).
        order_number:              Human-readable order number (nullable).
        source:                    Integration name (default 'leaflink').
        failure_stage:             Structured stage name (header_extract, header_transform,
                                   header_insert, line_item_extract, line_item_transform,
                                   line_item_insert).
        failure_category:          Specific failure category (see FAILURE_CATEGORIES).
        exception_type:            Python exception class name (e.g. KeyError, ValueError).
        exception_message:         Full exception message (up to 1000 chars).
        traceback_summary:         First 500 chars of the Python traceback.
        problematic_field:         The specific field that caused the failure.
        problematic_value_preview: First 100 chars of the problematic field value.
        customer_name:             Customer name from the payload (if available).
    """
    # Coerce brand_id and org_id to valid UUIDs or None before writing to the dead-letter table.
    # CAST(:brand_id AS uuid) in the SQL will fail if the value is not a valid UUID string.
    brand_id = safe_uuid_for_db(brand_id, "brand_id") or brand_id  # keep original if invalid so logging still works
    org_id = safe_uuid_for_db(org_id, "org_id")

    # Extract payload_keys for debugging (top-level keys of the raw payload)
    payload_keys_json: Optional[str] = None
    if isinstance(raw_payload, dict):
        try:
            payload_keys_json = json.dumps(list(raw_payload.keys()))
        except Exception:
            payload_keys_json = None

    # Use failure_stage as error_stage if not separately provided (backward compat)
    effective_error_stage = failure_stage or error_stage

    try:
        async with AsyncSessionLocal() as db:
            raw_payload_str = (
                json.dumps(make_json_safe(raw_payload))
                if raw_payload is not None
                else json.dumps({})
            )
            now = ensure_utc(utc_now(), "now")
            # Build params dict and sanitize — centralized sanitizer handles all type coercions
            params = sanitize_sql_params({
                "source": source,
                "brand_id": safe_uuid_for_db(brand_id, "brand_id") or brand_id,
                "org_id": safe_uuid_for_db(org_id, "org_id"),
                "external_id": external_id,
                "order_number": order_number,
                "customer_name": customer_name,
                "raw_payload": raw_payload_str,
                "error_stage": effective_error_stage,
                "error_message": error_message[:2000],
                "failure_stage": failure_stage,
                "failure_category": failure_category,
                "exception_type": exception_type,
                "exception_message": (exception_message or "")[:1000] if exception_message else None,
                "traceback_summary": (traceback_summary or "")[:500] if traceback_summary else None,
                "payload_keys": payload_keys_json,
                "problematic_field": problematic_field,
                "problematic_value_preview": (problematic_value_preview or "")[:100] if problematic_value_preview else None,
                "now": now,
            }, statement="_write_sync_dead_letter")
            # FINAL coercion — apply safe_uuid_for_db() directly into the params
            # dict immediately before execute() so no mutation after coercion can
            # slip through.
            params["org_id"] = safe_uuid_for_db(params.get("org_id"), "org_id")
            params["brand_id"] = safe_uuid_for_db(params.get("brand_id"), "brand_id") or params.get("brand_id")
            logger.debug(
                "[DEAD_LETTER_FINAL_TYPES] org_id=%s org_type=%s brand_id=%s brand_type=%s",
                params.get("org_id"),
                type(params.get("org_id")),
                params.get("brand_id"),
                type(params.get("brand_id")),
            )
            params = normalize_uuid_fields(params)
            params = normalize_datetime_fields(params)
            params = apply_uuid_str_to_params(params)
            # Final guard: scan params for raw naive datetimes before execute
            for _param_key, _param_val in list(params.items()):
                if isinstance(_param_val, datetime):
                    if _param_val.tzinfo is None:
                        logger.error("[RAW_DATETIME_DETECTED] field=%s value=%s", _param_key, _param_val.isoformat())
                        params[_param_key] = _param_val.replace(tzinfo=timezone.utc)
                    else:
                        params[_param_key] = _param_val.astimezone(timezone.utc)
            # Try to write with new detail columns; fall back to legacy schema if columns don't exist yet
            try:
                await db.execute(
                    text("""
                        INSERT INTO sync_dead_letters
                            (source, brand_id, org_id, external_id, order_number,
                             customer_name, raw_payload, error_stage, error_message,
                             failure_stage, failure_category, exception_type, exception_message,
                             traceback_summary, payload_keys, problematic_field,
                             problematic_value_preview, retry_count, created_at)
                        VALUES
                            (:source, CAST(:brand_id AS uuid), CAST(:org_id AS uuid),
                             :external_id, :order_number,
                             :customer_name, CAST(:raw_payload AS jsonb), :error_stage, :error_message,
                             :failure_stage, :failure_category, :exception_type, :exception_message,
                             :traceback_summary, CAST(:payload_keys AS jsonb), :problematic_field,
                             :problematic_value_preview, 0, :now)
                    """),
                    params,
                )
            except Exception as _detail_exc:
                # New columns may not exist yet (migration pending) — fall back to legacy insert
                _detail_err = str(_detail_exc).lower()
                if "column" in _detail_err and ("does not exist" in _detail_err or "unknown" in _detail_err):
                    logger.warning(
                        "[DEAD_LETTER_FALLBACK] new columns not yet migrated, using legacy schema: %s",
                        str(_detail_exc)[:200],
                    )
                    await db.execute(
                        text("""
                            INSERT INTO sync_dead_letters
                                (source, brand_id, org_id, external_id, order_number,
                                 raw_payload, error_stage, error_message, retry_count, created_at)
                            VALUES
                                (:source, CAST(:brand_id AS uuid), CAST(:org_id AS uuid),
                                 :external_id, :order_number,
                                 CAST(:raw_payload AS jsonb), :error_stage, :error_message,
                                 0, :now)
                        """),
                        params,
                    )
                else:
                    raise
            await db.commit()
        logger.warning(
            "[SYNC_DEAD_LETTER_WRITTEN] external_id=%s order_number=%s customer=%s "
            "failure_stage=%s failure_category=%s error_stage=%s brand_id=%s",
            external_id,
            order_number,
            customer_name or "unknown",
            failure_stage or error_stage,
            failure_category or "unclassified",
            error_stage,
            brand_id,
        )
    except Exception as exc:
        logger.error(
            "[SYNC_DEAD_LETTER_WRITE_ERROR] brand_id=%s external_id=%s error_stage=%s write_error=%s",
            brand_id,
            external_id,
            error_stage,
            str(exc)[:300],
        )


def utc_now() -> datetime:
    """Return current time as timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def to_utc_naive(dt: Any) -> datetime:
    """Convert datetime to UTC-naive for TIMESTAMP WITHOUT TIME ZONE columns.

    - If None: return current UTC time as naive
    - If naive: return as-is
    - If aware: convert to UTC and strip tzinfo
    """
    if dt is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    if not isinstance(dt, datetime):
        return dt
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def parse_dt(val: Any) -> datetime | None:
    """Parse an ISO datetime string to a datetime object, or pass through if already datetime."""
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(val, datetime):
        return val
    return None


def normalize_datetime(dt: Any) -> datetime | None:
    """Strictly enforce UTC-aware datetime, handling all input types.

    ALWAYS returns either a timezone-aware UTC datetime or None.
    Never returns naive datetimes.
    """
    if dt is None or dt == "":
        return None

    # Handle ISO datetime strings
    if isinstance(dt, str):
        try:
            # Parse ISO string (handles Z suffix and +00:00)
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            # Ensure UTC-aware
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (ValueError, TypeError):
            return None

    # Handle datetime objects
    if isinstance(dt, datetime):
        # If naive, assume UTC and make aware
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        # If aware, convert to UTC
        return dt.astimezone(timezone.utc)

    # Unsupported type
    return None


def ensure_utc(dt: Any, field_name: str = "unknown") -> "datetime | None":
    """Ensure a datetime is timezone-aware UTC.

    Handles:
    - None → None
    - str  → parsed via datetime.fromisoformat (Z suffix normalised to +00:00)
    - Naive datetime → assume UTC and make aware
    - Aware datetime → convert to UTC
    - Invalid input → passed through unchanged (not a datetime)

    ALWAYS returns either UTC-aware datetime or None.
    Never returns naive datetimes.

    Logs [DATETIME_NORMALIZED] for every conversion that changes the value.
    """
    if dt is None:
        return None

    # Handle ISO datetime strings before the isinstance(dt, datetime) check
    if isinstance(dt, str):
        if not dt:
            return None
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            # Fall through to the datetime normalisation logic below
        except (ValueError, TypeError):
            logger.warning(
                "[DATETIME_PARSE_FAILED] field=%s value=%r — not a valid ISO datetime string",
                field_name,
                dt[:100] if len(dt) > 100 else dt,
            )
            return None

    if not isinstance(dt, datetime):
        return dt  # type: ignore[return-value]

    # If naive, assume UTC and make aware
    if dt.tzinfo is None:
        normalized = dt.replace(tzinfo=timezone.utc)
        logger.debug(
            "[DATETIME_NORMALIZED] field=%s original=%s (naive) normalized=%s (UTC-aware)",
            field_name,
            dt.isoformat(),
            normalized.isoformat(),
        )
        return normalized

    # If aware, convert to UTC
    normalized = dt.astimezone(timezone.utc)
    if normalized != dt:
        logger.debug(
            "[DATETIME_NORMALIZED] field=%s original=%s (aware) normalized=%s (UTC)",
            field_name,
            dt.isoformat(),
            normalized.isoformat(),
        )
    return normalized


def get_allowed_model_fields(model_class) -> set:
    """Get the set of valid column names for a SQLAlchemy model.

    Args:
        model_class: SQLAlchemy model class (Order, OrderLine, etc.)

    Returns:
        Set of valid column names that can be passed to the model constructor
    """
    if not hasattr(model_class, '__table__'):
        return set()
    return set(model_class.__table__.columns.keys())


def filter_model_kwargs(model_class, kwargs: dict) -> dict:
    """Filter kwargs to only include valid columns for a model.

    Removes any kwargs that don't correspond to actual model columns.
    Logs removed fields for debugging.

    Args:
        model_class: SQLAlchemy model class
        kwargs: Dictionary of potential constructor arguments

    Returns:
        Filtered dictionary with only valid model columns
    """
    allowed_fields = get_allowed_model_fields(model_class)
    filtered = {}

    for key, value in kwargs.items():
        if key in allowed_fields:
            filtered[key] = value
        else:
            logger.warning(
                "[ORM_KWARG_FILTERED] model=%s field=%s reason=not_in_model",
                model_class.__name__,
                key,
            )

    return filtered


def _sanitize_json_value(value: Any, path: str = "") -> Any:
    """Recursively convert a value to be JSON-safe for raw_payload / JSONB columns.

    Rules applied inside JSON/JSONB payloads:
    - datetime  → ISO string (always, regardless of tz-awareness)
    - date       → ISO string
    - UUID       → str
    - Decimal    → str  (preserves precision; float would lose it)
    - dict       → recursively sanitized
    - list/tuple → recursively sanitized list
    - Everything else → passed through unchanged

    This is intentionally different from the SQL-param rules: inside a JSONB
    column we always want strings, never Python objects.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {k: _sanitize_json_value(v, f"{path}.{k}" if path else k) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_json_value(item, f"{path}[{i}]") for i, item in enumerate(value)]
    return value


def sanitize_sql_params(params: dict, statement: str = "unknown") -> dict:
    """Centralized SQL parameter sanitizer — runs immediately before every execute().

    Guarantees that no naive datetime, raw date, incompatible UUID, or
    non-JSON-safe value ever reaches asyncpg.

    Rules applied to every key/value pair:

    datetime (including subclasses):
      - Naive  → replace(tzinfo=timezone.utc)   [assume UTC]
      - Aware  → astimezone(timezone.utc)        [normalise to UTC]
      Never passes a naive datetime to asyncpg.

    date (but NOT datetime):
      - Converted to datetime at midnight UTC so it binds to TIMESTAMPTZ columns.
      - If the field name contains 'payload' or 'json', converted to ISO string
        instead (safe for JSONB / text columns).

    UUID:
      - Kept as uuid.UUID for SQL params targeting UUID columns (asyncpg accepts
        uuid.UUID natively and it avoids CAST ambiguity).
      - Inside raw_payload / JSON fields, converted to str by _sanitize_json_value.

    Decimal:
      - Passed through unchanged for numeric SQL columns (asyncpg handles Decimal).
      - Inside raw_payload / JSON fields, converted to str by _sanitize_json_value.

    str / int / float / bool / None:
      - Passed through unchanged.

    dict / list / tuple:
      - If the key is a JSON/payload field (contains 'payload', 'json', or
        'message'), recursively sanitized via _sanitize_json_value so the
        resulting string is safe to CAST as JSONB.
      - Otherwise passed through unchanged (e.g. SQLAlchemy may handle them).

    Audit logging:
      Every field that is mutated emits [SQL_PARAMS_SANITIZED] at INFO level.
      For datetime fields the log includes tzinfo and is_aware.

    Args:
        params:    SQL parameters dictionary (modified copy is returned).
        statement: Short name of the calling SQL statement for log context.

    Returns:
        New dict with all values safe for asyncpg / PostgreSQL.
    """
    if not isinstance(params, dict):
        return params

    sanitized: dict = {}

    _json_field_keywords = {"payload", "json", "message"}

    for key, value in params.items():
        original_value = value
        mutated = False

        # ------------------------------------------------------------------ #
        # datetime (covers datetime subclasses — check before date)           #
        # ------------------------------------------------------------------ #
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
                mutated = True
            else:
                utc_val = value.astimezone(timezone.utc)
                if utc_val != value:
                    mutated = True
                value = utc_val

            if mutated:
                logger.debug(
                    "[SQL_PARAMS_SANITIZED] statement=%s field=%s type=datetime"
                    " tzinfo=%s is_aware=%s",
                    statement, key,
                    value.tzinfo,
                    value.tzinfo is not None,
                )

        # ------------------------------------------------------------------ #
        # date (but NOT datetime — datetime is a subclass of date)            #
        # ------------------------------------------------------------------ #
        elif isinstance(value, date):
            key_lower = key.lower()
            is_json_field = any(kw in key_lower for kw in _json_field_keywords)
            if is_json_field:
                value = value.isoformat()
            else:
                # Bind to TIMESTAMPTZ column as midnight UTC datetime
                value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
            mutated = True
            logger.debug(
                "[SQL_PARAMS_SANITIZED] statement=%s field=%s type=date"
                " converted_to=%s",
                statement, key, type(value).__name__,
            )

        # ------------------------------------------------------------------ #
        # UUID                                                                 #
        # ------------------------------------------------------------------ #
        elif isinstance(value, UUID):
            # Keep as uuid.UUID for SQL params — asyncpg accepts it natively.
            # No mutation needed; log for audit trail.
            logger.debug(
                "[SQL_PARAMS_SANITIZED] statement=%s field=%s type=UUID action=kept_as_uuid",
                statement, key,
            )

        # ------------------------------------------------------------------ #
        # dict / list / tuple — sanitize if it's a JSON/payload field         #
        # ------------------------------------------------------------------ #
        elif isinstance(value, (dict, list, tuple)):
            key_lower = key.lower()
            is_json_field = any(kw in key_lower for kw in _json_field_keywords)
            if is_json_field:
                value = _sanitize_json_value(value, key)
                if value is not original_value:
                    mutated = True
                    logger.debug(
                        "[SQL_PARAMS_SANITIZED] statement=%s field=%s type=%s"
                        " action=json_sanitized",
                        statement, key, type(original_value).__name__,
                    )

        # ------------------------------------------------------------------ #
        # Everything else (str, int, float, bool, None, Decimal) — pass through
        # ------------------------------------------------------------------ #

        sanitized[key] = value

    return sanitized


# ---------------------------------------------------------------------------
# Final-barrier helpers — catch every naive datetime before asyncpg sees it
# ---------------------------------------------------------------------------

def assert_no_naive_datetimes(value: Any, path: str = "params") -> None:
    """Recursively walk *value* and raise AssertionError on any naive datetime.

    Logs [NAIVE_DATETIME_FOUND] with the full dotted path and the offending
    value before raising so the caller can see it in production logs even if
    the AssertionError is caught upstream.

    Examples of logged paths:
        params.raw_payload.line_items.0.created_at
        params.created_at
    """
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            logger.error(
                "[NAIVE_DATETIME_FOUND] path=%s value=%s",
                path,
                value,
            )
            raise AssertionError(
                f"[NAIVE_DATETIME_FOUND] path={path} value={value!r}"
            )
    elif isinstance(value, dict):
        for k, v in value.items():
            assert_no_naive_datetimes(v, f"{path}.{k}")
    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            assert_no_naive_datetimes(item, f"{path}.{i}")


def final_sanitize_order_lines_params(params: dict) -> dict:
    """Hard final sanitizer applied immediately before every order_lines execute().

    Walks the entire params dict recursively and:
    - datetime (naive)  → replace(tzinfo=timezone.utc)
    - datetime (aware)  → astimezone(timezone.utc)
    - date (not datetime) → midnight UTC datetime
    - dict/list/tuple   → recursively JSON-sanitized via _sanitize_json_value
                          (datetime/date/UUID/Decimal → strings)

    Logs [FINAL_SANITIZE_ORDER_LINES] for every field that is touched.

    Returns a new sanitized params dict.
    """
    if not isinstance(params, dict):
        return params

    result: dict = {}

    for field, value in params.items():
        if isinstance(value, datetime):
            if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                sanitized_val = value.replace(tzinfo=timezone.utc)
                action = "naive_datetime_to_utc"
            else:
                sanitized_val = value.astimezone(timezone.utc)
                action = "aware_datetime_to_utc"
            logger.debug(
                "[FINAL_SANITIZE_ORDER_LINES] field=%s type=%s action=%s",
                field,
                type(value).__name__,
                action,
            )
            result[field] = sanitized_val

        elif isinstance(value, date):
            # date but NOT datetime (datetime is a subclass of date)
            sanitized_val = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
            logger.debug(
                "[FINAL_SANITIZE_ORDER_LINES] field=%s type=%s action=%s",
                field,
                type(value).__name__,
                "date_to_midnight_utc",
            )
            result[field] = sanitized_val

        elif isinstance(value, (dict, list, tuple)):
            sanitized_val = _sanitize_json_value(value, field)
            logger.debug(
                "[FINAL_SANITIZE_ORDER_LINES] field=%s type=%s action=%s",
                field,
                type(value).__name__,
                "json_sanitized",
            )
            result[field] = sanitized_val

        else:
            result[field] = value

    return result


def safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def safe_uuid(value: str | None) -> str | None:
    """Cast string to UUID and back to ensure DB compatibility.

    Validates that the value is a well-formed UUID and returns it in canonical
    lowercase hyphenated form.  If the value is not a valid UUID it is returned
    as-is so that non-UUID brand/org identifiers still work.
    """
    if not value:
        return None
    try:
        return str(UUID(value))
    except (ValueError, TypeError):
        return value  # Return as-is if not a valid UUID


def safe_uuid_mapped_product(value: Any) -> str | None:
    """Coerce value to valid UUID string, or return None if invalid.

    Used exclusively for mapped_product_id fields that target UUID columns in
    PostgreSQL.  Unlike safe_uuid(), this function returns None (not the
    original value) when the input is not a well-formed UUID, preventing
    ``column "mapped_product_id" is of type uuid but expression is of type
    character varying`` errors from sending orders to the dead-letter queue.

    The original mapping value is always preserved in the order/line-item
    raw_payload JSONB column and can be reprocessed by reconciliation jobs.

    Args:
        value: Any value (string, UUID object, None, etc.)

    Returns:
        Valid UUID string in canonical form, or None if invalid.

    Logs:
        [PRODUCT_MAPPING_INVALID_UUID] if value is not a valid UUID
    """
    if value is None or value == "":
        return None

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, str):
        try:
            return str(UUID(value))
        except (ValueError, TypeError):
            logger.warning(
                "[PRODUCT_MAPPING_INVALID_UUID] value=%s reason=not_valid_uuid",
                value[:100] if len(value) > 100 else value,
            )
            return None

    logger.warning(
        "[PRODUCT_MAPPING_INVALID_UUID] value_type=%s reason=unexpected_type",
        type(value).__name__,
    )
    return None


def safe_uuid_for_db(value: Any, field_name: str = "uuid_field") -> str | None:
    """Coerce value to valid UUID string, or return None if invalid.

    Used for org_id, brand_id, and other UUID columns that must be strict.
    Returns None (not original value) when input is not a well-formed UUID,
    preventing ``column "org_id" is of type uuid but expression is of type
    character varying`` errors from sending orders to the dead-letter queue.

    The original value is always preserved in the order raw_payload JSONB
    column and can be reprocessed by reconciliation jobs.

    Args:
        value:      Any value (string, UUID object, None, etc.)
        field_name: Field name for logging (e.g., "org_id", "brand_id")

    Returns:
        Valid UUID string in canonical form, or None if invalid.

    Logs:
        [ORG_UUID_COERCED]      if value is coerced to valid UUID
        [ORG_UUID_NULL_APPLIED] if value is invalid and becomes NULL
    """
    if value is None or value == "":
        return None

    if isinstance(value, UUID):
        coerced = str(value)
        logger.debug(
            "[ORG_UUID_COERCED] field=%s original_type=UUID coerced=%s",
            field_name,
            coerced,
        )
        return coerced

    if isinstance(value, str):
        try:
            coerced = str(UUID(value))
            if coerced != value:
                logger.debug(
                    "[ORG_UUID_COERCED] field=%s original=%s coerced=%s",
                    field_name,
                    value[:100],
                    coerced,
                )
            return coerced
        except (ValueError, TypeError):
            logger.warning(
                "[ORG_UUID_NULL_APPLIED] field=%s value=%s reason=not_valid_uuid",
                field_name,
                value[:100] if len(value) > 100 else value,
            )
            return None

    logger.warning(
        "[ORG_UUID_NULL_APPLIED] field=%s value_type=%s reason=unexpected_type",
        field_name,
        type(value).__name__,
    )
    return None


def normalize_uuid_fields(params: dict) -> dict:
    """Normalize UUID-type parameters before SQL execution.

    Iterates all SQL params and for any key ending in:
    - "_id"
    - "_uuid"
    - "mapped_product_id"

    Apply transformations:
    - None stays None
    - Empty string becomes None
    - Valid UUID strings become uuid.UUID(value)
    - Invalid UUID strings: log [UUID_PARAM_INVALID], set to None, NEVER raise

    Returns normalized params dict.
    """
    normalized = dict(params)

    for key, value in params.items():
        is_uuid_field = (
            key.endswith("_id")
            or key.endswith("_uuid")
            or key == "mapped_product_id"
        )
        if not is_uuid_field:
            continue

        original_type = type(value).__name__

        if value is None:
            # Keep as None — no transformation needed
            continue

        if value == "":
            normalized[key] = None
            logger.debug(
                "[UUID_PARAM_NORMALIZED] key=%s original_type=%s final_type=%s reason=empty_string_to_null",
                key,
                original_type,
                "NoneType",
            )
            continue

        if isinstance(value, UUID):
            # Already a UUID object — pass through as-is (asyncpg accepts uuid.UUID)
            logger.debug(
                "[UUID_PARAM_NORMALIZED] key=%s original_type=UUID final_type=UUID",
                key,
            )
            continue

        if isinstance(value, str):
            try:
                normalized[key] = UUID(value)
                logger.debug(
                    "[UUID_PARAM_NORMALIZED] key=%s original_type=str final_type=UUID",
                    key,
                )
            except (ValueError, TypeError):
                logger.warning(
                    "[UUID_PARAM_INVALID] key=%s value=%s reason=not_valid_uuid setting_to_null",
                    key,
                    value[:100] if len(value) > 100 else value,
                )
                normalized[key] = None
            continue

        # Non-string, non-UUID, non-None value for a UUID field — leave as-is
        # (e.g. integer PKs that happen to end in _id should not be coerced)

    return normalized


def ensure_uuid_str(value: Any) -> Optional[str]:
    """Convert UUID objects to strings for asyncpg bind params.

    asyncpg requires string values for TEXT/VARCHAR columns and for
    CAST(:x AS uuid) expressions — it rejects raw uuid.UUID objects with
    "expected str, got UUID".  Call this on every UUID-typed bind parameter
    immediately before execute() to guarantee asyncpg receives a str.

    Args:
        value: Any value — UUID object, string, or None.

    Returns:
        str (canonical UUID form) if value is a non-empty UUID or string,
        None if value is None or empty.
    """
    if value is None:
        return None
    if isinstance(value, UUID):
        converted = str(value)
        logger.debug(
            "[UUID_NORMALIZED] key=<direct_call> original_type=UUID final_type=str value=%s",
            converted,
        )
        return converted
    if isinstance(value, str):
        return value if value else None
    # Fallback: stringify anything else (e.g. int PKs that slipped through)
    return str(value) if value else None


def apply_uuid_str_to_params(params: dict) -> dict:
    """Convert all UUID-typed bind params to strings before asyncpg execute().

    Walks every key in *params* and converts uuid.UUID values to str so that
    asyncpg never receives a raw UUID object.  Logs every conversion with the
    [UUID_NORMALIZED] prefix for full observability.

    This is the final barrier before execute() — call it after
    normalize_uuid_fields() and sanitize_sql_params() have run.

    Args:
        params: SQL parameters dict (not mutated — a new dict is returned).

    Returns:
        New dict with all uuid.UUID values replaced by their str equivalents.
    """
    result: dict = {}
    for key, value in params.items():
        if isinstance(value, UUID):
            converted = str(value)
            logger.debug(
                "[UUID_NORMALIZED] key=%s original_type=%s final_type=%s",
                key,
                type(value).__name__,
                type(converted).__name__,
            )
            result[key] = converted
        else:
            result[key] = value
    return result


def normalize_datetime_fields(params: dict, field_prefix: str = "") -> dict:
    """Recursively normalize all datetime values in SQL parameters to UTC-aware.

    Walks dicts and lists to find and normalize nested datetimes.

    For any datetime value:
    - None stays None
    - Naive datetime → UTC-aware with replace(tzinfo=timezone.utc)
    - Aware datetime → normalized to UTC with astimezone(timezone.utc)

    Returns normalized params dict.
    """
    if not isinstance(params, dict):
        return params

    normalized = dict(params)

    for key, value in list(params.items()):
        field_name = f"{field_prefix}.{key}" if field_prefix else key

        if isinstance(value, datetime):
            normalized[key] = ensure_utc(value)

        elif isinstance(value, dict):
            normalized[key] = normalize_datetime_fields(value, field_name)

        elif isinstance(value, list):
            normalized[key] = []
            for i, item in enumerate(value):
                if isinstance(item, datetime):
                    normalized[key].append(ensure_utc(item))
                elif isinstance(item, dict):
                    normalized[key].append(normalize_datetime_fields(item, f"{field_name}[{i}]"))
                else:
                    normalized[key].append(item)

        else:
            normalized[key] = value

    return normalized


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_decimal(value: Any) -> Decimal | None:
    try:
        if value is None or value == "":
            return None
        return Decimal(str(value))
    except Exception:
        return None


def decimal_to_cents(value: Decimal | None) -> int | None:
    if value is None:
        return None
    return int((value * 100).quantize(Decimal("1")))


def normalize_line_items(raw_line_items: Any) -> list[dict[str, Any]]:
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

        sku = safe_str(item.get("sku") or item.get("product_sku") or item.get("external_sku"))
        product_name = safe_str(item.get("product_name") or item.get("name") or item.get("product"))
        quantity = safe_int(item.get("quantity") or item.get("qty") or item.get("units"), default=0)

        unit_price = safe_decimal(item.get("unit_price"))
        if unit_price is None and item.get("unit_price_cents") is not None:
            cents = safe_int(item.get("unit_price_cents"), default=0)
            unit_price = Decimal(cents) / Decimal("100")

        total_price = safe_decimal(item.get("total_price"))
        if total_price is None and item.get("total_price_cents") is not None:
            cents = safe_int(item.get("total_price_cents"), default=0)
            total_price = Decimal(cents) / Decimal("100")

        if total_price is None and unit_price is not None and quantity:
            total_price = unit_price * Decimal(quantity)

        mapping_status = safe_str(item.get("mapping_status")) or ("unknown" if not sku else "unmapped")
        mapping_issue = safe_str(item.get("mapping_issue"))
        if not sku and not mapping_issue:
            mapping_issue = "Unknown SKU"

        # Coerce mapped_product_id to a valid UUID or None.
        # safe_uuid_mapped_product() logs [PRODUCT_MAPPING_INVALID_UUID] if the
        # raw value is present but not a valid UUID.  We then log
        # [PRODUCT_MAPPING_FALLBACK] + [PRODUCT_MAPPING_NULL_APPLIED] so that
        # operators can trace exactly which line items had their mapping dropped.
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

        normalized.append(
            {
                "sku": sku,
                "product_name": product_name,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": total_price,
                "unit_price_cents": decimal_to_cents(unit_price),
                "total_price_cents": decimal_to_cents(total_price),
                "mapped_product_id": mapped_product_id_coerced,
                "mapping_status": mapping_status,
                "mapping_issue": mapping_issue,
                "raw_payload": item,
            }
        )

    return normalized


def derive_review_status(line_items: list[dict[str, Any]]) -> str:
    """Derive review_status during sync ingestion.

    During sync, we don't have the full order context (no sync_status yet),
    so we use a simpler rule:
    - If line_items is empty: return "ok" (not "needs_review" — sync may still be in progress)
    - If any line item has an unknown SKU or mapping issue: return "blocked"
    - Otherwise: return "ready"

    The "needs_review" status is only set post-sync when sync_status="partial" or "failed".
    """
    if not line_items:
        # During ingestion, empty line items is not necessarily a problem.
        # The sync_health_status field will be set to "partial" if line_items are missing.
        return "ok"

    for item in line_items:
        if (
            not item.get("sku")
            or item.get("mapping_status") in {"unknown", "unmapped", None}
            or item.get("mapping_issue")
        ):
            return "blocked"

    return "ready"


async def _insert_line_items_standalone(
    db: AsyncSession,
    order_id: int,
    normalized_line_items: list[dict],
) -> tuple[int, int, int]:
    """Module-level helper to insert OrderLine rows for a given order_id.

    Uses savepoints so a single bad line item never poisons the whole
    batch transaction.  Deletes stale rows before inserting fresh ones.

    Returns:
        (inserted_count, skipped_count, failed_count)

    Intended for use by external scripts (e.g. scripts/retry_line_items.py)
    that need to re-insert line items without going through the full sync flow.
    """
    optional_columns = [
        "packed_qty", "unit_price_cents", "total_price_cents",
        "mapped_product_id", "mapping_status", "mapping_issue",
        "raw_payload", "created_at", "updated_at",
    ]
    enabled_columns: dict[str, bool] = {
        col: has_column("order_lines", col) for col in optional_columns
    }

    # Core columns always present; optional columns appended only if they exist
    insert_columns = ["order_id", "sku", "product_name", "quantity", "unit_price", "total_price"]
    for col in optional_columns:
        if enabled_columns.get(col, False):
            insert_columns.append(col)


    # UUID columns that require explicit CAST in the SQL VALUES clause so
    # PostgreSQL never infers the parameter type as character varying.
    _uuid_columns = {"mapped_product_id"}
    # TIMESTAMP (without time zone) columns — bind UTC-naive datetimes
    _timestamp_columns = {"created_at", "updated_at"}

    columns_str = ", ".join(insert_columns)
    placeholders = ", ".join(
        f"CAST(:{col} AS UUID)" if col in _uuid_columns
        else f"CAST(:{col} AS TIMESTAMP)" if col in _timestamp_columns
        else f":{col}"
        for col in insert_columns
    )
    logger.debug(
        "[UUID_SQL_CAST_APPLIED] statement=_insert_line_items_standalone columns=%s",
        ",".join(col for col in insert_columns if col in _uuid_columns),
    )

    # Build the ON CONFLICT DO UPDATE clause — only include columns that exist
    _standalone_update_clauses = [
        "quantity = EXCLUDED.quantity",
        "unit_price = EXCLUDED.unit_price",
        "total_price = EXCLUDED.total_price",
    ]
    if enabled_columns.get("packed_qty", False):
        _standalone_update_clauses.append("packed_qty = EXCLUDED.packed_qty")
    if enabled_columns.get("unit_price_cents", False):
        _standalone_update_clauses.append("unit_price_cents = EXCLUDED.unit_price_cents")
    if enabled_columns.get("total_price_cents", False):
        _standalone_update_clauses.append("total_price_cents = EXCLUDED.total_price_cents")
    if enabled_columns.get("mapped_product_id", False):
        _standalone_update_clauses.append("mapped_product_id = EXCLUDED.mapped_product_id")
    if enabled_columns.get("mapping_status", False):
        _standalone_update_clauses.append("mapping_status = EXCLUDED.mapping_status")
    if enabled_columns.get("mapping_issue", False):
        _standalone_update_clauses.append("mapping_issue = EXCLUDED.mapping_issue")
    if enabled_columns.get("raw_payload", False):
        _standalone_update_clauses.append("raw_payload = EXCLUDED.raw_payload")
    if enabled_columns.get("updated_at", False):
        _standalone_update_clauses.append("updated_at = EXCLUDED.updated_at")
    _standalone_update_set_str = ",\n        ".join(_standalone_update_clauses)

    # Build conflict clause separately to avoid nested braces in f-string
    _standalone_conflict_clause = (
        "ON CONFLICT (order_id, sku, product_name) "
        "WHERE sku IS NOT NULL AND product_name IS NOT NULL "
        "DO UPDATE SET "
    )

    # Build the full SQL statement using safe string concatenation
    line_insert_stmt = (
        f"INSERT INTO public.order_lines ({columns_str}) "
        f"VALUES ({placeholders}) "
        f"{_standalone_conflict_clause}{_standalone_update_set_str}"
    )


    # Delete stale line items before inserting fresh ones
    await db.execute(delete(OrderLine).where(OrderLine.order_id == order_id))

    inserted = 0
    skipped = 0
    failed = 0

    for line_number, item in enumerate(normalized_line_items, 1):
        sku = item.get("sku")

        if not order_id or not sku:
            logger.error(
                "[LINE_ITEM_SKIP] order_id=%s line_number=%s sku=%s — missing required field",
                order_id,
                line_number,
                sku,
            )
            skipped += 1
            continue

        savepoint_name = f"line_item_{line_number}"
        try:
            await db.execute(text(f"SAVEPOINT {savepoint_name}"))

            raw_payload_val = item.get("raw_payload")
            raw_payload_str = (
                json.dumps(make_json_safe(raw_payload_val))
                if raw_payload_val is not None
                else None
            )
            # AUDIT: All datetime fields wrapped with ensure_utc() at write boundary
            created_at_val = ensure_utc(utc_now(), "created_at")
            updated_at_val = ensure_utc(utc_now(), "updated_at")

            insert_params: dict[str, Any] = {
                "order_id": order_id,
                "sku": sku,
                "product_name": item.get("product_name"),
                "quantity": item.get("quantity"),
                "unit_price": item.get("unit_price"),
                "total_price": item.get("total_price"),
            }


            _mapped_product_id_raw = item.get("mapped_product_id")
            logger.debug(
                "[MAPPED_PRODUCT_ID_BEFORE_SQL] value=%s type=%s",
                _mapped_product_id_raw,
                type(_mapped_product_id_raw),
            )
            if enabled_columns.get("mapped_product_id", False):
                insert_params["mapped_product_id"] = safe_uuid_for_db(_mapped_product_id_raw, "mapped_product_id")
            if enabled_columns.get("mapping_status", False):
                insert_params["mapping_status"] = item.get("mapping_status")
            if enabled_columns.get("mapping_issue", False):
                insert_params["mapping_issue"] = item.get("mapping_issue")
            if enabled_columns.get("raw_payload", False):
                insert_params["raw_payload"] = raw_payload_str
            if enabled_columns.get("created_at", False):
                insert_params["created_at"] = created_at_val
            if enabled_columns.get("updated_at", False):
                insert_params["updated_at"] = updated_at_val
            if enabled_columns.get("packed_qty", False):
                insert_params["packed_qty"] = 0
            if enabled_columns.get("unit_price_cents", False):
                insert_params["unit_price_cents"] = item.get("unit_price_cents")
            if enabled_columns.get("total_price_cents", False):
                insert_params["total_price_cents"] = item.get("total_price_cents")



            # FINAL validation — enforce coercion directly into params dict
            # immediately before execute() so no mutation after coercion can slip through.
            if "mapped_product_id" in insert_params:
                insert_params["mapped_product_id"] = safe_uuid_mapped_product(insert_params.get("mapped_product_id"))
            assert (
                insert_params.get("mapped_product_id") is None
                or isinstance(insert_params.get("mapped_product_id"), (str, UUID))
            ), (
                f"mapped_product_id must be None, str, or UUID, "
                f"got {type(insert_params.get('mapped_product_id'))}"
            )
            logger.debug(
                "[FINAL_SQL_PARAMS_AUDIT] mapped_product_id=%s type=%s is_none=%s is_str=%s is_uuid=%s"
                " function=_insert_line_items_for_order",
                insert_params.get("mapped_product_id"),
                type(insert_params.get("mapped_product_id")).__name__,
                insert_params.get("mapped_product_id") is None,
                isinstance(insert_params.get("mapped_product_id"), str),
                isinstance(insert_params.get("mapped_product_id"), UUID),
            )

            # Centralized sanitizer: fix naive datetimes, date objects, UUID types, JSON payloads
            insert_params = sanitize_sql_params(insert_params, statement="_insert_line_items_standalone")
            insert_params = normalize_uuid_fields(insert_params)
            insert_params = normalize_datetime_fields(insert_params)
            # Belt-and-suspenders: explicitly ensure created_at/updated_at are UTC-aware
            # immediately before execute() so no intermediate mutation can introduce a naive datetime.
            if "created_at" in insert_params:
                insert_params["created_at"] = ensure_utc(insert_params["created_at"], "created_at") or utc_now()
            if "updated_at" in insert_params:
                insert_params["updated_at"] = ensure_utc(insert_params["updated_at"], "updated_at") or utc_now()

            # Fail-fast assertions before execute
            if "created_at" in insert_params:
                _ca = insert_params["created_at"]
                assert isinstance(_ca, datetime) and _ca.tzinfo is not None and _ca.tzinfo.utcoffset(_ca) is not None, (
                    f"[FAIL_FAST] created_at is naive or not a datetime: {_ca!r}"
                )
            if "updated_at" in insert_params:
                _ua = insert_params["updated_at"]
                assert isinstance(_ua, datetime) and _ua.tzinfo is not None and _ua.tzinfo.utcoffset(_ua) is not None, (
                    f"[FAIL_FAST] updated_at is naive or not a datetime: {_ua!r}"
                )

            # Log every parameter in final order with position (DEBUG only — high volume)
            for idx, (k, v) in enumerate(insert_params.items(), start=1):
                logger.debug(
                    "[ORDER_LINES_PARAM_POSITION] idx=%s key=%s type=%s value=%r tzinfo=%s aware=%s",
                    idx,
                    k,
                    type(v).__name__,
                    v if not isinstance(v, (dict, list)) else f"<{type(v).__name__} len={len(v)}>",
                    getattr(v, "tzinfo", None) if isinstance(v, datetime) else "N/A",
                    bool(getattr(v, "tzinfo", None) and getattr(v, "tzinfo", None).utcoffset(v))
                    if isinstance(v, datetime) else None,
                )

            # Apply final hard sanitization
            insert_params = final_sanitize_order_lines_params(insert_params)

            # Assert no naive datetimes remain anywhere
            assert_no_naive_datetimes(insert_params, "params")

            # Convert created_at and updated_at to UTC-naive for TIMESTAMP WITHOUT TIME ZONE columns
            if "created_at" in insert_params:
                insert_params["created_at"] = to_utc_naive(insert_params["created_at"])
            if "updated_at" in insert_params:
                insert_params["updated_at"] = to_utc_naive(insert_params["updated_at"])

            # Final barrier: ensure all UUID objects are strings before asyncpg execute()
            insert_params = apply_uuid_str_to_params(insert_params)

            # Log the final bind values
            logger.debug(
                "[ORDER_LINES_FINAL_DATETIME_BIND] created_at=%s type=%s tzinfo=%s updated_at=%s type=%s tzinfo=%s column_type=TIMESTAMP",
                insert_params.get("created_at"),
                type(insert_params.get("created_at")).__name__,
                getattr(insert_params.get("created_at"), "tzinfo", None) if isinstance(insert_params.get("created_at"), datetime) else "N/A",
                insert_params.get("updated_at"),
                type(insert_params.get("updated_at")).__name__,
                getattr(insert_params.get("updated_at"), "tzinfo", None) if isinstance(insert_params.get("updated_at"), datetime) else "N/A",
            )
            await db.execute(text(line_insert_stmt), insert_params)
            inserted += 1

            await db.execute(text(f"RELEASE SAVEPOINT {savepoint_name}"))

        except Exception as line_error:
            try:
                await db.execute(text(f"ROLLBACK TO SAVEPOINT {savepoint_name}"))
            except Exception:
                pass

            logger.error(
                "[ORDER_LINES_UPSERT_FAIL] order_id=%s sku=%s error=%s",
                order_id,
                sku,
                str(line_error)[:200],
            )
            logger.error(
                "[LINE_ITEM_INSERT_FAILED] order_id=%s line_number=%s error=%s",
                order_id,
                line_number,
                str(line_error)[:200],
            )
            failed += 1
            continue

    return inserted, skipped, failed



async def sync_leaflink_orders(
    db: AsyncSession,
    brand_id: str,
    orders: list[dict],
    pages_fetched: int = 0,
    org_id: Optional[str] = None,
) -> dict[str, Any]:
    """Upsert *pre-fetched* LeafLink orders and their line items.

    Resilient ingestion pipeline:
    - Always saves raw payload first (in raw_payload JSONB)
    - Per-order error isolation: one bad order never fails the whole sync
    - Partial success: header saved even if line items fail (sync_status='partial')
    - Dead-letter: unfixable orders written to sync_dead_letters for reprocessing
    - Non-blocking: returns ok=true if majority succeeds, warning=true if some failed

    The caller is responsible for:
    - Credential lookup and API fetch (before calling this function)
    - Committing or rolling back the session after this function returns

    This function performs only DB writes (no credential lookup, no HTTP).
    It must NOT call db.commit(), db.rollback(), or db.begin() — the caller
    owns the transaction lifecycle.

    Args:
        db: Active async database session.
        brand_id: Brand UUID used to scope orders.
        orders: Pre-fetched, normalised order dicts from LeafLink.
        pages_fetched: Number of API pages retrieved (for metadata).
        org_id: Organization UUID for multi-tenant isolation. Written to every
                order row so GET /orders can filter by org_id AND brand_id.
    """
    sync_start = time.monotonic()

    # Coerce org_id and brand_id to valid UUIDs or None before any DB write.
    # This prevents "column is of type uuid but expression is of type character varying"
    # errors from sending entire orders to the dead-letter queue.
    org_id = safe_uuid_for_db(org_id, "org_id")
    brand_id = safe_uuid_for_db(brand_id, "brand_id") or brand_id  # keep original if invalid so logging still works

    logger.debug(
        "[SYNC_ENTRY] sync_leaflink_orders brand_id=%s org_id=%s order_count=%s pages_fetched=%s",
        brand_id,
        org_id,
        len(orders),
        pages_fetched,
    )

    using_mock = any(isinstance(o, dict) and o.get("mock_data") for o in orders)
    if using_mock:
        logger.warning(
            "leaflink: sync_using_mock_data brand_id=%s — real API unavailable, MOCK_MODE active",
            brand_id,
        )

    # [LEAFLINK_TIMESTAMP_STRATEGY] debug only
    logger.debug(
        "[LEAFLINK_TIMESTAMP_STRATEGY] using=server_time_only reason=provider_dates_unreliable brand_id=%s",
        brand_id,
    )
    # [LEAFLINK_FILTER_DISABLED] debug only
    logger.debug(
        "[LEAFLINK_FILTER_DISABLED] filter_type=freshness reason=provider_dates_unreliable brand_id=%s",
        brand_id,
    )

    created = 0
    updated = 0
    skipped = 0
    partial = 0          # header saved, line items failed
    dead_letter = 0      # header insert failed → written to sync_dead_letters
    total_lines_written = 0
    errors: list[str] = []
    newest_order_date: datetime | None = None


    # ------------------------------------------------------------------
    # Upsert orders and write line items using the caller's session.
    # No begin() here — the route handler owns the transaction lifecycle.
    # Per-order error isolation: one bad order never crashes the loop.
    # ------------------------------------------------------------------
    for o in orders:
        order_external_id_for_log = "unknown"
        order_number_for_log: Optional[str] = None
        raw_payload_for_dead_letter: Any = o

        try:
            if not isinstance(o, dict):
                skipped += 1
                continue

            external_id = safe_str(o.get("external_id"))
            order_external_id_for_log = external_id or "missing"
            order_number_for_log = safe_str(o.get("order_number"))
            raw_payload_for_dead_letter = o

            if not external_id:
                skipped += 1
                continue

            logger.debug(
                "[SYNC_ORDER_PROCESSING] external_id=%s order_number=%s brand_id=%s",
                external_id,
                order_number_for_log,
                brand_id,
            )

            # Safe defaults — missing optional fields never kill the order header.
            customer_name = safe_str(o.get("customer_name")) or "Unknown Customer"
            # order_number: fall back to ORDER-{external_id[:8]} if missing
            order_number = safe_str(o.get("order_number")) or f"ORDER-{(external_id or 'UNKNOWN')[:8]}"
            # status: fall back to "submitted" if missing or invalid
            status = (safe_str(o.get("status")) or "submitted").lower()

            # amount: fault-tolerant — invalid money defaults to 0 with sync_health note
            _raw_amount_v1 = (
                o.get("total_amount")  # Primary: from normalized client
                or o.get("amount")
                or o.get("total")
                or o.get("subtotal")
                or o.get("price")
            )
            amount_decimal = safe_decimal(_raw_amount_v1)
            _amount_sync_note_v1: str | None = None
            if _raw_amount_v1 is not None and amount_decimal is None:
                # Value present but unparseable — default to 0 and note it
                amount_decimal = safe_decimal("0")
                _amount_sync_note_v1 = "amount_invalid_value"
                logger.warning(
                    "[SAFE_AMOUNT_FALLBACK] external_id=%s raw_amount=%r — defaulting to 0",
                    external_id,
                    str(_raw_amount_v1)[:50],
                )
            elif amount_decimal is None:
                logger.warning(
                    "leaflink: sync_amount_missing external_id=%s — no pricing field found in order",
                    external_id,
                )

            total_cents = decimal_to_cents(amount_decimal) or 0

            item_count = safe_int(o.get("item_count"), default=0)
            unit_count = safe_int(o.get("unit_count"), default=0)

            raw_line_items = o.get("line_items", [])
            normalized_line_items = normalize_line_items(raw_line_items)

            if item_count == 0:
                item_count = len(normalized_line_items)

            if unit_count == 0:
                unit_count = sum(item.get("quantity", 0) or 0 for item in normalized_line_items)

            review_status = derive_review_status(normalized_line_items)

            now = ensure_utc(utc_now(), "synced_at")

            # Always save raw payload first (core resilience principle)
            raw_payload = o.get("raw_payload") if isinstance(o.get("raw_payload"), dict) else o

            existing_result = await db.execute(
                select(Order).where(
                    Order.brand_id == brand_id,
                    Order.external_order_id == external_id,
                )
            )
            existing = existing_result.scalar_one_or_none()

            # ----------------------------------------------------------
            # Header insert/update — isolated in its own try/except.
            # On failure: write to sync_dead_letters, continue to next order.
            # ----------------------------------------------------------
            try:
                # Re-coerce immediately before every DB write — belt-and-suspenders guard
                _write_org_id = safe_uuid_for_db(org_id, "org_id")
                _write_brand_id = safe_uuid_for_db(brand_id, "brand_id") or brand_id
                logger.debug(
                    "[ORG_ID_BEFORE_SQL] field=org_id value=%s external_id=%s function=sync_leaflink_orders",
                    _write_org_id,
                    external_id,
                )
                logger.debug(
                    "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s external_id=%s function=sync_leaflink_orders",
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
                    logger.debug(
                        "[REVIEW_STATUS_RESOLVED] value=%s source=derive_review_status external_id=%s",
                        review_status,
                        external_id,
                    )
                    existing.review_status = review_status
                    # Compute sync health
                    _sh_missing_upd: list[str] = []
                    if not customer_name or customer_name == "Unknown Customer":
                        _sh_missing_upd.append("customer_name")
                    if amount_decimal is None:
                        _sh_missing_upd.append("amount")
                    elif _amount_sync_note_v1:
                        _sh_missing_upd.append(_amount_sync_note_v1)
                    if not normalized_line_items:
                        _sh_missing_upd.append("line_items")
                    if not status:
                        _sh_missing_upd.append("status")
                    _sh_status_upd = "partial" if _sh_missing_upd else "ok"
                    existing.sync_status = _sh_status_upd
                    existing.sync_health_status = _sh_status_upd
                    existing.sync_health_missing_fields = _sh_missing_upd if _sh_missing_upd else None
                    # Use server timestamps for all DB columns — bulletproof mode
                    existing.synced_at = now
                    existing.last_synced_at = now
                    # Always stamp org_id so existing rows get backfilled on re-sync
                    if _write_org_id:
                        existing.org_id = _write_org_id
                    order_row = existing
                    updated += 1
                else:
                    # Compute sync health for new order
                    _sh_missing_new: list[str] = []
                    if not customer_name or customer_name == "Unknown Customer":
                        _sh_missing_new.append("customer_name")
                    if amount_decimal is None:
                        _sh_missing_new.append("amount")
                    elif _amount_sync_note_v1:
                        _sh_missing_new.append(_amount_sync_note_v1)
                    if not normalized_line_items:
                        _sh_missing_new.append("line_items")
                    if not status:
                        _sh_missing_new.append("status")
                    _sh_status_new = "partial" if _sh_missing_new else "ok"
                    # Filter kwargs to only include valid Order model columns
                    order_kwargs = filter_model_kwargs(Order, {
                        'org_id': _write_org_id,
                        'brand_id': _write_brand_id,
                        'external_order_id': external_id,
                        'order_number': order_number,
                        'customer_name': customer_name,
                        'status': status,
                        'total_cents': total_cents,
                        'amount': amount_decimal,
                        'item_count': item_count,
                        'unit_count': unit_count,
                        'line_items_json': make_json_safe(normalized_line_items),
                        'raw_payload': make_json_safe(raw_payload),
                        'source': 'leaflink',
                        'review_status': review_status,
                        'sync_status': _sh_status_new,
                        'sync_health_status': _sh_status_new,
                        'sync_health_missing_fields': _sh_missing_new if _sh_missing_new else None,
                        'external_created_at': None,  # Never use provider dates
                        'external_updated_at': None,  # Never use provider dates
                        # Use server timestamps for all DB columns — bulletproof mode
                        'synced_at': now,
                        'last_synced_at': now,
                    })
                    order_row = Order(**order_kwargs)
                    db.add(order_row)
                    # Flush to get the auto-generated order_row.id before writing lines.
                    await db.flush()
                    created += 1

                logger.debug(
                    "[SYNC_HEADER_SAVED] external_id=%s order_number=%s brand_id=%s action=%s",
                    external_id,
                    order_number,
                    brand_id,
                    "updated" if existing else "created",
                )

            except Exception as header_exc:
                # Header insert failed — write to dead-letter queue and skip this order
                header_err_msg = str(header_exc)[:500]
                _payload_size = len(str(raw_payload_for_dead_letter)) if raw_payload_for_dead_letter else 0
                _hdr_category, _hdr_exc_type, _hdr_tb = log_sync_order_failed(
                    external_id=external_id,
                    order_number=order_number,
                    failure_stage="header_insert",
                    exception=header_exc,
                    payload_size=_payload_size,
                    customer_name=customer_name,
                )
                logger.error(
                    "[SYNC_HEADER_INSERT_FAILED] external_id=%s order_number=%s brand_id=%s error=%s",
                    external_id,
                    order_number,
                    brand_id,
                    header_err_msg,
                    exc_info=True,
                )
                await _write_sync_dead_letter(
                    brand_id=brand_id,
                    raw_payload=raw_payload_for_dead_letter,
                    error_stage="header_insert",
                    error_message=header_err_msg,
                    org_id=org_id,
                    external_id=external_id,
                    order_number=order_number,
                    customer_name=customer_name,
                    failure_stage="header_insert",
                    failure_category=_hdr_category,
                    exception_type=_hdr_exc_type,
                    exception_message=header_err_msg,
                    traceback_summary=_hdr_tb,
                )
                dead_letter += 1
                if len(errors) < 10:
                    errors.append(f"header_insert external_id={external_id}: {header_err_msg[:200]}")
                continue

            # ----------------------------------------------------------
            # Line item insert — isolated in its own try/except.
            # On failure: mark header as 'partial', log, continue.
            # The raw payload is already saved in the order row.
            # ----------------------------------------------------------
            try:
                # Delete stale line items then insert fresh ones.
                await db.execute(
                    delete(OrderLine).where(OrderLine.order_id == order_row.id)
                )

                # Build raw SQL INSERT with explicit CAST(:mapped_product_id AS UUID)
                # so PostgreSQL never infers the parameter type as character varying.
                # Using raw SQL instead of ORM avoids the ORM model's String(120)
                # type annotation overriding the actual UUID column type in the DB.
                _li_optional_columns = [
                    "packed_qty", "unit_price_cents", "total_price_cents",
                    "mapped_product_id", "mapping_status", "mapping_issue",
                    "raw_payload", "created_at", "updated_at",
                ]
                _li_enabled: dict[str, bool] = {
                    col: has_column("order_lines", col) for col in _li_optional_columns
                }
                _li_insert_columns = [
                    "order_id", "sku", "product_name", "quantity",
                    "unit_price", "total_price",
                ]
                for _col in _li_optional_columns:
                    if _li_enabled.get(_col, False):
                        _li_insert_columns.append(_col)

                # UUID columns that require explicit CAST in the SQL VALUES clause
                _li_uuid_columns = {"mapped_product_id"}
                # TIMESTAMP (without time zone) columns — bind UTC-naive datetimes
                _li_timestamp_columns = {"created_at", "updated_at"}

                _li_columns_str = ", ".join(_li_insert_columns)
                _li_placeholders = ", ".join(
                    f"CAST(:{col} AS UUID)" if col in _li_uuid_columns
                    else f"CAST(:{col} AS TIMESTAMP)" if col in _li_timestamp_columns
                    else f":{col}"
                    for col in _li_insert_columns
                )
                logger.debug(
                    "[UUID_SQL_CAST_APPLIED] statement=sync_leaflink_orders columns=%s",
                    ",".join(col for col in _li_insert_columns if col in _li_uuid_columns),
                )

                # Build the ON CONFLICT DO UPDATE clause — only include columns that exist
                _li_update_clauses = [
                    "quantity = EXCLUDED.quantity",
                    "unit_price = EXCLUDED.unit_price",
                    "total_price = EXCLUDED.total_price",
                ]
                if _li_enabled.get("packed_qty", False):
                    _li_update_clauses.append("packed_qty = EXCLUDED.packed_qty")
                if _li_enabled.get("unit_price_cents", False):
                    _li_update_clauses.append("unit_price_cents = EXCLUDED.unit_price_cents")
                if _li_enabled.get("total_price_cents", False):
                    _li_update_clauses.append("total_price_cents = EXCLUDED.total_price_cents")
                if _li_enabled.get("mapped_product_id", False):
                    _li_update_clauses.append("mapped_product_id = EXCLUDED.mapped_product_id")
                if _li_enabled.get("mapping_status", False):
                    _li_update_clauses.append("mapping_status = EXCLUDED.mapping_status")
                if _li_enabled.get("mapping_issue", False):
                    _li_update_clauses.append("mapping_issue = EXCLUDED.mapping_issue")
                if _li_enabled.get("raw_payload", False):
                    _li_update_clauses.append("raw_payload = EXCLUDED.raw_payload")
                if _li_enabled.get("updated_at", False):
                    _li_update_clauses.append("updated_at = EXCLUDED.updated_at")
                _li_update_set_str = ",\n                    ".join(_li_update_clauses)

                # Build conflict clause separately to avoid nested braces in f-string
                _li_conflict_clause = (
                    "ON CONFLICT (order_id, sku, product_name) "
                    "WHERE sku IS NOT NULL AND product_name IS NOT NULL "
                    "DO UPDATE SET "
                )

                # Build the full SQL statement using safe string concatenation
                _li_insert_stmt = (
                    f"INSERT INTO public.order_lines ({_li_columns_str}) "
                    f"VALUES ({_li_placeholders}) "
                    f"{_li_conflict_clause}{_li_update_set_str}"
                )



                # Use server time for line item timestamps — bulletproof mode
                line_now = ensure_utc(utc_now(), "created_at")
                for item in normalized_line_items:
                    _mapped_product_id_raw = item.get("mapped_product_id")
                    logger.debug(
                        "[MAPPED_PRODUCT_ID_BEFORE_SQL] value=%s type=%s",
                        _mapped_product_id_raw,
                        type(_mapped_product_id_raw),
                    )
                    _mapped_product_id_coerced = safe_uuid_mapped_product(_mapped_product_id_raw)
                    assert (
                        _mapped_product_id_coerced is None
                        or isinstance(_mapped_product_id_coerced, (str, UUID))
                    ), (
                        f"mapped_product_id must be None, str, or UUID, "
                        f"got {type(_mapped_product_id_coerced)}"
                    )
                    logger.debug(
                        "[FINAL_SQL_PARAMS_AUDIT] mapped_product_id=%s type=%s is_none=%s is_str=%s is_uuid=%s"
                        " function=sync_leaflink_orders",
                        _mapped_product_id_coerced,
                        type(_mapped_product_id_coerced).__name__,
                        _mapped_product_id_coerced is None,
                        isinstance(_mapped_product_id_coerced, str),
                        isinstance(_mapped_product_id_coerced, UUID),
                    )

                    _raw_payload_val = item.get("raw_payload")
                    _raw_payload_str = (
                        json.dumps(make_json_safe(_raw_payload_val))
                        if _raw_payload_val is not None
                        else None
                    )

                    _li_params: dict[str, Any] = {
                        "order_id": order_row.id,
                        "sku": item.get("sku"),
                        "product_name": item.get("product_name"),
                        "quantity": item.get("quantity"),
                        "unit_price": item.get("unit_price"),
                        "total_price": item.get("total_price"),
                    }
                    if _li_enabled.get("mapped_product_id", False):
                        _li_params["mapped_product_id"] = _mapped_product_id_coerced
                    if _li_enabled.get("mapping_status", False):
                        _li_params["mapping_status"] = item.get("mapping_status")
                    if _li_enabled.get("mapping_issue", False):
                        _li_params["mapping_issue"] = item.get("mapping_issue")
                    if _li_enabled.get("raw_payload", False):
                        _li_params["raw_payload"] = _raw_payload_str
                    if _li_enabled.get("created_at", False):
                        _li_params["created_at"] = line_now
                    if _li_enabled.get("updated_at", False):
                        _li_params["updated_at"] = line_now
                    if _li_enabled.get("packed_qty", False):
                        _li_params["packed_qty"] = 0
                    if _li_enabled.get("unit_price_cents", False):
                        _li_params["unit_price_cents"] = item.get("unit_price_cents")
                    if _li_enabled.get("total_price_cents", False):
                        _li_params["total_price_cents"] = item.get("total_price_cents")

                    # Centralized sanitizer: fix naive datetimes, date objects, UUID types, JSON payloads
                    _li_params = sanitize_sql_params(_li_params, statement="sync_leaflink_orders_line_items")
                    _li_params = normalize_uuid_fields(_li_params)
                    _li_params = normalize_datetime_fields(_li_params)
                    # Belt-and-suspenders: explicitly ensure created_at/updated_at are UTC-aware
                    # immediately before execute() so no intermediate mutation can introduce a naive datetime.
                    if "created_at" in _li_params:
                        _li_params["created_at"] = ensure_utc(_li_params["created_at"], "created_at") or utc_now()
                    if "updated_at" in _li_params:
                        _li_params["updated_at"] = ensure_utc(_li_params["updated_at"], "updated_at") or utc_now()

                    # Fail-fast assertions before execute
                    if "created_at" in _li_params:
                        _ca = _li_params["created_at"]
                        assert isinstance(_ca, datetime) and _ca.tzinfo is not None and _ca.tzinfo.utcoffset(_ca) is not None, (
                            f"[FAIL_FAST] created_at is naive or not a datetime: {_ca!r}"
                        )
                    if "updated_at" in _li_params:
                        _ua = _li_params["updated_at"]
                        assert isinstance(_ua, datetime) and _ua.tzinfo is not None and _ua.tzinfo.utcoffset(_ua) is not None, (
                            f"[FAIL_FAST] updated_at is naive or not a datetime: {_ua!r}"
                        )

                    # Log every parameter in final order with position (DEBUG only — high volume)
                    for idx, (k, v) in enumerate(_li_params.items(), start=1):
                        logger.debug(
                            "[ORDER_LINES_PARAM_POSITION] idx=%s key=%s type=%s value=%r tzinfo=%s aware=%s",
                            idx,
                            k,
                            type(v).__name__,
                            v if not isinstance(v, (dict, list)) else f"<{type(v).__name__} len={len(v)}>",
                            getattr(v, "tzinfo", None) if isinstance(v, datetime) else "N/A",
                            bool(getattr(v, "tzinfo", None) and getattr(v, "tzinfo", None).utcoffset(v))
                            if isinstance(v, datetime) else None,
                        )

                    # Apply final hard sanitization
                    _li_params = final_sanitize_order_lines_params(_li_params)

                    # Assert no naive datetimes remain anywhere
                    assert_no_naive_datetimes(_li_params, "params")

                    # Convert created_at and updated_at to UTC-naive for TIMESTAMP WITHOUT TIME ZONE columns
                    if "created_at" in _li_params:
                        _li_params["created_at"] = to_utc_naive(_li_params["created_at"])
                    if "updated_at" in _li_params:
                        _li_params["updated_at"] = to_utc_naive(_li_params["updated_at"])

                    # Final barrier: ensure all UUID objects are strings before asyncpg execute()
                    _li_params = apply_uuid_str_to_params(_li_params)

                    # Log the final bind values
                    logger.debug(
                        "[ORDER_LINES_FINAL_DATETIME_BIND] created_at=%s type=%s tzinfo=%s updated_at=%s type=%s tzinfo=%s column_type=TIMESTAMP",
                        _li_params.get("created_at"),
                        type(_li_params.get("created_at")).__name__,
                        getattr(_li_params.get("created_at"), "tzinfo", None) if isinstance(_li_params.get("created_at"), datetime) else "N/A",
                        _li_params.get("updated_at"),
                        type(_li_params.get("updated_at")).__name__,
                        getattr(_li_params.get("updated_at"), "tzinfo", None) if isinstance(_li_params.get("updated_at"), datetime) else "N/A",
                    )
                    await db.execute(text(_li_insert_stmt), _li_params)


                total_lines_written += len(normalized_line_items)

            except Exception as line_exc:
                # Line item failure — save header with sync_status='partial', log, continue.
                # Header is already committed; line item failures do NOT mark the order as dead-letter.
                line_err_msg = str(line_exc)[:500]
                _li_payload_size = len(str(raw_payload_for_dead_letter)) if raw_payload_for_dead_letter else 0
                _li_category, _li_exc_type, _li_tb = log_sync_order_failed(
                    external_id=external_id,
                    order_number=order_number,
                    failure_stage="line_item_insert",
                    exception=line_exc,
                    payload_size=_li_payload_size,
                    customer_name=customer_name,
                    failure_category="malformed_line_items",
                )
                logger.error(
                    "[SYNC_PARTIAL_SUCCESS] external_id=%s brand_id=%s header_saved=true line_items_failed=true error=%s",
                    external_id,
                    brand_id,
                    line_err_msg,
                )
                # Mark the order as partial so it can be reprocessed later.
                # The order header is still valid — only line items failed.
                try:
                    order_row.sync_status = "partial"
                    order_row.sync_health_status = "partial"
                    if not hasattr(order_row, "sync_health_missing_fields") or order_row.sync_health_missing_fields is None:
                        order_row.sync_health_missing_fields = ["line_items"]
                    order_row.sync_health_last_error = line_err_msg[:500] if line_err_msg else None
                except Exception:
                    pass
                partial += 1
                if len(errors) < 10:
                    errors.append(f"line_item_insert external_id={external_id}: {line_err_msg[:200]}")

        except Exception as order_exc:
            # Outer per-order failure: log, record error, skip this order, continue
            err_reason = str(order_exc)[:300]
            _outer_payload_size = len(str(raw_payload_for_dead_letter)) if raw_payload_for_dead_letter else 0
            _outer_category, _outer_exc_type, _outer_tb = log_sync_order_failed(
                external_id=order_external_id_for_log if order_external_id_for_log != "unknown" else None,
                order_number=order_number_for_log,
                failure_stage="header_transform",
                exception=order_exc,
                payload_size=_outer_payload_size,
            )
            logger.warning(
                "[ORDER_TRANSFORM_SKIPPED] external_id=%s reason=%s",
                order_external_id_for_log,
                err_reason,
            )
            logger.error(
                "leaflink: upsert_order_failed brand_id=%s order=%s error=%s",
                brand_id,
                order_external_id_for_log,
                err_reason,
                exc_info=True,
            )
            # Write to dead-letter for any unhandled outer exception
            await _write_sync_dead_letter(
                brand_id=brand_id,
                raw_payload=raw_payload_for_dead_letter,
                error_stage="order_transform",
                error_message=err_reason,
                org_id=org_id,
                external_id=order_external_id_for_log if order_external_id_for_log != "unknown" else None,
                order_number=order_number_for_log,
                failure_stage="header_transform",
                failure_category=_outer_category,
                exception_type=_outer_exc_type,
                exception_message=err_reason,
                traceback_summary=_outer_tb,
            )
            dead_letter += 1
            if len(errors) < 10:
                errors.append(f"order={order_external_id_for_log} error={err_reason}")
            skipped += 1
            continue

    sync_duration = round(time.monotonic() - sync_start, 2)
    error_count = len(errors)
    fetched_count = len(orders)
    # ok=true if at least some orders succeeded OR no orders were fetched
    all_failed = (created == 0 and updated == 0 and partial == 0 and error_count > 0 and fetched_count > 0)
    has_warning = error_count > 0 or dead_letter > 0 or partial > 0

    logger.info(
        "[SYNC_FINAL_REPORT] fetched=%s inserted=%s updated=%s partial=%s skipped=%s dead_letter=%s error=%s duration=%ss brand_id=%s",
        fetched_count,
        created,
        updated,
        partial,
        skipped,
        dead_letter,
        error_count,
        sync_duration,
        brand_id,
    )
    logger.debug(
        "[LEAFLINK_FETCH_OK] raw_count=%s brand_id=%s",
        fetched_count,
        brand_id,
    )
    logger.debug(
        "[LEAFLINK_DB_WRITE_OK] inserted=%s updated=%s skipped=%s errors=%s brand_id=%s",
        created, updated, skipped, error_count, brand_id,
    )

    return {
        "ok": not all_failed,
        "warning": has_warning,
        "fetched_count": fetched_count,
        "inserted_count": created,
        "updated_count": updated,
        "partial_count": partial,
        "skipped_count": skipped,
        "dead_letter_count": dead_letter,
        "error_count": error_count,
        # Legacy keys kept for backward compatibility
        "orders_fetched": fetched_count,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "line_items_written": total_lines_written,
        "newest_order_date": newest_order_date,
        "pages_fetched": pages_fetched,
        "sync_duration_seconds": sync_duration,
        "errors": errors,
        "mock_data": using_mock,
        "message": (
            f"Synced {fetched_count} orders: {created} inserted, {updated} updated, "
            f"{partial} partial, {skipped} skipped, {dead_letter} dead_letter, {error_count} errors"
        ),
    }


async def sync_leaflink_orders_headers_only(
    brand_id: str,
    orders: list[dict],
    pages_fetched: int = 0,
    batch_size: int = HEADER_BATCH_SIZE,
    org_id: Optional[str] = None,
) -> dict[str, Any]:
    """Upsert ONLY order headers (Order table) for Phase 1 — no line items.

    Opens its own DB sessions and commits in small batches of ``batch_size``
    so that no single transaction holds locks for more than a handful of rows.
    Line item data is preserved in ``line_items_json`` on the Order row so the
    background worker can read it back without re-fetching from LeafLink.

    All header batches are committed BEFORE spawning the fire-and-forget asyncio
    task that writes OrderLine rows, guaranteeing the parent Order rows exist in
    the database when the background worker queries for them.

    Returns a summary dict compatible with the existing sync_result contract.
    """
    logger.debug("[ORDER_SAVE_START] entering order save function brand=%s fetched=%s org_id=%s", brand_id, len(orders), org_id)
    logger.debug(
        "[ORG_CONTEXT] stage=headers_only_entry org_id=%s brand_id=%s"
        " external_order_id=N/A orders_count=%s",
        org_id,
        brand_id,
        len(orders),
    )
    logger.info(
        "[SYNC_PHASE_1_START] brand_id=%s orders_to_fetch=%s org_id=%s",
        brand_id,
        len(orders),
        org_id,
    )

    sync_start = time.monotonic()

    # Coerce brand_id and org_id to valid UUIDs or None before any DB write.
    # safe_uuid_for_db() returns None (not the original value) for non-UUID strings,
    # preventing "column is of type uuid but expression is of type character varying"
    # errors. The original values are preserved in raw_payload JSONB.
    brand_id_value = safe_uuid_for_db(brand_id, "brand_id") or brand_id  # keep original if invalid so logging still works
    org_id_value = safe_uuid_for_db(org_id, "org_id") if org_id else None

    total_created = 0
    total_updated = 0
    total_skipped = 0
    total_dead_letter = 0
    errors: list[str] = []
    newest_order_date: datetime | None = None
    fetched_count = len(orders)

    # Persistence counters — track every outcome for [SYNC_FINAL_ACCOUNTING]
    insert_success = 0
    update_success = 0
    insert_rollback = 0
    update_rollback = 0
    dead_letter_written = 0
    skipped_duplicate = 0
    skipped_validation = 0
    skipped_org_missing = 0
    malformed_payload = 0
    line_item_failure = 0
    transaction_abort = 0
    datetime_normalization_failures = 0

    # Split orders into batches of batch_size
    batches = [
        orders[i : i + batch_size]
        for i in range(0, len(orders), batch_size)
    ]

    logger.info(
        "[OrdersSync] sync_start brand=%s total_orders=%s batch_size=%s total_batches=%s",
        brand_id_value,
        len(orders),
        batch_size,
        len(batches),
    )

    # [LEAFLINK_TIMESTAMP_STRATEGY] debug only — suppress from production logs
    logger.debug(
        "[LEAFLINK_TIMESTAMP_STRATEGY] using=server_time_only reason=provider_dates_unreliable brand_id=%s",
        brand_id_value,
    )
    # [LEAFLINK_FILTER_DISABLED] debug only
    logger.debug(
        "[LEAFLINK_FILTER_DISABLED] filter_type=freshness reason=provider_dates_unreliable brand_id=%s",
        brand_id_value,
    )
    # [LEAFLINK_UPSERT_KEY] debug only
    logger.debug("[LEAFLINK_UPSERT_KEY] key=external_id brand_id=%s", brand_id_value)

    # ------------------------------------------------------------------
    # Phase 1: Upsert all order headers and commit each batch before
    # spawning the line-item background task.  This guarantees that every
    # Order row is visible to subsequent DB sessions when the background
    # worker looks them up by (brand_id, external_order_id).
    # ------------------------------------------------------------------
    for batch_num, batch in enumerate(batches, 1):
        batch_start = time.monotonic()
        current_batch_size = len(batch)
        batch_created = 0
        batch_updated = 0
        batch_skipped = 0
        batch_dead_letter = 0

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    for o in batch:
                        _order_ext_id_for_log = "unknown"
                        _order_number_for_log = None
                        _raw_payload_ref: Any = o
                        try:
                          if not isinstance(o, dict):
                            logger.warning(
                                "[ORDER_SKIPPED] brand_id=%s external_order_id=unknown reason=invalid_payload details=not_a_dict",
                                brand_id_value,
                            )
                            batch_skipped += 1
                            continue

                          # Map external_order_id: use order["id"] as primary source
                          # (LeafLink raw payloads carry the order PK in "id").
                          external_id = safe_str(o.get("id") or o.get("external_id") or o.get("external_order_id"))
                          _order_ext_id_for_log = external_id or "missing"
                          _order_number_for_log = safe_str(o.get("order_number"))
                          _raw_payload_ref = o
                          if not external_id:
                            logger.error(
                                "[OrdersSync] skip_no_external_id batch=%s order_number=%s",
                                batch_num,
                                safe_str(o.get("order_number")),
                            )
                            logger.warning(
                                "[ORDER_SKIPPED] brand_id=%s external_order_id=unknown reason=missing_external_order_id",
                                brand_id_value,
                            )
                            batch_skipped += 1
                            continue

                          logger.debug(
                              "[ORG_CONTEXT] stage=order_processing org_id=%s brand_id=%s"
                              " external_order_id=%s",
                              org_id_value,
                              brand_id_value,
                              external_id,
                          )
                          logger.debug(
                              "[SYNC_ORDER_PROCESSING] external_id=%s order_number=%s brand_id=%s batch=%s/%s",
                              external_id,
                              _order_number_for_log,
                              brand_id_value,
                              batch_num,
                              len(batches),
                          )

                          # [COMPANY_ID_MISMATCH] Log payload company_id for alignment tracing.
                          # Compare against credential_company_id from [BRAND_CONFIG_AUDIT] logs.
                          # Logged only when the payload carries a company_id or source field.
                          _payload_company_id = o.get("company_id") or o.get("source")
                          if _payload_company_id:
                            logger.debug(
                                "[COMPANY_ID_MISMATCH] brand_id=%s external_order_id=%s payload_company_id=%s note=compare_with_BRAND_CONFIG_AUDIT_for_credential_company_id",
                                brand_id_value,
                                external_id,
                                _payload_company_id,
                            )

                          # Safe defaults — missing optional fields never kill the order header.
                          # customer_name: fall back to "Unknown Customer" (order still inserts)
                          customer_name = safe_str(o.get("customer_name")) or "Unknown Customer"
                          # order_number: fall back to ORDER-{external_id[:8]} if missing
                          order_number = safe_str(o.get("order_number")) or f"ORDER-{(external_id or 'UNKNOWN')[:8]}"
                          # status: fall back to "submitted" if missing or invalid
                          status = (safe_str(o.get("status")) or "submitted").lower()

                          # amount: fault-tolerant — invalid money defaults to 0 with sync_health note
                          _raw_amount = (
                            o.get("total_amount")
                            or o.get("amount")
                            or o.get("total")
                            or o.get("subtotal")
                            or o.get("price")
                          )
                          amount_decimal = safe_decimal(_raw_amount)
                          _amount_sync_note: str | None = None
                          if _raw_amount is not None and amount_decimal is None:
                            # Value present but unparseable — default to 0 and note it
                            amount_decimal = safe_decimal("0")
                            _amount_sync_note = "amount_invalid_value"
                            logger.warning(
                                "[SAFE_AMOUNT_FALLBACK] external_id=%s raw_amount=%r — defaulting to 0",
                                external_id,
                                str(_raw_amount)[:50],
                            )
                          total_cents = decimal_to_cents(amount_decimal) or 0

                          item_count = safe_int(o.get("item_count"), default=0)
                          unit_count = safe_int(o.get("unit_count"), default=0)

                          # Normalise line items and store as JSON — do NOT write
                          # OrderLine rows here; that is deferred to the background worker.
                          raw_line_items = o.get("line_items", [])
                          normalized_line_items = normalize_line_items(raw_line_items)

                          if item_count == 0:
                            item_count = len(normalized_line_items)
                          if unit_count == 0:
                            unit_count = sum(
                                item.get("quantity", 0) or 0
                                for item in normalized_line_items
                            )

                          review_status = derive_review_status(normalized_line_items)

                          # Use server time for all DB timestamp columns — bulletproof mode.
                          # LeafLink date fields (created_on, paid_date, etc.) are stored
                          # inside raw_payload JSONB exactly as received from the API.
                          now = ensure_utc(utc_now(), "synced_at")

                          # raw_payload stores the full LeafLink order dict (including all date fields)
                          raw_payload = (
                            o.get("raw_payload")
                            if isinstance(o.get("raw_payload"), dict)
                            else o
                          )

                          existing_result = await db.execute(
                            select(Order).where(
                                Order.brand_id == brand_id_value,
                                Order.external_order_id == external_id,
                            )
                          )
                          existing = existing_result.scalar_one_or_none()

                          if existing:
                            # Use raw SQL UPDATE with CAST so PostgreSQL receives
                            # explicit type coercion for UUID columns instead of
                            # rejecting a character-varying bound parameter.
                            # Use server timestamps only — bulletproof mode.
                            update_stmt = """
UPDATE orders SET
    org_id = CAST(:org_id AS uuid),
    customer_name = :customer_name,
    status = :status,
    order_number = :order_number,
    total_cents = :total_cents,
    amount = :amount,
    item_count = :item_count,
    unit_count = :unit_count,
    line_items_json = :line_items_json,
    raw_payload = :raw_payload,
    review_status = :review_status,
    sync_status = :sync_status,
    sync_health_status = :sync_health_status,
    sync_health_missing_fields = CAST(:sync_health_missing_fields AS jsonb),
    synced_at = :synced_at,
    last_synced_at = :last_synced_at,
    updated_at = :updated_at
WHERE CAST(brand_id AS uuid) = CAST(:brand_id AS uuid) AND external_order_id = :external_order_id
"""

                            # cannot encode Python list/dict directly; it needs strings.
                            line_items_json_str = json.dumps(make_json_safe(normalized_line_items)) if normalized_line_items else None
                            raw_payload_str = json.dumps(make_json_safe(raw_payload)) if raw_payload else None

                            # Use server timestamps — bulletproof mode
                            synced_at_val = now
                            updated_at_val = now

                            # Re-coerce immediately before SQL params — belt-and-suspenders guard
                            _sql_org_id = safe_uuid_for_db(org_id_value, "org_id")
                            _sql_brand_id = safe_uuid_for_db(brand_id_value, "brand_id") or brand_id_value
                            logger.debug(
                                "[ORG_ID_BEFORE_SQL] field=org_id value=%s external_id=%s function=sync_leaflink_orders_headers_only action=update",
                                _sql_org_id,
                                external_id,
                            )
                            logger.debug(
                                "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s external_id=%s function=sync_leaflink_orders_headers_only action=update",
                                _sql_brand_id,
                                external_id,
                            )

                            # Compute sync health for this order
                            _sh_missing: list[str] = []
                            if not customer_name or customer_name == "Unknown Customer":
                                _sh_missing.append("customer_name")
                            if amount_decimal is None:
                                _sh_missing.append("amount")
                            elif _amount_sync_note:
                                _sh_missing.append(_amount_sync_note)
                            if not normalized_line_items:
                                _sh_missing.append("line_items")
                            if not status:
                                _sh_missing.append("status")
                            _sh_status = "partial" if _sh_missing else "ok"
                            _sh_missing_json = json.dumps(_sh_missing) if _sh_missing else None

                            update_params = {
                                "org_id": _sql_org_id,
                                "brand_id": _sql_brand_id,
                                "external_order_id": external_id,
                                "customer_name": customer_name,
                                "status": status,
                                "order_number": order_number,
                                "total_cents": total_cents,
                                "amount": amount_decimal,
                                "item_count": item_count,
                                "unit_count": unit_count,
                                "line_items_json": line_items_json_str,
                                "raw_payload": raw_payload_str,
                                "review_status": review_status,
                                "sync_status": _sh_status,
                                "sync_health_status": _sh_status,
                                "sync_health_missing_fields": _sh_missing_json,
                                "synced_at": synced_at_val,
                                "last_synced_at": synced_at_val,
                                "updated_at": updated_at_val,
                            }

                            # FINAL validation — enforce coercion directly into params dict
                            # immediately before execute() so no mutation after coercion can slip through.
                            update_params["org_id"] = safe_uuid_for_db(update_params.get("org_id"), "org_id")
                            update_params["brand_id"] = safe_uuid_for_db(update_params.get("brand_id"), "brand_id") or update_params.get("brand_id")
                            logger.debug(
                                "[FINAL_SQL_PARAMS] org_id=%s org_type=%s brand_id=%s brand_type=%s"
                                " external_id=%s action=update function=sync_leaflink_orders_headers_only",
                                update_params.get("org_id"),
                                type(update_params.get("org_id")).__name__,
                                update_params.get("brand_id"),
                                type(update_params.get("brand_id")).__name__,
                                external_id,
                            )

                            logger.debug("[ORG_ID_BEFORE_SQL] org_id=%s", org_id_value)
                            logger.debug("[BRAND_ID_BEFORE_SQL] brand_id=%s", brand_id_value)
                            # Coerce all datetime params to UTC-aware immediately before SQL execution
                            update_params["synced_at"] = ensure_utc(update_params.get("synced_at"), "synced_at") or utc_now()
                            update_params["last_synced_at"] = ensure_utc(update_params.get("last_synced_at"), "last_synced_at") or utc_now()
                            update_params["updated_at"] = ensure_utc(update_params.get("updated_at"), "updated_at") or utc_now()
                            logger.debug(
                                "[DATETIME_FINAL_PARAMS] synced_at=%s type=%s last_synced_at=%s type=%s updated_at=%s type=%s",
                                update_params.get("synced_at"),
                                type(update_params.get("synced_at")),
                                update_params.get("last_synced_at"),
                                type(update_params.get("last_synced_at")),
                                update_params.get("updated_at"),
                                type(update_params.get("updated_at")),
                            )

                            update_params = sanitize_sql_params(update_params, statement="sync_leaflink_orders_headers_only_update")
                            update_params = normalize_uuid_fields(update_params)
                            update_params = normalize_datetime_fields(update_params)
                            update_params = apply_uuid_str_to_params(update_params)

                            # Normalize ALL datetime fields to UTC-aware before SQL execution
                            update_params["synced_at"] = ensure_utc_datetime(update_params.get("synced_at"))
                            update_params["last_synced_at"] = ensure_utc_datetime(update_params.get("last_synced_at"))
                            update_params["updated_at"] = ensure_utc_datetime(update_params.get("updated_at"))
                            update_params["created_at"] = ensure_utc_datetime(update_params.get("created_at"))
                            update_params["external_created_at"] = ensure_utc_datetime(update_params.get("external_created_at"))
                            update_params["external_updated_at"] = ensure_utc_datetime(update_params.get("external_updated_at"))
                            update_params["due_date"] = ensure_utc_datetime(update_params.get("due_date"))
                            update_params["payment_date"] = ensure_utc_datetime(update_params.get("payment_date"))
                            update_params["delivery_date"] = ensure_utc_datetime(update_params.get("delivery_date"))

                            # Assertion: no naive datetimes should remain
                            for _field, _value in update_params.items():
                                if isinstance(_value, datetime):
                                    if _value.tzinfo is None:
                                        datetime_normalization_failures += 1
                                    assert _value.tzinfo is not None, f"[DATETIME_NORMALIZATION_FAILED] {_field} is naive"
                                    logger.debug("[DATETIME_AUDIT] field=%s tzinfo=%s aware=true", _field, _value.tzinfo)
                            # Hard block: never execute UPDATE with org_id=None
                            if update_params.get("org_id") is None:
                                logger.error(
                                    "[ORG_CONTEXT_MISSING_FATAL] brand_id=%s external_id=%s",
                                    brand_id_value, external_id,
                                )
                                asyncio.create_task(
                                    _write_sync_dead_letter(
                                        brand_id=brand_id_value,
                                        raw_payload=_raw_payload_ref,
                                        error_stage="org_context_missing",
                                        error_message="org_id is None at update time",
                                        org_id=None,
                                        external_id=external_id,
                                        failure_category="missing_org_context",
                                        failure_stage="header_insert",
                                    )
                                )
                                batch_skipped += 1
                                continue

                            await db.execute(
                                text(update_stmt),
                                update_params,
                            )
                            batch_updated += 1
                            update_success += 1
                            logger.debug(
                                "[ORDER_UPDATE_COMMIT] external_id=%s brand_id=%s",
                                external_id,
                                brand_id_value,
                            )
                          else:
                            # Use raw SQL INSERT with CAST so PostgreSQL receives
                            # explicit type coercion for UUID columns instead of
                            # rejecting a character-varying bound parameter.
                            # Use server timestamps only — bulletproof mode.
                            insert_stmt = """
INSERT INTO orders (
    org_id, brand_id, external_order_id, order_number, customer_name, status,
    total_cents, amount, item_count, unit_count, line_items_json, raw_payload,
    source, review_status, sync_status, sync_health_status, sync_health_missing_fields,
    synced_at, last_synced_at,
    created_at, updated_at
) VALUES (
    CAST(:org_id AS uuid), CAST(:brand_id AS uuid), :external_order_id, :order_number,
    :customer_name, :status, :total_cents, :amount, :item_count, :unit_count,
    :line_items_json, :raw_payload, :source, :review_status, :sync_status,
    :sync_health_status, CAST(:sync_health_missing_fields AS jsonb),
    :synced_at, :last_synced_at,
    :created_at, :updated_at
)
"""

                            # Serialize JSON fields for raw SQL binding — asyncpg
                            # cannot encode Python list/dict directly; it needs strings.
                            line_items_json_str = json.dumps(make_json_safe(normalized_line_items)) if normalized_line_items else None
                            raw_payload_str = json.dumps(make_json_safe(raw_payload)) if raw_payload else None

                            # Use server timestamps — bulletproof mode
                            created_at_val = now
                            synced_at_val = now

                            # Re-coerce immediately before SQL params — belt-and-suspenders guard
                            _sql_org_id = safe_uuid_for_db(org_id_value, "org_id")
                            _sql_brand_id = safe_uuid_for_db(brand_id_value, "brand_id") or brand_id_value
                            logger.debug(
                                "[ORG_ID_BEFORE_SQL] field=org_id value=%s external_id=%s function=sync_leaflink_orders_headers_only action=insert",
                                _sql_org_id,
                                external_id,
                            )
                            logger.debug(
                                "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s external_id=%s function=sync_leaflink_orders_headers_only action=insert",
                                _sql_brand_id,
                                external_id,
                            )

                            # Compute sync health for insert
                            _sh_missing_ins: list[str] = []
                            if not customer_name or customer_name == "Unknown Customer":
                                _sh_missing_ins.append("customer_name")
                            if amount_decimal is None:
                                _sh_missing_ins.append("amount")
                            elif _amount_sync_note:
                                _sh_missing_ins.append(_amount_sync_note)
                            if not normalized_line_items:
                                _sh_missing_ins.append("line_items")
                            if not status:
                                _sh_missing_ins.append("status")
                            _sh_status_ins = "partial" if _sh_missing_ins else "ok"
                            _sh_missing_json_ins = json.dumps(_sh_missing_ins) if _sh_missing_ins else None

                            logger.info(
                                "[ORDER_INSERT_PREP] org_id=%s brand_id=%s external_order_id=%s"
                                " customer=%s amount=%s status=%s",
                                _sql_org_id,
                                _sql_brand_id,
                                external_id,
                                customer_name,
                                amount_decimal,
                                status,
                            )
                            insert_params = {
                                "org_id": _sql_org_id,
                                "brand_id": _sql_brand_id,
                                "external_order_id": external_id,
                                "order_number": order_number,
                                "customer_name": customer_name,
                                "status": status,
                                "total_cents": total_cents,
                                "amount": amount_decimal,
                                "item_count": item_count,
                                "unit_count": unit_count,
                                "line_items_json": line_items_json_str,
                                "raw_payload": raw_payload_str,
                                "source": "leaflink",
                                "review_status": review_status,
                                "sync_status": _sh_status_ins,
                                "sync_health_status": _sh_status_ins,
                                "sync_health_missing_fields": _sh_missing_json_ins,
                                "synced_at": synced_at_val,
                                "last_synced_at": synced_at_val,
                                "created_at": created_at_val,
                                "updated_at": created_at_val,
                            }

                            # FINAL validation — enforce coercion directly into params dict
                            # immediately before execute() so no mutation after coercion can slip through.
                            insert_params["org_id"] = safe_uuid_for_db(insert_params.get("org_id"), "org_id")
                            insert_params["brand_id"] = safe_uuid_for_db(insert_params.get("brand_id"), "brand_id") or insert_params.get("brand_id")
                            logger.debug(
                                "[FINAL_SQL_PARAMS] org_id=%s org_type=%s brand_id=%s brand_type=%s"
                                " external_id=%s action=insert function=sync_leaflink_orders_headers_only",
                                insert_params.get("org_id"),
                                type(insert_params.get("org_id")).__name__,
                                insert_params.get("brand_id"),
                                type(insert_params.get("brand_id")).__name__,
                                external_id,
                            )
                            logger.debug("[ORG_ID_BEFORE_SQL] org_id=%s", org_id_value)
                            logger.debug("[BRAND_ID_BEFORE_SQL] brand_id=%s", brand_id_value)
                            # Coerce all datetime params to UTC-aware immediately before SQL execution
                            insert_params["synced_at"] = ensure_utc(insert_params.get("synced_at"), "synced_at") or utc_now()
                            insert_params["last_synced_at"] = ensure_utc(insert_params.get("last_synced_at"), "last_synced_at") or utc_now()
                            insert_params["created_at"] = ensure_utc(insert_params.get("created_at"), "created_at") or utc_now()
                            insert_params["updated_at"] = ensure_utc(insert_params.get("updated_at"), "updated_at") or utc_now()
                            logger.debug(
                                "[DATETIME_FINAL_PARAMS] synced_at=%s type=%s last_synced_at=%s type=%s updated_at=%s type=%s",
                                insert_params.get("synced_at"),
                                type(insert_params.get("synced_at")),
                                insert_params.get("last_synced_at"),
                                type(insert_params.get("last_synced_at")),
                                insert_params.get("updated_at"),
                                type(insert_params.get("updated_at")),
                            )
                            # Centralized sanitizer: fix naive datetimes, date objects, UUID types, JSON payloads
                            insert_params = sanitize_sql_params(insert_params, statement="sync_leaflink_orders_headers_only_insert")
                            insert_params = normalize_uuid_fields(insert_params)
                            insert_params = normalize_datetime_fields(insert_params)
                            insert_params = apply_uuid_str_to_params(insert_params)

                            # Normalize ALL datetime fields to UTC-aware before SQL execution
                            insert_params["synced_at"] = ensure_utc_datetime(insert_params.get("synced_at"))
                            insert_params["last_synced_at"] = ensure_utc_datetime(insert_params.get("last_synced_at"))
                            insert_params["created_at"] = ensure_utc_datetime(insert_params.get("created_at"))
                            insert_params["updated_at"] = ensure_utc_datetime(insert_params.get("updated_at"))
                            insert_params["external_created_at"] = ensure_utc_datetime(insert_params.get("external_created_at"))
                            insert_params["external_updated_at"] = ensure_utc_datetime(insert_params.get("external_updated_at"))
                            insert_params["due_date"] = ensure_utc_datetime(insert_params.get("due_date"))
                            insert_params["payment_date"] = ensure_utc_datetime(insert_params.get("payment_date"))
                            insert_params["delivery_date"] = ensure_utc_datetime(insert_params.get("delivery_date"))

                            # Assertion: no naive datetimes should remain
                            for _field, _value in insert_params.items():
                                if isinstance(_value, datetime):
                                    if _value.tzinfo is None:
                                        datetime_normalization_failures += 1
                                    assert _value.tzinfo is not None, f"[DATETIME_NORMALIZATION_FAILED] {_field} is naive"
                                    logger.debug("[DATETIME_AUDIT] field=%s tzinfo=%s aware=true", _field, _value.tzinfo)

                            # Hard block: never execute INSERT with org_id=None
                            if insert_params.get("org_id") is None:
                                logger.error(
                                    "[ORG_CONTEXT_MISSING_FATAL] brand_id=%s external_id=%s",
                                    brand_id_value, external_id,
                                )
                                asyncio.create_task(
                                    _write_sync_dead_letter(
                                        brand_id=brand_id_value,
                                        raw_payload=_raw_payload_ref,
                                        error_stage="org_context_missing",
                                        error_message="org_id is None at insert time",
                                        org_id=None,
                                        external_id=external_id,
                                        failure_category="missing_org_context",
                                        failure_stage="header_insert",
                                    )
                                )
                                batch_skipped += 1
                                continue
                            await db.execute(
                                text(insert_stmt),
                                insert_params,
                            )
                            batch_created += 1
                            insert_success += 1
                            logger.debug(
                                "[ORDER_INSERT_COMMIT] external_id=%s brand_id=%s",
                                external_id,
                                brand_id_value,
                            )
                        except IntegrityError as _order_exc:
                            # Integrity violation — duplicate key or constraint failure
                            _err_reason = str(_order_exc)[:300]
                            insert_rollback += 1
                            if "duplicate" in _err_reason.lower() or "uq_" in _err_reason.lower():
                                skipped_duplicate += 1
                                logger.debug(
                                    "[ORDER_SKIPPED_DUPLICATE] external_id=%s brand_id=%s",
                                    _order_ext_id_for_log,
                                    brand_id_value,
                                )
                            else:
                                logger.error(
                                    "[ORDER_INSERT_ROLLBACK] external_id=%s error=%s",
                                    _order_ext_id_for_log,
                                    _err_reason[:200],
                                )
                            batch_dead_letter += 1
                            batch_skipped += 1
                            dead_letter_written += 1
                            _dl_customer = safe_str(o.get("customer_name")) if isinstance(o, dict) else None
                            asyncio.create_task(
                                _write_sync_dead_letter(
                                    brand_id=brand_id_value,
                                    raw_payload=_raw_payload_ref,
                                    error_stage="header_insert",
                                    error_message=_err_reason,
                                    org_id=org_id_value,
                                    external_id=_order_ext_id_for_log if _order_ext_id_for_log != "unknown" else None,
                                    order_number=_order_number_for_log,
                                    customer_name=_dl_customer,
                                    failure_stage="header_insert",
                                    failure_category="duplicate_external_id",
                                    exception_type=type(_order_exc).__name__,
                                    exception_message=_err_reason,
                                )
                            )
                            if len(errors) < 10:
                                errors.append(
                                    f"integrity_error external_id={_order_ext_id_for_log}: {_err_reason[:200]}"
                                )
                            continue
                        except Exception as _order_exc:
                            # Per-order failure — log, write dead-letter, continue
                            _err_reason = str(_order_exc)[:300]
                            _batch_payload_size = len(str(_raw_payload_ref)) if _raw_payload_ref else 0
                            # Extract customer_name for dead-letter metadata (may not be set yet)
                            _dl_customer = safe_str(o.get("customer_name")) if isinstance(o, dict) else None
                            _batch_category, _batch_exc_type, _batch_tb = log_sync_order_failed(
                                external_id=_order_ext_id_for_log if _order_ext_id_for_log != "unknown" else None,
                                order_number=_order_number_for_log,
                                failure_stage="header_insert",
                                exception=_order_exc,
                                payload_size=_batch_payload_size,
                                customer_name=_dl_customer,
                            )
                            insert_rollback += 1
                            transaction_abort += 1
                            logger.error(
                                "[TRANSACTION_ABORT] external_id=%s error=%s",
                                _order_ext_id_for_log,
                                _err_reason[:200],
                            )
                            logger.error(
                                "[ORDER_INSERT_ROLLBACK] external_id=%s error=%s stage=header_insert",
                                _order_ext_id_for_log,
                                _err_reason[:200],
                            )
                            # Write to dead-letter asynchronously (own session, non-blocking)
                            asyncio.create_task(
                                _write_sync_dead_letter(
                                    brand_id=brand_id_value,
                                    raw_payload=_raw_payload_ref,
                                    error_stage="header_insert",
                                    error_message=_err_reason,
                                    org_id=org_id_value,
                                    external_id=_order_ext_id_for_log if _order_ext_id_for_log != "unknown" else None,
                                    order_number=_order_number_for_log,
                                    customer_name=_dl_customer,
                                    failure_stage="header_insert",
                                    failure_category=_batch_category,
                                    exception_type=_batch_exc_type,
                                    exception_message=_err_reason,
                                    traceback_summary=_batch_tb,
                                )
                            )
                            batch_dead_letter += 1
                            dead_letter_written += 1
                            batch_skipped += 1
                            if len(errors) < 10:
                                errors.append(
                                    f"header_insert external_id={_order_ext_id_for_log}: {_err_reason[:200]}"
                                )
                            continue
                # Explicit commit guarantee
                await db.commit()

            # Batch committed successfully (async with db.begin() exited cleanly)
            batch_duration = round(time.monotonic() - batch_start, 2)
            logger.info(
                "[OrdersSync] batch_committed batch=%s/%s brand=%s created=%s updated=%s skipped=%s dead_letter=%s duration=%ss",
                batch_num,
                len(batches),
                brand_id_value,
                batch_created,
                batch_updated,
                batch_skipped,
                batch_dead_letter,
                batch_duration,
            )
            total_created += batch_created
            total_updated += batch_updated
            total_skipped += batch_skipped
            total_dead_letter += batch_dead_letter

            # Visibility verification every 25 successful inserts
            if insert_success > 0 and insert_success % 25 == 0 and org_id_value is not None:
                try:
                    async with AsyncSessionLocal() as _vis_db:
                        _vis_result = await _vis_db.execute(
                            select(func.count(Order.id))
                            .where(Order.brand_id == brand_id_value)
                            .where(Order.org_id == org_id_value)
                        )
                        _visible_count = _vis_result.scalar() or 0
                        if _visible_count < insert_success:
                            logger.warning(
                                "[VISIBILITY_GAP] inserted=%s visible=%s gap=%s brand_id=%s",
                                insert_success,
                                _visible_count,
                                insert_success - _visible_count,
                                brand_id_value,
                            )
                        else:
                            logger.debug(
                                "[VISIBILITY_OK] inserted=%s visible=%s brand_id=%s",
                                insert_success,
                                _visible_count,
                                brand_id_value,
                            )
                except Exception as _vis_exc:
                    logger.warning(
                        "[VISIBILITY_CHECK_ERROR] brand_id=%s error=%s",
                        brand_id_value,
                        str(_vis_exc)[:200],
                    )

        except Exception as batch_exc:
            batch_duration = round(time.monotonic() - batch_start, 2)
            err_msg = str(batch_exc)
            insert_rollback += batch_created  # any inserts in this batch are rolled back
            update_rollback += batch_updated
            transaction_abort += 1
            logger.error(
                "[TRANSACTION_ABORT] brand_id=%s batch=%s/%s error=%s stage=batch_commit",
                brand_id_value,
                batch_num,
                len(batches),
                err_msg[:200],
            )
            logger.error(
                "[ORDER_INSERT_ROLLBACK] brand_id=%s batch=%s/%s error=%s stage=batch_commit",
                brand_id_value,
                batch_num,
                len(batches),
                err_msg[:200],
                exc_info=True,
            )
            errors.append(err_msg)
            total_skipped += current_batch_size
            continue

    sync_duration = round(time.monotonic() - sync_start, 2)

    # Emit critical alert if we fetched orders but saved none at all
    if fetched_count > 0 and total_created == 0 and total_updated == 0:
        logger.error(
            "[CRITICAL] orders_not_saved fetched=%s but saved=0 brand=%s",
            fetched_count,
            brand_id_value,
        )

    logger.info(
        "[OrdersSync] all_batches_complete brand=%s total_batches=%s total_created=%s total_updated=%s total_skipped=%s sync_duration=%ss",
        brand_id_value,
        len(batches),
        total_created,
        total_updated,
        total_skipped,
        sync_duration,
    )
    logger.info(
        "[SYNC_PHASE_1_COMPLETE] brand_id=%s created=%s updated=%s duration=%ss",
        brand_id_value,
        total_created,
        total_updated,
        sync_duration,
    )

    # [ORDER_COUNT_AFTER_SYNC] Query DB to verify orders are present and check for null fields
    try:
        from sqlalchemy import func as _func
        async with AsyncSessionLocal() as _count_db:
            _total_res = await _count_db.execute(
                select(_func.count()).select_from(Order).where(Order.brand_id == brand_id_value)
            )
            _total = _total_res.scalar() or 0

            _null_ext_id_res = await _count_db.execute(
                select(_func.count()).select_from(Order).where(
                    Order.brand_id == brand_id_value,
                    Order.external_order_id.is_(None),
                )
            )
            _null_ext_id = _null_ext_id_res.scalar() or 0

            _null_org_id_res = await _count_db.execute(
                select(_func.count()).select_from(Order).where(
                    Order.brand_id == brand_id_value,
                    Order.org_id.is_(None),
                )
            )
            _null_org_id = _null_org_id_res.scalar() or 0

            _null_brand_id_res = await _count_db.execute(
                select(_func.count()).select_from(Order).where(
                    Order.brand_id == brand_id_value,
                    Order.brand_id.is_(None),
                )
            )
            _null_brand_id = _null_brand_id_res.scalar() or 0

            _null_synced_at_res = await _count_db.execute(
                select(_func.count()).select_from(Order).where(
                    Order.brand_id == brand_id_value,
                    Order.synced_at.is_(None),
                )
            )
            _null_synced_at = _null_synced_at_res.scalar() or 0

            _null_ext_created_res = await _count_db.execute(
                select(_func.count()).select_from(Order).where(
                    Order.brand_id == brand_id_value,
                    Order.external_created_at.is_(None),
                )
            )
            _null_ext_created = _null_ext_created_res.scalar() or 0

        logger.info(
            "[ORDER_COUNT_AFTER_SYNC] brand_id=%s total=%s null_external_order_id=%s null_org_id=%s null_brand_id=%s null_synced_at=%s null_external_created_at=%s",
            brand_id_value,
            _total,
            _null_ext_id,
            _null_org_id,
            _null_brand_id,
            _null_synced_at,
            _null_ext_created,
        )
    except Exception as _count_exc:
        logger.error(
            "[ORDER_COUNT_AFTER_SYNC] brand_id=%s error=%s",
            brand_id_value,
            str(_count_exc)[:200],
        )

    # Update sync health after Phase 1 commits
    await _update_sync_health_phase1(
        brand_id=brand_id_value,
        orders_count=(total_created or 0) + (total_updated or 0),
    )

    # ------------------------------------------------------------------
    # Phase 2: All order header batches are now committed to the DB.
    # Spawn the line-item background task AFTER the commit loop so that
    # every Order row is guaranteed to exist when the worker queries for
    # it by (brand_id, external_order_id).
    # ------------------------------------------------------------------
    async def _background_line_items() -> None:
        await sync_leaflink_line_items(brand_id_value, orders, org_id=org_id_value)

    asyncio.create_task(_background_line_items())

    error_count = len(errors)
    has_warning = error_count > 0 or total_dead_letter > 0

    # [SYNC_FINAL_ACCOUNTING] — final persistence metrics for this batch call
    _records_seen = fetched_count
    _persistence_ratio = (insert_success + update_success) / _records_seen if _records_seen > 0 else 0
    _rollback_ratio = (insert_rollback + update_rollback) / _records_seen if _records_seen > 0 else 0
    _dead_letter_ratio = dead_letter_written / _records_seen if _records_seen > 0 else 0

    # Query final visible count for this batch's brand+org scope
    _final_visible_count = 0
    try:
        async with AsyncSessionLocal() as _final_db:
            _final_vis_result = await _final_db.execute(
                select(func.count(Order.id))
                .where(Order.brand_id == brand_id_value)
                .where(Order.org_id == org_id_value)
            )
            _final_visible_count = _final_vis_result.scalar() or 0
    except Exception as _fv_exc:
        logger.warning(
            "[SYNC_FINAL_ACCOUNTING] final_visible_query_failed brand_id=%s error=%s",
            brand_id_value,
            str(_fv_exc)[:200],
        )

    logger.info(
        "[SYNC_FINAL_ACCOUNTING] brand_id=%s org_id=%s "
        "leaflink_seen=%s headers_inserted=%s headers_updated=%s headers_skipped=%s "
        "headers_failed=%s db_visible_count=%s dead_letter_count=%s rollback_count=%s "
        "transaction_failures=%s "
        "persistence_ratio=%.1f%% rollback_ratio=%.1f%% dead_letter_ratio=%.1f%% "
        "datetime_normalization_failures=%s",
        brand_id_value,
        org_id_value,
        _records_seen,
        insert_success,
        update_success,
        skipped_duplicate + skipped_validation + skipped_org_missing,
        malformed_payload,
        _final_visible_count,
        dead_letter_written,
        insert_rollback + update_rollback,
        transaction_abort,
        _persistence_ratio * 100,
        _rollback_ratio * 100,
        _dead_letter_ratio * 100,
        datetime_normalization_failures,
    )

    logger.info(
        "[SYNC_FINAL_REPORT] fetched=%s inserted=%s updated=%s partial=0 skipped=%s dead_letter=%s error=%s duration=%ss brand_id=%s",
        fetched_count,
        total_created,
        total_updated,
        total_skipped,
        total_dead_letter,
        error_count,
        sync_duration,
        brand_id_value,
    )
    logger.info(
        "[LEAFLINK_SYNC_COMPLETE] fetched=%s inserted=%s updated=%s brand_id=%s",
        fetched_count, total_created, total_updated, brand_id_value,
    )

    return {
        "ok": len(errors) == 0,
        "warning": has_warning,
        "fetched_count": fetched_count,
        "inserted_count": total_created,
        "updated_count": total_updated,
        "partial_count": 0,
        "skipped_count": total_skipped,
        "dead_letter_count": total_dead_letter,
        "error_count": error_count,
        # Legacy keys kept for backward compatibility
        "orders_fetched": fetched_count,
        "created": total_created,
        "updated": total_updated,
        "skipped": total_skipped,
        "line_items_written": 0,
        "newest_order_date": newest_order_date,
        "pages_fetched": pages_fetched,
        "sync_duration_seconds": sync_duration,
        "errors": errors,
        "mock_data": any(isinstance(o, dict) and o.get("mock_data") for o in orders),
        "message": (
            f"Upserted {total_created + total_updated} order headers "
            f"({total_created} new, {total_updated} updated, {total_dead_letter} dead_letter)"
        ),
    }


async def sync_leaflink_line_items(
    brand_id: str,
    orders: list[dict],
    org_id: Optional[str] = None,
) -> dict[str, Any]:
    """Write OrderLine rows for a list of pre-fetched orders (background Phase 2).

    Two-phase design guarantees parent orders exist before line items are written:
      - Phase 1 (sync_leaflink_orders_headers_only) commits all Order rows first.
      - Phase 2 (this function) runs after Phase 1 commits, so FK lookups always
        succeed.

    Idempotent writes: uses INSERT ... ON CONFLICT (order_id, sku, product_name)
    DO UPDATE so retries and overlapping syncs never create duplicates.

    Dead-letter queue: items that fail after MAX_LINE_ITEM_RETRIES are moved to
    dead_letter_line_items for admin inspection and manual replay.

    Sync health: updates sync_health table after Phase 2 completes or on error.

    Runs in its own session per order so a single failure never aborts the rest.
    """
    from sqlalchemy import func

    # Safety guard: verify orders exist before processing line items
    logger.info("[LINE_ITEM_CHECK_START] checking for parent orders brand=%s", brand_id)
    logger.info(
        "[SYNC_PHASE_2_START] brand_id=%s line_items_to_sync=%s",
        brand_id,
        len(orders),
    )

    bg_start = time.monotonic()
    errors: list[str] = []

    # Coerce brand_id and org_id to valid UUIDs or None to match what was written in Phase 1.
    # safe_uuid_for_db() returns None (not the original value) for non-UUID strings,
    # preventing "column is of type uuid but expression is of type character varying" errors.
    brand_id_value = safe_uuid_for_db(brand_id, "brand_id") or brand_id  # keep original if invalid so logging still works
    org_id_value = safe_uuid_for_db(org_id, "org_id") if org_id else None

    logger.debug(
        "[ORG_ID_BEFORE_SQL] field=org_id value=%s function=sync_leaflink_line_items",
        org_id_value,
    )
    logger.debug(
        "[BRAND_ID_BEFORE_SQL] field=brand_id value=%s function=sync_leaflink_line_items",
        brand_id_value,
    )

    try:
        async with AsyncSessionLocal() as check_db:
            result = await check_db.execute(
                select(func.count()).select_from(Order).where(
                    Order.brand_id == brand_id_value,
                    Order.source == "leaflink"
                )
            )
            order_count = result.scalar_one()
    except Exception as e:
        logger.error("[LINE_ITEM_CHECK_ERROR] failed to count orders: %s", e)
        order_count = 0

    logger.info("[LINE_ITEM_CHECK_RESULT] found %s orders brand=%s", order_count, brand_id_value)

    if order_count == 0:
        logger.error("[LINE_ITEM_BLOCKED] no orders in DB — aborting line item sync brand=%s", brand_id_value)
        await _record_sync_error(brand_id_value, Exception("No parent orders found in database"))
        return {
            "ok": False,
            "lines_written": 0,
            "errors": ["No parent orders found in database"],
            "duration_seconds": 0,
        }

    # ------------------------------------------------------------------
    # Helper: build the idempotent UPSERT statement once.
    # Uses ON CONFLICT (order_id, sku, product_name) DO UPDATE so retries
    # never create duplicate rows (requires uq_order_line_identity constraint).
    # Inspect actual schema so inserts work with any schema variant.
    # ------------------------------------------------------------------
    optional_columns = [
        "packed_qty",
        "unit_price_cents",
        "total_price_cents",
        "mapped_product_id",
        "mapping_status",
        "mapping_issue",
        "raw_payload",
        "created_at",
        "updated_at",
    ]
    enabled_columns: dict[str, bool] = {
        col: has_column("order_lines", col) for col in optional_columns
    }

    logger.info(
        "[LINE_ITEM_COLUMNS_ENABLED] columns=%s",
        json.dumps({col: enabled for col, enabled in enabled_columns.items()}),
    )

    # Always-present core columns
    insert_columns = [
        "order_id",
        "sku",
        "product_name",
        "quantity",
        "unit_price",
        "total_price",
    ]
    # Append optional columns only if they exist in the live schema
    for col in optional_columns:
        if enabled_columns.get(col, False):
            insert_columns.append(col)

    # UUID columns that require explicit CAST in the SQL VALUES clause so
    # PostgreSQL never infers the parameter type as character varying.
    _uuid_columns = {"mapped_product_id"}
    # TIMESTAMP (without time zone) columns — bind UTC-naive datetimes
    _timestamp_columns = {"created_at", "updated_at"}

    columns_str = ", ".join(insert_columns)
    placeholders = ", ".join(
        f"CAST(:{col} AS UUID)" if col in _uuid_columns
        else f"CAST(:{col} AS TIMESTAMP)" if col in _timestamp_columns
        else f":{col}"
        for col in insert_columns
    )
    logger.debug(
        "[UUID_SQL_CAST_APPLIED] statement=_upsert_line_items columns=%s",
        ",".join(col for col in insert_columns if col in _uuid_columns),
    )


    # Build the ON CONFLICT DO UPDATE clause — only include columns that exist
    update_set_clauses = [
        "quantity = EXCLUDED.quantity",
        "unit_price = EXCLUDED.unit_price",
        "total_price = EXCLUDED.total_price",
    ]
    if enabled_columns.get("packed_qty", False):
        update_set_clauses.append("packed_qty = EXCLUDED.packed_qty")
    if enabled_columns.get("unit_price_cents", False):
        update_set_clauses.append("unit_price_cents = EXCLUDED.unit_price_cents")
    if enabled_columns.get("total_price_cents", False):
        update_set_clauses.append("total_price_cents = EXCLUDED.total_price_cents")
    if enabled_columns.get("mapped_product_id", False):
        update_set_clauses.append("mapped_product_id = EXCLUDED.mapped_product_id")
    if enabled_columns.get("mapping_status", False):
        update_set_clauses.append("mapping_status = EXCLUDED.mapping_status")
    if enabled_columns.get("mapping_issue", False):
        update_set_clauses.append("mapping_issue = EXCLUDED.mapping_issue")
    if enabled_columns.get("raw_payload", False):
        update_set_clauses.append("raw_payload = EXCLUDED.raw_payload")
    if enabled_columns.get("updated_at", False):
        update_set_clauses.append("updated_at = EXCLUDED.updated_at")

    update_set_str = ",\n        ".join(update_set_clauses)

    line_upsert_stmt = f"""
        INSERT INTO public.order_lines ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (order_id, sku, product_name)
        WHERE sku IS NOT NULL AND product_name IS NOT NULL
        DO UPDATE SET
        {update_set_str}
    """

    # ------------------------------------------------------------------
    # Inner helper: upsert all line items for one order using savepoints
    # so a single bad item never poisons the whole batch transaction.
    # Returns (inserted_count, skipped_count, failed_items).
    # failed_items is a list of (item_dict, error_str) for dead-lettering.
    # ------------------------------------------------------------------
    async def _upsert_line_items(
        db: AsyncSession,
        order_row: Any,
        normalized_line_items: list[dict],
    ) -> tuple[int, int, list[tuple[dict, str]]]:
        order_id_val = order_row.id
        inserted = 0
        skipped = 0
        failed_items: list[tuple[dict, str]] = []

        for line_number, item in enumerate(normalized_line_items, 1):
            sku = item.get("sku")

            if not order_id_val or not sku:
                logger.error(
                    "[LINE_ITEM_SKIP] order_id=%s line_number=%s sku=%s — missing required field",
                    order_id_val,
                    line_number,
                    sku,
                )
                skipped += 1
                continue

            savepoint_name = f"line_item_{line_number}"
            try:
                await db.execute(text(f"SAVEPOINT {savepoint_name}"))

                raw_payload_val = item.get("raw_payload")
                raw_payload_str = (
                    json.dumps(make_json_safe(raw_payload_val))
                    if raw_payload_val is not None
                    else None
                )

                # Defensive null checks — ensure required numeric fields have defaults
                quantity = item.get("quantity") or 0
                unit_price = item.get("unit_price") or 0
                total_price = item.get("total_price") or 0
                # AUDIT: All datetime fields wrapped with ensure_utc() at write boundary
                now_val = ensure_utc(utc_now(), "created_at")

                insert_params: dict[str, Any] = {
                    "order_id": order_id_val,
                    "sku": sku,
                    "product_name": item.get("product_name"),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_price": total_price,
                }

                _mapped_product_id_raw = item.get("mapped_product_id")
                logger.debug(
                    "[MAPPED_PRODUCT_ID_BEFORE_SQL] value=%s type=%s",
                    _mapped_product_id_raw,
                    type(_mapped_product_id_raw),
                )
                if enabled_columns.get("mapped_product_id", False):
                    insert_params["mapped_product_id"] = safe_uuid_for_db(_mapped_product_id_raw, "mapped_product_id")
                if enabled_columns.get("mapping_status", False):
                    insert_params["mapping_status"] = item.get("mapping_status")
                if enabled_columns.get("mapping_issue", False):
                    insert_params["mapping_issue"] = item.get("mapping_issue")
                if enabled_columns.get("raw_payload", False):
                    insert_params["raw_payload"] = raw_payload_str
                if enabled_columns.get("created_at", False):
                    insert_params["created_at"] = now_val
                if enabled_columns.get("updated_at", False):
                    insert_params["updated_at"] = now_val
                if enabled_columns.get("packed_qty", False):
                    insert_params["packed_qty"] = 0
                if enabled_columns.get("unit_price_cents", False):
                    insert_params["unit_price_cents"] = item.get("unit_price_cents")
                if enabled_columns.get("total_price_cents", False):
                    insert_params["total_price_cents"] = item.get("total_price_cents")

                # FINAL validation — enforce coercion directly into params dict
                # immediately before execute() so no mutation after coercion can slip through.
                if "mapped_product_id" in insert_params:
                    insert_params["mapped_product_id"] = safe_uuid_mapped_product(insert_params.get("mapped_product_id"))
                assert (
                    insert_params.get("mapped_product_id") is None
                    or isinstance(insert_params.get("mapped_product_id"), (str, UUID))
                ), (
                    f"mapped_product_id must be None, str, or UUID, "
                    f"got {type(insert_params.get('mapped_product_id'))}"
                )
                logger.debug(
                    "[FINAL_SQL_PARAMS] org_id=%s org_type=%s brand_id=%s brand_type=%s"
                    " mapped_product_id=%s mapped_type=%s order_id=%s sku=%s function=_upsert_line_items",
                    org_id_value,
                    type(org_id_value).__name__,
                    brand_id_value,
                    type(brand_id_value).__name__,
                    insert_params.get("mapped_product_id"),
                    type(insert_params.get("mapped_product_id")).__name__,
                    order_id_val,
                    sku,
                )
                logger.debug(
                    "[FINAL_SQL_PARAMS_AUDIT] mapped_product_id=%s type=%s is_none=%s is_str=%s is_uuid=%s"
                    " function=_upsert_line_items",
                    insert_params.get("mapped_product_id"),
                    type(insert_params.get("mapped_product_id")).__name__,
                    insert_params.get("mapped_product_id") is None,
                    isinstance(insert_params.get("mapped_product_id"), str),
                    isinstance(insert_params.get("mapped_product_id"), UUID),
                )
                # Centralized sanitizer: fix naive datetimes, date objects, UUID types, JSON payloads
                insert_params = sanitize_sql_params(insert_params, statement="_upsert_line_items")
                insert_params = normalize_uuid_fields(insert_params)
                insert_params = normalize_datetime_fields(insert_params)
                # Belt-and-suspenders: explicitly ensure created_at/updated_at are UTC-aware
                # immediately before execute() so no intermediate mutation can introduce a naive datetime.
                if "created_at" in insert_params:
                    insert_params["created_at"] = ensure_utc(insert_params["created_at"], "created_at") or utc_now()
                if "updated_at" in insert_params:
                    insert_params["updated_at"] = ensure_utc(insert_params["updated_at"], "updated_at") or utc_now()

                # Fail-fast assertions before execute
                if "created_at" in insert_params:
                    _ca = insert_params["created_at"]
                    assert isinstance(_ca, datetime) and _ca.tzinfo is not None and _ca.tzinfo.utcoffset(_ca) is not None, (
                        f"[FAIL_FAST] created_at is naive or not a datetime: {_ca!r}"
                    )
                if "updated_at" in insert_params:
                    _ua = insert_params["updated_at"]
                    assert isinstance(_ua, datetime) and _ua.tzinfo is not None and _ua.tzinfo.utcoffset(_ua) is not None, (
                        f"[FAIL_FAST] updated_at is naive or not a datetime: {_ua!r}"
                    )

                # Log every parameter in final order with position (DEBUG only — high volume)
                for idx, (k, v) in enumerate(insert_params.items(), start=1):
                    logger.debug(
                        "[ORDER_LINES_PARAM_POSITION] idx=%s key=%s type=%s value=%r tzinfo=%s aware=%s",
                        idx,
                        k,
                        type(v).__name__,
                        v if not isinstance(v, (dict, list)) else f"<{type(v).__name__} len={len(v)}>",
                        getattr(v, "tzinfo", None) if isinstance(v, datetime) else "N/A",
                        bool(getattr(v, "tzinfo", None) and getattr(v, "tzinfo", None).utcoffset(v))
                        if isinstance(v, datetime) else None,
                    )

                # Apply final hard sanitization
                insert_params = final_sanitize_order_lines_params(insert_params)

                # Convert created_at and updated_at to UTC-naive for TIMESTAMP WITHOUT TIME ZONE columns
                if "created_at" in insert_params:
                    insert_params["created_at"] = to_utc_naive(insert_params["created_at"])
                if "updated_at" in insert_params:
                    insert_params["updated_at"] = to_utc_naive(insert_params["updated_at"])

                # Final barrier: ensure all UUID objects are strings before asyncpg execute()
                insert_params = apply_uuid_str_to_params(insert_params)

                # Log the final bind values
                logger.debug(
                    "[ORDER_LINES_FINAL_DATETIME_BIND] created_at=%s type=%s tzinfo=%s updated_at=%s type=%s tzinfo=%s column_type=TIMESTAMP",
                    insert_params.get("created_at"),
                    type(insert_params.get("created_at")).__name__,
                    getattr(insert_params.get("created_at"), "tzinfo", None) if isinstance(insert_params.get("created_at"), datetime) else "N/A",
                    insert_params.get("updated_at"),
                    type(insert_params.get("updated_at")).__name__,
                    getattr(insert_params.get("updated_at"), "tzinfo", None) if isinstance(insert_params.get("updated_at"), datetime) else "N/A",
                )

                await db.execute(text(line_upsert_stmt), insert_params)
                inserted += 1

                await db.execute(text(f"RELEASE SAVEPOINT {savepoint_name}"))

            except Exception as line_error:
                try:
                    await db.execute(text(f"ROLLBACK TO SAVEPOINT {savepoint_name}"))
                except Exception:
                    pass

                logger.error(
                    "[ORDER_LINES_UPSERT_FAIL] order_id=%s sku=%s error=%s",
                    order_id_val,
                    sku,
                    str(line_error)[:200],
                )
                logger.error(
                    "[LINE_ITEM_INSERT_FAILED] order_id=%s line_number=%s sku=%s error=%s",
                    order_id_val,
                    line_number,
                    sku,
                    str(line_error)[:200],
                )
                logger.error(
                    "[LINE_ITEM_INSERT_FAILED_DEBUG] order_id=%s line_number=%s payload=%s",
                    order_id_val,
                    line_number,
                    json.dumps({
                        "sku": sku,
                        "product_name": item.get("product_name"),
                        "quantity": item.get("quantity"),
                        "unit_price": item.get("unit_price"),
                        "total_price": item.get("total_price"),
                        "unit_price_cents": item.get("unit_price_cents"),
                        "total_price_cents": item.get("total_price_cents"),
                        "mapped_product_id": item.get("mapped_product_id"),
                        "mapping_status": item.get("mapping_status"),
                        "mapping_issue": item.get("mapping_issue"),
                        "created_at": str(now_val) if now_val else None,
                        "updated_at": str(now_val) if now_val else None,
                    }, default=str)[:1000],
                )
                failed_items.append((item, str(line_error)[:500]))

        return inserted, skipped, failed_items

    # ------------------------------------------------------------------
    # Aggregate counters across all orders
    # ------------------------------------------------------------------
    total_inserted = 0
    total_skipped = 0
    total_failed = 0
    total_retried = 0
    total_orders = 0

    # ------------------------------------------------------------------
    # First pass: try to upsert line items for orders that exist in DB.
    # Collect orders whose parent row was not found for a retry pass.
    # ------------------------------------------------------------------
    retry_items: list[tuple[str, dict]] = []  # (leaflink_order_id, raw_order_dict)

    for o in orders:
        if not isinstance(o, dict):
            continue

        # Use the same external_id resolution as sync_leaflink_orders_headers_only
        # so the DB lookup always matches the key that was written during Phase 1.
        external_id = safe_str(o.get("id") or o.get("external_id") or o.get("external_order_id"))
        if not external_id:
            continue

        leaflink_order_id = safe_str(external_id)

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    result = await db.execute(
                        select(Order).where(
                            Order.brand_id == brand_id_value,
                            Order.external_order_id == leaflink_order_id,
                        )
                    )
                    order_row = result.scalar_one_or_none()

                    if order_row is None:
                        logger.warning(
                            "[LINE_ITEM_ORDER_LOOKUP_FAIL] leaflink_order_id=%s",
                            leaflink_order_id,
                        )
                        retry_items.append((leaflink_order_id, o))
                        continue

                    logger.info(
                        "[LINE_ITEM_ORDER_LOOKUP] leaflink_order_id=%s matched_order_id=%s",
                        leaflink_order_id,
                        order_row.id,
                    )

                    # Read line items from the DB row (written during Phase 1)
                    db_line_items = order_row.line_items_json
                    if isinstance(db_line_items, list):
                        normalized_line_items = db_line_items
                    else:
                        raw_line_items = o.get("line_items", [])
                        normalized_line_items = normalize_line_items(raw_line_items)

                    if not normalized_line_items:
                        continue

                    order_id_val = order_row.id
                    inserted_count, skipped_count, failed_items = await _upsert_line_items(
                        db, order_row, normalized_line_items
                    )

                    logger.debug(
                        "[LINE_ITEM_UPSERT_COMMIT] brand_id=%s inserted=%s updated=0 duration=0s",
                        brand_id_value,
                        inserted_count,
                    )
                    logger.debug(
                        "[LINE_ITEM_BATCH_RESULT] order_id=%s total=%s inserted=%s skipped=%s failed=%s retried=0",
                        order_id_val,
                        len(normalized_line_items),
                        inserted_count,
                        skipped_count,
                        len(failed_items),
                    )

                    total_inserted += inserted_count
                    total_skipped += skipped_count
                    total_failed += len(failed_items)
                    total_orders += 1

                    # Dead-letter permanently failed items
                    for failed_item, fail_reason in failed_items:
                        await _dead_letter_line_item(
                            brand_id=brand_id_value,
                            external_order_id=leaflink_order_id,
                            order_id=order_id_val,
                            sku=failed_item.get("sku"),
                            product_name=failed_item.get("product_name"),
                            raw_payload=failed_item.get("raw_payload"),
                            failure_reason=fail_reason,
                            failure_count=1,
                        )

        except Exception as line_exc:
            err_msg = str(line_exc)
            logger.error(
                "[OrdersSync] line_items_error brand=%s external_id=%s error=%s",
                brand_id_value,
                external_id,
                err_msg,
                exc_info=True,
            )
            errors.append(err_msg)

    # ------------------------------------------------------------------
    # Retry pass: attempt once more for orders whose parent row was not
    # found during the first pass (they may have been committed by now).
    # Items that still fail after retry are dead-lettered.
    # ------------------------------------------------------------------
    for leaflink_order_id, o in retry_items:
        logger.info("[LINE_ITEM_RETRY] leaflink_order_id=%s", leaflink_order_id)

        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    result = await db.execute(
                        select(Order).where(
                            Order.brand_id == brand_id_value,
                            Order.external_order_id == leaflink_order_id,
                        )
                    )
                    order_row = result.scalar_one_or_none()

                    if order_row is None:
                        logger.warning(
                            "[LINE_ITEM_RETRY_GIVEUP] leaflink_order_id=%s — dead-lettering all items",
                            leaflink_order_id,
                        )
                        # Dead-letter all items for this order since parent was never found
                        raw_line_items = o.get("line_items", [])
                        normalized_line_items = normalize_line_items(raw_line_items)
                        for item in normalized_line_items:
                            await _dead_letter_line_item(
                                brand_id=brand_id_value,
                                external_order_id=leaflink_order_id,
                                order_id=None,
                                sku=item.get("sku"),
                                product_name=item.get("product_name"),
                                raw_payload=item.get("raw_payload"),
                                failure_reason="Parent order not found after retry",
                                failure_count=MAX_LINE_ITEM_RETRIES,
                            )
                        continue

                    logger.info(
                        "[LINE_ITEM_RETRY_SUCCESS] leaflink_order_id=%s matched_order_id=%s",
                        leaflink_order_id,
                        order_row.id,
                    )

                    db_line_items = order_row.line_items_json
                    if isinstance(db_line_items, list):
                        normalized_line_items = db_line_items
                    else:
                        raw_line_items = o.get("line_items", [])
                        normalized_line_items = normalize_line_items(raw_line_items)

                    if not normalized_line_items:
                        continue

                    order_id_val = order_row.id
                    inserted_count, skipped_count, failed_items = await _upsert_line_items(
                        db, order_row, normalized_line_items
                    )

                    logger.debug(
                        "[LINE_ITEM_UPSERT_COMMIT] brand_id=%s inserted=%s updated=0 duration=0s",
                        brand_id_value,
                        inserted_count,
                    )
                    logger.debug(
                        "[LINE_ITEM_BATCH_RESULT] order_id=%s total=%s inserted=%s skipped=%s failed=%s retried=1",
                        order_id_val,
                        len(normalized_line_items),
                        inserted_count,
                        skipped_count,
                        len(failed_items),
                    )

                    total_inserted += inserted_count
                    total_skipped += skipped_count
                    total_failed += len(failed_items)
                    total_retried += 1
                    total_orders += 1

                    # Dead-letter items that still failed after retry
                    for failed_item, fail_reason in failed_items:
                        await _dead_letter_line_item(
                            brand_id=brand_id_value,
                            external_order_id=leaflink_order_id,
                            order_id=order_id_val,
                            sku=failed_item.get("sku"),
                            product_name=failed_item.get("product_name"),
                            raw_payload=failed_item.get("raw_payload"),
                            failure_reason=fail_reason,
                            failure_count=MAX_LINE_ITEM_RETRIES,
                        )

        except Exception as retry_exc:
            err_msg = str(retry_exc)
            logger.error(
                "[OrdersSync] line_items_retry_error brand=%s external_id=%s error=%s",
                brand_id_value,
                leaflink_order_id,
                err_msg,
                exc_info=True,
            )
            errors.append(err_msg)

    bg_duration = round(time.monotonic() - bg_start, 2)

    logger.info(
        "[SYNC_PHASE_2_COMPLETE] brand_id=%s orders_processed=%s line_items_total=%s inserted=%s skipped=%s failed=%s retried=%s duration=%ss",
        brand_id_value,
        total_orders,
        total_inserted + total_skipped + total_failed,
        total_inserted,
        total_skipped,
        total_failed,
        total_retried,
        bg_duration,
    )
    logger.info(
        "[SYNC_COMPLETE] brand_id=%s orders_created=0 orders_updated=%s line_items_inserted=%s line_items_failed=%s retryable_errors=0 duration=%ss",
        brand_id_value,
        total_orders,
        total_inserted,
        total_failed,
        bg_duration,
    )
    logger.info(
        "[OrdersSync] line_items_complete brand=%s lines_written=%s duration=%ss errors=%s",
        brand_id_value,
        total_inserted,
        bg_duration,
        len(errors),
    )
    logger.debug(
        "[UUID_NORMALIZATION_COMPLETE] brand_id=%s orders_processed=%s",
        brand_id_value,
        total_orders,
    )

    # Update sync health after Phase 2 completes
    if len(errors) == 0:
        await _update_sync_health_phase2(
            brand_id=brand_id_value,
            line_items_count=(total_inserted or 0),
        )
    else:
        await _record_sync_error(
            brand_id=brand_id_value,
            error=Exception(f"Phase 2 completed with {len(errors)} errors"),
        )

    return {
        "ok": len(errors) == 0,
        "lines_written": total_inserted,
        "errors": errors,
        "duration_seconds": bg_duration,
    }


async def upsert_sync_metrics_snapshot(
    brand_id: str,
    sync_run_id: Optional[int] = None,
    pages_processed: Optional[int] = None,
    records_processed: Optional[int] = None,
    sync_rate: Optional[float] = None,
    estimated_completion: Optional[datetime] = None,
    last_successful_sync_at: Optional[datetime] = None,
) -> None:
    """Write a lightweight sync metrics snapshot for a brand.

    Called after each batch of records during sync so that /sync-metrics,
    /sync-status, and /runtime-health can read cached values instead of
    performing full DB scans inline.

    Uses a raw SQL UPSERT (INSERT … ON CONFLICT DO UPDATE) so the operation
    is a single round-trip and never blocks the sync loop for more than a
    few milliseconds.

    Order counts (total_local_orders, total_ok, total_partial, total_failed)
    and dead_letter_count are computed here from the DB so the snapshot is
    always accurate at write time.  These counts are cheap because they use
    indexed columns.
    """
    try:
        from sqlalchemy import func as _func_snap, text as _text_snap
        from models import Order as _Order

        now = datetime.now(timezone.utc)
        _run_id_str = str(sync_run_id) if sync_run_id is not None else None

        async with AsyncSessionLocal() as _snap_db:
            # Gather counts in a single session (no transaction needed for reads)
            try:
                _total_res = await _snap_db.execute(
                    select(_func_snap.count(_Order.id)).where(_Order.brand_id == brand_id)
                )
                total_local = _total_res.scalar() or 0
            except Exception:
                total_local = None

            try:
                _ok_res = await _snap_db.execute(
                    select(_func_snap.count(_Order.id)).where(
                        _Order.brand_id == brand_id,
                        _Order.sync_status == "ok",
                    )
                )
                total_ok = _ok_res.scalar() or 0
            except Exception:
                total_ok = None

            try:
                _partial_res = await _snap_db.execute(
                    select(_func_snap.count(_Order.id)).where(
                        _Order.brand_id == brand_id,
                        _Order.sync_status == "partial",
                    )
                )
                total_partial = _partial_res.scalar() or 0
            except Exception:
                total_partial = None

            try:
                _failed_res = await _snap_db.execute(
                    select(_func_snap.count(_Order.id)).where(
                        _Order.brand_id == brand_id,
                        _Order.sync_status == "failed",
                    )
                )
                total_failed = _failed_res.scalar() or 0
            except Exception:
                total_failed = None

            try:
                _dl_res = await _snap_db.execute(
                    _text_snap(
                        "SELECT COUNT(*) FROM sync_dead_letters "
                        "WHERE brand_id = CAST(:brand_id AS uuid) AND resolved_at IS NULL"
                    ),
                    {"brand_id": brand_id},
                )
                dead_letter_count = _dl_res.scalar() or 0
            except Exception:
                dead_letter_count = None

            # Upsert the snapshot row
            try:
                import json as _json_snap
                _cat_json = None  # category breakdown not computed here (expensive)

                await _snap_db.execute(
                    _text_snap("""
                        INSERT INTO sync_metrics_snapshots
                            (brand_id, sync_run_id,
                             total_local_orders, total_ok, total_partial, total_failed,
                             dead_letter_count, count_by_failure_category,
                             pages_processed, records_processed,
                             sync_rate, estimated_completion,
                             last_successful_sync_at, updated_at)
                        VALUES
                            (:brand_id, :sync_run_id,
                             :total_local_orders, :total_ok, :total_partial, :total_failed,
                             :dead_letter_count, CAST(:count_by_failure_category AS jsonb),
                             :pages_processed, :records_processed,
                             :sync_rate, :estimated_completion,
                             :last_successful_sync_at, :updated_at)
                        ON CONFLICT (brand_id, sync_run_id) DO UPDATE SET
                            total_local_orders      = EXCLUDED.total_local_orders,
                            total_ok                = EXCLUDED.total_ok,
                            total_partial           = EXCLUDED.total_partial,
                            total_failed            = EXCLUDED.total_failed,
                            dead_letter_count       = EXCLUDED.dead_letter_count,
                            pages_processed         = EXCLUDED.pages_processed,
                            records_processed       = EXCLUDED.records_processed,
                            sync_rate               = EXCLUDED.sync_rate,
                            estimated_completion    = EXCLUDED.estimated_completion,
                            last_successful_sync_at = COALESCE(EXCLUDED.last_successful_sync_at,
                                                               sync_metrics_snapshots.last_successful_sync_at),
                            updated_at              = EXCLUDED.updated_at
                    """),
                    {
                        "brand_id": brand_id,
                        "sync_run_id": _run_id_str,
                        "total_local_orders": total_local,
                        "total_ok": total_ok,
                        "total_partial": total_partial,
                        "total_failed": total_failed,
                        "dead_letter_count": dead_letter_count,
                        "count_by_failure_category": _cat_json,
                        "pages_processed": pages_processed,
                        "records_processed": records_processed,
                        "sync_rate": sync_rate,
                        "estimated_completion": estimated_completion,
                        "last_successful_sync_at": last_successful_sync_at,
                        "updated_at": now,
                    },
                )
                await _snap_db.commit()
                logger.info(
                    "[SyncMetricsSnapshot] upserted brand_id=%s run_id=%s"
                    " total=%s ok=%s partial=%s failed=%s dl=%s pages=%s records=%s",
                    brand_id,
                    _run_id_str,
                    total_local,
                    total_ok,
                    total_partial,
                    total_failed,
                    dead_letter_count,
                    pages_processed,
                    records_processed,
                )
            except Exception as upsert_exc:
                logger.warning(
                    "[SyncMetricsSnapshot] upsert_error brand_id=%s error=%s",
                    brand_id,
                    str(upsert_exc)[:200],
                )
    except Exception as outer_exc:
        # Never let snapshot writes crash the sync loop
        logger.warning(
            "[SyncMetricsSnapshot] outer_error brand_id=%s error=%s",
            brand_id,
            str(outer_exc)[:200],
        )


async def sync_leaflink_full_resync(
    brand_id: str,
    api_key: str,
    company_id: str,
    auth_scheme: str = "Token",
    base_url: Optional[str] = None,
    org_id: Optional[str] = None,
    sync_run_id: Optional[int] = None,
) -> dict[str, Any]:
    """Full backfill/resync of all LeafLink orders for a brand.

    Walks every page from LeafLink until all available orders are stored locally.
    Uses the same pagination loop as sync_leaflink_background_continuous but
    runs synchronously (awaited) and returns final counts.

    This is the correct function to call for a full resync — it will fetch
    ALL pages, not just the first batch.

    Args:
        brand_id:    Brand UUID.
        api_key:     LeafLink API key.
        company_id:  LeafLink company ID.
        auth_scheme: Auth scheme (Token, Bearer, Raw).
        base_url:    LeafLink base URL.
        org_id:      Organization UUID for multi-tenant isolation.
        sync_run_id: Optional SyncRun.id for progress tracking.

    Returns:
        dict with total_fetched, total_inserted, total_updated, failed_count,
        pages_synced, duration_seconds, total_local_orders.
    """
    from services.leaflink_client import LeafLinkClient

    resync_start = time.monotonic()
    total_fetched = 0
    total_inserted = 0
    total_updated = 0
    failed_count = 0
    pages_synced = 0
    errors: list[str] = []

    logger.info(
        "[LEAFLINK_SYNC_DEBUG] full_resync_start brand_id=%s api_key_len=%s"
        " auth_scheme=%s base_url=%s org_id=%s",
        brand_id,
        len(api_key) if api_key else 0,
        auth_scheme,
        base_url or "canonical",
        org_id,
    )
    logger.info(
        "[ORG_CONTEXT] stage=full_resync_start org_id=%s brand_id=%s"
        " external_order_id=N/A",
        org_id,
        brand_id,
    )

    try:
        client = LeafLinkClient(
            api_key=api_key,
            company_id=company_id,
            brand_id=brand_id,
            auth_scheme=auth_scheme,
            base_url=base_url,
        )
    except Exception as client_exc:
        logger.error(
            "[LEAFLINK_SYNC_DEBUG] full_resync_client_init_error brand_id=%s error=%s",
            brand_id,
            client_exc,
        )
        return {
            "ok": False,
            "error": str(client_exc)[:300],
            "total_fetched": 0,
            "total_inserted": 0,
            "total_updated": 0,
            "failed_count": 0,
            "pages_synced": 0,
            "duration_seconds": 0,
            "total_local_orders": 0,
        }

    loop = asyncio.get_event_loop()
    current_page = 1
    resume_url: Optional[str] = None
    page_size = 100

    while True:
        try:
            _capture_page = current_page
            _capture_url = resume_url
            fetch_result = await loop.run_in_executor(
                _leaflink_executor,
                lambda: client.fetch_orders_page_range(
                    start_page=_capture_page,
                    num_pages=1,
                    page_size=page_size,
                    normalize=True,
                    brand=brand_id,
                    resume_url=_capture_url,
                ),
            )
        except Exception as fetch_exc:
            err_msg = str(fetch_exc)[:300]
            logger.error(
                "[LEAFLINK_SYNC_DEBUG] full_resync_fetch_error brand_id=%s page=%s error=%s",
                brand_id,
                current_page,
                err_msg,
            )
            errors.append(f"page={current_page}: {err_msg}")
            break

        batch_orders = fetch_result.get("orders", [])
        next_url = fetch_result.get("next_url")
        next_page = fetch_result.get("next_page")
        total_count_from_api = fetch_result.get("total_count")
        pages_fetched_this_batch = fetch_result.get("pages_fetched", 1)


        logger.debug(
            "[LEAFLINK_SYNC_DEBUG] page=%s count=%s running_total=%s has_next=%s"
            " total_available=%s brand_id=%s",
            current_page,
            len(batch_orders),
            total_fetched + len(batch_orders),
            "true" if next_url else "false",
            total_count_from_api or "unknown",
            brand_id,
        )

        if batch_orders:
            try:
                persist_result = await sync_leaflink_orders_headers_only(
                    brand_id=brand_id,
                    orders=batch_orders,
                    pages_fetched=pages_fetched_this_batch,
                    org_id=org_id,
                )
                _inserted = persist_result.get("inserted_count", persist_result.get("created", 0))
                _updated = persist_result.get("updated_count", persist_result.get("updated", 0))
                _errors = persist_result.get("error_count", 0)
                total_inserted += _inserted
                total_updated += _updated
                failed_count += _errors

                logger.debug(
                    "[LEAFLINK_SYNC_DEBUG] page=%s count=%s running_total=%s"
                    " inserted=%s updated=%s errors=%s brand_id=%s",
                    current_page,
                    len(batch_orders),
                    total_fetched + len(batch_orders),
                    _inserted,
                    _updated,
                    _errors,
                    brand_id,
                )
            except Exception as upsert_exc:
                err_msg = str(upsert_exc)[:300]
                logger.error(
                    "[LEAFLINK_SYNC_DEBUG] full_resync_upsert_error brand_id=%s page=%s error=%s",
                    brand_id,
                    current_page,
                    err_msg,
                )
                errors.append(f"upsert page={current_page}: {err_msg}")

        total_fetched += len(batch_orders)
        pages_synced += pages_fetched_this_batch

        # Update SyncRun progress if we have a run ID
        if sync_run_id:
            try:
                from services.sync_run_manager import update_progress as _srm_update_progress
                async with AsyncSessionLocal() as _prog_db:
                    async with _prog_db.begin():
                        await _srm_update_progress(
                            _prog_db,
                            sync_run_id=sync_run_id,
                            pages_synced=pages_synced,
                            orders_loaded=total_fetched,
                            cursor=next_url,
                            page=current_page,
                            total_pages=None,
                            total_orders_available=total_count_from_api,
                        )
            except Exception as prog_exc:
                logger.warning(
                    "[LEAFLINK_SYNC_DEBUG] full_resync_progress_error run_id=%s error=%s",
                    sync_run_id,
                    prog_exc,
                )

        # Write metrics snapshot after each batch so endpoints can read cached values
        # Fire-and-forget: never let snapshot errors block the sync loop
        try:
            _elapsed = time.monotonic() - resync_start
            _snap_rate = round(total_fetched / _elapsed, 1) if _elapsed > 0 and total_fetched > 0 else None
            asyncio.create_task(
                upsert_sync_metrics_snapshot(
                    brand_id=brand_id,
                    sync_run_id=sync_run_id,
                    pages_processed=pages_synced,
                    records_processed=total_fetched,
                    sync_rate=_snap_rate,
                )
            )
        except Exception:
            pass  # snapshot is best-effort

        # Stop if no more pages
        if not next_url or not batch_orders:
            logger.info(
                "[LEAFLINK_SYNC_DEBUG] full_resync_pagination_complete brand_id=%s"
                " pages=%s total_fetched=%s",
                brand_id,
                pages_synced,
                total_fetched,
            )
            break

        resume_url = next_url
        if next_page:
            current_page = next_page
        else:
            current_page += 1

    duration = round(time.monotonic() - resync_start, 2)

    # Get final local order count
    try:
        from sqlalchemy import func as _func_resync
        async with AsyncSessionLocal() as _count_db:
            _count_res = await _count_db.execute(
                select(_func_resync.count()).select_from(Order).where(Order.brand_id == brand_id)
            )
            total_local_orders = _count_res.scalar() or 0
    except Exception:
        total_local_orders = total_fetched

    logger.info(
        "[LEAFLINK_SYNC_DEBUG] full_resync_complete brand_id=%s total_fetched=%s"
        " inserted=%s updated=%s failed=%s pages=%s duration=%ss total_local=%s",
        brand_id,
        total_fetched,
        total_inserted,
        total_updated,
        failed_count,
        pages_synced,
        duration,
        total_local_orders,
    )

    # Write final snapshot marking sync as complete
    try:
        _final_rate = round(total_fetched / duration, 1) if duration > 0 and total_fetched > 0 else None
        _last_sync_at = datetime.now(timezone.utc)
        asyncio.create_task(
            upsert_sync_metrics_snapshot(
                brand_id=brand_id,
                sync_run_id=sync_run_id,
                pages_processed=pages_synced,
                records_processed=total_fetched,
                sync_rate=_final_rate,
                last_successful_sync_at=_last_sync_at if len(errors) == 0 else None,
            )
        )
    except Exception:
        pass  # snapshot is best-effort

    return {
        "ok": len(errors) == 0,
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "failed_count": failed_count,
        "pages_synced": pages_synced,
        "duration_seconds": duration,
        "total_local_orders": total_local_orders,
        "errors": errors[:10],
    }


# ---------------------------------------------------------------------------
# Background continuous sync (Phase 2)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Safe-mode configuration — reduce resource usage during instability
# ---------------------------------------------------------------------------

# Safe-mode: smaller batches, sequential line item processing
SAFE_MODE_BATCH_SIZE = 25  # Down from 100
SAFE_MODE_SEQUENTIAL_LINE_ITEMS = True  # No concurrent processing

# Batch size bounds for adaptive pacing
_BG_BATCH_MIN = 3
_BG_BATCH_MAX = 5
_BG_BATCH_DEFAULT = 4

# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60
BACKOFF_MULTIPLIER = 2.0

# Transient error codes (retry these)
TRANSIENT_ERROR_CODES = {429, 502, 503, 504}

# Thresholds (seconds) for adaptive batch sizing
_BG_BATCH_FAST_THRESHOLD = 5.0   # < 5 s → increase batch size
_BG_BATCH_SLOW_THRESHOLD = 15.0  # > 15 s → decrease batch size

# Hard timeout for the entire background sync run (30 minutes)
_BG_SYNC_TIMEOUT = 1800


async def sync_leaflink_background_continuous(
    brand_id: str,
    api_key: str,
    company_id: str,
    start_page: int,
    total_pages: Optional[int],
    manager: Optional["BackgroundSyncManager"] = None,
    total_orders_available: Optional[int] = None,
    sync_run_id: Optional[int] = None,
    auth_scheme: str = "Token",
    base_url: Optional[str] = None,
    org_id: Optional[str] = None,
    last_next_url: Optional[str] = None,
) -> None:
    """
    Fetch all remaining LeafLink pages in adaptive batches and upsert to DB.

    Designed to run as a fire-and-forget asyncio task after Phase 1 completes.
    Progress is persisted exclusively to the SyncRun table after every page so
    the sync can resume from the last completed page if the service restarts.

    Batch size adapts based on observed fetch latency:
      - < 5 s per batch  → increase to _BG_BATCH_MAX (5 pages)
      - > 15 s per batch → decrease to _BG_BATCH_MIN (3 pages)
      - otherwise        → keep at _BG_BATCH_DEFAULT (4 pages)

    Cursor-loop detection: if the same cursor is returned twice in a row the
    run is marked stalled with reason=cursor_loop_detected.

    Args:
        brand_id:               Brand UUID.
        api_key:                LeafLink API key (from DB credential).
        company_id:             LeafLink company ID (from DB credential).
        start_page:             First page to fetch (Phase 1 already fetched pages before this).
        total_pages:            Total pages reported by LeafLink API, or None for cursor-based pagination.
        manager:                Optional BackgroundSyncManager instance for in-memory tracking.
        total_orders_available: Total orders reported by LeafLink (for progress display).
        sync_run_id:            Optional SyncRun.id to persist progress against.
        base_url:               LeafLink base URL (from DB credential, required).
        org_id:                 Organization UUID for multi-tenant isolation. MUST be set so
                                orders are inserted with the correct org_id and are visible
                                through the GET /orders endpoint which filters by org_id.
        last_next_url:          LeafLink cursor URL to resume from (from a previous interrupted run).
    """
    from services.leaflink_client import LeafLinkClient
    from services.sync_run_manager import (
        update_progress as _srm_update_progress,
        mark_completed as _srm_mark_completed,
        mark_stalled as _srm_mark_stalled,
        mark_failed as _srm_mark_failed,
    )

    # Startup self-test: validate datetime normalization
    try:
        _test_naive = datetime(2026, 5, 8, 12, 0, 0)
        _test_aware = datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc)
        _test_string = "2026-05-08T12:00:00Z"
        _test_date = date(2026, 5, 8)

        assert ensure_utc_datetime(_test_naive).tzinfo is not None
        assert ensure_utc_datetime(_test_aware).tzinfo is not None
        assert ensure_utc_datetime(_test_string).tzinfo is not None
        assert ensure_utc_datetime(_test_date).tzinfo is not None
        assert ensure_utc_datetime(None) is None

        logger.info("[DATETIME_SELF_TEST_OK] all normalization paths validated")
    except Exception as _test_exc:
        logger.error("[DATETIME_SELF_TEST_FAILED] error=%s", str(_test_exc)[:200])
        raise

    # [SYNC_ENTRY] Log entry into the background continuous sync function
    logger.info(
        "[SYNC_ENTRY] sync_leaflink_background_continuous entered brand_id=%s"
        " start_page=%s total_pages=%s sync_run_id=%s org_id=%s resuming=%s",
        brand_id,
        start_page,
        total_pages if total_pages is not None else "cursor_based",
        sync_run_id,
        org_id,
        last_next_url is not None,
    )
    if org_id is None:
        logger.warning(
            "[ORG_CONTEXT] stage=bg_continuous_entry org_id=MISSING brand_id=%s"
            " — orders will be blocked at insert time (missing_org_context)",
            brand_id,
        )
    else:
        logger.debug(
            "[ORG_CONTEXT] stage=bg_continuous_entry org_id=%s brand_id=%s"
            " external_order_id=N/A",
            org_id,
            brand_id,
        )

    logger.info(
        "[SYNC_SESSION_START] run_id=%s brand_id=%s task_id=%s",
        sync_run_id,
        brand_id,
        id(asyncio.current_task()),
    )
    # Initialize tracking variables before try block so exception handlers can reference them
    pages_processed = 0
    records_seen_from_leaflink = 0
    next_cursor: Optional[str] = last_next_url

    # ---------------------------------------------------------------------------
    # Watchdog heartbeat — logs every 30s to confirm event loop is alive
    # ---------------------------------------------------------------------------
    async def _heartbeat_task() -> None:
        while True:
            try:
                await asyncio.sleep(30)
                logger.info(
                    "[SYNC_HEARTBEAT] run_id=%s page=%s records_seen=%s "
                    "last_progress_at=%s event_loop_alive=true",
                    sync_run_id,
                    pages_processed,
                    records_seen_from_leaflink,
                    datetime.now(timezone.utc).isoformat(),
                )
            except asyncio.CancelledError:
                break
            except Exception as _hb_exc:
                logger.warning("[HEARTBEAT_ERROR] error=%s", str(_hb_exc)[:200])

    _heartbeat = asyncio.create_task(_heartbeat_task())

    try:
        bg_start = time.monotonic()
        current_page = start_page
        batch_size = _BG_BATCH_DEFAULT
        total_orders_synced = 0
        # Resume from last_next_url if provided (worker restart recovery)
        resume_url: Optional[str] = last_next_url
        _prev_cursor: Optional[str] = None  # for cursor-loop detection
        # Pagination tracking for [SYNC_PAGE_SUMMARY] and completion logic
        last_progress_at = datetime.now(timezone.utc)
        leaflink_total_count: Optional[int] = total_orders_available

        async def _persist_run_progress(
            pages_synced: int,
            orders_loaded: int,
            cursor: Optional[str],
            page: int,
            total_pages_val: Optional[int] = None,
            total_orders_available: Optional[int] = None,
        ) -> None:
            """Persist progress to SyncRun (if sync_run_id set) and BackgroundSyncManager."""
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _prog_db:
                        async with _prog_db.begin():
                            await _srm_update_progress(
                                _prog_db,
                                sync_run_id=sync_run_id,
                                pages_synced=pages_synced,
                                orders_loaded=orders_loaded,
                                cursor=cursor,
                                page=page,
                                total_pages=total_pages_val,
                                total_orders_available=total_orders_available,
                            )
                except Exception as _prog_exc:
                    logger.error(
                        "[OrdersSync] sync_run_progress_persist_error run_id=%s error=%s",
                        sync_run_id,
                        _prog_exc,
                    )

        try:
            client = LeafLinkClient(
                api_key=api_key,
                company_id=company_id,
                brand_id=brand_id,
                auth_scheme=auth_scheme,
                base_url=base_url,
            )
        except Exception as client_exc:
            logger.error(
                "[OrdersSync] bg_sync_client_init_error brand=%s error=%s",
                brand_id,
                client_exc,
            )
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _fail_db:
                        async with _fail_db.begin():
                            await _srm_mark_failed(_fail_db, sync_run_id, str(client_exc))
                except Exception:
                    pass
            return

        # [LEAFLINK_COMPANY_ROLE] Log company_id at sync start for role verification diagnostics
        logger.info(
            "[LEAFLINK_COMPANY_ROLE] company_id=%s credential_company_id=%s brand_id=%s note=verify_company_id_role_in_leaflink_account",
            company_id,
            company_id,
            brand_id,
        )

        # [LEAFLINK_SYNC_START] Log sync start with all relevant parameters
        logger.info(
            "[LEAFLINK_SYNC_START] brand_id=%s company_id=%s start_page=%s"
            " total_pages=%s total_orders_available=%s",
            brand_id,
            company_id or "none",
            start_page,
            total_pages or "unknown",
            total_orders_available or "unknown",
        )

        loop = asyncio.get_event_loop()

        async def _fetch_with_retry(
            start_page: int,
            num_pages: int,
            max_retries: int = MAX_RETRIES,
        ) -> dict:
            """Fetch pages with exponential backoff retry on transient errors."""

            backoff_seconds = INITIAL_BACKOFF_SECONDS
            last_error = None

            for attempt in range(max_retries + 1):
                try:
                    _capture_page = start_page
                    _capture_url = resume_url
                    _capture_num = num_pages

                    result = await loop.run_in_executor(
                        _leaflink_executor,
                        lambda: client.fetch_orders_page_range(
                            start_page=_capture_page,
                            num_pages=_capture_num,
                            page_size=HEADER_BATCH_SIZE * 4,  # 100 orders per page
                            normalize=True,
                            brand=brand_id,
                            resume_url=_capture_url,
                        ),
                    )

                    return result

                except Exception as fetch_exc:
                    last_error = fetch_exc
                    error_code = getattr(fetch_exc, "status_code", None)

                    # Network-level errors (DNS, connection reset, timeout) are always retryable
                    is_network_error = isinstance(fetch_exc, (ConnectionError, OSError, TimeoutError))
                    # HTTP-level transient errors
                    is_transient = is_network_error or (error_code in TRANSIENT_ERROR_CODES)

                    if not is_transient or attempt >= max_retries:
                        # Not transient or out of retries — give up
                        logger.error(
                            "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=fetch_orders_page_range error=%s max_retries_exceeded",
                            type(fetch_exc).__name__,
                        )
                        logger.error(
                            "[SYNC_RETRYABLE_ERROR] brand_id=%s error=%s",
                            brand_id,
                            type(fetch_exc).__name__,
                        )
                        raise

                    # Transient error — retry with backoff
                    import random as _random
                    jitter = _random.uniform(0, backoff_seconds * 0.1)
                    backoff_total = round(backoff_seconds + jitter, 2)
                    logger.warning(
                        "[LEAFLINK_REQUEST_RETRY] method=GET endpoint=fetch_orders_page_range error=%s attempt=%s backoff=%ss",
                        type(fetch_exc).__name__,
                        attempt + 1,
                        backoff_total,
                    )

                    await asyncio.sleep(backoff_total)
                    backoff_seconds = min(backoff_seconds * BACKOFF_MULTIPLIER, MAX_BACKOFF_SECONDS)

            raise last_error

        # [SYNC_LOOP_START] Log entry into the main pagination loop
        logger.info(
            "[SYNC_LOOP_START] brand_id=%s start_page=%s total_pages=%s batch_size=%s sync_run_id=%s",
            brand_id,
            start_page,
            total_pages if total_pages is not None else "cursor_based",
            batch_size,
            sync_run_id,
        )
        logger.info(
            "[LEAFLINK_SYNC_DEBUG] sync_start brand_id=%s api_key_len=%s auth_scheme=%s"
            " base_url=%s start_page=%s total_pages=%s",
            brand_id,
            len(api_key) if api_key else 0,
            auth_scheme,
            base_url or "canonical",
            start_page,
            total_pages if total_pages is not None else "cursor_based",
        )

        # When total_pages is None (cursor-based pagination), loop until the API
        # returns no next_url. When total_pages is known, also enforce the page bound.
        while total_pages is None or current_page <= total_pages:
            # Hard timeout guard
            elapsed_total = time.monotonic() - bg_start
            if elapsed_total > _BG_SYNC_TIMEOUT:
                pages_done = current_page - start_page
                pages_total = (total_pages - start_page + 1) if total_pages is not None else "?"
                logger.warning(
                    "[OrdersSync] bg_sync_timeout brand=%s elapsed=%.1fs pages_done=%s/%s",
                    brand_id,
                    elapsed_total,
                    pages_done,
                    pages_total,
                )
                break

            batch_start = time.monotonic()

            # ------------------------------------------------------------------ #
            # Fetch a batch of pages from LeafLink (with retry on transient err) #
            # ------------------------------------------------------------------ #

            # No-progress detection
            if (datetime.now(timezone.utc) - last_progress_at).total_seconds() > 30:
                logger.warning(
                    "[NO_PROGRESS] page=%s last_progress=%s",
                    pages_processed,
                    last_progress_at.isoformat(),
                )

            try:
                fetch_start = time.monotonic()
                try:
                    fetch_result = await asyncio.wait_for(
                        _fetch_with_retry(
                            start_page=current_page,
                            num_pages=batch_size,
                        ),
                        timeout=60,
                    )
                except asyncio.TimeoutError:
                    logger.error("[FETCH_TIMEOUT] page=%s", pages_processed)
                    break
                fetch_duration = time.monotonic() - fetch_start
                if fetch_duration > 5:
                    logger.warning(
                        "[SLOW_FETCH] page=%s duration=%.1fs",
                        pages_processed,
                        fetch_duration,
                    )
            except Exception as fetch_exc:
                error_code = getattr(fetch_exc, "status_code", None)
                is_transient = error_code in TRANSIENT_ERROR_CODES
                _err_str = str(fetch_exc)

                if is_transient:
                    # Transient error exhausted all retries — mark as paused, not failed
                    logger.warning(
                        "[OrdersSync] sync_paused_transient_error brand=%s error_code=%s error=%s",
                        brand_id,
                        error_code,
                        fetch_exc,
                    )

                    if sync_run_id:
                        try:
                            async with AsyncSessionLocal() as _stall_db:
                                async with _stall_db.begin():
                                    await _srm_mark_stalled(
                                        _stall_db,
                                        sync_run_id,
                                        f"transient_error_http_{error_code}",
                                    )
                        except Exception:
                            pass

                    # Return gracefully — worker will retry on next poll
                    return
                else:
                    # Permanent error — detect auth failures specifically before propagating
                    _err_lower = _err_str.lower()
                    _is_auth_failure = (
                        "status=401" in _err_lower
                        or "auth failed" in _err_lower
                        or "authentication failed" in _err_lower
                        or "status=403" in _err_lower
                        or "forbidden" in _err_lower
                        or "invalid_token" in _err_lower
                    )
                    if "status=401" in _err_lower or "auth failed" in _err_lower or "authentication failed" in _err_lower:
                        logger.error(
                            "[LeafLinkSync] auth_failed id=%s status=401 error=%s",
                            sync_run_id,
                            _err_str[:300],
                        )
                        _err_str = f"LeafLink authentication failed (401): {_err_str[:300]}"
                    elif "status=403" in _err_lower or "forbidden" in _err_lower or "invalid_token" in _err_lower:
                        logger.error(
                            "[LeafLinkSync] forbidden id=%s status=403 error=%s",
                            sync_run_id,
                            _err_str[:300],
                        )
                        _err_str = f"LeafLink access forbidden (403): {_err_str[:300]}"
                    else:
                        logger.error(
                            "[LeafLinkSync] api_error id=%s error=%s",
                            sync_run_id,
                            _err_str[:500],
                        )

                    # Circuit breaker: mark brand credential as auth_failed to stop retries
                    if _is_auth_failure:
                        logger.error(
                            "[LEAFLINK_AUTH_CIRCUIT_BREAKER] brand_id=%s marking_auth_failed"
                            " — stopping retries. Check API key in brand_api_credentials.",
                            brand_id,
                        )
                        try:
                            async with AsyncSessionLocal() as _auth_db:
                                async with _auth_db.begin():
                                    await _auth_db.execute(
                                        text("""
                                            UPDATE brand_api_credentials
                                            SET sync_status = 'auth_failed',
                                                last_error = :error,
                                                updated_at = NOW()
                                            WHERE brand_id = CAST(:brand_id AS uuid)
                                              AND integration_name = 'leaflink'
                                              AND is_active = true
                                        """),
                                        apply_uuid_str_to_params({
                                            "brand_id": safe_uuid_for_db(brand_id, "brand_id") or brand_id,
                                            "error": _err_str[:500],
                                        }),
                                    )
                        except Exception as _auth_cb_exc:
                            logger.error(
                                "[LEAFLINK_AUTH_CIRCUIT_BREAKER] failed_to_mark_auth_failed brand_id=%s error=%s",
                                brand_id,
                                str(_auth_cb_exc)[:200],
                            )

                    logger.error(
                        "[OrdersSync] sync_failed_permanent_error brand=%s error=%s",
                        brand_id,
                        fetch_exc,
                        exc_info=True,
                    )

                    if sync_run_id:
                        try:
                            async with AsyncSessionLocal() as _fail_db2:
                                async with _fail_db2.begin():
                                    await _srm_mark_failed(_fail_db2, sync_run_id, _err_str[:500])
                        except Exception:
                            pass

                    raise

            # Log raw page-1 response structure BEFORE any extraction
            if pages_processed == 0:  # First page only
                _response_keys = list(fetch_result.keys()) if isinstance(fetch_result, dict) else []
                _count_val = fetch_result.get("total_count") if isinstance(fetch_result, dict) else None
                _next_val = fetch_result.get("next_url") if isinstance(fetch_result, dict) else None
                _results_len = len(fetch_result.get("orders", [])) if isinstance(fetch_result, dict) else 0

                logger.debug(
                    "[PAGE1_RESPONSE_KEYS] top_level_keys=%s count=%s next_present=%s next_value=%s results_len=%s response_type=%s",
                    _response_keys,
                    _count_val,
                    _next_val is not None,
                    _next_val[:50] if _next_val else None,
                    _results_len,
                    type(fetch_result).__name__,
                )

            batch_orders = fetch_result.get("orders", [])
            pages_fetched_this_batch = fetch_result.get("pages_fetched", batch_size)
            next_cursor = fetch_result.get("next_url")
            next_page = fetch_result.get("next_page")
            _total_count_from_api = fetch_result.get("total_count")

            # Update pagination tracking state
            if _total_count_from_api is not None:
                leaflink_total_count = _total_count_from_api

            # [PAGINATION_EXTRACT] Log extracted pagination fields after every page (debug only)
            logger.debug(
                "[PAGINATION_EXTRACT] page=%s count=%s next_present=%s next_value=%s results_len=%s",
                pages_processed,
                leaflink_total_count,
                next_cursor is not None,
                next_cursor[:50] if next_cursor else None,
                len(batch_orders),
            )

            # [SYNC_PAGE_FETCHED] Log raw and parsed counts for this batch (debug only)
            _orders_count = len(batch_orders)
            _resp_type = "list" if isinstance(batch_orders, list) else type(batch_orders).__name__
            logger.debug(
                "[SYNC_PAGE_FETCHED] page=%s raw_count=%s parsed_count=%s pages_in_batch=%s brand_id=%s",
                current_page,
                _orders_count,
                _orders_count,
                pages_fetched_this_batch,
                brand_id,
            )
            # [LEAFLINK_SYNC_DEBUG] Structured debug log per page (debug only)
            logger.debug(
                "[LEAFLINK_SYNC_DEBUG] page=%s count=%s running_total=%s has_next=%s"
                " total_available=%s brand_id=%s",
                current_page,
                _orders_count,
                total_orders_synced + _orders_count,
                "true" if next_cursor else "false",
                _total_count_from_api or "unknown",
                brand_id,
            )

            # [SYNC_FILTER_DISABLED] debug only — suppress from production logs
            logger.debug(
                "[SYNC_FILTER_DISABLED] filter_type=status reason=debug_mode brand_id=%s page=%s",
                brand_id,
                current_page,
            )
            logger.debug(
                "[SYNC_FILTER_DISABLED] filter_type=freshness reason=provider_dates_unreliable brand_id=%s page=%s",
                brand_id,
                current_page,
            )
            logger.debug(
                "[SYNC_FILTER_DISABLED] filter_type=date reason=provider_dates_unreliable brand_id=%s page=%s",
                brand_id,
                current_page,
            )

            # [SYNC_AFTER_FILTERS] debug only
            logger.debug(
                "[SYNC_AFTER_FILTERS] remaining=%s reason=no_filters_applied page=%s brand_id=%s",
                _orders_count,
                current_page,
                brand_id,
            )

            # [LEAFLINK_SYNC_RESPONSE] debug only
            logger.debug(
                "[LEAFLINK_SYNC_RESPONSE] brand_id=%s orders_count=%s response_type=%s"
                " pages_fetched=%s next_cursor=%s",
                brand_id,
                _orders_count,
                _resp_type,
                pages_fetched_this_batch,
                "present" if next_cursor else "none",
            )

            # [SYNC_PAGINATION] debug only
            logger.debug(
                "[SYNC_PAGINATION] page=%s has_next=%s next_url=%s brand_id=%s",
                current_page,
                "true" if next_cursor else "false",
                next_cursor[:80] if next_cursor else "none",
                brand_id,
            )

            # [LEAFLINK_SYNC_ZERO_ORDERS] Explicitly flag when API returns zero orders
            if _orders_count == 0:
                logger.warning(
                    "[LEAFLINK_SYNC_ZERO_ORDERS] brand_id=%s reason=api_returned_empty"
                    " page=%s company_id=%s",
                    brand_id,
                    current_page,
                    company_id or "none",
                )

            # [LEAFLINK_ENDPOINT_COMPARE] If first batch returns zero orders, run endpoint comparison
            # to diagnose whether the endpoint or company_id is the issue.
            if not batch_orders and current_page == start_page:
                logger.warning(
                    "[LEAFLINK_EMPTY_RESULT] brand_id=%s company_id=%s page=%s pages_fetched=%s — zero orders returned, running endpoint comparison",
                    brand_id,
                    company_id,
                    current_page,
                    pages_fetched_this_batch,
                )
                try:
                    await loop.run_in_executor(
                        _leaflink_executor,
                        client.test_endpoint_comparison,
                    )
                except Exception as _cmp_exc:
                    logger.warning(
                        "[LEAFLINK_ENDPOINT_COMPARE] comparison_failed brand=%s error=%s",
                        brand_id,
                        str(_cmp_exc)[:200],
                    )

                # Also log company role inference (no first order available)
                logger.info(
                    "[LEAFLINK_COMPANY_ROLE] company_id=%s is_seller=unknown is_buyer=unknown first_order_seller_id=none first_order_buyer_id=none note=no_orders_returned_cannot_infer_role",
                    company_id,
                )

            # ------------------------------------------------------------------ #
            # Cursor-loop detection: same cursor returned twice → stalled         #
            # ------------------------------------------------------------------ #
            next_cursor_hash = _cursor_hash(next_cursor)

            if next_cursor and next_cursor == _prev_cursor:
                _loop_reason = "cursor_loop_detected"
                logger.error(
                    "[OrdersSync] cursor_loop_detected brand=%s page=%s cursor_hash=%s — marking stalled",
                    brand_id,
                    current_page,
                    next_cursor_hash,
                )
                if sync_run_id:
                    try:
                        async with AsyncSessionLocal() as _loop_db:
                            async with _loop_db.begin():
                                await _srm_mark_stalled(_loop_db, sync_run_id, _loop_reason)
                    except Exception:
                        pass
                return

            _prev_cursor = next_cursor
            resume_url = next_cursor

            # ================================================================== #
            # PHASE A: Persist pagination state BEFORE any order processing      #
            # This MUST succeed even if all orders fail.                          #
            # Phase A failure → abort entire sync.                               #
            # Phase B failure → skip orders, continue pagination.                #
            # ================================================================== #

            total_orders_synced += len(batch_orders)
            pages_processed += 1
            records_seen_from_leaflink += _orders_count
            last_progress_at = datetime.now(timezone.utc)

            # When total_pages is None (cursor-based), fall back to current_page
            last_completed_page = (next_page - 1) if next_page else (total_pages or current_page)

            logger.debug("[PHASE_A_START] page=%s", pages_processed)

            # [PAGINATION_STATE_PERSISTED] — persist state BEFORE any order processing
            logger.info(
                "[PAGINATION_STATE_PERSISTED] page=%s count=%s next_present=%s results_len=%s",
                pages_processed,
                leaflink_total_count,
                next_cursor is not None,
                _orders_count,
            )

            if manager:
                manager.record_page_complete(brand_id, last_completed_page)

            await _persist_run_progress(
                pages_synced=last_completed_page,
                orders_loaded=total_orders_synced,
                cursor=next_cursor,
                page=last_completed_page,
                total_pages_val=total_pages,
                total_orders_available=leaflink_total_count,
            )

            # Explicit commit of pagination state — isolated from order processing
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _state_db:
                        async with _state_db.begin():
                            await _state_db.execute(
                                text("""
                                    UPDATE sync_runs
                                    SET last_next_url = :next_url,
                                        total_orders_available = COALESCE(:total_count, total_orders_available),
                                        current_page = :page,
                                        last_progress_at = :now
                                    WHERE id = :run_id
                                """),
                                {
                                    "next_url": next_cursor,
                                    "total_count": leaflink_total_count,
                                    "page": pages_processed,
                                    "now": datetime.now(timezone.utc),
                                    "run_id": sync_run_id,
                                },
                            )
                        await _state_db.commit()
                    logger.info(
                        "[PAGINATION_STATE_COMMITTED] page=%s count=%s next_present=%s",
                        pages_processed,
                        leaflink_total_count,
                        next_cursor is not None,
                    )
                except Exception as _url_exc:
                    logger.error(
                        "[OrdersSync] last_next_url_persist_error run_id=%s error=%s",
                        sync_run_id,
                        _url_exc,
                    )

            # [SYNC_ACCUMULATOR] debug only
            logger.debug(
                "[SYNC_ACCUMULATOR] total_fetched=%s after_page=%s brand_id=%s",
                total_orders_synced,
                current_page,
                brand_id,
            )

            # Memory diagnostics every 10 pages
            if pages_processed % 10 == 0:
                try:
                    import psutil as _psutil
                    _process = _psutil.Process(os.getpid())
                    _rss_mb = _process.memory_info().rss / 1024 / 1024
                    logger.info(
                        "[SYNC_MEMORY] page=%s records_seen=%s rss_mb=%.1f",
                        pages_processed,
                        records_seen_from_leaflink,
                        _rss_mb,
                    )
                except Exception as _mem_exc:
                    logger.warning("[MEMORY_CHECK_ERROR] error=%s", str(_mem_exc)[:200])

            # Log sampled progress every 10 pages (debug only — heartbeat covers this)
            if pages_processed % 10 == 0:
                logger.debug(
                    "[LeafLinkSync] progress id=%s page=%s orders_seen=%s leaflink_total=%s",
                    sync_run_id,
                    pages_processed,
                    records_seen_from_leaflink,
                    leaflink_total_count,
                )

            # ================================================================== #
            # PHASE B: Transform → Insert/Update (isolated from Phase A)         #
            # Failures here do NOT rollback pagination state.                    #
            # ================================================================== #

            logger.debug("[PHASE_B_START] page=%s orders_count=%s", pages_processed, _orders_count)

            persist_result: Optional[dict] = None
            if batch_orders:
                # [LEAFLINK_COMPANY_ROLE] Infer company role from first order on first page
                if current_page == start_page and len(batch_orders) > 0:
                    try:
                        _first_order = batch_orders[0]
                        if isinstance(_first_order, dict):
                            _seller = _first_order.get("seller") or _first_order.get("seller_company") or {}
                            _buyer = _first_order.get("buyer") or _first_order.get("buyer_company") or {}
                            _seller_id = _seller.get("id") if isinstance(_seller, dict) else _seller
                            _buyer_id = _buyer.get("id") if isinstance(_buyer, dict) else _buyer
                            _is_seller = str(_seller_id) == str(company_id) if _seller_id else False
                            _is_buyer = str(_buyer_id) == str(company_id) if _buyer_id else False
                            logger.info(
                                "[LEAFLINK_COMPANY_ROLE] company_id=%s is_seller=%s is_buyer=%s first_order_seller_id=%s first_order_buyer_id=%s",
                                company_id,
                                _is_seller,
                                _is_buyer,
                                _seller_id,
                                _buyer_id,
                            )
                    except Exception as _role_exc:
                        logger.warning(
                            "[LEAFLINK_COMPANY_ROLE] role_inference_failed company_id=%s error=%s",
                            company_id,
                            str(_role_exc)[:200],
                        )

                try:
                    logger.info(
                        "[FIRST_ORDER_PROCESSING_START] page=%s orders_count=%s",
                        pages_processed,
                        _orders_count,
                    )
                    logger.debug(
                        "[ORG_CONTEXT] stage=bg_continuous_before_upsert org_id=%s brand_id=%s"
                        " external_order_id=N/A batch_size=%s page=%s",
                        org_id,
                        brand_id,
                        len(batch_orders),
                        current_page,
                    )
                    tx_start = time.monotonic()
                    persist_result = await sync_leaflink_orders_headers_only(
                        brand_id=brand_id,
                        orders=batch_orders,
                        pages_fetched=pages_fetched_this_batch,
                        org_id=org_id,
                        batch_size=SAFE_MODE_BATCH_SIZE,
                    )
                    tx_duration = time.monotonic() - tx_start
                    if tx_duration > 10:
                        logger.warning(
                            "[SLOW_TRANSACTION] page=%s duration=%.1fs",
                            pages_processed,
                            tx_duration,
                        )
                    # Line items are deferred inside sync_leaflink_orders_headers_only
                    # via asyncio.create_task — no need to spawn a second task here.

                    # [SYNC_TRANSFORM_RESULT] Log transformation results (debug only)
                    _input_count = len(batch_orders)
                    _transformed = persist_result.get("created", 0) + persist_result.get("updated", 0) if persist_result else 0
                    _skipped = persist_result.get("skipped", 0) if persist_result else 0
                    logger.debug(
                        "[SYNC_TRANSFORM_RESULT] page=%s input_count=%s transformed=%s skipped=%s brand_id=%s org_id=%s",
                        current_page,
                        _input_count,
                        _transformed,
                        _skipped,
                        brand_id,
                        org_id,
                    )

                    logger.info(
                        "[FIRST_ORDER_PROCESSING_SUCCESS] page=%s inserted=%s updated=%s",
                        pages_processed,
                        persist_result.get("created", 0) if persist_result else 0,
                        persist_result.get("updated", 0) if persist_result else 0,
                    )

                except Exception as upsert_exc:
                    logger.error(
                        "[FIRST_ORDER_PROCESSING_FAIL] page=%s error=%s",
                        pages_processed,
                        str(upsert_exc)[:300],
                        exc_info=True,
                    )
                    logger.error(
                        "[OrdersSync] bg_batch_upsert_error brand=%s page=%s error=%s",
                        brand_id,
                        current_page,
                        upsert_exc,
                        exc_info=False,
                    )
                    # Continue to next page — pagination state is already persisted (Phase A)
                else:
                    # Log upsert result in debug format
                    _inserted = persist_result.get("inserted_count", persist_result.get("created", 0)) if persist_result else 0
                    _updated = persist_result.get("updated_count", persist_result.get("updated", 0)) if persist_result else 0
                    _errors = persist_result.get("error_count", 0) if persist_result else 0
                    logger.debug(
                        "[LEAFLINK_SYNC_DEBUG] page=%s count=%s running_total=%s"
                        " inserted=%s updated=%s errors=%s brand_id=%s",
                        current_page,
                        _orders_count,
                        total_orders_synced + _orders_count,
                        _inserted,
                        _updated,
                        _errors,
                        brand_id,
                    )

            # [PHASE_B_COMPLETE] Log after order processing
            _batch_upserted = (persist_result.get("created", 0) + persist_result.get("updated", 0)) if batch_orders and persist_result else 0
            _batch_skipped = persist_result.get("skipped", 0) if batch_orders and persist_result else 0
            _batch_failed = persist_result.get("failed_count", 0) if batch_orders and persist_result else 0
            logger.info(
                "[PHASE_B_COMPLETE] page=%s inserted=%s updated=%s skipped=%s",
                pages_processed,
                persist_result.get("created", 0) if persist_result else 0,
                persist_result.get("updated", 0) if persist_result else 0,
                _batch_skipped,
            )

            # [SYNC_PAGE_COMPLETE] debug only
            _page_elapsed = round(time.monotonic() - batch_start, 2)
            logger.debug(
                "[SYNC_PAGE_COMPLETE] page=%s count=%s running_total=%s has_next=%s"
                " next_cursor=%s elapsed=%ss brand_id=%s",
                current_page,
                _orders_count,
                total_orders_synced,
                "true" if next_cursor else "false",
                _cursor_hash(next_cursor) or "none",
                _page_elapsed,
                brand_id,
            )

            # [SYNC_PAGE_SUMMARY] Structured per-page summary for observability
            logger.info(
                "[SYNC_PAGE_SUMMARY] run_id=%s page=%s offset=%s fetched_count=%s "
                "upserted_headers=%s skipped=%s failed=%s total_seen_so_far=%s "
                "leaflink_total_count=%s has_next=%s",
                sync_run_id,
                pages_processed,
                (current_page - 1) * 100,
                _orders_count,
                _batch_upserted,
                _batch_skipped,
                _batch_failed,
                records_seen_from_leaflink,
                leaflink_total_count,
                next_cursor is not None,
            )

            # ------------------------------------------------------------------ #
            # Adaptive batch sizing based on fetch latency                        #
            # ------------------------------------------------------------------ #
            batch_seconds = (time.monotonic() - batch_start)
            if batch_seconds < _BG_BATCH_FAST_THRESHOLD:
                batch_size = min(batch_size + 1, _BG_BATCH_MAX)
            elif batch_seconds > _BG_BATCH_SLOW_THRESHOLD:
                batch_size = max(batch_size - 1, _BG_BATCH_MIN)

            # Advance page pointer — continue while LeafLink returns a next URL
            if next_cursor:
                resume_url = next_cursor
                if next_page:
                    current_page = next_page
                else:
                    current_page += pages_fetched_this_batch
            else:
                # next_cursor is None — all pages fetched
                break

        # ---------------------------------------------------------------------- #
        # Sync complete — determine completion status                            #
        # ---------------------------------------------------------------------- #
        total_duration = round(time.monotonic() - bg_start, 1)
        # When total_pages is None (cursor-based), final_page is simply the last page visited
        final_page = pages_processed

        # Calculate completion percentage
        completion_percent = 0.0
        if leaflink_total_count and leaflink_total_count > 0:
            completion_percent = (records_seen_from_leaflink / leaflink_total_count) * 100

        # CRITICAL: Do NOT mark completed if next_cursor exists — pagination is incomplete.
        # A non-None next_cursor means LeafLink has more pages; the loop exited early
        # (e.g. timeout) rather than reaching the true end of the result set.
        if next_cursor:
            logger.warning(
                "[LeafLinkSync] stopped_with_cursor id=%s brand=%s total_loaded=%s cursor_hash=%s",
                sync_run_id,
                brand_id,
                total_orders_synced,
                _cursor_hash(next_cursor),
            )
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _stall_db:
                        async with _stall_db.begin():
                            await _srm_mark_stalled(
                                _stall_db,
                                sync_run_id,
                                f"pagination_incomplete: next_cursor_present after {final_page} pages",
                            )
                except Exception as _stall_exc:
                    logger.error(
                        "[LeafLinkSync] mark_stalled_error id=%s error=%s",
                        sync_run_id,
                        _stall_exc,
                    )
        elif completion_percent >= 95:
            # No next cursor and >= 95% complete — pagination complete
            logger.info(
                "[SYNC_COMPLETE] run_id=%s brand_id=%s org_id=%s "
                "pages_processed=%s records_seen=%s leaflink_total=%s completion_percent=%.1f%%",
                sync_run_id, brand_id, org_id,
                pages_processed, records_seen_from_leaflink, leaflink_total_count,
                completion_percent,
            )
            if sync_run_id:
                try:
                    async with AsyncSessionLocal() as _done_db:
                        async with _done_db.begin():
                            await _srm_mark_completed(_done_db, sync_run_id)
                except Exception as _done_exc:
                    logger.error(
                        "[LeafLinkSync] mark_completed_error id=%s error=%s",
                        sync_run_id,
                        _done_exc,
                    )
        else:
            # No next cursor but < 95% complete — mark as incomplete
            _incomplete_reason = f"pagination_stopped_early: {completion_percent:.1f}% complete"
            logger.warning(
                "[SYNC_INCOMPLETE] run_id=%s brand_id=%s org_id=%s "
                "pages_processed=%s records_seen=%s leaflink_total=%s completion_percent=%.1f%% "
                "has_next=%s",
                sync_run_id, brand_id, org_id,
                pages_processed, records_seen_from_leaflink, leaflink_total_count,
                completion_percent, next_cursor is not None,
            )
            if sync_run_id:
                try:
                    from models import SyncRun as _SyncRun
                    async with AsyncSessionLocal() as _incomplete_db:
                        async with _incomplete_db.begin():
                            _inc_result = await _incomplete_db.execute(
                                select(_SyncRun).where(_SyncRun.id == sync_run_id)
                            )
                            _inc_run = _inc_result.scalar_one_or_none()
                            if _inc_run:
                                _inc_run.status = "incomplete"
                                _inc_run.last_error = _incomplete_reason
                                _inc_run.completed_at = datetime.now(timezone.utc)
                except Exception as _inc_exc:
                    logger.error(
                        "[LeafLinkSync] mark_incomplete_error id=%s error=%s",
                        sync_run_id,
                        _inc_exc,
                    )

        # [SYNC_FINAL_COUNTS] Log final summary for the entire sync run
        logger.info(
            "[SYNC_FINAL_COUNTS] fetched=%s created=deferred updated=deferred skipped=deferred errors=0"
            " final_page=%s total_pages=%s duration=%ss brand_id=%s sync_run_id=%s"
            " completion_percent=%.1f%%",
            total_orders_synced,
            final_page,
            total_pages,
            total_duration,
            brand_id,
            sync_run_id,
            completion_percent,
        )

        logger.info(
            "[OrdersSync] bg_sync_complete brand=%s final_page=%s total_pages=%s "
            "total_orders=%s duration_seconds=%s sync_run_id=%s",
            brand_id,
            final_page,
            total_pages,
            total_orders_synced,
            total_duration,
            sync_run_id,
        )

        # [LEAFLINK_SYNC_DEBUG] Final summary log
        try:
            from sqlalchemy import func as _func_final
            async with AsyncSessionLocal() as _final_count_db:
                _final_res = await _final_count_db.execute(
                    select(_func_final.count()).select_from(Order).where(Order.brand_id == brand_id)
                )
                _final_total = _final_res.scalar() or 0
        except Exception:
            _final_total = total_orders_synced

        logger.info(
            "[LEAFLINK_SYNC_DEBUG] sync_complete brand_id=%s total_fetched=%s"
            " final_page=%s total_pages=%s duration=%ss total_local_orders=%s"
            " completion_percent=%.1f%%",
            brand_id,
            total_orders_synced,
            final_page,
            total_pages if total_pages is not None else "cursor_based",
            total_duration,
            _final_total,
            completion_percent,
        )

    except asyncio.CancelledError:
        logger.warning("[SYNC_CANCELLED] run_id=%s brand_id=%s", sync_run_id, brand_id)
        raise
    except asyncio.TimeoutError as e:
        logger.error(
            "[SYNC_TIMEOUT] run_id=%s brand_id=%s error=%s",
            sync_run_id,
            brand_id,
            str(e)[:200],
        )
        # Persist timeout state
        try:
            async with AsyncSessionLocal() as _timeout_db:
                await _timeout_db.execute(
                    text("UPDATE sync_runs SET status='incomplete', last_error=:err WHERE id=:id"),
                    {"err": "timeout", "id": sync_run_id},
                )
                await _timeout_db.commit()
        except Exception:
            pass
        raise
    except Exception as e:
        logger.error(
            "[FATAL_SYNC_EXCEPTION] run_id=%s brand_id=%s exception_type=%s message=%s "
            "current_page=%s records_seen=%s next_url_present=%s",
            sync_run_id,
            brand_id,
            type(e).__name__,
            str(e)[:300],
            pages_processed,
            records_seen_from_leaflink,
            next_cursor is not None,
            exc_info=True,
        )
        # Persist final state
        try:
            async with AsyncSessionLocal() as _exc_db:
                await _exc_db.execute(
                    text("UPDATE sync_runs SET status='failed', last_error=:err WHERE id=:id"),
                    {"err": str(e)[:500], "id": sync_run_id},
                )
                await _exc_db.commit()
        except Exception:
            pass
        sys.stdout.flush()
        raise
    finally:
        # Cancel heartbeat task
        _heartbeat.cancel()
        try:
            await _heartbeat
        except asyncio.CancelledError:
            pass
        logger.info("[SYNC_SESSION_END] run_id=%s brand_id=%s", sync_run_id, brand_id)
