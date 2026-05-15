import logging
from decimal import Decimal
from datetime import date, datetime, timezone
from uuid import UUID
from typing import Any, Optional

logger = logging.getLogger(__name__)


def make_json_safe(value: Any, _depth: int = 0, _seen: set = None) -> Any:
    """Recursively convert non-JSON-serializable objects to JSON-safe types.

    Conversions:
    - datetime → ISO 8601 UTC string (naive datetimes are assumed UTC and made aware first)
    - date (non-datetime) → ISO date string
    - Decimal → float
    - UUID → string
    - dict → recursively clean values (with depth/circular-reference protection)
    - list/tuple → recursively clean items (with depth/circular-reference protection)
    - Other types → pass through unchanged

    Handles mixed naive/aware datetime payloads safely — all datetimes are
    normalised to UTC-aware before serialisation so json.dumps never receives
    a raw datetime object and Python never attempts to compare naive vs aware
    datetimes during encoding.
    """
    MAX_DEPTH = 20
    MAX_CONTAINER_SIZE = 5000

    if _seen is None:
        _seen = set()

    # Depth guard
    if _depth > MAX_DEPTH:
        return "[max_depth_exceeded]"

    if value is None:
        return None

    # datetime must be checked before date because datetime is a subclass of date.
    # Normalise naive datetimes to UTC-aware before calling isoformat() so that
    # json.dumps never receives a raw datetime and Python never tries to compare
    # naive vs aware datetimes during encoding (the root cause of the
    # "can't subtract offset-naive and offset-aware datetimes" error).
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()

    # Plain date (not datetime)
    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, UUID):
        return str(value)

    # Circular-reference protection for containers
    obj_id = id(value)

    if isinstance(value, dict):
        if obj_id in _seen:
            return "[circular_reference]"
        if len(value) > MAX_CONTAINER_SIZE:
            return f"[dict_too_large: {len(value)} items]"
        _seen.add(obj_id)
        result = {k: make_json_safe(v, _depth + 1, _seen) for k, v in value.items()}
        _seen.discard(obj_id)
        return result

    if isinstance(value, (list, tuple)):
        if obj_id in _seen:
            return "[circular_reference]"
        if len(value) > MAX_CONTAINER_SIZE:
            return f"[list_too_large: {len(value)} items]"
        _seen.add(obj_id)
        result = [make_json_safe(item, _depth + 1, _seen) for item in value]
        _seen.discard(obj_id)
        return result

    # Primitive JSON-safe types pass through unchanged
    if isinstance(value, (str, int, float, bool)):
        return value

    # Fallback: stringify unknown types (truncated to avoid oversized payloads)
    return str(value)[:500]


def normalize_datetime(value: Any) -> Optional[datetime]:
    """Normalize any datetime value to UTC-aware datetime.

    Handles:
    - datetime objects (naive → UTC-aware, aware → UTC)
    - ISO 8601 strings (parsed to UTC-aware datetime)
    - date objects (converted to UTC midnight datetime)
    - None (returned as None)

    Returns UTC-aware datetime or None.
    Never raises exceptions — logs and returns None on parse failure.
    """
    if value is None:
        return None

    # ISO string → parse to datetime
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            # Ensure UTC-aware
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
                logger.debug(
                    "[DATETIME_NORMALIZED] source=string value=%s result=utc_aware (was naive after parse)",
                    value,
                )
            else:
                parsed = parsed.astimezone(timezone.utc)
                logger.debug(
                    "[DATETIME_NORMALIZED] source=string value=%s result=utc",
                    value,
                )
            return parsed
        except (ValueError, TypeError) as exc:
            logger.warning(
                "[DATETIME_PARSE_FAILED] value=%r error=%s — returning None",
                value[:100] if len(value) > 100 else value,
                str(exc),
            )
            return None

    # date (but NOT datetime) → UTC midnight
    if isinstance(value, date) and not isinstance(value, datetime):
        result = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        logger.debug(
            "[DATETIME_NORMALIZED] source=date value=%s result=%s",
            value.isoformat(),
            result.isoformat(),
        )
        return result

    # datetime → ensure UTC-aware
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Naive datetime — assume UTC
            result = value.replace(tzinfo=timezone.utc)
            logger.debug(
                "[DATETIME_NORMALIZED] source=datetime value=%s (naive) result=%s (utc_aware)",
                value.isoformat(),
                result.isoformat(),
            )
            return result
        # Already aware — convert to UTC
        result = value.astimezone(timezone.utc)
        if result != value:
            logger.debug(
                "[DATETIME_NORMALIZED] source=datetime value=%s result=%s (converted_to_utc)",
                value.isoformat(),
                result.isoformat(),
            )
        return result

    # Unsupported type — log and return None
    logger.warning(
        "[DATETIME_PARSE_FAILED] value=%r type=%s — unsupported type, returning None",
        str(value)[:100],
        type(value).__name__,
    )
    return None


def sanitize_sql_params(params: Any) -> Any:
    """Recursively sanitize SQL parameters to ensure no naive datetimes reach asyncpg.

    Walks all dicts/lists/tuples and converts:
    - datetime (naive) → UTC-aware (replace tzinfo=timezone.utc)
    - datetime (aware) → UTC (astimezone(timezone.utc))
    - date (not datetime) → UTC-aware datetime at midnight
    - ISO strings → attempt parse to UTC-aware datetime (only if value looks like a datetime string)

    Never raises exceptions. Logs all fixes with [SQL_PARAM_DATETIME_FIXED].
    Returns sanitized copy of params.
    """
    return _sanitize_sql_params_recursive(params, path="params")



# ---------------------------------------------------------------------------
# Field-aware sets for validate_final_sql_params()
# ---------------------------------------------------------------------------

# SQL TIMESTAMP columns — native datetime objects are REQUIRED here.
# asyncpg binds these directly to PostgreSQL TIMESTAMP/TIMESTAMPTZ columns.
ALLOWED_DATETIME_SQL_FIELDS: frozenset = frozenset({
    "created_at",
    "updated_at",
    "external_created_at",
    "external_updated_at",
    "synced_at",
    "last_synced_at",
    "last_attempted_sync_at",
    "last_successful_sync_at",
    "last_error_at",
    "now",
    "estimated_completion",
})

# JSON/JSONB columns — datetimes MUST be ISO strings here, never native objects.
JSON_FIELD_PATTERNS: frozenset = frozenset({
    "raw_payload",
    "line_items_json",
    "metadata",
    "payload",
    "response_json",
    "review_status",
    "sync_health_missing_fields",
    "sync_health_status",
    "payload_keys",
    "category_breakdown",
})


def validate_final_sql_params(params: Any) -> Any:
    """Field-aware final pre-bind validator for SQL parameters.

    Replaces the legacy ``validate_and_fix_sql_params()`` blanket rejector.
    Runs on the EXACT params dict passed to ``db.execute()`` and applies
    field-aware rules:

    - **TIMESTAMP columns** (``ALLOWED_DATETIME_SQL_FIELDS``): native
      ``datetime`` objects are *required* — they are left as UTC-aware
      datetimes and logged with ``[FINAL_SQL_PARAM_OK]``.
    - **JSON/JSONB columns** (``JSON_FIELD_PATTERNS`` or ``*_json`` /
      ``*_payload`` suffixes): any ``datetime`` found inside these fields is
      converted to an ISO UTC string and logged with
      ``[FINAL_JSON_PAYLOAD_OK]``.
    - **All other fields**: naive datetimes are forced to UTC-aware and logged
      with ``[SQL_PARAM_PREBIND_NAIVE_DATETIME]`` at ERROR level; aware
      datetimes are normalised to UTC silently.

    Never raises exceptions — all errors are caught and logged.

    Args:
        params: The SQL parameters dict (or list/tuple) about to be passed to
                ``db.execute()``.

    Returns:
        The sanitized params object (same type as input).
    """
    if not isinstance(params, dict):
        # For non-dict params fall back to the legacy recursive fixer
        try:
            return _validate_and_fix_recursive(params, path="params")
        except Exception as exc:  # pragma: no cover
            logger.error(
                "[SQL_PARAM_PREBIND_ERROR] unexpected error during pre-bind validation: %s",
                str(exc)[:300],
            )
            return params

    try:
        for field, value in list(params.items()):
            if value is None:
                continue

            # ----------------------------------------------------------------
            # TIMESTAMP columns: datetime objects are REQUIRED — allow them.
            # ----------------------------------------------------------------
            if field in ALLOWED_DATETIME_SQL_FIELDS:
                if isinstance(value, datetime):
                    # Ensure UTC-aware (fix naive silently)
                    if value.tzinfo is None:
                        params[field] = value.replace(tzinfo=timezone.utc)
                    else:
                        params[field] = value.astimezone(timezone.utc)
                    logger.info(
                        "[FINAL_SQL_PARAM_OK] field=%s type=%s tzinfo=%s",
                        field,
                        type(params[field]).__name__,
                        params[field].tzinfo,
                    )
                elif isinstance(value, date):
                    # Plain date -> UTC midnight datetime
                    params[field] = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
                    logger.info(
                        "[FINAL_SQL_PARAM_OK] field=%s type=date->datetime tzinfo=UTC",
                        field,
                    )
                continue

            # ----------------------------------------------------------------
            # JSON/JSONB columns: datetimes MUST be ISO strings — sanitize.
            # ----------------------------------------------------------------
            is_json_field = (
                field in JSON_FIELD_PATTERNS
                or field.endswith("_json")
                or field.endswith("_payload")
                or field.endswith("_status")
                or field.endswith("_fields")
            )
            if is_json_field:
                if isinstance(value, (dict, list, tuple, datetime, date)):
                    params[field] = _make_json_safe_for_bind(value)
                    logger.info(
                        "[FINAL_JSON_PAYLOAD_OK] field=%s datetime_count=0",
                        field,
                    )
                continue

            # ----------------------------------------------------------------
            # All other fields: fix any stray datetimes.
            # ----------------------------------------------------------------
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    fixed = value.replace(tzinfo=timezone.utc)
                    logger.error(
                        "[SQL_PARAM_PREBIND_NAIVE_DATETIME] path=params.%s value=%s — forcing UTC-aware",
                        field,
                        value.isoformat(),
                    )
                    params[field] = fixed
                else:
                    params[field] = value.astimezone(timezone.utc)
            elif isinstance(value, date):
                fixed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
                logger.warning(
                    "[SQL_PARAM_PREBIND_DATE_COERCED] path=params.%s value=%s — converted to UTC midnight datetime",
                    field,
                    value.isoformat(),
                )
                params[field] = fixed

        return params

    except Exception as exc:  # pragma: no cover
        logger.error(
            "[SQL_PARAM_PREBIND_ERROR] unexpected error during pre-bind validation: %s",
            str(exc)[:300],
        )
        return params


def _make_json_safe_for_bind(value: Any, _depth: int = 0) -> Any:
    """Recursively convert datetime/date objects to ISO strings for JSON/JSONB binding.

    Lightweight version of make_json_safe() used only inside validate_final_sql_params()
    to sanitize JSON field values without touching TIMESTAMP column values.
    """
    MAX_DEPTH = 20
    if _depth > MAX_DEPTH:
        return "[max_depth_exceeded]"

    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, dict):
        return {k: _make_json_safe_for_bind(v, _depth + 1) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [_make_json_safe_for_bind(item, _depth + 1) for item in value]

    if isinstance(value, (str, int, float, bool)):
        return value

    return str(value)[:500]


def validate_and_fix_sql_params(params: Any) -> Any:
    """Deprecated alias for validate_final_sql_params().

    .. deprecated::
        Use ``validate_final_sql_params()`` instead.  This alias is kept for
        backward compatibility only and will be removed in a future release.
        The new function is field-aware and does NOT reject native datetime
        objects in SQL TIMESTAMP columns.
    """
    logger.debug(
        "[VALIDATE_AND_FIX_SQL_PARAMS_DEPRECATED] use validate_final_sql_params() instead"
    )
    return validate_final_sql_params(params)


def _validate_and_fix_recursive(value: Any, path: str = "params") -> Any:
    """Internal recursive worker for validate_and_fix_sql_params."""
    # datetime must be checked before date (datetime is a subclass of date)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            fixed = value.replace(tzinfo=timezone.utc)
            logger.error(
                "[SQL_PARAM_PREBIND_NAIVE_DATETIME] path=%s value=%s — forcing UTC-aware",
                path,
                value.isoformat(),
            )
            return fixed
        return value.astimezone(timezone.utc)

    # Plain date (not datetime) → UTC midnight datetime
    if isinstance(value, date):
        fixed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        logger.warning(
            "[SQL_PARAM_PREBIND_DATE_COERCED] path=%s value=%s — converted to UTC midnight datetime",
            path,
            value.isoformat(),
        )
        return fixed

    # dict → recurse over values, modify in-place and return
    if isinstance(value, dict):
        for k in list(value.keys()):
            value[k] = _validate_and_fix_recursive(value[k], path=f"{path}.{k}")
        return value

    # list → recurse over items, modify in-place and return
    if isinstance(value, list):
        for i in range(len(value)):
            value[i] = _validate_and_fix_recursive(value[i], path=f"{path}[{i}]")
        return value

    # tuple → recurse over items, return as list (mutable)
    if isinstance(value, tuple):
        return [
            _validate_and_fix_recursive(item, path=f"{path}[{i}]")
            for i, item in enumerate(value)
        ]

    # All other types pass through unchanged
    return value


def _sanitize_sql_params_recursive(value: Any, path: str = "params") -> Any:
    """Internal recursive worker for sanitize_sql_params."""
    # datetime must be checked before date (datetime is a subclass of date)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            fixed = value.replace(tzinfo=timezone.utc)
            logger.info(
                "[SQL_PARAM_DATETIME_FIXED] path=%s action=naive_to_utc original=%s fixed=%s",
                path,
                value.isoformat(),
                fixed.isoformat(),
            )
            return fixed
        utc_val = value.astimezone(timezone.utc)
        if utc_val != value:
            logger.info(
                "[SQL_PARAM_DATETIME_FIXED] path=%s action=aware_to_utc original=%s fixed=%s",
                path,
                value.isoformat(),
                utc_val.isoformat(),
            )
        return utc_val

    # Plain date (not datetime) → UTC midnight datetime
    if isinstance(value, date):
        fixed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        logger.info(
            "[SQL_PARAM_DATETIME_FIXED] path=%s action=date_to_utc_midnight original=%s fixed=%s",
            path,
            value.isoformat(),
            fixed.isoformat(),
        )
        return fixed

    # dict → recurse over values
    if isinstance(value, dict):
        return {
            k: _sanitize_sql_params_recursive(v, path=f"{path}.{k}")
            for k, v in value.items()
        }

    # list → recurse over items
    if isinstance(value, list):
        return [
            _sanitize_sql_params_recursive(item, path=f"{path}[{i}]")
            for i, item in enumerate(value)
        ]

    # tuple → recurse over items, return as list (mutable)
    if isinstance(value, tuple):
        return [
            _sanitize_sql_params_recursive(item, path=f"{path}[{i}]")
            for i, item in enumerate(value)
        ]

    # str → attempt ISO datetime parse only for strings that look like timestamps
    if isinstance(value, str) and value and len(value) >= 10:
        # Only attempt parse if the string starts with a 4-digit year (YYYY-)
        # to avoid expensive parse attempts on arbitrary strings.
        if len(value) >= 19 and value[4] == "-" and value[7] == "-":
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                else:
                    parsed = parsed.astimezone(timezone.utc)
                logger.info(
                    "[SQL_PARAM_DATETIME_FIXED] path=%s action=iso_string_to_utc original=%s fixed=%s",
                    path,
                    value,
                    parsed.isoformat(),
                )
                return parsed
            except (ValueError, TypeError):
                # Not a datetime string — pass through unchanged
                pass

    # All other types (str, int, float, bool, None, Decimal, UUID, etc.) pass through
    return value
