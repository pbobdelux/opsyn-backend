import logging
from decimal import Decimal
from datetime import date, datetime, timezone
from uuid import UUID
from typing import Any, Optional

from utils.sanitizers import (
    deep_convert_all_datetimes,
    assert_no_datetime_anywhere,
    log_sanitization,
)

# Re-export so callers can import from either module
__all__ = [
    "make_json_safe",
    "normalize_datetime",
    "sanitize_sql_params",
    "validate_and_fix_sql_params",
    "_sanitize_params_for_sql_execution",
    "sanitize_json_fields_only",
    "preserve_sql_datetime_fields",
    "deep_convert_all_datetimes",
    "assert_no_datetime_anywhere",
    "log_sanitization",
]

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


def validate_and_fix_sql_params(params: Any) -> Any:
    """Final pre-bind validator — scans the EXACT params object passed to execute() and fixes
    any remaining naive datetimes in-place.

    This is the LAST line of defense before asyncpg binding.  It is called
    immediately before every ``db.execute(text(...), params)`` call so that
    no mutation between the earlier sanitization passes and the actual bind
    can slip a naive datetime through.

    Behaviour:
    - Walks every value in *params* (dict, list, or tuple).
    - For any ``datetime`` with ``tzinfo is None``:
        * Logs ``[SQL_PARAM_PREBIND_NAIVE_DATETIME]`` at ERROR level.
        * Converts to ISO 8601 string with ``+00:00`` offset.
    - For any ``datetime`` that is already UTC-aware:
        * Converts to UTC and returns ISO 8601 string.
    - For any plain ``date`` (not datetime):
        * Logs ``[SQL_PARAM_PREBIND_DATE_COERCED]`` at WARNING level.
        * Converts to ISO date string (``YYYY-MM-DD``).
    - Never raises exceptions — all errors are caught and logged.
    - Returns the (possibly modified) params object with all datetime/date
      values replaced by ISO 8601 strings so asyncpg never receives a raw
      Python datetime object.

    Args:
        params: The SQL parameters dict (or list/tuple) about to be passed to
                ``db.execute()``.  Modified in-place when *params* is a dict.

    Returns:
        The sanitized params object (same type as input, with datetime/date
        values converted to ISO 8601 strings).
    """
    try:
        return _validate_and_fix_recursive(params, path="params")
    except Exception as exc:  # pragma: no cover
        logger.error(
            "[SQL_PARAM_PREBIND_ERROR] unexpected error during pre-bind validation: %s",
            str(exc)[:300],
        )
        return params


def _validate_and_fix_recursive(value: Any, path: str = "params") -> Any:
    """Internal recursive worker for validate_and_fix_sql_params."""
    # datetime must be checked before date (datetime is a subclass of date)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Naive datetime → assume UTC and convert to ISO string
            fixed = value.replace(tzinfo=timezone.utc)
            logger.error(
                "[SQL_PARAM_PREBIND_NAIVE_DATETIME] path=%s value=%s — converting to ISO8601 string",
                path,
                value.isoformat(),
            )
            return fixed.isoformat()  # Return ISO string, not datetime object
        # Aware datetime → convert to UTC and return ISO string
        utc_dt = value.astimezone(timezone.utc)
        return utc_dt.isoformat()  # Return ISO string, not datetime object

    # Plain date (not datetime) → ISO date string
    if isinstance(value, date):
        iso_str = value.isoformat()
        logger.warning(
            "[SQL_PARAM_PREBIND_DATE_COERCED] path=%s value=%s — converted to ISO date string",
            path,
            value.isoformat(),
        )
        return iso_str  # Return ISO string, not datetime object

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


def _sanitize_params_for_sql_execution(params: Any, label: str = "unknown") -> Any:
    """
    DEPRECATED — superseded by preserve_sql_datetime_fields() (PR #467).

    This legacy blanket validator converted ALL datetime objects to ISO strings,
    including valid SQL TIMESTAMP bind params (created_at, updated_at, etc.) that
    asyncpg requires as native datetime objects for TIMESTAMPTZ columns.

    Callers should use preserve_sql_datetime_fields() instead, which correctly
    distinguishes between JSON payload fields (must be ISO strings) and SQL
    TIMESTAMP columns (must be native datetime objects).

    This function is retained for backward compatibility but is a no-op pass-through
    to avoid breaking any callers that still reference it.  It does NOT call
    assert_no_datetime_anywhere() or deep_convert_all_datetimes() — those blanket
    validators are incompatible with type-aware SQL parameter handling.

    Args:
        params: The params dict about to be passed to db.execute()
        label: Statement label for logging (e.g., "line_item_upsert")

    Returns:
        params unchanged — use preserve_sql_datetime_fields() for actual sanitization.
    """
    logger.debug(
        "[LEGACY_SANITIZER_BYPASSED] label=%s — _sanitize_params_for_sql_execution is deprecated,"
        " use preserve_sql_datetime_fields() instead",
        label,
    )
    return params


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


# ---------------------------------------------------------------------------
# Type-aware sanitizer — JSON fields only, SQL datetime params preserved
# ---------------------------------------------------------------------------

# Field names whose values are JSON blobs and MUST have all datetime objects
# converted to ISO strings before being passed to asyncpg as JSONB/text params.
_JSON_FIELD_NAMES: frozenset[str] = frozenset({
    "raw_payload",
    "line_items_json",
    "sync_health_missing_fields",
    "exception_details",
    "failure_context",
    "metadata",
    "count_by_failure_category",
    "payload_keys",
})

# Suffixes that identify JSON/payload fields regardless of exact name.
_JSON_FIELD_SUFFIXES: tuple[str, ...] = ("_json", "_payload")

# Field names that are SQL TIMESTAMP bind parameters and MUST remain as native
# Python datetime objects (asyncpg expects datetime, not ISO strings).
_SQL_DATETIME_FIELD_NAMES: frozenset[str] = frozenset({
    "created_at",
    "updated_at",
    "synced_at",
    "last_synced_at",
    "external_created_at",
    "external_updated_at",
    "now",
    "last_attempted_sync_at",
    "last_error_at",
    "last_progress_at",
    "last_successful_sync_at",
    "estimated_completion",
})

# Suffix that identifies SQL datetime columns regardless of exact name.
_SQL_DATETIME_SUFFIX: str = "_at"


def _is_json_field(name: str) -> bool:
    """Return True if *name* identifies a JSON/JSONB payload field."""
    if name in _JSON_FIELD_NAMES:
        return True
    return any(name.endswith(sfx) for sfx in _JSON_FIELD_SUFFIXES)


def _is_sql_datetime_field(name: str) -> bool:
    """Return True if *name* identifies a SQL TIMESTAMP bind parameter.

    JSON fields that happen to end with ``_at`` are NOT datetime fields —
    ``_is_json_field`` takes precedence.
    """
    if _is_json_field(name):
        return False
    if name in _SQL_DATETIME_FIELD_NAMES:
        return True
    return name.endswith(_SQL_DATETIME_SUFFIX)


def _recursive_json_field_sanitize(obj: Any, _depth: int = 0) -> Any:
    """Recursively convert datetime/date objects inside a JSON field value to ISO strings.

    This is intentionally identical to ``make_json_safe`` but kept separate so
    the intent is explicit: this function is ONLY called on JSON field values,
    never on SQL bind parameters.
    """
    MAX_DEPTH = 20
    if _depth > MAX_DEPTH:
        return "[max_depth_exceeded]"

    if obj is None:
        return None

    # datetime before date (datetime is a subclass of date)
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat()

    if isinstance(obj, date):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, UUID):
        return str(obj)

    if isinstance(obj, dict):
        return {k: _recursive_json_field_sanitize(v, _depth + 1) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [_recursive_json_field_sanitize(item, _depth + 1) for item in obj]

    if isinstance(obj, (str, int, float, bool)):
        return obj

    return str(obj)[:500]


def _count_datetimes_in_value(obj: Any, _depth: int = 0) -> int:
    """Count datetime/date objects recursively in *obj*."""
    if _depth > 20:
        return 0
    if isinstance(obj, (datetime, date)):
        return 1
    if isinstance(obj, dict):
        return sum(_count_datetimes_in_value(v, _depth + 1) for v in obj.values())
    if isinstance(obj, (list, tuple)):
        return sum(_count_datetimes_in_value(item, _depth + 1) for item in obj)
    return 0


def sanitize_json_fields_only(params: dict) -> dict:
    """Type-aware sanitizer: convert datetimes inside JSON fields, preserve SQL datetime params.

    Iterates over every top-level key in *params*:

    - **JSON fields** (``raw_payload``, ``line_items_json``, ``*_json``, ``*_payload``,
      ``sync_health_missing_fields``, etc.): recursively convert any
      ``datetime``/``date`` objects to ISO 8601 strings so the value is safe
      to pass as a JSONB/text bind parameter.

    - **SQL datetime columns** (``created_at``, ``updated_at``, ``synced_at``,
      ``*_at``, ``now``, etc.): left completely untouched — asyncpg requires
      native ``datetime`` objects for TIMESTAMP columns.

    - **All other fields**: passed through unchanged.

    Logs ``[SQL_PARAM_TYPES]`` for every field and
    ``[JSON_FIELD_SANITIZED]`` for every JSON field that is processed.

    Args:
        params: SQL parameter dict about to be passed to ``db.execute()``.

    Returns:
        New dict with JSON fields sanitized and SQL datetime params preserved.
    """
    if not isinstance(params, dict):
        logger.warning(
            "[SQL_PARAM_TYPES] params_type=%s — expected dict, returning unchanged",
            type(params).__name__,
        )
        return params  # type: ignore[return-value]

    result: dict = {}

    for name, value in params.items():
        is_json = _is_json_field(name)
        is_dt = _is_sql_datetime_field(name)

        logger.info(
            "[SQL_PARAM_TYPES] field=%s type=%s is_json_field=%s is_datetime_field=%s",
            name,
            type(value).__name__,
            is_json,
            is_dt,
        )

        if is_json and value is not None:
            before_count = _count_datetimes_in_value(value)
            sanitized_value = _recursive_json_field_sanitize(value)
            after_count = _count_datetimes_in_value(sanitized_value)
            logger.info(
                "[JSON_FIELD_SANITIZED] field=%s datetime_count_before=%d datetime_count_after=%d",
                name,
                before_count,
                after_count,
            )
            result[name] = sanitized_value
        elif is_dt and value is not None:
            logger.info(
                "[SQL_DATETIME_PRESERVED] field=%s type=%s tzinfo=%s",
                name,
                type(value).__name__,
                str(value.tzinfo) if isinstance(value, datetime) else "n/a",
            )
            result[name] = value
        else:
            result[name] = value

    return result


def preserve_sql_datetime_fields(params: dict) -> dict:
    """Sanitize JSON fields and validate type fidelity for SQL datetime columns.

    Calls ``sanitize_json_fields_only()`` then validates:

    - Every JSON field contains **no** ``datetime``/``date`` objects after
      sanitization (raises ``AssertionError`` if any remain).
    - Every SQL datetime column that is present and non-None **is** a native
      ``datetime`` object with timezone info (raises ``AssertionError`` if it
      is a string or naive datetime).

    Logs ``[JSON_FIELD_VALIDATION]`` and ``[SQL_DATETIME_VALIDATION]`` for
    every relevant field.

    Args:
        params: SQL parameter dict about to be passed to ``db.execute()``.

    Returns:
        Sanitized params dict with JSON fields clean and SQL datetimes intact.

    Raises:
        AssertionError: If a JSON field still contains a datetime after
            sanitization, or if a SQL datetime column is a string / naive
            datetime.
    """
    sanitized = sanitize_json_fields_only(params)

    # --- Validate JSON fields ---
    for name, value in sanitized.items():
        if not _is_json_field(name) or value is None:
            continue
        dt_count = _count_datetimes_in_value(value)
        status = "ok" if dt_count == 0 else "failed"
        log_fn = logger.info if status == "ok" else logger.error
        log_fn(
            "[JSON_FIELD_VALIDATION] field=%s datetime_count=%d status=%s",
            name,
            dt_count,
            status,
        )
        if dt_count > 0:
            logger.error(
                "[JSON_FIELD_VALIDATION_FAILED] field=%s datetime_count=%d"
                " — datetime objects must not remain in JSON fields after sanitization",
                name,
                dt_count,
            )
            raise AssertionError(
                f"JSON field '{name}' still contains {dt_count} datetime object(s) "
                f"after sanitization — all datetimes in JSON fields must be ISO strings"
            )

    # --- Validate SQL datetime columns ---
    for name, value in sanitized.items():
        if not _is_sql_datetime_field(name) or value is None:
            continue
        is_dt_obj = isinstance(value, datetime)
        has_tz = is_dt_obj and value.tzinfo is not None
        status = "ok" if (is_dt_obj and has_tz) else "failed"
        log_fn = logger.info if status == "ok" else logger.error
        log_fn(
            "[SQL_DATETIME_VALIDATION] field=%s is_datetime=%s has_tz=%s status=%s",
            name,
            is_dt_obj,
            has_tz,
            status,
        )
        if not is_dt_obj:
            logger.error(
                "[SQL_DATETIME_VALIDATION_FAILED] field=%s type=%s value=%r"
                " — SQL datetime column must be a native datetime object, not %s",
                name,
                type(value).__name__,
                str(value)[:100],
                type(value).__name__,
            )
            raise AssertionError(
                f"SQL datetime column '{name}' is type {type(value).__name__!r} "
                f"(value={str(value)[:100]!r}), expected a native datetime object. "
                f"asyncpg requires datetime objects for TIMESTAMP columns."
            )
        if not has_tz:
            logger.error(
                "[SQL_DATETIME_VALIDATION_FAILED] field=%s value=%s"
                " — SQL datetime column is naive (no tzinfo), must be UTC-aware",
                name,
                value.isoformat(),
            )
            raise AssertionError(
                f"SQL datetime column '{name}' is naive (tzinfo=None, value={value.isoformat()!r}). "
                f"All SQL datetime params must be UTC-aware datetime objects."
            )

    return sanitized
