import copy
import logging
from decimal import Decimal
from datetime import date, datetime, timezone
from uuid import UUID
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical deep datetime converter — THE single source of truth for
# converting all datetime objects to ISO strings before SQL execution.
# ---------------------------------------------------------------------------

def deep_convert_all_datetimes(params: Any) -> Any:
    """Recursively convert ALL datetime/date objects to ISO 8601 strings.

    This is the CANONICAL final-boundary sanitizer.  It must be called
    immediately before every ``db.execute(text(...), params)`` call so that
    no datetime object — regardless of how it was created or mutated — ever
    reaches asyncpg.

    Rules:
    - ``datetime`` (naive)  → assume UTC, attach tzinfo, return ``.isoformat()``
    - ``datetime`` (aware)  → convert to UTC, return ``.isoformat()``
    - ``date`` (not datetime) → return ``.isoformat()`` (YYYY-MM-DD string)
    - ``dict``               → recurse over values, return new dict
    - ``list`` / ``tuple``   → recurse over items, return new list
    - All other types        → pass through unchanged (str, int, float,
                               Decimal, bool, None, UUID, …)

    Returns a fully sanitized *copy* — the original is never mutated.
    """
    return _deep_convert_recursive(params, path="root")


def _deep_convert_recursive(value: Any, path: str) -> Any:
    """Internal recursive worker for deep_convert_all_datetimes."""
    # datetime must be checked before date (datetime is a subclass of date)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        iso = value.astimezone(timezone.utc).isoformat()
        logger.debug(
            "[DEEP_CONVERT_DATETIME] path=%s converted_to=%s",
            path,
            iso,
        )
        return iso

    if isinstance(value, date):
        iso = value.isoformat()
        logger.debug(
            "[DEEP_CONVERT_DATE] path=%s converted_to=%s",
            path,
            iso,
        )
        return iso

    if isinstance(value, dict):
        return {k: _deep_convert_recursive(v, f"{path}.{k}") for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [_deep_convert_recursive(item, f"{path}[{i}]") for i, item in enumerate(value)]

    # All other types (str, int, float, bool, None, Decimal, UUID, …) pass through
    return value


def assert_no_datetime_anywhere(params: Any, label: str = "unknown") -> None:
    """Assert that *params* contains zero datetime/date objects at any depth.

    Must be called immediately after ``deep_convert_all_datetimes()`` and
    before ``db.execute()``.  If any datetime is found the full dotted path
    is logged at ERROR level and an ``AssertionError`` is raised — SQL
    execution is aborted.

    Args:
        params: The sanitized params object about to be passed to execute().
        label:  Caller label for log context (e.g. ``"order_header_upsert"``).
    """
    _assert_no_datetime_recursive(params, path="root", label=label)


def _assert_no_datetime_recursive(value: Any, path: str, label: str) -> None:
    """Internal recursive worker for assert_no_datetime_anywhere."""
    if isinstance(value, (datetime, date)):
        logger.error(
            "[DATETIME_FOUND_IN_PARAMS] label=%s path=%s type=%s value=%r"
            " — aborting SQL execution",
            label,
            path,
            type(value).__name__,
            str(value)[:100],
        )
        raise AssertionError(
            f"[DATETIME_FOUND_IN_PARAMS] label={label} path={path} "
            f"type={type(value).__name__} value={value!r}"
        )

    if isinstance(value, dict):
        for k, v in value.items():
            _assert_no_datetime_recursive(v, f"{path}.{k}", label)

    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            _assert_no_datetime_recursive(item, f"{path}[{i}]", label)


def _count_datetimes(value: Any) -> int:
    """Count the number of datetime/date objects at any depth in *value*."""
    if isinstance(value, (datetime, date)):
        return 1
    if isinstance(value, dict):
        return sum(_count_datetimes(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return sum(_count_datetimes(item) for item in value)
    return 0


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
    FINAL SQL EXECUTION BOUNDARY: Deep-copy and recursively convert all datetime/date
    values to ISO8601 strings immediately before db.execute().

    This is the LAST chance to sanitize params before they reach asyncpg.

    Args:
        params: The params dict about to be passed to db.execute()
        label: Statement label for logging (e.g., "line_item_upsert")

    Returns:
        A new params dict with all datetime/date values converted to ISO8601 strings.
        The caller MUST use this returned dict for execution, not the original.
    """
    import copy

    # Deep-copy to avoid mutating the original
    sanitized = copy.deepcopy(params)

    def _convert_datetimes(obj: Any) -> Any:
        if isinstance(obj, datetime):
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=timezone.utc)
            return obj.astimezone(timezone.utc).isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: _convert_datetimes(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_convert_datetimes(item) for item in obj]
        if isinstance(obj, tuple):
            return [_convert_datetimes(item) for item in obj]
        return obj

    sanitized = _convert_datetimes(sanitized)

    # Log the type of created_at to prove it's now a string
    if isinstance(sanitized, dict) and "created_at" in sanitized:
        logger.info(
            "[SAFE_EXECUTE_FINAL_PARAMS] label=%s created_at_type=%s",
            label,
            type(sanitized["created_at"]).__name__,
        )

    return sanitized


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
    # All other types (str, int, float, bool, None, Decimal, UUID, etc.) pass through
    # NOTE: ISO datetime strings are intentionally NOT re-parsed back to datetime objects.
    # Converting ISO strings back to datetime objects was the root cause of datetimes
    # surviving to SQL execution after _validate_and_fix_sql_params had already
    # converted them to strings.  Strings are safe for asyncpg — leave them alone.
    return value
