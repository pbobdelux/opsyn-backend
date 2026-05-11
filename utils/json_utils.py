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
