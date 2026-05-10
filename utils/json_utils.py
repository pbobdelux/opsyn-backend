from decimal import Decimal
from datetime import date, datetime, timezone
from uuid import UUID
from typing import Any


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
