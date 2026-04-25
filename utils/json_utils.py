from decimal import Decimal
from datetime import date, datetime
from uuid import UUID
from typing import Any


def make_json_safe(value: Any) -> Any:
    """Recursively convert non-JSON-serializable objects to JSON-safe types.

    Conversions:
    - Decimal → float
    - datetime/date → ISO format string
    - UUID → string
    - dict → recursively clean values
    - list → recursively clean items
    - Other types → pass through unchanged
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [make_json_safe(v) for v in value]
    return value
