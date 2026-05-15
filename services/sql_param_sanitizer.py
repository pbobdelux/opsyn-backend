"""
services/sql_param_sanitizer.py — Canonical final-boundary SQL parameter sanitizer.

This module is the SINGLE authoritative sanitizer that runs immediately before
every session.execute / db.execute call in the sync pipeline.

Problem it solves:
  asyncpg raises "can't subtract offset-naive and offset-aware datetimes" when
  datetime objects leak through JSON payloads (raw_payload, line_items_json,
  dead-letter payloads) into bind parameters.  Top-level SQL TIMESTAMP columns
  (created_at, updated_at, synced_at) must remain as native datetime objects;
  nested datetime objects inside JSON blobs must become ISO-8601 strings.

Public API:
  sanitize_sql_params(params, known_datetime_fields)
      → sanitized params dict safe for asyncpg

  assert_no_datetime_in_json(obj, path)
      → raises RuntimeError with full path if any datetime/date survives

  convert_datetime_to_iso(obj)
      → recursively converts datetime/date/Decimal/UUID to JSON-safe primitives
"""

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

logger = logging.getLogger("sql_param_sanitizer")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def sanitize_sql_params(
    params: dict,
    known_datetime_fields: "set[str]",
) -> dict:
    """Sanitize SQL parameters immediately before every db.execute() call.

    Distinguishes between two categories of parameters:

    **known_datetime_fields** — top-level keys that MUST be timezone-aware
    datetime objects (e.g. ``{"created_at", "updated_at", "synced_at"}``):
      - None → kept as None
      - timezone-aware datetime → kept as-is
      - naive datetime → converted to UTC-aware (logs warning)
      - string / other → logged as warning, kept unchanged

    **All other keys** (JSON payloads, scalar fields, etc.):
      - dict / list / tuple → recursively walk via convert_datetime_to_iso()
        so every nested datetime/date becomes an ISO-8601 string
      - datetime / date at root level → converted to ISO string
      - Decimal → float
      - UUID → string
      - Everything else → unchanged

    After conversion a final assertion scan runs over all non-datetime-field
    values to guarantee no datetime/date object survives into asyncpg.

    Args:
        params:                Raw params dict from ORM / query builder.
        known_datetime_fields: Set of top-level keys that must stay as native
                               datetime objects for SQL TIMESTAMP columns.

    Returns:
        Sanitized params dict safe for asyncpg.

    Raises:
        RuntimeError: If a datetime/date object survives in a non-datetime
                      field after sanitization (hard guard — never executes
                      SQL if triggered).
    """
    if not isinstance(params, dict):
        logger.warning(
            "[FINAL_SQL_PARAM_SANITIZER] params_type=%s — expected dict, returning unchanged",
            type(params).__name__,
        )
        return params  # type: ignore[return-value]

    result: dict = {}

    for key, value in params.items():
        if key in known_datetime_fields:
            # SQL TIMESTAMP column — must remain a native datetime object
            result[key] = _normalize_sql_datetime(key, value)
        else:
            # JSON payload / scalar field — convert all nested datetimes to ISO strings
            result[key] = convert_datetime_to_iso(value)

    # Final assertion: no datetime/date must survive outside known_datetime_fields
    for key, value in result.items():
        if key in known_datetime_fields:
            continue
        try:
            assert_no_datetime_in_json(value, path=f"root.{key}")
        except RuntimeError as exc:
            # Re-raise with FINAL_SQL_DATETIME_LEAK prefix so callers can match it
            raise RuntimeError(
                f"FINAL_SQL_DATETIME_LEAK: {exc}"
            ) from exc

    return result


def assert_no_datetime_in_json(obj: Any, path: str = "root") -> None:
    """Recursively assert no datetime/date objects exist in *obj*.

    Raises RuntimeError with the full dotted path if a datetime or date is
    found.  Logs [FINAL_SQL_DATETIME_LEAK] with label, param_key, full_path,
    value_type, and value_preview before raising.

    Args:
        obj:  Any Python object (dict, list, tuple, scalar, …).
        path: Dotted path prefix for error messages (default "root").

    Raises:
        RuntimeError: If a datetime or date object is found anywhere in *obj*.
    """
    _assert_no_datetime_recursive(obj, path)


def convert_datetime_to_iso(obj: Any, _depth: int = 0) -> Any:
    """Recursively convert datetime/date/Decimal/UUID objects to JSON-safe types.

    Conversions applied at every level of nesting:
      - datetime → ISO-8601 string (UTC-normalised; naive datetimes assumed UTC)
      - date (non-datetime) → ISO-8601 date string
      - Decimal → float
      - UUID → string
      - dict → recursively converted (structure preserved)
      - list / tuple → recursively converted (returned as list)
      - All other types → unchanged

    Args:
        obj:    Any Python object.
        _depth: Internal recursion depth guard (max 20).

    Returns:
        JSON-safe version of *obj*.
    """
    MAX_DEPTH = 20
    if _depth > MAX_DEPTH:
        return "[max_depth_exceeded]"

    if obj is None:
        return None

    # datetime before date — datetime is a subclass of date
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
        return {k: convert_datetime_to_iso(v, _depth + 1) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [convert_datetime_to_iso(item, _depth + 1) for item in obj]

    # Primitives and everything else pass through unchanged
    return obj


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_sql_datetime(field_name: str, value: Any) -> Any:
    """Normalize a known SQL TIMESTAMP field to a UTC-aware datetime or None.

    Handles:
      - None → None
      - timezone-aware datetime → kept as-is (already correct)
      - naive datetime → converted to UTC-aware (logs warning)
      - string / other → logs warning, returned unchanged

    Args:
        field_name: Field name for log messages.
        value:      Raw field value.

    Returns:
        UTC-aware datetime, None, or the original value if it cannot be
        normalised (with a warning logged).
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            fixed = value.replace(tzinfo=timezone.utc)
            logger.warning(
                "[SQL_DATETIME_NAIVE_FIXED] field=%s original=%s fixed=%s"
                " — naive datetime normalised to UTC-aware",
                field_name,
                value.isoformat(),
                fixed.isoformat(),
            )
            return fixed
        # Already aware — return as-is (caller may have already converted to UTC)
        return value

    # Non-datetime value in a known datetime field — log and pass through
    logger.warning(
        "[SQL_DATETIME_UNEXPECTED_TYPE] field=%s type=%s value=%r"
        " — expected datetime, keeping as-is",
        field_name,
        type(value).__name__,
        str(value)[:100],
    )
    return value


def _assert_no_datetime_recursive(obj: Any, path: str) -> None:
    """Recursive worker for assert_no_datetime_in_json."""
    if isinstance(obj, (datetime, date)):
        # Extract param_key from path (e.g. "root.raw_payload.line_items[0].created_at" → "raw_payload")
        path_parts = path.split(".")
        param_key = path_parts[1] if len(path_parts) > 1 else path_parts[0]

        value_preview = str(obj)[:100]
        value_type = type(obj).__name__

        logger.error(
            "[FINAL_SQL_DATETIME_LEAK] param_key=%s full_path=%s value_type=%s value_preview=%s"
            " — datetime object must not reach asyncpg bind parameters",
            param_key,
            path,
            value_type,
            value_preview,
        )
        raise RuntimeError(
            f"datetime object found at '{path}' (type={value_type}, value={value_preview!r})"
        )

    if isinstance(obj, dict):
        for k, v in obj.items():
            _assert_no_datetime_recursive(v, f"{path}.{k}")

    elif isinstance(obj, (list, tuple)):
        for i, item in enumerate(obj):
            _assert_no_datetime_recursive(item, f"{path}[{i}]")
