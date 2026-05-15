"""
utils/sanitizers.py — Absolute final SQL sanitization boundary.

This module provides the canonical, centralized, recursive datetime sanitizer
that MUST be applied immediately before every db.execute() call.

The contract:
  - deep_convert_all_datetimes() converts ALL datetime/date objects to ISO
    strings, recursively, with no exceptions.
  - assert_no_datetime_anywhere() raises AssertionError if any datetime
    survives after conversion (hard guard — never executes SQL if triggered).
  - log_sanitization() emits [FINAL_SQL_SANITIZED] for observability.

No caller trust. No earlier-only sanitation. No bypass path.
"""

import logging
from datetime import date, datetime, timezone
from typing import Any

logger = logging.getLogger("opsyn-backend")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def deep_convert_all_datetimes(obj: Any) -> Any:
    """
    Recursively traverse and convert ALL datetime/date objects to ISO strings.

    Handles:
    - dict (including nested)
    - list/tuple (including nested)
    - datetime.datetime -> isoformat() with UTC normalization
    - datetime.date (non-datetime) -> isoformat()
    - Preserves timezone info (aware datetimes are converted to UTC first)
    - Preserves all other types (str, int, float, Decimal, bool, None)

    Returns sanitized object safe for SQLAlchemy/asyncpg binding.
    Never raises — all errors are caught and logged.
    """
    try:
        return _deep_convert(obj)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "[FINAL_SQL_SANITIZED] deep_convert_all_datetimes unexpected error: %s",
            str(exc)[:300],
        )
        return obj


def assert_no_datetime_anywhere(obj: Any, path: str = "root") -> None:
    """
    Recursively assert NO datetime objects exist in params.

    If a datetime is found:
    - Logs full path (e.g. "root.synced_at", "root.line_items[0].created_at")
    - Raises AssertionError with path
    - SQL execution MUST NOT proceed after this raises

    Call this immediately after deep_convert_all_datetimes() and before
    db.execute() to enforce the hard boundary.
    """
    _assert_no_datetime(obj, path)


def log_sanitization(label: str, params_before: Any, params_after: Any) -> None:
    """
    Log a sanitization event with before/after datetime counts.

    Emits [FINAL_SQL_SANITIZED] with:
    - label (operation name)
    - datetime_count_before
    - datetime_count_after
    - top_level_keys (if params is a dict)

    Format:
      [FINAL_SQL_SANITIZED] label=... datetime_count_before=... datetime_count_after=...
    """
    before_count = _count_datetimes(params_before)
    after_count = _count_datetimes(params_after)

    top_keys: Any = "n/a"
    if isinstance(params_after, dict):
        top_keys = list(params_after.keys())

    logger.info(
        "[FINAL_SQL_SANITIZED] label=%s datetime_count_before=%d datetime_count_after=%d"
        " top_level_keys=%s",
        label,
        before_count,
        after_count,
        top_keys,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _deep_convert(obj: Any) -> Any:
    """Recursive worker for deep_convert_all_datetimes."""
    # datetime must be checked before date (datetime is a subclass of date)
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat()

    if isinstance(obj, date):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {k: _deep_convert(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_deep_convert(item) for item in obj]

    if isinstance(obj, tuple):
        return [_deep_convert(item) for item in obj]

    # str, int, float, Decimal, bool, None, UUID, etc. — pass through unchanged
    return obj


def _assert_no_datetime(obj: Any, path: str) -> None:
    """Recursive worker for assert_no_datetime_anywhere."""
    if isinstance(obj, (datetime, date)):
        logger.error(
            "[DATETIME_FOUND_IN_PARAMS] path=%s type=%s value=%r"
            " — datetime object reached SQL execution boundary, raising AssertionError",
            path,
            type(obj).__name__,
            repr(obj)[:100],
        )
        raise AssertionError(
            f"Python datetime object found in params at '{path}'\n"
            f"All datetime objects must be converted to ISO strings before database binding"
        )

    if isinstance(obj, dict):
        for k, v in obj.items():
            _assert_no_datetime(v, f"{path}.{k}")

    elif isinstance(obj, (list, tuple)):
        for i, item in enumerate(obj):
            _assert_no_datetime(item, f"{path}[{i}]")


def _count_datetimes(obj: Any, _depth: int = 0) -> int:
    """Count all datetime/date objects in obj recursively."""
    if _depth > 20:
        return 0

    if isinstance(obj, (datetime, date)):
        return 1

    if isinstance(obj, dict):
        return sum(_count_datetimes(v, _depth + 1) for v in obj.values())

    if isinstance(obj, (list, tuple)):
        return sum(_count_datetimes(item, _depth + 1) for item in obj)

    return 0
