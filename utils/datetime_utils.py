"""
utils/datetime_utils.py — Canonical datetime normalization layer.

Fixes BLOCKER #1: offset-naive vs offset-aware datetime arithmetic.

All datetime creation and comparison in the sync pipeline MUST go through
these functions to guarantee that no naive datetime ever reaches asyncpg or
participates in arithmetic/comparison with an aware datetime.

Public API:
  normalize_datetime_utc(value)       — convert any value to UTC-aware datetime or None
  get_utc_now()                       — replacement for datetime.utcnow() and datetime.now()
  assert_datetime_aware(dt, context)  — validate datetime is aware before arithmetic
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

logger = logging.getLogger("datetime_utils")


def get_utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime.

    This is the canonical replacement for:
      - datetime.utcnow()       — returns naive datetime (WRONG)
      - datetime.now()          — returns local-time naive datetime (WRONG)
      - datetime.now(timezone.utc) — correct, but use this wrapper for consistency

    Returns:
        datetime with tzinfo=timezone.utc (always aware, always UTC)
    """
    return datetime.now(timezone.utc)


def normalize_datetime_utc(value: Any) -> Optional[datetime]:
    """Normalize any datetime-like value to a UTC-aware datetime.

    Handles all input types and always returns a UTC-aware datetime or None.
    Never returns naive datetimes — this is the primary fix for:
      "cannot subtract offset-naive and offset-aware datetimes"

    Conversion rules:
      - None / empty string → None
      - ISO string          → parsed to UTC-aware datetime
      - naive datetime      → assume UTC, attach tzinfo=timezone.utc
      - aware datetime      → convert to UTC via astimezone()
      - date (not datetime) → UTC midnight datetime
      - Other types         → None (logged as warning)

    Args:
        value: Any value — datetime, date, ISO string, or None.

    Returns:
        UTC-aware datetime, or None if value is None/unparseable.
    """
    if value is None:
        return None

    # Empty string → None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            # Handle both "Z" suffix and "+00:00" offset
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                # Naive after parse (no offset in string) — assume UTC
                result = parsed.replace(tzinfo=timezone.utc)
                logger.debug(
                    "[DATETIME_NORMALIZE] source=iso_string_naive value=%r result=%s",
                    value[:50],
                    result.isoformat(),
                )
                return result
            result = parsed.astimezone(timezone.utc)
            logger.debug(
                "[DATETIME_NORMALIZE] source=iso_string_aware value=%r result=%s",
                value[:50],
                result.isoformat(),
            )
            return result
        except (ValueError, TypeError) as exc:
            logger.warning(
                "[DATETIME_NORMALIZE_FAILED] source=iso_string value=%r error=%s — returning None",
                value[:100],
                str(exc),
            )
            return None

    # date (but NOT datetime — datetime is a subclass of date)
    if isinstance(value, date) and not isinstance(value, datetime):
        result = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        logger.debug(
            "[DATETIME_NORMALIZE] source=date value=%s result=%s",
            value.isoformat(),
            result.isoformat(),
        )
        return result

    # datetime object
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Naive datetime — assume UTC and make aware
            result = value.replace(tzinfo=timezone.utc)
            logger.debug(
                "[DATETIME_NORMALIZE] source=naive_datetime value=%s result=%s (assumed_utc)",
                value.isoformat(),
                result.isoformat(),
            )
            return result
        # Already aware — convert to UTC
        result = value.astimezone(timezone.utc)
        if result.utcoffset().total_seconds() != 0 or result != value:
            logger.debug(
                "[DATETIME_NORMALIZE] source=aware_datetime value=%s result=%s (converted_to_utc)",
                value.isoformat(),
                result.isoformat(),
            )
        return result

    # Unsupported type
    logger.warning(
        "[DATETIME_NORMALIZE_FAILED] source=unsupported_type type=%s value=%r — returning None",
        type(value).__name__,
        str(value)[:100],
    )
    return None


def assert_datetime_aware(dt: Any, context_name: str = "unknown") -> datetime:
    """Validate that a datetime is timezone-aware before arithmetic or comparison.

    Logs [DATETIME_COMPARE_CHECK] with full type/tz diagnostics so production
    logs show exactly which field caused a naive/aware mismatch.

    If the datetime is naive, it is normalized to UTC-aware and returned
    (never raises — fix-and-continue strategy to avoid crashing the sync loop).

    Args:
        dt:           The datetime value to validate.
        context_name: Human-readable label for the calling site (e.g. "is_stalled.last_progress_at").

    Returns:
        UTC-aware datetime (normalized if it was naive).

    Logs:
        [DATETIME_COMPARE_CHECK] — always, with left_tz, right_tz, left_type, right_type
        [DATETIME_NAIVE_FIXED]   — when a naive datetime is normalized
    """
    if not isinstance(dt, datetime):
        logger.warning(
            "[DATETIME_COMPARE_CHECK] context=%s value_type=%s value=%r — not a datetime, returning as-is",
            context_name,
            type(dt).__name__,
            str(dt)[:50],
        )
        return dt  # type: ignore[return-value]

    tz_str = str(dt.tzinfo) if dt.tzinfo is not None else "None"
    is_aware = dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None

    logger.debug(
        "[DATETIME_COMPARE_CHECK] context=%s left_type=%s left_tz=%s left_aware=%s",
        context_name,
        type(dt).__name__,
        tz_str,
        is_aware,
    )

    if not is_aware:
        # Naive datetime — normalize to UTC-aware and log the fix
        fixed = dt.replace(tzinfo=timezone.utc)
        logger.warning(
            "[DATETIME_NAIVE_FIXED] context=%s original=%s (naive) fixed=%s (UTC-aware)"
            " — naive datetime normalized before arithmetic/comparison",
            context_name,
            dt.isoformat(),
            fixed.isoformat(),
        )
        return fixed

    return dt.astimezone(timezone.utc)
