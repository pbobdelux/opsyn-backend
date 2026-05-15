"""
Startup timezone audit for opsyn-backend.

Queries information_schema.columns for every datetime column across all
ORM-managed tables and verifies that each one is TIMESTAMPTZ
(timestamp with time zone).  Any column still typed as
TIMESTAMP WITHOUT TIME ZONE is a potential source of asyncpg errors:

    can't subtract offset-naive and offset-aware datetimes
    invalid input for query argument $N: datetime.datetime(... tzinfo=UTC)

Behavior:
  - Logs [DB_TIMEZONE_AUDIT] for every datetime column found, with:
      table_name, column_name, postgres_type, orm_timezone, status
  - Logs [DB_TIMEZONE_AUDIT_SUMMARY] with total counts at the end.
  - Raises RuntimeError (crashes startup) if ANY column is still
    TIMESTAMP WITHOUT TIME ZONE, so the problem is caught before the
    first request rather than at runtime inside a sync loop.

The ORM models already declare DateTime(timezone=True) for all datetime
columns.  This audit confirms the live database schema matches.
"""

import logging
from typing import Any

logger = logging.getLogger("startup_timezone_audit")

# ---------------------------------------------------------------------------
# Tables and their datetime columns as declared in the ORM models.
# Used to cross-reference the live schema against what the ORM expects.
# ---------------------------------------------------------------------------
_ORM_DATETIME_COLUMNS: dict[str, list[str]] = {
    "orders": [
        "external_created_at",
        "external_updated_at",
        "synced_at",
        "last_synced_at",
        "created_at",
        "updated_at",
    ],
    "order_lines": [
        "created_at",
        "updated_at",
    ],
    "sync_health": [
        "last_successful_sync_at",
        "last_attempted_sync_at",
        "last_error_at",
        "created_at",
        "updated_at",
    ],
    "dead_letter_line_items": [
        "last_failed_at",
        "created_at",
    ],
    "sync_runs": [
        "started_at",
        "last_progress_at",
        "completed_at",
        "last_heartbeat_at",
        "created_at",
        "updated_at",
    ],
    "brand_api_credentials": [
        "last_sync_at",
        "last_checked_at",
        "created_at",
        "updated_at",
    ],
    "sync_requests": [
        "created_at",
        "started_at",
        "completed_at",
    ],
    # The dispatch routes table is named 'routes' in the actual schema.
    "routes": [
        "created_at",
        "updated_at",
    ],
    "route_stops": [
        "created_at",
        "updated_at",
    ],
    "assistant_sessions": [
        "created_at",
        "updated_at",
    ],
    "assistant_messages": [
        "created_at",
    ],
    "assistant_pending_actions": [
        "created_at",
        "updated_at",
    ],
    "assistant_audit_logs": [
        "created_at",
    ],
}

# The PostgreSQL data_type value that indicates a timezone-aware column.
_TIMESTAMPTZ_TYPE = "timestamp with time zone"
_TIMESTAMP_NAIVE_TYPE = "timestamp without time zone"


async def run_timezone_audit() -> dict[str, Any]:
    """
    Audit all ORM datetime columns in the live database.

    Returns a summary dict:
      {
        "ok": bool,                  # True only if zero mismatches found
        "total_checked": int,        # Total columns inspected
        "total_ok": int,             # Columns already TIMESTAMPTZ
        "total_mismatch": int,       # Columns still TIMESTAMP (naive)
        "total_missing": int,        # ORM columns not found in live schema
        "mismatches": [str],         # "table.column" strings for mismatches
        "missing": [str],            # "table.column" strings not in live schema
      }

    Raises RuntimeError if any TIMESTAMP WITHOUT TIME ZONE columns remain,
    so the application fails fast before serving requests.
    """
    from database import engine

    if engine is None:
        logger.warning(
            "[DB_TIMEZONE_AUDIT] engine not available — skipping timezone audit"
        )
        return {
            "ok": False,
            "total_checked": 0,
            "total_ok": 0,
            "total_mismatch": 0,
            "total_missing": 0,
            "mismatches": [],
            "missing": [],
        }

    from sqlalchemy import text

    # Fetch all datetime columns for the tables we care about in one query.
    table_names = list(_ORM_DATETIME_COLUMNS.keys())

    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name   = ANY(:tables)
                      AND data_type    IN (
                          'timestamp with time zone',
                          'timestamp without time zone'
                      )
                    ORDER BY table_name, column_name
                """),
                {"tables": table_names},
            )
            live_columns: dict[tuple[str, str], str] = {
                (row[0], row[1]): row[2] for row in result.fetchall()
            }
    except Exception as exc:
        logger.error(
            "[DB_TIMEZONE_AUDIT] failed to query information_schema error=%s",
            exc,
            exc_info=True,
        )
        return {
            "ok": False,
            "total_checked": 0,
            "total_ok": 0,
            "total_mismatch": 0,
            "total_missing": 0,
            "mismatches": [],
            "missing": [],
        }

    total_checked = 0
    total_ok = 0
    total_mismatch = 0
    total_missing = 0
    mismatches: list[str] = []
    missing: list[str] = []

    for table_name, columns in _ORM_DATETIME_COLUMNS.items():
        for column_name in columns:
            total_checked += 1
            key = (table_name, column_name)

            if key not in live_columns:
                # Column not found in live schema — table may not exist yet
                # (e.g. assistant tables on a fresh DB before migrations run).
                total_missing += 1
                missing.append(f"{table_name}.{column_name}")
                logger.warning(
                    "[DB_TIMEZONE_AUDIT] table=%s column=%s postgres_type=NOT_FOUND "
                    "orm_timezone=True status=missing",
                    table_name,
                    column_name,
                )
                continue

            postgres_type = live_columns[key]
            is_timestamptz = postgres_type == _TIMESTAMPTZ_TYPE

            if is_timestamptz:
                total_ok += 1
                status = "ok"
            else:
                total_mismatch += 1
                status = "mismatch"
                mismatches.append(f"{table_name}.{column_name}")

            logger.info(
                "[DB_TIMEZONE_AUDIT] table=%s column=%s postgres_type=%s "
                "orm_timezone=True status=%s",
                table_name,
                column_name,
                postgres_type,
                status,
            )

    overall_ok = total_mismatch == 0

    logger.info(
        "[DB_TIMEZONE_AUDIT_SUMMARY] total_checked=%d ok=%d mismatch=%d "
        "missing=%d overall_ok=%s",
        total_checked,
        total_ok,
        total_mismatch,
        total_missing,
        overall_ok,
    )

    if not overall_ok:
        mismatch_list = ", ".join(mismatches)
        logger.error(
            "[DB_TIMEZONE_AUDIT_FAIL] %d column(s) are still TIMESTAMP WITHOUT "
            "TIME ZONE — asyncpg will reject UTC-aware datetimes. "
            "Run migration 2026_05_15_02_convert_timestamps_to_timestamptz.sql. "
            "Affected columns: %s",
            total_mismatch,
            mismatch_list,
        )
        raise RuntimeError(
            f"[DB_TIMEZONE_AUDIT_FAIL] {total_mismatch} datetime column(s) are "
            f"TIMESTAMP WITHOUT TIME ZONE — this causes asyncpg errors when "
            f"inserting UTC-aware datetimes. "
            f"Run migration 2026_05_15_02_convert_timestamps_to_timestamptz.sql "
            f"to fix the schema. Affected: {mismatch_list}"
        )

    return {
        "ok": overall_ok,
        "total_checked": total_checked,
        "total_ok": total_ok,
        "total_mismatch": total_mismatch,
        "total_missing": total_missing,
        "mismatches": mismatches,
        "missing": missing,
    }
