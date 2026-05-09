"""
Migration validator — post-migration schema integrity checks.

Called after the migration runner and recovery module complete.  Validates
that the live schema matches the expected post-migration state:

  1. All target tables have UUID primary keys (no INTEGER PKs remaining).
  2. No NULL primary keys in any migrated table.
  3. FK type consistency: FK columns match the type of the PK they reference.
  4. No orphaned FK rows (referential integrity spot-checks).

All checks are READ-ONLY — this module never modifies the database.

Log markers emitted:
  [MIGRATION_VALIDATION_OK]      — all checks passed
  [MIGRATION_VALIDATION_WARN]    — non-fatal issues found (schema usable)
  [MIGRATION_VALIDATION_FAIL]    — fatal issues found (schema inconsistent)
  [MIGRATION_VALIDATION_ERROR]   — unexpected error during validation
"""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("migration_validator")

# ---------------------------------------------------------------------------
# Tables whose primary key MUST be UUID after all migrations have run.
# ---------------------------------------------------------------------------
_UUID_PK_TABLES: list[str] = [
    "orders",
    "order_lines",
    "brand_api_credentials",
    "tenant_credentials",
    "sync_runs",
    "sync_requests",
    "sync_health",
    "dead_letter_line_items",
    "sync_dead_letters",
    "sync_metrics_snapshots",
    "organization_brand_bindings",
    "drivers",
    "routes",
    "route_stops",
    "route_events",
    "driver_locations",
    "driver_route_history",
    "leaflink_webhook_events",
]

# ---------------------------------------------------------------------------
# FK relationships to validate for type consistency.
# Each tuple: (child_table, child_column, parent_table, parent_column)
# ---------------------------------------------------------------------------
_FK_TYPE_CHECKS: list[tuple[str, str, str, str]] = [
    ("route_events",        "route_id",   "routes",  "id"),
    ("driver_locations",    "driver_id",  "drivers", "id"),
    ("driver_locations",    "route_id",   "routes",  "id"),
    ("driver_route_history","driver_id",  "drivers", "id"),
    ("driver_route_history","route_id",   "routes",  "id"),
    ("order_lines",         "order_id",   "orders",  "id"),
    ("orders",              "sync_run_id","sync_runs","id"),
]

# ---------------------------------------------------------------------------
# Orphan FK spot-checks.
# Each tuple: (child_table, child_fk_col, parent_table, parent_pk_col, nullable)
# nullable=True means the FK column may be NULL (skip NULL rows in the check).
# ---------------------------------------------------------------------------
_ORPHAN_CHECKS: list[tuple[str, str, str, str, bool]] = [
    ("route_stops",          "route_id",    "routes",    "id", False),
    ("route_events",         "route_id",    "routes",    "id", False),
    ("driver_locations",     "driver_id",   "drivers",   "id", False),
    ("driver_locations",     "route_id",    "routes",    "id", True),
    ("driver_route_history", "driver_id",   "drivers",   "id", False),
    ("driver_route_history", "route_id",    "routes",    "id", False),
    ("order_lines",          "order_id",    "orders",    "id", False),
    ("orders",               "sync_run_id", "sync_runs", "id", True),
]


async def _table_exists(db: AsyncSession, table_name: str) -> bool:
    """Return True if *table_name* exists in the public schema."""
    result = await db.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = :t"
        ),
        {"t": table_name},
    )
    return result.fetchone() is not None


async def _column_type(db: AsyncSession, table: str, column: str) -> str | None:
    """Return the data_type of *column* in *table*, or None if not found."""
    result = await db.execute(
        text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c"
        ),
        {"t": table, "c": column},
    )
    row = result.fetchone()
    return row[0] if row else None


async def validate_migration_result(db: AsyncSession) -> dict[str, Any]:
    """
    Validate the live schema after all migrations have been applied.

    Performs four categories of checks:
      1. UUID PK enforcement — all target tables must have UUID primary keys.
      2. NULL PK check — no table should have rows with a NULL primary key.
      3. FK type consistency — FK columns must match the type of the PK they
         reference (catches UUID vs INTEGER mismatches).
      4. Orphan FK check — spot-checks that no child rows reference a
         non-existent parent row.

    Returns a dict:
      {
        "ok": bool,                  # True only if zero fatal issues
        "issues": list[str],         # Fatal issue descriptions
        "warnings": list[str],       # Non-fatal observations
        "checks_run": int,           # Total number of individual checks executed
      }
    """
    issues: list[str] = []
    warnings: list[str] = []
    checks_run = 0

    # -----------------------------------------------------------------------
    # CHECK 1: UUID PK enforcement
    # -----------------------------------------------------------------------
    logger.debug("[MIGRATION_VALIDATION] running check 1: UUID PK types")
    for table in _UUID_PK_TABLES:
        checks_run += 1
        try:
            if not await _table_exists(db, table):
                warnings.append(f"Table '{table}' does not exist — skipping PK type check")
                continue

            pk_type = await _column_type(db, table, "id")
            if pk_type is None:
                warnings.append(f"Table '{table}' has no 'id' column — cannot verify PK type")
            elif pk_type != "uuid":
                issues.append(
                    f"Table '{table}' has non-UUID PK: data_type={pk_type!r} (expected 'uuid')"
                )
            else:
                logger.debug("[MIGRATION_VALIDATION] [OK] %s.id type=uuid", table)
        except Exception as exc:
            warnings.append(f"PK type check failed for '{table}': {exc!s:.200}")

    # -----------------------------------------------------------------------
    # CHECK 2: NULL PK rows
    # -----------------------------------------------------------------------
    logger.debug("[MIGRATION_VALIDATION] running check 2: NULL PK rows")
    for table in _UUID_PK_TABLES:
        checks_run += 1
        try:
            if not await _table_exists(db, table):
                continue  # Already warned in check 1

            result = await db.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE id IS NULL")  # noqa: S608
            )
            null_count = result.scalar() or 0
            if null_count > 0:
                issues.append(
                    f"Table '{table}' has {null_count} row(s) with NULL primary key"
                )
            else:
                logger.debug("[MIGRATION_VALIDATION] [OK] %s has no NULL PKs", table)
        except Exception as exc:
            warnings.append(f"NULL PK check failed for '{table}': {exc!s:.200}")

    # -----------------------------------------------------------------------
    # CHECK 3: FK type consistency
    # -----------------------------------------------------------------------
    logger.debug("[MIGRATION_VALIDATION] running check 3: FK type consistency")
    for child_table, child_col, parent_table, parent_col in _FK_TYPE_CHECKS:
        checks_run += 1
        try:
            child_exists = await _table_exists(db, child_table)
            parent_exists = await _table_exists(db, parent_table)
            if not child_exists or not parent_exists:
                continue  # Table absent — skip silently

            child_type = await _column_type(db, child_table, child_col)
            parent_type = await _column_type(db, parent_table, parent_col)

            if child_type is None or parent_type is None:
                warnings.append(
                    f"FK type check skipped: {child_table}.{child_col} or "
                    f"{parent_table}.{parent_col} column not found"
                )
                continue

            if child_type != parent_type:
                issues.append(
                    f"FK type mismatch: {child_table}.{child_col}={child_type!r} "
                    f"but {parent_table}.{parent_col}={parent_type!r}"
                )
            else:
                logger.debug(
                    "[MIGRATION_VALIDATION] [OK] %s.%s → %s.%s type=%s",
                    child_table, child_col, parent_table, parent_col, child_type,
                )
        except Exception as exc:
            warnings.append(
                f"FK type check failed for {child_table}.{child_col}: {exc!s:.200}"
            )

    # -----------------------------------------------------------------------
    # CHECK 4: Orphan FK rows
    # -----------------------------------------------------------------------
    logger.debug("[MIGRATION_VALIDATION] running check 4: orphan FK rows")
    for child_table, child_fk, parent_table, parent_pk, nullable in _ORPHAN_CHECKS:
        checks_run += 1
        try:
            child_exists = await _table_exists(db, child_table)
            parent_exists = await _table_exists(db, parent_table)
            if not child_exists or not parent_exists:
                continue

            null_clause = f"AND c.{child_fk} IS NOT NULL" if nullable else ""
            query = text(
                f"SELECT COUNT(*) FROM {child_table} c "  # noqa: S608
                f"LEFT JOIN {parent_table} p ON p.{parent_pk} = c.{child_fk} "
                f"WHERE p.{parent_pk} IS NULL {null_clause}"
            )
            result = await db.execute(query)
            orphan_count = result.scalar() or 0
            if orphan_count > 0:
                issues.append(
                    f"Orphaned FK rows: {child_table}.{child_fk} has {orphan_count} "
                    f"row(s) not found in {parent_table}.{parent_pk}"
                )
            else:
                logger.debug(
                    "[MIGRATION_VALIDATION] [OK] %s.%s → %s.%s no orphans",
                    child_table, child_fk, parent_table, parent_pk,
                )
        except Exception as exc:
            warnings.append(
                f"Orphan check failed for {child_table}.{child_fk}: {exc!s:.200}"
            )

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    ok = len(issues) == 0

    if ok and not warnings:
        logger.info(
            "[MIGRATION_VALIDATION_OK] checks_run=%d issues=0 warnings=0",
            checks_run,
        )
    elif ok:
        logger.warning(
            "[MIGRATION_VALIDATION_WARN] checks_run=%d issues=0 warnings=%d: %s",
            checks_run,
            len(warnings),
            "; ".join(warnings),
        )
    else:
        logger.error(
            "[MIGRATION_VALIDATION_FAIL] checks_run=%d issues=%d warnings=%d issues_detail=%s",
            checks_run,
            len(issues),
            len(warnings),
            "; ".join(issues),
        )

    return {
        "ok": ok,
        "issues": issues,
        "warnings": warnings,
        "checks_run": checks_run,
    }
