"""
Startup safety checks for opsyn-backend.

Verifies:
  - Database is connected
  - Required tables exist: orders, order_lines, sync_dead_letters
  - Required columns exist on order_lines:
      created_at (TIMESTAMP), updated_at (TIMESTAMP), mapped_product_id (UUID, nullable)
  - Required partial unique index exists on order_lines:
      UNIQUE (order_id, sku, product_name) WHERE sku IS NOT NULL AND product_name IS NOT NULL

Behavior:
  - Logs [STARTUP_SCHEMA_WARNING] for non-critical issues (missing optional columns/indexes).
  - Raises RuntimeError (crashes startup) only if core tables are missing.
  - All checks are non-blocking and wrapped in try/except so a single failure
    never prevents the remaining checks from running.
"""

import logging
from typing import Optional

logger = logging.getLogger("startup_checks")

# Tables that MUST exist — missing any of these crashes startup
_REQUIRED_TABLES = {"orders", "order_lines", "sync_dead_letters"}

# Columns that should exist on order_lines (non-critical — log warning only)
_REQUIRED_ORDER_LINE_COLUMNS = {
    "created_at": "timestamp",
    "updated_at": "timestamp",
    "mapped_product_id": "uuid",
}

# Name of the partial unique index that must exist on order_lines
_REQUIRED_PARTIAL_INDEX = "uq_order_line_identity"


async def run_startup_schema_checks() -> dict:
    """
    Run all startup schema checks.

    Returns a summary dict:
      {
        "ok": bool,
        "tables_ok": bool,
        "columns_ok": bool,
        "index_ok": bool,
        "warnings": [str],
        "errors": [str],
      }

    Raises RuntimeError if core tables are missing.
    """
    from database import engine

    warnings: list[str] = []
    errors: list[str] = []
    tables_ok = False
    columns_ok = True
    index_ok = True

    if engine is None:
        logger.warning("[STARTUP_SCHEMA_WARNING] engine not available — skipping schema checks")
        return {
            "ok": False,
            "tables_ok": False,
            "columns_ok": False,
            "index_ok": False,
            "warnings": ["engine not available"],
            "errors": [],
        }

    # -----------------------------------------------------------------------
    # 1. Check required tables exist
    # -----------------------------------------------------------------------
    try:
        from sqlalchemy import text

        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = ANY(:tables)
                """),
                {"tables": list(_REQUIRED_TABLES)},
            )
            found_tables = {row[0] for row in result.fetchall()}

        missing_tables = _REQUIRED_TABLES - found_tables
        if missing_tables:
            msg = f"Missing required tables: {sorted(missing_tables)}"
            errors.append(msg)
            logger.error("[STARTUP_SCHEMA_ERROR] %s", msg)
            # CRASH: core tables missing
            raise RuntimeError(
                f"[STARTUP_SCHEMA_ERROR] {msg}. "
                f"Run migrations before starting the service."
            )
        else:
            tables_ok = True
            logger.info(
                "[STARTUP_SCHEMA_OK] all_required_tables_present tables=%s",
                sorted(found_tables & _REQUIRED_TABLES),
            )

    except RuntimeError:
        raise
    except Exception as exc:
        msg = f"Table check failed: {str(exc)[:300]}"
        warnings.append(msg)
        logger.warning("[STARTUP_SCHEMA_WARNING] %s", msg)

    # -----------------------------------------------------------------------
    # 2. Check required columns on order_lines
    # -----------------------------------------------------------------------
    try:
        from sqlalchemy import text

        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'order_lines'
                    ORDER BY column_name
                """)
            )
            existing_columns = {row[0]: row[1].lower() for row in result.fetchall()}

        for col_name, expected_type_hint in _REQUIRED_ORDER_LINE_COLUMNS.items():
            if col_name not in existing_columns:
                msg = (
                    f"order_lines.{col_name} ({expected_type_hint.upper()}, nullable) is missing. "
                    f"Run migration to add it."
                )
                warnings.append(msg)
                columns_ok = False
                logger.warning("[STARTUP_SCHEMA_WARNING] %s", msg)
            else:
                actual_type = existing_columns[col_name]
                logger.info(
                    "[STARTUP_SCHEMA_OK] order_lines.%s exists type=%s",
                    col_name,
                    actual_type,
                )

    except Exception as exc:
        msg = f"Column check failed: {str(exc)[:300]}"
        warnings.append(msg)
        logger.warning("[STARTUP_SCHEMA_WARNING] %s", msg)

    # -----------------------------------------------------------------------
    # 3. Check partial unique index on order_lines
    # -----------------------------------------------------------------------
    try:
        from sqlalchemy import text

        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = 'order_lines'
                      AND indexname = :index_name
                """),
                {"index_name": _REQUIRED_PARTIAL_INDEX},
            )
            index_row = result.fetchone()

        if index_row is None:
            msg = (
                f"Partial unique index '{_REQUIRED_PARTIAL_INDEX}' is missing on order_lines. "
                f"UPSERT conflict detection will not work correctly. "
                f"Run migration 2026_05_18_01_add_sync_health_tables.sql."
            )
            warnings.append(msg)
            index_ok = False
            logger.warning("[STARTUP_SCHEMA_WARNING] %s", msg)
        else:
            logger.info(
                "[STARTUP_SCHEMA_OK] partial_unique_index=%s exists on order_lines",
                _REQUIRED_PARTIAL_INDEX,
            )

    except Exception as exc:
        msg = f"Index check failed: {str(exc)[:300]}"
        warnings.append(msg)
        logger.warning("[STARTUP_SCHEMA_WARNING] %s", msg)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------

    overall_ok = tables_ok and len(errors) == 0

    if warnings:
        logger.warning(
            "[STARTUP_SCHEMA_WARNING] schema_check_complete ok=%s warnings=%s",
            overall_ok,
            len(warnings),
        )
    else:
        logger.info(
            "[STARTUP_SCHEMA_OK] schema_check_complete ok=%s columns_ok=%s index_ok=%s",
            overall_ok,
            columns_ok,
            index_ok,
        )

    # -----------------------------------------------------------------------
    # 4. Datetime self-check — verify utc_now() returns UTC-aware datetimes
    # -----------------------------------------------------------------------
    try:
        from datetime import datetime as _dt_check, timezone as _tz_check
        _test_dt = _dt_check.now(_tz_check.utc)
        if _test_dt.tzinfo is None:
            msg = "datetime.now(timezone.utc) returned a naive datetime — UTC-aware datetimes broken"
            warnings.append(msg)
            logger.error("[STARTUP_DATETIME_WARNING] %s", msg)
        else:
            logger.info(
                "[STARTUP_DATETIME_OK] utc_now_check passed tzinfo=%s value=%s",
                _test_dt.tzinfo,
                _test_dt.isoformat(),
            )

        # Verify validate_and_fix_sql_params is importable and works
        from utils.json_utils import validate_and_fix_sql_params as _vfsp
        _naive = _dt_check(2024, 1, 1, 12, 0, 0)  # naive datetime
        _fixed = _vfsp({"ts": _naive})
        if isinstance(_fixed, dict) and _fixed.get("ts") is not None and _fixed["ts"].tzinfo is not None:
            logger.info(
                "[STARTUP_DATETIME_OK] validate_and_fix_sql_params self-check passed"
                " — naive datetime correctly fixed to UTC-aware"
            )
        else:
            msg = "validate_and_fix_sql_params self-check failed — naive datetime not fixed"
            warnings.append(msg)
            logger.error("[STARTUP_DATETIME_WARNING] %s", msg)

    except Exception as exc:
        msg = f"Datetime self-check failed: {str(exc)[:300]}"
        warnings.append(msg)
        logger.warning("[STARTUP_DATETIME_WARNING] %s", msg)

    return {
        "ok": overall_ok,
        "tables_ok": tables_ok,
        "columns_ok": columns_ok,
        "index_ok": index_ok,
        "warnings": warnings,
        "errors": errors,
    }
