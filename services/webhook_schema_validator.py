"""
Webhook schema validator for opsyn-backend startup checks.

Validates that all tables and columns required by the webhook-status endpoint
exist in the live database before the application starts serving traffic.

Called from main.py lifespan startup with [WEBHOOK_SCHEMA_VALIDATION] log marker.

Returns a structured dict so callers can decide whether to warn or crash:
  {
    "valid": bool,
    "missing_tables": list[str],
    "missing_columns": dict[str, list[str]],   # {table: [missing_col, ...]}
    "warnings": list[str],
    "errors": list[str],
  }
"""

import logging
from typing import Any

logger = logging.getLogger("webhook_schema_validator")

# Tables required by the webhook-status endpoint
_WEBHOOK_TABLES = {
    "leaflink_webhook_events",
    "brand_api_credentials",
}

# Columns required on brand_api_credentials for webhook functionality
_BRAND_API_CREDENTIAL_WEBHOOK_COLUMNS = [
    "webhook_key_secret_ref",
    "webhook_key_last4",
    "webhook_enabled",
    "webhook_signature_required",
    "leaflink_company_id",
]

# Columns required on leaflink_webhook_events
_WEBHOOK_EVENT_COLUMNS = [
    "id",
    "brand_id",
    "org_id",
    "event_type",
    "idempotency_key",
    "raw_payload",
    "tenant_resolution_status",
    "signature_verification_status",
    "signature_verification_error",
    "duplicate_of_event_id",
    "enqueued_job_id",
    "processing_error",
    "received_at",
    "processed_at",
    "created_at",
]

# Map of table → required columns
_REQUIRED_COLUMNS: dict[str, list[str]] = {
    "brand_api_credentials": _BRAND_API_CREDENTIAL_WEBHOOK_COLUMNS,
    "leaflink_webhook_events": _WEBHOOK_EVENT_COLUMNS,
}


async def validate_webhook_schema(db: Any) -> dict:
    """
    Validate that all webhook-related tables and columns exist in the database.

    Args:
        db: An active AsyncSession (SQLAlchemy async session).

    Returns:
        {
          "valid": bool,
          "missing_tables": list[str],
          "missing_columns": dict[str, list[str]],
          "warnings": list[str],
          "errors": list[str],
        }

    Never raises — all exceptions are caught and reported in the result dict.
    """
    from sqlalchemy import text

    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    warnings: list[str] = []
    errors: list[str] = []

    # -----------------------------------------------------------------------
    # 1. Check required tables exist
    # -----------------------------------------------------------------------
    try:
        result = await db.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = ANY(:tables)
            """),
            {"tables": list(_WEBHOOK_TABLES)},
        )
        found_tables = {row[0] for row in result.fetchall()}
        missing_tables = sorted(_WEBHOOK_TABLES - found_tables)

        if missing_tables:
            for tbl in missing_tables:
                msg = (
                    f"[WEBHOOK_SCHEMA_VALIDATION] table_missing table={tbl} "
                    f"action=run_migration_2026_05_27_02_add_webhook_event_status_fields"
                )
                warnings.append(msg)
                logger.warning(
                    "[WEBHOOK_SCHEMA_VALIDATION] table_missing table=%s "
                    "action=run_migration_2026_05_27_02_add_webhook_event_status_fields",
                    tbl,
                )
        else:
            logger.info(
                "[WEBHOOK_SCHEMA_VALIDATION] all_webhook_tables_present tables=%s",
                sorted(found_tables & _WEBHOOK_TABLES),
            )

    except Exception as exc:
        msg = f"[WEBHOOK_SCHEMA_VALIDATION] table_check_failed error={str(exc)[:300]}"
        errors.append(msg)
        logger.error(
            "[WEBHOOK_SCHEMA_VALIDATION] table_check_failed error=%s",
            str(exc)[:300],
            exc_info=True,
        )
        # Cannot check columns if table check failed
        return {
            "valid": False,
            "missing_tables": missing_tables,
            "missing_columns": missing_columns,
            "warnings": warnings,
            "errors": errors,
        }

    # -----------------------------------------------------------------------
    # 2. Check required columns on each table (skip tables that don't exist)
    # -----------------------------------------------------------------------
    for table_name, required_cols in _REQUIRED_COLUMNS.items():
        if table_name in missing_tables:
            # Table doesn't exist — all columns are implicitly missing
            missing_columns[table_name] = required_cols
            logger.warning(
                "[WEBHOOK_SCHEMA_VALIDATION] skipping_column_check table=%s reason=table_missing",
                table_name,
            )
            continue

        try:
            result = await db.execute(
                text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                    ORDER BY column_name
                """),
                {"table_name": table_name},
            )
            existing_cols = {row[0] for row in result.fetchall()}
            table_missing = [c for c in required_cols if c not in existing_cols]

            if table_missing:
                missing_columns[table_name] = table_missing
                for col in table_missing:
                    msg = (
                        f"[WEBHOOK_SCHEMA_VALIDATION] column_missing "
                        f"table={table_name} column={col}"
                    )
                    warnings.append(msg)
                    logger.warning(
                        "[WEBHOOK_SCHEMA_VALIDATION] column_missing table=%s column=%s",
                        table_name,
                        col,
                    )
            else:
                logger.info(
                    "[WEBHOOK_SCHEMA_VALIDATION] all_columns_present table=%s columns=%s",
                    table_name,
                    required_cols,
                )

        except Exception as exc:
            msg = (
                f"[WEBHOOK_SCHEMA_VALIDATION] column_check_failed "
                f"table={table_name} error={str(exc)[:300]}"
            )
            errors.append(msg)
            logger.error(
                "[WEBHOOK_SCHEMA_VALIDATION] column_check_failed table=%s error=%s",
                table_name,
                str(exc)[:300],
                exc_info=True,
            )

    # -----------------------------------------------------------------------
    # 3. Summary
    # -----------------------------------------------------------------------
    valid = len(missing_tables) == 0 and len(missing_columns) == 0 and len(errors) == 0

    if valid:
        logger.info(
            "[WEBHOOK_SCHEMA_VALIDATION] validation_passed "
            "tables_ok=true columns_ok=true"
        )
    else:
        logger.warning(
            "[WEBHOOK_SCHEMA_VALIDATION] validation_failed "
            "missing_tables=%s missing_columns=%s errors=%s warnings=%s",
            missing_tables,
            missing_columns,
            len(errors),
            len(warnings),
        )

    return {
        "valid": valid,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "warnings": warnings,
        "errors": errors,
    }
