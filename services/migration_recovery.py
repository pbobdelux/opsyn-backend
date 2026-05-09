"""
Emergency self-healing migration recovery for opsyn-backend.

Executed at startup AFTER the normal migration runner completes.  If any
critical webhook schema columns are missing (because migrations were skipped
or silently failed on Railway), this module executes the required ALTER TABLE
and CREATE TABLE statements directly via raw SQL and commits them.

Design goals:
  - Idempotent: every statement uses IF NOT EXISTS / IF NOT EXISTS guards.
  - Non-destructive: only ADDs columns / tables; never drops or modifies.
  - Fully logged: every action emits a structured log line with a grep-able
    marker so Railway log tailing can confirm recovery status.
  - Returns a structured result dict so main.py can decide whether to
    hard-fail or continue.

Log markers emitted:
  [WEBHOOK_SCHEMA_AUTO_RECOVERY] — recovery SQL executed (columns added / tables created)
  [WEBHOOK_SCHEMA_VERIFIED]      — post-recovery column verification passed
  [WEBHOOK_SCHEMA_RECOVERY_SKIP] — all columns already present; no action needed
  [WEBHOOK_SCHEMA_RECOVERY_ERROR]— unexpected error during recovery
"""

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("migration_recovery")

# ---------------------------------------------------------------------------
# Critical columns that MUST exist on brand_api_credentials
# ---------------------------------------------------------------------------
_CRITICAL_CREDENTIAL_COLUMNS = [
    "webhook_key_secret_ref",
    "webhook_key_last4",
    "webhook_enabled",
    "webhook_signature_required",
    "leaflink_company_id",
]

# ---------------------------------------------------------------------------
# Recovery SQL — each statement is idempotent (IF NOT EXISTS)
# ---------------------------------------------------------------------------

_CREDENTIAL_COLUMN_SQL: list[tuple[str, str]] = [
    (
        "webhook_key_secret_ref",
        "ALTER TABLE brand_api_credentials "
        "ADD COLUMN IF NOT EXISTS webhook_key_secret_ref VARCHAR(255)",
    ),
    (
        "webhook_key_last4",
        "ALTER TABLE brand_api_credentials "
        "ADD COLUMN IF NOT EXISTS webhook_key_last4 VARCHAR(4)",
    ),
    (
        "webhook_enabled",
        "ALTER TABLE brand_api_credentials "
        "ADD COLUMN IF NOT EXISTS webhook_enabled BOOLEAN NOT NULL DEFAULT false",
    ),
    (
        "webhook_signature_required",
        "ALTER TABLE brand_api_credentials "
        "ADD COLUMN IF NOT EXISTS webhook_signature_required BOOLEAN NOT NULL DEFAULT true",
    ),
    (
        "leaflink_company_id",
        "ALTER TABLE brand_api_credentials "
        "ADD COLUMN IF NOT EXISTS leaflink_company_id VARCHAR(120)",
    ),
]

_WEBHOOK_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS leaflink_webhook_events (
    id                            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id                      VARCHAR(120),
    org_id                        VARCHAR(120),
    event_type                    VARCHAR(80),
    idempotency_key               VARCHAR(255),
    raw_payload                   JSONB,
    tenant_resolution_status      VARCHAR(20) NOT NULL DEFAULT 'unresolved',
    signature_verification_status VARCHAR(20),
    signature_verification_error  TEXT,
    duplicate_of_event_id         UUID REFERENCES leaflink_webhook_events(id) ON DELETE SET NULL,
    enqueued_job_id               INTEGER,
    processing_error              TEXT,
    received_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at                  TIMESTAMPTZ,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_WEBHOOK_EVENTS_INDEX_SQL: list[tuple[str, str]] = [
    (
        "ix_leaflink_webhook_events_brand_id",
        "CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_brand_id "
        "ON leaflink_webhook_events (brand_id)",
    ),
    (
        "ix_leaflink_webhook_events_tenant_status",
        "CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_tenant_status "
        "ON leaflink_webhook_events (tenant_resolution_status)",
    ),
    (
        "ix_leaflink_webhook_events_received_at",
        "CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_received_at "
        "ON leaflink_webhook_events (received_at DESC)",
    ),
    (
        "ix_leaflink_webhook_events_sig_status",
        "CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_sig_status "
        "ON leaflink_webhook_events (signature_verification_status)",
    ),
    (
        "ix_leaflink_webhook_events_brand_idempotency",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_leaflink_webhook_events_brand_idempotency "
        "ON leaflink_webhook_events (brand_id, idempotency_key) "
        "WHERE brand_id IS NOT NULL",
    ),
]


async def _get_existing_credential_columns(db: AsyncSession) -> set[str]:
    """Query information_schema for current columns on brand_api_credentials."""
    from sqlalchemy import text

    result = await db.execute(
        text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'brand_api_credentials'
        """)
    )
    return {row[0] for row in result.fetchall()}


async def _webhook_events_table_exists(db: AsyncSession) -> bool:
    """Return True if leaflink_webhook_events table exists in the public schema."""
    from sqlalchemy import text

    result = await db.execute(
        text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'leaflink_webhook_events'
        """)
    )
    return result.fetchone() is not None


async def execute_webhook_schema_recovery(db: AsyncSession) -> dict:
    """
    Inspect the live schema and apply any missing webhook columns / tables.

    Executed at startup after the normal migration runner.  Uses the provided
    AsyncSession so it participates in the caller's connection lifecycle.

    Returns:
        {
          "recovery_needed": bool,
          "columns_added": list[str],
          "tables_created": list[str],
          "indexes_created": list[str],
          "status": "success" | "skipped" | "partial_failure",
          "errors": list[str],
          "recovered_at": str | None,   # ISO-8601 UTC timestamp
          "critical_columns_present": bool,
          "missing_after_recovery": list[str],
        }
    """
    from sqlalchemy import text

    columns_added: list[str] = []
    tables_created: list[str] = []
    indexes_created: list[str] = []
    errors: list[str] = []

    # ------------------------------------------------------------------
    # 1. Determine which credential columns are missing
    # ------------------------------------------------------------------
    try:
        existing_cols = await _get_existing_credential_columns(db)
    except Exception as exc:
        err = f"column_check_failed error={str(exc)[:300]}"
        logger.error("[WEBHOOK_SCHEMA_RECOVERY_ERROR] %s", err, exc_info=True)
        return {
            "recovery_needed": True,
            "columns_added": [],
            "tables_created": [],
            "indexes_created": [],
            "status": "partial_failure",
            "errors": [err],
            "recovered_at": None,
            "critical_columns_present": False,
            "missing_after_recovery": _CRITICAL_CREDENTIAL_COLUMNS,
        }

    missing_cols = [c for c in _CRITICAL_CREDENTIAL_COLUMNS if c not in existing_cols]

    # ------------------------------------------------------------------
    # 2. Determine if the webhook events table is missing
    # ------------------------------------------------------------------
    try:
        events_table_missing = not await _webhook_events_table_exists(db)
    except Exception as exc:
        err = f"table_check_failed error={str(exc)[:300]}"
        logger.error("[WEBHOOK_SCHEMA_RECOVERY_ERROR] %s", err, exc_info=True)
        events_table_missing = True  # Assume missing; recovery SQL is idempotent

    recovery_needed = bool(missing_cols) or events_table_missing

    if not recovery_needed:
        logger.info(
            "[WEBHOOK_SCHEMA_RECOVERY_SKIP] all_critical_columns_present "
            "events_table_exists=true no_recovery_needed=true"
        )
        return {
            "recovery_needed": False,
            "columns_added": [],
            "tables_created": [],
            "indexes_created": [],
            "status": "skipped",
            "errors": [],
            "recovered_at": None,
            "critical_columns_present": True,
            "missing_after_recovery": [],
        }

    logger.warning(
        "[WEBHOOK_SCHEMA_AUTO_RECOVERY] recovery_needed=true "
        "missing_columns=%s events_table_missing=%s",
        missing_cols,
        events_table_missing,
    )

    # ------------------------------------------------------------------
    # 3. Add missing credential columns (each in its own transaction so
    #    one failure does not block the others)
    # ------------------------------------------------------------------
    for col_name, col_sql in _CREDENTIAL_COLUMN_SQL:
        if col_name not in missing_cols:
            continue
        try:
            await db.execute(text(col_sql))
            await db.commit()
            columns_added.append(col_name)
            logger.info(
                "[WEBHOOK_SCHEMA_AUTO_RECOVERY] column_added table=brand_api_credentials "
                "column=%s",
                col_name,
            )
        except Exception as exc:
            await db.rollback()
            err = f"add_column_failed column={col_name} error={str(exc)[:300]}"
            errors.append(err)
            logger.error(
                "[WEBHOOK_SCHEMA_RECOVERY_ERROR] %s", err, exc_info=True
            )

    # ------------------------------------------------------------------
    # 4. Create leaflink_webhook_events table if missing
    # ------------------------------------------------------------------
    if events_table_missing:
        try:
            await db.execute(text(_WEBHOOK_EVENTS_TABLE_SQL))
            await db.commit()
            tables_created.append("leaflink_webhook_events")
            logger.info(
                "[WEBHOOK_SCHEMA_AUTO_RECOVERY] table_created table=leaflink_webhook_events"
            )
        except Exception as exc:
            await db.rollback()
            err = f"create_table_failed table=leaflink_webhook_events error={str(exc)[:300]}"
            errors.append(err)
            logger.error(
                "[WEBHOOK_SCHEMA_RECOVERY_ERROR] %s", err, exc_info=True
            )

    # ------------------------------------------------------------------
    # 5. Create indexes (always attempt — IF NOT EXISTS guards are safe)
    # ------------------------------------------------------------------
    for idx_name, idx_sql in _WEBHOOK_EVENTS_INDEX_SQL:
        try:
            await db.execute(text(idx_sql))
            await db.commit()
            indexes_created.append(idx_name)
            logger.info(
                "[WEBHOOK_SCHEMA_AUTO_RECOVERY] index_created index=%s", idx_name
            )
        except Exception as exc:
            await db.rollback()
            err = f"create_index_failed index={idx_name} error={str(exc)[:300]}"
            errors.append(err)
            logger.error(
                "[WEBHOOK_SCHEMA_RECOVERY_ERROR] %s", err, exc_info=True
            )

    # ------------------------------------------------------------------
    # 6. Post-recovery verification
    # ------------------------------------------------------------------
    try:
        post_cols = await _get_existing_credential_columns(db)
        still_missing = [c for c in _CRITICAL_CREDENTIAL_COLUMNS if c not in post_cols]
    except Exception as exc:
        logger.error(
            "[WEBHOOK_SCHEMA_RECOVERY_ERROR] post_recovery_verification_failed error=%s",
            str(exc)[:300],
            exc_info=True,
        )
        still_missing = _CRITICAL_CREDENTIAL_COLUMNS  # Assume worst case

    critical_columns_present = len(still_missing) == 0
    recovered_at = datetime.now(timezone.utc).isoformat()

    if critical_columns_present:
        logger.info(
            "[WEBHOOK_SCHEMA_VERIFIED] post_recovery_check=passed "
            "critical_columns_present=true column_count=%d "
            "columns_added=%s tables_created=%s",
            len(_CRITICAL_CREDENTIAL_COLUMNS),
            columns_added,
            tables_created,
        )
    else:
        logger.error(
            "[WEBHOOK_SCHEMA_RECOVERY_ERROR] post_recovery_check=failed "
            "still_missing=%s",
            still_missing,
        )

    status = "success" if not errors else "partial_failure"

    logger.info(
        "[WEBHOOK_SCHEMA_AUTO_RECOVERY] columns_added=%d tables_created=%d "
        "indexes_created=%d status=%s errors=%d",
        len(columns_added),
        len(tables_created),
        len(indexes_created),
        status,
        len(errors),
    )

    return {
        "recovery_needed": True,
        "columns_added": columns_added,
        "tables_created": tables_created,
        "indexes_created": indexes_created,
        "status": status,
        "errors": errors,
        "recovered_at": recovered_at,
        "critical_columns_present": critical_columns_present,
        "missing_after_recovery": still_missing,
    }


async def verify_critical_webhook_columns(db: AsyncSession) -> dict:
    """
    Verify that all critical webhook columns exist in the live schema.

    Intended to be called after both the migration runner and recovery have
    completed.  Returns a dict suitable for logging and hard-fail decisions.

    Returns:
        {
          "ok": bool,
          "critical_columns_present": bool,
          "column_status": {col: bool, ...},
          "missing": list[str],
        }
    """
    try:
        existing = await _get_existing_credential_columns(db)
        column_status = {c: (c in existing) for c in _CRITICAL_CREDENTIAL_COLUMNS}
        missing = [c for c, present in column_status.items() if not present]
        ok = len(missing) == 0

        if ok:
            logger.info(
                "[WEBHOOK_SCHEMA_VERIFIED] critical_columns_present=true "
                "column_count=%d",
                len(_CRITICAL_CREDENTIAL_COLUMNS),
            )
        else:
            logger.error(
                "[WEBHOOK_SCHEMA_CRITICAL_MISSING] missing_columns=%s "
                "action=hard_fail",
                missing,
            )

        return {
            "ok": ok,
            "critical_columns_present": ok,
            "column_status": column_status,
            "missing": missing,
        }

    except Exception as exc:
        logger.error(
            "[WEBHOOK_SCHEMA_RECOVERY_ERROR] verify_critical_columns_failed "
            "error=%s",
            str(exc)[:300],
            exc_info=True,
        )
        return {
            "ok": False,
            "critical_columns_present": False,
            "column_status": {},
            "missing": _CRITICAL_CREDENTIAL_COLUMNS,
        }
