"""
Bootstrap schema recovery for opsyn-backend.

Executes PURE SQL schema recovery BEFORE any ORM model imports.  This module
must NEVER import any SQLAlchemy ORM models — it operates exclusively via raw
SQL through the engine's connection interface.

The recovery is designed to run at the very start of the FastAPI lifespan()
function, before any ORM-mapped classes are imported.  This prevents the
SQLAlchemy ORM from raising UndefinedColumnError when it validates the live
schema against the model definition.

Design goals:
  - Pure SQL only: no ORM imports, no model references.
  - Idempotent: every statement uses IF NOT EXISTS guards.
  - Non-destructive: only ADDs columns / tables; never drops or modifies.
  - Fully logged: every action emits a structured log line with a grep-able
    marker so Railway log tailing can confirm recovery status.
  - Returns a structured result dict so main.py can decide whether to
    hard-fail or continue.

Log markers emitted:
  [BOOTSTRAP_SCHEMA_CHECK_START]      — bootstrap recovery starting
  [BOOTSTRAP_SCHEMA_RECOVERY_START]   — recovery SQL executing (columns/tables missing)
  [BOOTSTRAP_SCHEMA_RECOVERY_SUCCESS] — recovery completed successfully
  [BOOTSTRAP_SCHEMA_RECOVERY_FAILED]  — recovery failed (hard fail)
  [BOOTSTRAP_SCHEMA_CHECK_COMPLETE]   — bootstrap phase complete, safe to import ORM
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import text

logger = logging.getLogger("bootstrap_schema_recovery")

# ---------------------------------------------------------------------------
# Critical columns that MUST exist on brand_api_credentials
# ---------------------------------------------------------------------------
_CRITICAL_CREDENTIAL_COLUMNS: list[tuple[str, str]] = [
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

# ---------------------------------------------------------------------------
# leaflink_webhook_events table DDL
# ---------------------------------------------------------------------------
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


async def bootstrap_schema_recovery(engine) -> dict:
    """
    Execute pure SQL schema recovery BEFORE any ORM imports.

    Takes a SQLAlchemy async engine (not an AsyncSession) and operates
    directly on a raw connection so that no ORM machinery is involved.

    This function MUST be called before any ORM model classes are imported.
    It ensures that all critical columns and tables exist in the live schema
    so that subsequent ORM imports do not raise UndefinedColumnError.

    Args:
        engine: SQLAlchemy AsyncEngine instance (from database.engine).

    Returns:
        {
            "ok": bool,
            "columns_added": list[str],
            "tables_created": list[str],
            "indexes_created": list[str],
            "errors": list[str],
            "recovered_at": str | None,   # ISO-8601 UTC timestamp
        }
    """
    logger.info("[BOOTSTRAP_SCHEMA_CHECK_START] starting pure-SQL bootstrap schema recovery")

    columns_added: list[str] = []
    tables_created: list[str] = []
    indexes_created: list[str] = []
    errors: list[str] = []

    if engine is None:
        err = "engine is None — cannot run bootstrap schema recovery"
        logger.error("[BOOTSTRAP_SCHEMA_RECOVERY_FAILED] %s", err)
        return {
            "ok": False,
            "columns_added": [],
            "tables_created": [],
            "indexes_created": [],
            "errors": [err],
            "recovered_at": None,
        }

    try:
        async with engine.connect() as conn:
            # ------------------------------------------------------------------
            # 1. Query existing columns on brand_api_credentials
            # ------------------------------------------------------------------
            try:
                result = await conn.execute(
                    text("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'brand_api_credentials'
                    """)
                )
                existing_cols = {row[0] for row in result.fetchall()}
                logger.info(
                    "[BOOTSTRAP_SCHEMA_CHECK_START] brand_api_credentials existing_columns=%s",
                    sorted(existing_cols),
                )
            except Exception as exc:
                err = f"column_check_failed error={str(exc)[:300]}"
                logger.error("[BOOTSTRAP_SCHEMA_RECOVERY_FAILED] %s", err, exc_info=True)
                return {
                    "ok": False,
                    "columns_added": [],
                    "tables_created": [],
                    "indexes_created": [],
                    "errors": [err],
                    "recovered_at": None,
                }

            # ------------------------------------------------------------------
            # 2. Check if leaflink_webhook_events table exists
            # ------------------------------------------------------------------
            try:
                result = await conn.execute(
                    text("""
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'leaflink_webhook_events'
                    """)
                )
                events_table_exists = result.fetchone() is not None
            except Exception as exc:
                err = f"table_check_failed error={str(exc)[:300]}"
                logger.warning(
                    "[BOOTSTRAP_SCHEMA_CHECK_START] %s — assuming table missing", err
                )
                events_table_exists = False  # Assume missing; DDL is idempotent

            # ------------------------------------------------------------------
            # 3. Determine what needs recovery
            # ------------------------------------------------------------------
            missing_cols = [
                (col_name, col_sql)
                for col_name, col_sql in _CRITICAL_CREDENTIAL_COLUMNS
                if col_name not in existing_cols
            ]
            recovery_needed = bool(missing_cols) or not events_table_exists

            if not recovery_needed:
                logger.info(
                    "[BOOTSTRAP_SCHEMA_CHECK_COMPLETE] all_critical_columns_present=true "
                    "events_table_exists=true no_recovery_needed=true"
                )
                return {
                    "ok": True,
                    "columns_added": [],
                    "tables_created": [],
                    "indexes_created": [],
                    "errors": [],
                    "recovered_at": None,
                }

            # ------------------------------------------------------------------
            # 4. Recovery needed — log and execute
            # ------------------------------------------------------------------
            logger.warning(
                "[BOOTSTRAP_SCHEMA_RECOVERY_START] recovery_needed=true "
                "missing_columns=%s events_table_missing=%s",
                [c for c, _ in missing_cols],
                not events_table_exists,
            )

            # Add missing credential columns (each in its own savepoint so one
            # failure does not block the others)
            for col_name, col_sql in missing_cols:
                try:
                    await conn.execute(text(col_sql))
                    await conn.commit()
                    columns_added.append(col_name)
                    logger.info(
                        "[BOOTSTRAP_SCHEMA_RECOVERY_START] column_added "
                        "table=brand_api_credentials column=%s",
                        col_name,
                    )
                except Exception as exc:
                    # Roll back the failed statement so the connection stays usable
                    try:
                        await conn.rollback()
                    except Exception:
                        pass
                    err = f"add_column_failed column={col_name} error={str(exc)[:300]}"
                    errors.append(err)
                    logger.error(
                        "[BOOTSTRAP_SCHEMA_RECOVERY_FAILED] %s", err, exc_info=True
                    )

            # Create leaflink_webhook_events table if missing
            if not events_table_exists:
                try:
                    await conn.execute(text(_WEBHOOK_EVENTS_TABLE_SQL))
                    await conn.commit()
                    tables_created.append("leaflink_webhook_events")
                    logger.info(
                        "[BOOTSTRAP_SCHEMA_RECOVERY_START] table_created "
                        "table=leaflink_webhook_events"
                    )
                except Exception as exc:
                    try:
                        await conn.rollback()
                    except Exception:
                        pass
                    err = (
                        f"create_table_failed table=leaflink_webhook_events "
                        f"error={str(exc)[:300]}"
                    )
                    errors.append(err)
                    logger.error(
                        "[BOOTSTRAP_SCHEMA_RECOVERY_FAILED] %s", err, exc_info=True
                    )

            # Create indexes (IF NOT EXISTS guards make these safe to always attempt)
            for idx_name, idx_sql in _WEBHOOK_EVENTS_INDEX_SQL:
                try:
                    await conn.execute(text(idx_sql))
                    await conn.commit()
                    indexes_created.append(idx_name)
                    logger.info(
                        "[BOOTSTRAP_SCHEMA_RECOVERY_START] index_created index=%s",
                        idx_name,
                    )
                except Exception as exc:
                    try:
                        await conn.rollback()
                    except Exception:
                        pass
                    err = f"create_index_failed index={idx_name} error={str(exc)[:300]}"
                    errors.append(err)
                    logger.error(
                        "[BOOTSTRAP_SCHEMA_RECOVERY_FAILED] %s", err, exc_info=True
                    )

    except Exception as exc:
        err = f"bootstrap_connection_failed error={str(exc)[:300]}"
        logger.error("[BOOTSTRAP_SCHEMA_RECOVERY_FAILED] %s", err, exc_info=True)
        return {
            "ok": False,
            "columns_added": columns_added,
            "tables_created": tables_created,
            "indexes_created": indexes_created,
            "errors": errors + [err],
            "recovered_at": None,
        }

    # ------------------------------------------------------------------
    # 5. Determine overall result
    # ------------------------------------------------------------------
    ok = len(errors) == 0
    recovered_at = datetime.now(timezone.utc).isoformat() if (columns_added or tables_created) else None

    if ok:
        logger.info(
            "[BOOTSTRAP_SCHEMA_RECOVERY_SUCCESS] columns_added=%s tables_created=%s "
            "indexes_created=%s",
            columns_added,
            tables_created,
            indexes_created,
        )
    else:
        logger.error(
            "[BOOTSTRAP_SCHEMA_RECOVERY_FAILED] errors=%s columns_added=%s "
            "tables_created=%s",
            errors,
            columns_added,
            tables_created,
        )

    logger.info(
        "[BOOTSTRAP_SCHEMA_CHECK_COMPLETE] ok=%s columns_added=%d tables_created=%d "
        "indexes_created=%d errors=%d",
        ok,
        len(columns_added),
        len(tables_created),
        len(indexes_created),
        len(errors),
    )

    return {
        "ok": ok,
        "columns_added": columns_added,
        "tables_created": tables_created,
        "indexes_created": indexes_created,
        "errors": errors,
        "recovered_at": recovered_at,
    }
