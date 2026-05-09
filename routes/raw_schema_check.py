"""
Emergency raw schema check endpoint for opsyn-backend.

Uses ONLY raw SQL — no ORM imports allowed.  This endpoint is safe to call
even before ORM models have been validated against the live schema, making it
suitable for health checks and pre-ORM diagnostics.

Routes:
  GET /admin/raw-schema-check — query information_schema directly for critical columns

Log markers:
  [RAW_SCHEMA_CHECK] — emitted on each request with column presence results
"""

import logging

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db

logger = logging.getLogger("raw_schema_check")

router = APIRouter(tags=["admin"])

# Columns to check on brand_api_credentials
_WEBHOOK_COLUMNS = [
    "webhook_key_secret_ref",
    "webhook_enabled",
    "webhook_signature_required",
    "leaflink_company_id",
]


@router.get("/raw-schema-check")
async def raw_schema_check(
    db: AsyncSession = Depends(get_db),
):
    """
    Emergency schema check using ONLY raw SQL.

    Queries information_schema.columns directly — no ORM imports, no model
    references.  Returns the presence of each critical webhook column so that
    callers can confirm the schema is ready before making ORM-dependent calls.

    Returns:
        {
            "ok": bool,
            "webhook_key_secret_ref": bool,
            "webhook_enabled": bool,
            "webhook_signature_required": bool,
            "leaflink_company_id": bool,
            "leaflink_webhook_events_table": bool,
        }
    """
    result = await db.execute(
        text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'brand_api_credentials'
        """)
    )
    existing_cols = {row[0] for row in result.fetchall()}

    # Check if leaflink_webhook_events table exists
    table_result = await db.execute(
        text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'leaflink_webhook_events'
        """)
    )
    events_table_exists = table_result.fetchone() is not None

    column_status = {col: (col in existing_cols) for col in _WEBHOOK_COLUMNS}
    ok = all(column_status.values()) and events_table_exists

    logger.info(
        "[RAW_SCHEMA_CHECK] ok=%s columns=%s events_table=%s",
        ok,
        column_status,
        events_table_exists,
    )

    return {
        "ok": ok,
        **column_status,
        "leaflink_webhook_events_table": events_table_exists,
    }
