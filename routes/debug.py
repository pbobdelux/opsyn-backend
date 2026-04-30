import logging
import os
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential

logger = logging.getLogger("opsyn-backend")
router = APIRouter(prefix="/debug", tags=["debug"])


@router.get("/db-check")
async def debug_db_check(
    brand: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Debug endpoint to diagnose database connection and tenant credentials."""
    logger.info("[DB] debug_db_check_request brand=%s", brand)

    # Parse DATABASE_URL
    database_url = os.getenv("DATABASE_URL", "")
    db_host = "unknown"
    db_port = 5432
    db_name = "unknown"

    if database_url:
        try:
            parsed = urlparse(database_url)
            db_host = parsed.hostname or "unknown"
            db_port = parsed.port or 5432
            db_name = parsed.path.lstrip("/") or "unknown"
        except Exception as exc:
            logger.error("[DB] parse_error error=%s", exc)

    # Count total credentials
    total_count = None
    try:
        count_result = await db.execute(
            select(func.count(BrandAPICredential.id))
        )
        total_count = count_result.scalar_one() or 0
    except Exception as exc:
        logger.error("[DB] count_error error=%s", exc)

    # Check for specific brand if provided
    if brand:
        logger.info("[DB] debug_db_check_request brand=%s", brand)

        # Use shared resolver
        from services.credential_resolver import resolve_brand_credential

        cred_row = await resolve_brand_credential(db, brand, "leaflink")

        if cred_row:
            credential_id = cred_row[0]
            credential_brand = cred_row[1]
            credential_integration = cred_row[2]
            company_id = cred_row[3]
            api_key = cred_row[4]
            is_active = cred_row[5]
            sync_status = cred_row[6]
            last_synced_page = cred_row[7]

            credential_exists = True
            api_key_len = len(api_key) if api_key else None
            resolver_match_found = True
        else:
            credential_exists = False
            credential_id = None
            company_id = None
            api_key_len = None
            is_active = None
            sync_status = None
            last_synced_page = None
            resolver_match_found = False

        return {
            "ok": True,
            "database_name": db_name,
            "brand_id": brand,
            "credential_exists": credential_exists,
            "credential_id": credential_id,
            "company_id": company_id,
            "api_key_len": api_key_len,
            "is_active": is_active,
            "sync_status": sync_status,
            "last_synced_page": last_synced_page,
            "brand_api_credentials_count": total_count,
            "resolver_match_found": resolver_match_found,
            "credential_source": "db_only",
            "env_checked": False,
        }
    else:
        # Return general database info
        return {
            "ok": True,
            "database_url_host": db_host,
            "database_url_port": db_port,
            "database_name": db_name,
            "brand_api_credentials_count": total_count,
            "credential_source": "db_only",
            "env_checked": False,
        }


@router.get("/brand-api-credentials")
async def debug_brand_api_credentials(
    db: AsyncSession = Depends(get_db),
):
    """Debug endpoint to inspect all brand_api_credentials rows with exact values."""

    logger.info("[DEBUG] brand_api_credentials_dump_request")

    try:
        result = await db.execute(
            text("""
                SELECT
                    id,
                    brand_id,
                    LENGTH(brand_id) AS brand_id_length,
                    quote_literal(brand_id) AS quoted_brand_id,
                    integration_name,
                    LENGTH(integration_name) AS integration_name_length,
                    quote_literal(integration_name) AS quoted_integration_name,
                    company_id,
                    is_active,
                    sync_status,
                    last_synced_page,
                    LENGTH(api_key) AS api_key_len,
                    created_at,
                    updated_at
                FROM brand_api_credentials
                ORDER BY created_at DESC
            """)
        )

        rows = result.fetchall()

        credentials = []
        for row in rows:
            credentials.append({
                "id": row[0],
                "brand_id": row[1],
                "brand_id_length": row[2],
                "quoted_brand_id": row[3],
                "integration_name": row[4],
                "integration_name_length": row[5],
                "quoted_integration_name": row[6],
                "company_id": row[7],
                "is_active": row[8],
                "sync_status": row[9],
                "last_synced_page": row[10],
                "api_key_len": row[11],
                "created_at": row[12],
                "updated_at": row[13],
            })

        logger.info("[DEBUG] brand_api_credentials_dump total_rows=%s", len(credentials))

        return {
            "ok": True,
            "total_credentials": len(credentials),
            "credentials": credentials,
        }

    except Exception as exc:
        logger.error("[DEBUG] brand_api_credentials_dump_error error=%s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))
