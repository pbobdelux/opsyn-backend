import logging
import os
from urllib.parse import urlparse

from fastapi import APIRouter, Depends
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential

logger = logging.getLogger("opsyn-backend")
router = APIRouter(prefix="/debug", tags=["debug"])


@router.get("/db-check")
async def debug_db_check(db: AsyncSession = Depends(get_db)):
    """
    Debug endpoint to diagnose database connection.

    Returns which database backend is connected to and what credentials exist.
    """
    logger.info("[DB] debug_db_check_request")

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

    # Check for noble-nectar
    noble_nectar_exists = False
    noble_nectar_key_len = None
    cred = None

    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == "noble-nectar",
                BrandAPICredential.integration_name == "leaflink",
            )
        )
        cred = result.scalar_one_or_none()

        if cred:
            noble_nectar_exists = True
            api_key = (cred.api_key or "").strip()
            noble_nectar_key_len = len(api_key)

            logger.info(
                "[DB] noble_nectar_found key_len=%s company_id=%s is_active=%s",
                noble_nectar_key_len,
                cred.company_id,
                cred.is_active,
            )
        else:
            logger.warning("[DB] noble_nectar_not_found")

    except Exception as exc:
        logger.error("[DB] noble_nectar_query_error error=%s", exc)

    # Get database version
    db_version = "unknown"
    try:
        version_result = await db.execute(text("SELECT version()"))
        version_row = version_result.scalar_one_or_none()
        if version_row:
            db_version = version_row.split(",")[0] if version_row else "unknown"
    except Exception as exc:
        logger.error("[DB] version_query_error error=%s", exc)

    logger.info(
        "[DB] debug_response host=%s port=%s db=%s total_creds=%s noble_nectar=%s key_len=%s",
        db_host,
        db_port,
        db_name,
        total_count,
        noble_nectar_exists,
        noble_nectar_key_len,
    )

    return {
        "ok": True,
        "database_url_host": db_host,
        "database_url_port": db_port,
        "database_name": db_name,
        "database_version": db_version,
        "brand_api_credentials_count": total_count,
        "noble_nectar_exists": noble_nectar_exists,
        "noble_nectar_key_len": noble_nectar_key_len,
        "noble_nectar_company_id": cred.company_id if cred else None,
        "noble_nectar_is_active": cred.is_active if cred else None,
        "credential_source": "db_only",
        "env_checked": False,
    }
