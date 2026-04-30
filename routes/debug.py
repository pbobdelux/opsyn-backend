import logging
import os
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
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

    # Log the active DATABASE_URL being used
    database_url = os.getenv("DATABASE_URL", "NOT_SET")
    if database_url and database_url != "NOT_SET":
        try:
            parsed = urlparse(database_url)
            db_host = parsed.hostname or "unknown"
            db_port = parsed.port or 5432
            db_name = parsed.path.lstrip("/") or "unknown"

            logger.info(
                "[DB] debug_check_active_url host=%s port=%s database=%s",
                db_host,
                db_port,
                db_name,
            )
        except Exception as exc:
            logger.error("[DB] debug_check_parse_error error=%s", exc)
            db_host = "unknown"
            db_port = 5432
            db_name = "unknown"
    else:
        logger.error("[DB] debug_check DATABASE_URL not set")
        db_host = "unknown"
        db_port = 5432
        db_name = "unknown"

    logger.info("[DB] debug_db_check_request brand=%s", brand)

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
        credential_exists = False
        credential_id = None
        company_id = None
        api_key_len = None
        is_active = None

        try:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand,
                    BrandAPICredential.integration_name == "leaflink",
                )
            )
            cred = result.scalar_one_or_none()

            if cred:
                credential_exists = True
                credential_id = cred.id
                company_id = cred.company_id
                api_key_len = len((cred.api_key or "").strip())
                is_active = cred.is_active

                logger.info(
                    "[DB] brand_credential_found brand=%s credential_id=%s key_len=%s is_active=%s",
                    brand,
                    credential_id,
                    api_key_len,
                    is_active,
                )
            else:
                logger.warning("[DB] brand_credential_not_found brand=%s", brand)

        except Exception as exc:
            logger.error("[DB] brand_credential_query_error brand=%s error=%s", brand, exc)

        return {
            "ok": True,
            "database_name": db_name,
            "brand_id": brand,
            "credential_exists": credential_exists,
            "credential_id": credential_id,
            "company_id": company_id,
            "api_key_len": api_key_len,
            "is_active": is_active,
            "brand_api_credentials_count": total_count,
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
