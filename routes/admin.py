import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

# Admin seed token from environment
ADMIN_SEED_TOKEN = os.getenv("ADMIN_SEED_TOKEN", "opsyn-seed-2026")


@router.post("/seed-noble-nectar-credential")
async def seed_noble_nectar_credential(
    x_admin_seed_token: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Temporary endpoint to seed noble-nectar LeafLink credential.

    Requires X-Admin-Seed-Token header.
    Uses the backend's existing database connection.
    """

    logger.info("[SEED] noble_nectar_seed_start")

    # Validate token
    if not x_admin_seed_token:
        logger.error("[SEED] missing_token")
        raise HTTPException(status_code=403, detail="Missing X-Admin-Seed-Token header")

    if x_admin_seed_token != ADMIN_SEED_TOKEN:
        logger.error("[SEED] invalid_token")
        raise HTTPException(status_code=403, detail="Invalid X-Admin-Seed-Token")

    try:
        # Execute UPSERT
        logger.info("[SEED] executing_upsert")

        await db.execute(
            text("""
                INSERT INTO brand_api_credentials (
                    brand_id,
                    integration_name,
                    company_id,
                    api_key,
                    is_active,
                    created_at,
                    updated_at
                ) VALUES (
                    'noble-nectar',
                    'leaflink',
                    '9008',
                    'daa1586d10978bb5bc104b0fc63685ae47a6308e',
                    true,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (brand_id, integration_name)
                DO UPDATE SET
                    company_id = EXCLUDED.company_id,
                    api_key = EXCLUDED.api_key,
                    is_active = true,
                    updated_at = NOW()
            """)
        )

        await db.commit()
        logger.info("[SEED] upsert_committed")

        # Verify
        logger.info("[SEED] verifying_credential")

        result = await db.execute(
            text("""
                SELECT brand_id, company_id, is_active, LENGTH(api_key) AS key_len
                FROM brand_api_credentials
                WHERE brand_id='noble-nectar'
                AND integration_name='leaflink'
            """)
        )

        row = result.fetchone()

        if not row:
            logger.error("[SEED] verification_failed credential_not_found")
            raise RuntimeError("Credential not found after insert")

        brand_id, company_id, is_active, key_len = row

        logger.info(
            "[SEED] noble_nectar_seed_success key_len=%s company_id=%s",
            key_len,
            company_id,
        )

        return {
            "ok": True,
            "seeded": True,
            "brand_id": brand_id,
            "company_id": company_id,
            "is_active": is_active,
            "api_key_len": key_len,
        }

    except HTTPException:
        raise

    except Exception as exc:
        logger.error("[SEED] seed_error error=%s", exc, exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Seed failed: {str(exc)}",
        )
