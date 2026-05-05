import logging
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def resolve_brand_credential(
    db: AsyncSession,
    brand_id: str,
    integration_name: str = "leaflink",
) -> Optional[Tuple]:
    """
    Unified credential resolver with normalization.

    Normalizes brand_id and integration_name before lookup:
    - TRIM whitespace
    - LOWER case
    - is_active = true

    Returns tuple: (id, brand_id, integration_name, company_id, api_key, is_active, sync_status, last_synced_page, base_url, auth_scheme)
    or None if not found.
    """

    if not brand_id or not integration_name:
        logger.error("[RESOLVER] missing_brand_or_integration brand=%s integration=%s", brand_id, integration_name)
        return None

    normalized_brand = brand_id.strip().lower()
    normalized_integration = integration_name.strip().lower()

    logger.info(
        "[RESOLVER] resolving_credential normalized_brand=%s normalized_integration=%s",
        normalized_brand,
        normalized_integration,
    )

    try:
        result = await db.execute(
            text("""
                SELECT
                    id,
                    brand_id,
                    integration_name,
                    company_id,
                    api_key,
                    is_active,
                    sync_status,
                    last_synced_page,
                    base_url,
                    auth_scheme
                FROM brand_api_credentials
                WHERE TRIM(LOWER(brand_id)) = :normalized_brand
                AND TRIM(LOWER(integration_name)) = :normalized_integration
                AND is_active = true
                LIMIT 1
            """),
            {
                "normalized_brand": normalized_brand,
                "normalized_integration": normalized_integration,
            }
        )

        row = result.fetchone()

        if row:
            logger.info(
                "[RESOLVER] credential_found id=%s brand=%s integration=%s company_id=%s",
                row[0],
                row[1],
                row[2],
                row[3],
            )
            return row
        else:
            logger.warning(
                "[RESOLVER] credential_not_found normalized_brand=%s normalized_integration=%s",
                normalized_brand,
                normalized_integration,
            )
            return None

    except Exception as exc:
        logger.error("[RESOLVER] lookup_error error=%s", exc, exc_info=True)
        return None
