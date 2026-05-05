import logging
import os
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


def extract_db_host(database_url: str) -> str:
    """Extract hostname from DATABASE_URL for logging (no credentials exposed)."""
    try:
        if "@" in database_url:
            host_part = database_url.split("@")[1]
            host = host_part.split(":")[0]
            return host
        return "unknown"
    except Exception:
        return "unknown"


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

    Does NOT filter by org_id — org_id validation must be done separately
    through the brands table before calling this function.

    Returns tuple: (id, brand_id, integration_name, company_id, api_key, is_active, sync_status, last_synced_page, base_url, auth_scheme)
    or None if not found.
    """

    if not brand_id or not integration_name:
        logger.error("[RESOLVER] missing_brand_or_integration brand=%s integration=%s", brand_id, integration_name)
        return None

    normalized_brand = brand_id.strip().lower()
    normalized_integration = integration_name.strip().lower()

    _db_host = extract_db_host(os.getenv("DATABASE_URL", ""))

    logger.info(
        "[CredentialLookup] db_host=%s brand_id=%s integration_name=%s is_active=true",
        _db_host,
        brand_id,
        integration_name,
    )
    logger.info(
        "[CredentialResolver] lookup_start brand_id=%s integration_name=%s allow_env_fallback=false",
        brand_id,
        integration_name,
    )
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
            # row indices: 0=id, 1=brand_id, 2=integration_name, 3=company_id,
            #              4=api_key, 5=is_active, 6=sync_status, 7=last_synced_page,
            #              8=base_url, 9=auth_scheme
            _api_key_raw = row[4] or ""
            _api_key_len = len(_api_key_raw.strip())
            logger.info(
                "[CredentialLookup] row_count=1 is_active=%s api_key_present=%s"
                " api_key_len=%s base_url=%s auth_scheme=%s",
                row[5],
                bool(_api_key_raw.strip()),
                _api_key_len,
                row[8],
                row[9],
            )
            logger.info(
                "[CredentialResolver] lookup_result brand_id=%s integration_name=%s"
                " row_count=1 is_active=%s api_key_present=%s base_url=%s auth_scheme=%s",
                brand_id,
                integration_name,
                row[5],
                bool(row[4]),
                row[8],
                row[9],
            )
            logger.info(
                "[RESOLVER] credential_found id=%s brand=%s integration=%s company_id=%s",
                row[0],
                row[1],
                row[2],
                row[3],
            )
            return row
        else:
            logger.info(
                "[CredentialLookup] row_count=0 reason=no_rows_found brand_id=%s integration_name=%s",
                brand_id,
                integration_name,
            )
            logger.info(
                "[CredentialResolver] lookup_result brand_id=%s integration_name=%s row_count=0",
                brand_id,
                integration_name,
            )
            logger.error(
                "[CredentialResolver] lookup_failed brand_id=%s integration_name=%s reason=no_rows_found",
                brand_id,
                integration_name,
            )
            logger.warning(
                "[RESOLVER] credential_not_found normalized_brand=%s normalized_integration=%s",
                normalized_brand,
                normalized_integration,
            )
            return None

    except Exception as exc:
        logger.error("[RESOLVER] lookup_error error=%s", exc, exc_info=True)
        return None
