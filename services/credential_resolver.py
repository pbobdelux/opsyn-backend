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

            # ---------------------------------------------------------------------------
            # LeafLink credential canonicalization — fix stale DB values before returning
            # ---------------------------------------------------------------------------
            if normalized_integration == "leaflink":
                _LEAFLINK_CANONICAL_BASE_URL = "https://www.leaflink.com/api/v2"

                # Unpack mutable copies of the fields we may need to rewrite
                _raw_base_url = row[8] or ""
                _raw_auth_scheme = row[9] or ""

                _canonical_base_url = _raw_base_url
                _canonical_auth_scheme = _raw_auth_scheme

                # Fix base_url: rewrite marketplace domain → canonical www domain
                if "marketplace.leaflink.com" in _raw_base_url.lower():
                    _canonical_base_url = _LEAFLINK_CANONICAL_BASE_URL
                    logger.warning(
                        "[LEAFLINK_BASE_URL_CANONICALIZED] original=%s final=%s"
                        " brand_id=%s reason=marketplace_rewrite",
                        _raw_base_url,
                        _canonical_base_url,
                        brand_id,
                    )
                elif not _raw_base_url:
                    _canonical_base_url = _LEAFLINK_CANONICAL_BASE_URL
                    logger.info(
                        "[LEAFLINK_BASE_URL_CANONICALIZED] original=empty final=%s"
                        " brand_id=%s reason=empty_base_url",
                        _canonical_base_url,
                        brand_id,
                    )
                else:
                    # Ensure the URL ends with /api/v2
                    _stripped = _raw_base_url.rstrip("/")
                    if not _stripped.endswith("/api/v2"):
                        _canonical_base_url = _stripped + "/api/v2"
                        logger.info(
                            "[LEAFLINK_BASE_URL_CANONICALIZED] original=%s final=%s"
                            " brand_id=%s reason=appended_api_v2",
                            _raw_base_url,
                            _canonical_base_url,
                            brand_id,
                        )

                # Fix auth_scheme: rewrite deprecated Api-Key → Token
                if _raw_auth_scheme == "Api-Key":
                    _canonical_auth_scheme = "Token"
                    logger.warning(
                        "[LEAFLINK_AUTH_SCHEME_CANONICALIZED] original=%s final=%s"
                        " brand_id=%s reason=api_key_deprecated",
                        _raw_auth_scheme,
                        _canonical_auth_scheme,
                        brand_id,
                    )

                # If either value changed, rebuild the row tuple with corrected values
                if _canonical_base_url != _raw_base_url or _canonical_auth_scheme != _raw_auth_scheme:
                    row = (
                        row[0],  # id
                        row[1],  # brand_id
                        row[2],  # integration_name
                        row[3],  # company_id
                        row[4],  # api_key
                        row[5],  # is_active
                        row[6],  # sync_status
                        row[7],  # last_synced_page
                        _canonical_base_url,   # base_url  (canonicalized)
                        _canonical_auth_scheme, # auth_scheme (canonicalized)
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
