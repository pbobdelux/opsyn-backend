"""
Integration credential management service.

Handles per-brand credential lookup, validation, and status tracking for
third-party integrations (LeafLink, etc.).

Safety:
  - Never logs actual API keys or secrets
  - Never returns API keys in responses
  - Validates credentials before storing
  - Tracks credential health in the database
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import requests

from database import AsyncSessionLocal
from models import BrandAPICredential

logger = logging.getLogger("integration_credentials")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Credential lookup
# ---------------------------------------------------------------------------

async def get_leaflink_credentials(brand_id: str) -> Optional[BrandAPICredential]:
    """
    Find the active LeafLink credential for a given brand_id.

    Returns the BrandAPICredential ORM object (with api_key) if found and
    active, or None if no credential exists.

    Logs lookup result without ever logging the actual api_key.
    """
    if not brand_id:
        logger.warning("[IntegrationCredentials] lookup brand_id=None — skipping")
        return None

    try:
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.brand_id == brand_id,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            cred = result.scalar_one_or_none()

        found = cred is not None
        logger.info(
            "[IntegrationCredentials] lookup brand_id=%s found=%s source=db",
            brand_id,
            str(found).lower(),
        )
        return cred

    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] lookup_failed brand_id=%s error=%s",
            brand_id,
            exc,
        )
        return None


# ---------------------------------------------------------------------------
# Credential validation
# ---------------------------------------------------------------------------

def validate_leaflink_credential(credential: BrandAPICredential) -> bool:
    """
    Test a LeafLink credential by making a minimal API request (page_size=1).

    Updates credential.last_checked_at and credential.sync_status in-memory.
    The caller is responsible for persisting the credential to the database.

    Returns True if the credential is valid, False otherwise.
    Never logs the actual api_key.
    """
    brand_id = credential.brand_id or "unknown"

    api_key = (credential.api_key or "").strip()
    if api_key.startswith("Token "):
        api_key = api_key[6:].strip()

    base_url = (
        credential.base_url
        or "https://www.leaflink.com/api/v2"
    ).strip().rstrip("/")

    if not api_key:
        logger.warning(
            "[IntegrationCredentials] validation brand_id=%s valid=false reason=empty_api_key",
            brand_id,
        )
        credential.last_checked_at = _utc_now()
        credential.sync_status = "failed"
        return False

    try:
        test_url = f"{base_url}/orders-received/"
        resp = requests.get(
            test_url,
            headers={
                "Authorization": f"Token {api_key}",
                "Content-Type": "application/json",
            },
            params={"page": 1, "page_size": 1},
            timeout=15,
        )

        valid = resp.ok
        credential.last_checked_at = _utc_now()
        credential.sync_status = "ok" if valid else "failed"

        logger.info(
            "[IntegrationCredentials] validation brand_id=%s valid=%s status=%s",
            brand_id,
            str(valid).lower(),
            resp.status_code,
        )
        return valid

    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] validation_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        credential.last_checked_at = _utc_now()
        credential.sync_status = "failed"
        return False


# ---------------------------------------------------------------------------
# Credential status updates
# ---------------------------------------------------------------------------

async def mark_credential_invalid(
    credential: BrandAPICredential,
    error: str,
) -> None:
    """
    Mark a credential as failed in the database.

    Sets sync_status = "failed", last_error = error, last_checked_at = now.
    Logs the reason without exposing the api_key.
    """
    brand_id = credential.brand_id or "unknown"

    try:
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(BrandAPICredential).where(
                        BrandAPICredential.id == credential.id,
                    )
                )
                db_cred = result.scalar_one_or_none()
                if db_cred:
                    db_cred.sync_status = "failed"
                    db_cred.last_error = error
                    db_cred.last_checked_at = _utc_now()

        logger.info(
            "[IntegrationCredentials] marked_invalid brand_id=%s reason=%s",
            brand_id,
            error[:200] if error else "unknown",
        )

    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] mark_invalid_failed brand_id=%s error=%s",
            brand_id,
            exc,
        )


async def mark_credential_success(credential: BrandAPICredential) -> None:
    """
    Mark a credential as successfully used in the database.

    Sets sync_status = "ok", last_error = None, last_sync_at = now.
    """
    brand_id = credential.brand_id or "unknown"

    try:
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(BrandAPICredential).where(
                        BrandAPICredential.id == credential.id,
                    )
                )
                db_cred = result.scalar_one_or_none()
                if db_cred:
                    db_cred.sync_status = "ok"
                    db_cred.last_error = None
                    db_cred.last_sync_at = _utc_now()

        logger.info(
            "[IntegrationCredentials] marked_success brand_id=%s",
            brand_id,
        )

    except Exception as exc:
        logger.error(
            "[IntegrationCredentials] mark_success_failed brand_id=%s error=%s",
            brand_id,
            exc,
        )
