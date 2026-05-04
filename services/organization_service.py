import logging
from uuid import UUID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models.auth_models import Organization

logger = logging.getLogger("organization_service")


async def lookup_organization(db: AsyncSession, org_identifier: str) -> dict:
    """
    Look up organization by UUID or org_code.

    Args:
        db: Database session
        org_identifier: Either UUID string or org_code (e.g., "noble")

    Returns:
        {ok: bool, organization: Organization or None, error: str or None}
    """
    if not org_identifier:
        logger.warning("[Org] lookup_organization_empty_identifier")
        return {"ok": False, "organization": None, "error": "Organization identifier is required"}

    # Normalize input
    org_identifier = org_identifier.strip()
    org_code_lower = org_identifier.lower()

    logger.info("[Org] lookup_organization_start identifier=%s", org_identifier)

    try:
        # Try to parse as UUID
        try:
            org_uuid = UUID(org_identifier)
            logger.info("[Org] identifier_is_uuid uuid=%s", org_uuid)

            # Query by UUID
            result = await db.execute(
                select(Organization).where(Organization.id == str(org_uuid))
            )
            org = result.scalar_one_or_none()

            if org:
                logger.info("[Org] found_by_uuid org_id=%s org_code=%s", org.id, org.org_code)
                return {"ok": True, "organization": org, "error": None}
            else:
                logger.warning("[Org] not_found_by_uuid uuid=%s", org_uuid)
                return {"ok": False, "organization": None, "error": f"Organization not found: {org_identifier}"}

        except (ValueError, TypeError):
            # Not a UUID — try org_code (case-insensitive)
            logger.info("[Org] identifier_is_not_uuid trying_org_code code=%s", org_code_lower)

            result = await db.execute(
                select(Organization).where(
                    Organization.org_code == org_code_lower
                )
            )
            org = result.scalar_one_or_none()

            if org:
                logger.info("[Org] found_by_org_code org_id=%s org_code=%s", org.id, org.org_code)
                return {"ok": True, "organization": org, "error": None}
            else:
                logger.warning("[Org] not_found_by_org_code code=%s", org_code_lower)
                return {"ok": False, "organization": None, "error": f"Organization not found: {org_identifier}"}

    except Exception as e:
        logger.error("[Org] lookup_organization_failed error=%s", str(e)[:500], exc_info=True)
        return {"ok": False, "organization": None, "error": str(e)[:500]}
