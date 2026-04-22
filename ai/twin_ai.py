import logging
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential
from services.leaflink_sync import sync_leaflink_orders

logger = logging.getLogger("twin_ai")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TwinAI:
    """Twin AI - central orchestration layer for Opsyn sync flows."""

    @staticmethod
    async def plan_sync(db: AsyncSession, org_id: str, brand_id: str) -> Dict[str, Any]:
        """Planner: validate credentials and describe the intended sync action."""
        if not brand_id:
            return {
                "ok": False,
                "plan": "Cannot sync because no brand_id was provided.",
                "risk": "high",
                "requires_approval": False,
                "timestamp": utc_now_iso(),
            }

        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        cred = result.scalar_one_or_none()

        if not cred:
            logger.warning("No active LeafLink credential found for brand_id=%s", brand_id)
            return {
                "ok": False,
                "plan": f"Cannot sync - no active LeafLink credential found for brand '{brand_id}'.",
                "risk": "high",
                "requires_approval": False,
                "timestamp": utc_now_iso(),
            }

        if not cred.api_key:
            logger.warning("LeafLink credential exists but api_key is missing for brand_id=%s", brand_id)
            return {
                "ok": False,
                "plan": f"Cannot sync - LeafLink credential exists for brand '{brand_id}' but api_key is missing.",
                "risk": "high",
                "requires_approval": False,
                "timestamp": utc_now_iso(),
            }

        return {
            "ok": True,
            "plan": f"Will sync recent LeafLink orders for brand '{brand_id}' using stored credentials.",
            "details": {
                "org_id": org_id,
                "brand_id": brand_id,
                "integration": "leaflink",
                "action": "fetch_and_save_orders",
                "credential_status": {
                    "found": True,
                    "is_active": bool(cred.is_active),
                    "has_api_key": bool(cred.api_key),
                    "base_url": cred.base_url,
                    "company_id": cred.company_id,
                },
            },
            "risk": "low",
            "requires_approval": False,
            "timestamp": utc_now_iso(),
        }

    @staticmethod
    async def execute_sync(db: AsyncSession, org_id: str, brand_id: str) -> Dict[str, Any]:
        """Executor: perform the actual LeafLink sync and return a normalized result."""
        started_at = utc_now_iso()
        logger.info("Twin AI starting LeafLink sync for org_id=%s brand_id=%s", org_id, brand_id)

        try:
            result = await sync_leaflink_orders(db, brand_id)

            if result.get("ok"):
                logger.info("Twin AI sync succeeded for brand_id=%s", brand_id)
                return {
                    "ok": True,
                    "message": f"Twin AI successfully synced LeafLink orders for brand '{brand_id}'.",
                    "org_id": org_id,
                    "brand_id": brand_id,
                    "integration": "leaflink",
                    "started_at": started_at,
                    "executed_at": utc_now_iso(),
                    "details": result,
                }

            logger.warning(
                "Twin AI sync completed with failure result for brand_id=%s: %s",
                brand_id,
                result,
            )
            return {
                "ok": False,
                "message": f"Twin AI sync failed for brand '{brand_id}'.",
                "org_id": org_id,
                "brand_id": brand_id,
                "integration": "leaflink",
                "started_at": started_at,
                "executed_at": utc_now_iso(),
                "error": result.get("error", "Unknown sync error"),
                "details": result,
            }

        except Exception as exc:
            logger.exception("Twin AI execute_sync crashed for brand_id=%s", brand_id)
            return {
                "ok": False,
                "message": f"Twin AI sync crashed for brand '{brand_id}'.",
                "org_id": org_id,
                "brand_id": brand_id,
                "integration": "leaflink",
                "started_at": started_at,
                "executed_at": utc_now_iso(),
                "error": str(exc),
            }


async def handle_twin_sync(db: AsyncSession, org_id: str, brand_id: str) -> Dict[str, Any]:
    """Main entry point for Twin AI sync: plan first, then execute."""
    logger.info("handle_twin_sync called for org_id=%s brand_id=%s", org_id, brand_id)

    plan = await TwinAI.plan_sync(db, org_id, brand_id)
    if not plan.get("ok"):
        logger.warning("Twin AI plan_sync failed for brand_id=%s", brand_id)
        return plan

    result = await TwinAI.execute_sync(db, org_id, brand_id)

    return {
        "ok": result.get("ok", False),
        "plan": plan,
        "result": result,
        "timestamp": utc_now_iso(),
    }