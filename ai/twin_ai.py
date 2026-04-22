import logging
from datetime import datetime, timezone
from typing import Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential
from services.leaflink_sync import sync_leaflink_orders   # Corrected path

logger = logging.getLogger("twin_ai")

class TwinAI:
    """Twin AI - The central brain of Opsyn"""

    @staticmethod
    async def plan_sync(db: AsyncSession, org_id: str, brand_id: str) -> Dict[str, Any]:
        """Planner: Decides what to do and explains reasoning"""
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        cred = result.scalar_one_or_none()

        if not cred or not cred.api_key:
            return {
                "ok": False,
                "plan": "Cannot sync - no LeafLink credentials found for this brand.",
                "risk": "high",
                "requires_approval": False,
            }

        return {
            "ok": True,
            "plan": f"Will sync recent orders from LeafLink for brand '{brand_id}' using stored credentials.",
            "details": {
                "brand_id": brand_id,
                "integration": "leaflink",
                "action": "fetch_and_save_orders",
            },
            "risk": "low",
            "requires_approval": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    async def execute_sync(db: AsyncSession, org_id: str, brand_id: str) -> Dict[str, Any]:
        """Executor: Actually performs the sync"""
        try:
            result = await sync_leaflink_orders(db, brand_id)
            if result.get("ok"):
                return {
                    "ok": True,
                    "message": f"Twin AI successfully synced LeafLink orders for brand {brand_id}",
                    "details": result,
                    "executed_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                return {
                    "ok": False,
                    "message": f"Twin AI sync failed for brand {brand_id}",
                    "error": result.get("error"),
                }
        except Exception as e:
            logger.exception("Twin AI execute_sync failed")
            return {
                "ok": False,
                "message": f"Twin AI sync failed: {str(e)}",
            }


async def handle_twin_sync(db: AsyncSession, org_id: str, brand_id: str) -> Dict[str, Any]:
    """Main entry point for Twin AI sync - Plan -> Execute"""
    plan = await TwinAI.plan_sync(db, org_id, brand_id)
    if not plan["ok"]:
        return plan

    # Auto-execute for now (sync is safe)
    result = await TwinAI.execute_sync(db, org_id, brand_id)
    return result