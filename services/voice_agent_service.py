import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from models import Order, BrandAPICredential
from sqlalchemy import func, select

logger = logging.getLogger("voice_agent")


class VoiceAgentService:
    """Service for voice agent tool routing and context."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_dashboard_summary(self) -> dict[str, Any]:
        """Get CRM dashboard summary for voice context."""
        try:
            # Count unique customers
            customer_count_result = await self.db.execute(
                select(func.count(func.distinct(Order.customer_name))).select_from(Order)
            )
            customer_count = customer_count_result.scalar() or 0

            # Count total orders
            order_count_result = await self.db.execute(
                select(func.count(Order.id)).select_from(Order)
            )
            order_count = order_count_result.scalar() or 0

            # Sum total spend
            total_spend_result = await self.db.execute(
                select(func.sum(Order.amount)).select_from(Order)
            )
            total_spend = float(total_spend_result.scalar() or 0)

            # Get sync status
            sync_result = await self.db.execute(
                select(BrandAPICredential)
                .where(BrandAPICredential.is_active == True)
                .order_by(BrandAPICredential.last_sync_at.desc())
                .limit(1)
            )
            latest_sync = sync_result.scalar_one_or_none()

            return {
                "customer_count": customer_count,
                "order_count": order_count,
                "total_spend": total_spend,
                "last_sync_at": latest_sync.last_sync_at.isoformat() if latest_sync and latest_sync.last_sync_at else None,
                "sync_status": latest_sync.sync_status if latest_sync else "unknown",
            }

        except Exception as e:
            logger.error("voice_agent: get_dashboard_summary_failed error=%s", e)
            return {
                "error": str(e),
                "customer_count": 0,
                "order_count": 0,
                "total_spend": 0,
            }

    async def get_recent_orders(self, limit: int = 5) -> list[dict[str, Any]]:
        """Get recent orders for voice context."""
        try:
            orders_result = await self.db.execute(
                select(Order)
                .order_by(Order.external_updated_at.desc())
                .limit(limit)
            )
            orders = orders_result.scalars().all()

            return [
                {
                    "order_number": o.order_number,
                    "customer_name": o.customer_name,
                    "amount": float(o.amount or 0),
                    "status": o.status,
                    "created_at": o.external_created_at.isoformat() if o.external_created_at else None,
                }
                for o in orders
            ]

        except Exception as e:
            logger.error("voice_agent: get_recent_orders_failed error=%s", e)
            return []

    async def get_customer_summary(self, customer_name: str) -> dict[str, Any]:
        """Get summary for a specific customer."""
        try:
            orders_result = await self.db.execute(
                select(Order)
                .where(Order.customer_name == customer_name)
                .order_by(Order.external_updated_at.desc())
            )
            orders = orders_result.scalars().all()

            if not orders:
                return {"error": f"No orders found for {customer_name}"}

            order_count = len(orders)
            total_spend = sum(float(o.amount or 0) for o in orders)
            last_order_at = max((o.external_updated_at for o in orders if o.external_updated_at), default=None)

            return {
                "customer_name": customer_name,
                "order_count": order_count,
                "total_spend": total_spend,
                "last_order_at": last_order_at.isoformat() if last_order_at else None,
                "recent_orders": [
                    {
                        "order_number": o.order_number,
                        "amount": float(o.amount or 0),
                        "status": o.status,
                    }
                    for o in orders[:3]
                ],
            }

        except Exception as e:
            logger.error("voice_agent: get_customer_summary_failed customer_name=%s error=%s", customer_name, e)
            return {"error": str(e)}

    async def get_sync_status(self) -> dict[str, Any]:
        """Get sync status for all brands."""
        try:
            creds_result = await self.db.execute(
                select(BrandAPICredential)
                .where(BrandAPICredential.is_active == True)
                .order_by(BrandAPICredential.last_sync_at.desc())
            )
            creds = creds_result.scalars().all()

            brands = []
            for cred in creds:
                brands.append({
                    "brand_id": cred.brand_id,
                    "sync_status": cred.sync_status,
                    "last_sync_at": cred.last_sync_at.isoformat() if cred.last_sync_at else None,
                    "last_error": cred.last_error,
                })

            return {
                "brands": brands,
                "healthy": all(b["sync_status"] == "ok" for b in brands),
            }

        except Exception as e:
            logger.error("voice_agent: get_sync_status_failed error=%s", e)
            return {"error": str(e), "brands": []}

    async def prepare_action(self, action_type: str, details: dict[str, Any]) -> dict[str, Any]:
        """Prepare an action for confirmation (read-only plan)."""
        logger.info("voice_agent: prepare_action action_type=%s", action_type)

        if action_type == "send_message":
            return {
                "action_type": "send_message",
                "status": "pending_confirmation",
                "plan": f"Send message to {details.get('customer_name')}",
                "details": details,
            }

        elif action_type == "create_route":
            return {
                "action_type": "create_route",
                "status": "pending_confirmation",
                "plan": f"Create route with {len(details.get('stops', []))} stops",
                "details": details,
            }

        else:
            return {
                "action_type": action_type,
                "status": "unknown",
                "error": f"Unknown action type: {action_type}",
            }
