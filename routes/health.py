import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order
from utils.json_utils import make_json_safe

logger = logging.getLogger("health")

router = APIRouter(prefix="/health", tags=["health"])


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/data")
async def health_data(db: AsyncSession = Depends(get_db)):
    """
    Returns data freshness metrics for the opsyn-backend service.

    Response:
      {
        "ok": true,
        "orders_count": <int>,
        "customers_count": <int>,
        "last_sync": "<ISO timestamp | null>",
        "last_error": "<string | null>",
        "data_source": "live" | "error",
        "synced_at": "<ISO timestamp>"
      }

    Never returns empty — always returns counts even if 0.
    """
    synced_at = _utc_now_iso()

    try:
        # Count total orders in DB
        orders_count_result = await db.execute(
            select(func.count(Order.id)).select_from(Order)
        )
        orders_count: int = orders_count_result.scalar() or 0

        # Count distinct customers (derived from orders)
        customers_count_result = await db.execute(
            select(func.count(func.distinct(Order.customer_name))).select_from(Order)
        )
        customers_count: int = customers_count_result.scalar() or 0

        # Most recent credential sync info
        cred_result = await db.execute(
            select(BrandAPICredential)
            .where(BrandAPICredential.is_active == True)
            .order_by(BrandAPICredential.last_sync_at.desc())
            .limit(1)
        )
        cred = cred_result.scalar_one_or_none()

        last_sync = cred.last_sync_at.isoformat() if (cred and cred.last_sync_at) else None
        last_error = cred.last_error if cred else None

        data_source = "live" if (orders_count > 0 or customers_count > 0) else "empty"

        logger.info(
            "[Health] data_endpoint_hit orders=%s customers=%s last_sync=%s",
            orders_count,
            customers_count,
            last_sync,
        )

        return make_json_safe({
            "ok": True,
            "orders_count": orders_count,
            "customers_count": customers_count,
            "last_sync": last_sync,
            "last_error": last_error,
            "data_source": data_source,
            "synced_at": synced_at,
        })

    except Exception as e:
        logger.error("[Health] data_endpoint_error error=%s", e, exc_info=True)

        # Never return empty — always return counts even on error
        return make_json_safe({
            "ok": False,
            "orders_count": 0,
            "customers_count": 0,
            "last_sync": None,
            "last_error": str(e),
            "data_source": "error",
            "synced_at": synced_at,
        })
