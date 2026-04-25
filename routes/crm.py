import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order, OrderLine, BrandAPICredential
from utils.json_utils import make_json_safe

logger = logging.getLogger("crm")

router = APIRouter(prefix="/crm", tags=["crm"])


@router.get("/health")
async def crm_health():
    """Health check for CRM endpoints."""
    return {
        "ok": True,
        "service": "crm",
        "version": "1.0",
    }


@router.get("/dashboard")
async def crm_dashboard(db: AsyncSession = Depends(get_db)):
    """
    Get CRM dashboard summary with key metrics.
    Returns: total customers, total orders, total spend, recent activity.
    """
    try:
        logger.info("crm: dashboard_requested")

        # Count unique customers
        customer_count_result = await db.execute(
            select(func.count(func.distinct(Order.customer_name))).select_from(Order)
        )
        customer_count = customer_count_result.scalar() or 0

        # Count total orders
        order_count_result = await db.execute(
            select(func.count(Order.id)).select_from(Order)
        )
        order_count = order_count_result.scalar() or 0

        # Sum total spend
        total_spend_result = await db.execute(
            select(func.sum(Order.amount)).select_from(Order)
        )
        total_spend = float(total_spend_result.scalar() or 0)

        # Get most recent order
        recent_order_result = await db.execute(
            select(Order).order_by(Order.external_updated_at.desc()).limit(1)
        )
        recent_order = recent_order_result.scalar_one_or_none()

        # Safely extract datetime values
        last_order_at = None
        last_synced_at = None

        if recent_order:
            if recent_order.external_updated_at:
                last_order_at = recent_order.external_updated_at.isoformat()
                logger.info("crm: dashboard_last_order_at=%s", last_order_at)
            else:
                logger.warning("crm: dashboard_recent_order_has_no_external_updated_at")

            if recent_order.last_synced_at:
                last_synced_at = recent_order.last_synced_at.isoformat()
                logger.info("crm: dashboard_last_synced_at=%s", last_synced_at)
            else:
                logger.warning("crm: dashboard_recent_order_has_no_last_synced_at")
        else:
            logger.warning("crm: dashboard_no_orders_found")

        # Determine data source
        data_source = "live" if (customer_count > 0 or order_count > 0) else "empty"

        logger.info(
            "crm: dashboard_complete customers=%s orders=%s total_spend=%s data_source=%s",
            customer_count,
            order_count,
            total_spend,
            data_source,
        )

        return make_json_safe({
            "ok": True,
            "data_source": data_source,
            "customer_count": customer_count,
            "order_count": order_count,
            "total_spend": total_spend,
            "last_order_at": last_order_at,
            "last_synced_at": last_synced_at,
        })

    except Exception as e:
        logger.error("crm: dashboard_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
        }


@router.get("/customers")
async def crm_customers(
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    """
    Get list of customers with order counts and total spend.
    Returns: id, name, order_count, total_spend, last_order_at.
    """
    try:
        logger.info("crm: customers_requested limit=%s offset=%s", limit, offset)

        # Get distinct customers with aggregated data
        customers_result = await db.execute(
            select(
                Order.customer_name,
                func.count(Order.id).label("order_count"),
                func.sum(Order.amount).label("total_spend"),
                func.max(Order.external_updated_at).label("last_order_at"),
            )
            .group_by(Order.customer_name)
            .order_by(func.max(Order.external_updated_at).desc())
            .limit(limit)
            .offset(offset)
        )

        customers = []
        for row in customers_result:
            customers.append({
                "id": row.customer_name,  # Use customer_name as ID
                "name": row.customer_name,
                "order_count": row.order_count or 0,
                "total_spend": float(row.total_spend or 0),
                "last_order_at": row.last_order_at.isoformat() if row.last_order_at else None,
            })

        logger.info("crm: customers_complete count=%s", len(customers))

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "customers": customers,
            "count": len(customers),
        })

    except Exception as e:
        logger.error("crm: customers_failed error=%s", e)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
            "customers": [],
            "count": 0,
        }


@router.get("/customers/{customer_id}")
async def crm_customer_detail(
    customer_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed customer information.
    Returns: name, order_count, total_spend, last_order_at, contact info.
    """
    try:
        logger.info("crm: customer_detail_requested customer_id=%s", customer_id)

        # Get customer orders
        orders_result = await db.execute(
            select(Order)
            .where(Order.customer_name == customer_id)
            .order_by(Order.external_updated_at.desc())
        )
        orders = orders_result.scalars().all()

        if not orders:
            logger.warning("crm: customer_not_found customer_id=%s", customer_id)
            return {
                "ok": False,
                "error": "Customer not found",
                "data_source": "empty",
            }

        # Aggregate customer data
        order_count = len(orders)
        total_spend = sum(float(o.amount or 0) for o in orders)
        last_order_at = max((o.external_updated_at for o in orders if o.external_updated_at), default=None)

        logger.info(
            "crm: customer_detail_complete customer_id=%s orders=%s total_spend=%s",
            customer_id,
            order_count,
            total_spend,
        )

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "id": customer_id,
            "name": customer_id,
            "order_count": order_count,
            "total_spend": total_spend,
            "last_order_at": last_order_at.isoformat() if last_order_at else None,
            "contact_name": customer_id,
            "email": None,
            "phone": None,
            "address": None,
        })

    except Exception as e:
        logger.error("crm: customer_detail_failed customer_id=%s error=%s", customer_id, e)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
        }


@router.get("/customers/{customer_id}/orders")
async def crm_customer_orders(
    customer_id: str,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    """
    Get orders for a specific customer.
    Returns: order_id, order_number, status, amount, item_count, created_at.
    """
    try:
        logger.info(
            "crm: customer_orders_requested customer_id=%s limit=%s offset=%s",
            customer_id,
            limit,
            offset,
        )

        # Get customer orders
        orders_result = await db.execute(
            select(Order)
            .where(Order.customer_name == customer_id)
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
            .offset(offset)
        )
        orders = orders_result.scalars().all()

        orders_data = []
        for order in orders:
            orders_data.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0),
                "item_count": order.item_count or 0,
                "unit_count": order.unit_count or 0,
                "review_status": order.review_status,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        logger.info("crm: customer_orders_complete customer_id=%s count=%s", customer_id, len(orders_data))

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "customer_id": customer_id,
            "orders": orders_data,
            "count": len(orders_data),
        })

    except Exception as e:
        logger.error("crm: customer_orders_failed customer_id=%s error=%s", customer_id, e)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
            "orders": [],
            "count": 0,
        }


@router.get("/customers/{customer_id}/activity")
async def crm_customer_activity(
    customer_id: str,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Get recent activity for a customer (orders, updates).
    Returns: activity_type, timestamp, details.
    """
    try:
        logger.info("crm: customer_activity_requested customer_id=%s limit=%s", customer_id, limit)

        # Get recent orders as activity
        orders_result = await db.execute(
            select(Order)
            .where(Order.customer_name == customer_id)
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
        )
        orders = orders_result.scalars().all()

        activity = []
        for order in orders:
            activity.append({
                "type": "order",
                "timestamp": order.external_updated_at.isoformat() if order.external_updated_at else None,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0),
            })

        logger.info("crm: customer_activity_complete customer_id=%s count=%s", customer_id, len(activity))

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "customer_id": customer_id,
            "activity": activity,
            "count": len(activity),
        })

    except Exception as e:
        logger.error("crm: customer_activity_failed customer_id=%s error=%s", customer_id, e)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
            "activity": [],
            "count": 0,
        }


@router.get("/recent-orders")
async def crm_recent_orders(
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Get most recent orders across all customers.
    Returns: order_id, customer_name, order_number, status, amount, created_at.
    """
    try:
        logger.info("crm: recent_orders_requested limit=%s", limit)

        # Get recent orders
        orders_result = await db.execute(
            select(Order)
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
        )
        orders = orders_result.scalars().all()

        orders_data = []
        for order in orders:
            orders_data.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "customer_name": order.customer_name,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0),
                "item_count": order.item_count or 0,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        logger.info("crm: recent_orders_complete count=%s", len(orders_data))

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "orders": orders_data,
            "count": len(orders_data),
        })

    except Exception as e:
        logger.error("crm: recent_orders_failed error=%s", e)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
            "orders": [],
            "count": 0,
        }


@router.get("/sync-status")
async def crm_sync_status(db: AsyncSession = Depends(get_db)):
    """
    Get sync status for all brands.
    Returns: brand_id, sync_status, last_sync_at, last_error.
    """
    try:
        logger.info("crm: sync_status_requested")

        # Get sync status for all brands
        creds_result = await db.execute(
            select(BrandAPICredential)
            .where(BrandAPICredential.is_active == True)
            .order_by(BrandAPICredential.last_sync_at.desc())
        )
        creds = creds_result.scalars().all()

        brands = []
        for cred in creds:
            brands.append({
                "brand_id": cred.brand_id,
                "integration": cred.integration_name,
                "sync_status": cred.sync_status,
                "last_sync_at": cred.last_sync_at.isoformat() if cred.last_sync_at else None,
                "last_error": cred.last_error,
            })

        logger.info("crm: sync_status_complete brands=%s", len(brands))

        return make_json_safe({
            "ok": True,
            "data_source": "live",
            "brands": brands,
            "count": len(brands),
        })

    except Exception as e:
        logger.error("crm: sync_status_failed error=%s", e)
        return {
            "ok": False,
            "error": str(e),
            "data_source": "error",
            "brands": [],
            "count": 0,
        }
