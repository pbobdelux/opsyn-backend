import base64
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Order, OrderLine, BrandAPICredential
from utils.json_utils import make_json_safe

logger = logging.getLogger("crm")

router = APIRouter(prefix="/crm", tags=["crm"])


@router.get("/diagnostic")
async def crm_diagnostic(
    brand: str = Query(None, description="Optional brand filter"),
    db: AsyncSession = Depends(get_db),
):
    """
    Read-only diagnostic for CRM account/customer persistence and API freshness.
    
    Verifies:
    - Database customer count
    - API returned customer count
    - Latest customer update timestamp
    - Account ID stability
    - Whether new accounts are persisted
    """
    try:
        logger.info("[CRMDiagnostic] request brand=%s", brand)
        
        # 1. Count distinct customers in database
        customer_count_result = await db.execute(
            select(func.count(func.distinct(Order.customer_name)))
            .where(Order.customer_name != None)
        )
        db_customer_count = customer_count_result.scalar() or 0
        
        # 2. Get list of all customers with metadata
        customers_result = await db.execute(
            select(
                Order.customer_name,
                func.count(Order.id).label("order_count"),
                func.max(Order.external_updated_at).label("latest_order_updated_at"),
                func.max(Order.updated_at).label("latest_db_updated_at"),
            )
            .where(Order.customer_name != None)
            .group_by(Order.customer_name)
            .order_by(func.max(Order.updated_at).desc())
        )
        
        customers = []
        for row in customers_result:
            customers.append({
                "id": row.customer_name,  # Stable ID = customer_name
                "name": row.customer_name,
                "order_count": row.order_count or 0,
                "latest_order_updated_at": row.latest_order_updated_at.isoformat() if row.latest_order_updated_at else None,
                "latest_db_updated_at": row.latest_db_updated_at.isoformat() if row.latest_db_updated_at else None,
            })
        
        # 3. Get newest customer (most recently updated)
        newest_customer = customers[0] if customers else None
        
        # 4. Get oldest customer (least recently updated)
        oldest_customer = customers[-1] if customers else None
        
        # 5. Count orders by customer (to verify persistence)
        orders_by_customer_result = await db.execute(
            select(
                Order.customer_name,
                func.count(Order.id).label("order_count"),
            )
            .where(Order.customer_name != None)
            .group_by(Order.customer_name)
            .order_by(func.count(Order.id).desc())
            .limit(5)
        )
        
        top_customers = [
            {
                "name": row.customer_name,
                "order_count": row.order_count or 0,
            }
            for row in orders_by_customer_result
        ]
        
        # 6. Check for NULL customer_name orders (data quality issue)
        null_customer_result = await db.execute(
            select(func.count(Order.id))
            .where(Order.customer_name == None)
        )
        null_customer_count = null_customer_result.scalar() or 0
        
        # 7. Get date range of customers
        date_range_result = await db.execute(
            select(
                func.min(Order.updated_at),
                func.max(Order.updated_at),
            )
            .where(Order.customer_name != None)
        )
        oldest_update, newest_update = date_range_result.fetchone()
        
        logger.info(
            "[CRMDiagnostic] complete db_count=%s api_count=%s newest=%s",
            db_customer_count,
            len(customers),
            newest_customer["name"] if newest_customer else None,
        )
        
        return {
            "ok": True,
            "diagnostic": {
                "database": {
                    "total_customers": db_customer_count,
                    "customers_with_orders": len(customers),
                    "orders_with_null_customer": null_customer_count,
                    "date_range": {
                        "oldest_update": oldest_update.isoformat() if oldest_update else None,
                        "newest_update": newest_update.isoformat() if newest_update else None,
                    },
                },
                "api": {
                    "returned_count": len(customers),
                    "customers": customers,
                },
                "persistence": {
                    "newest_customer": newest_customer,
                    "oldest_customer": oldest_customer,
                    "top_5_by_order_count": top_customers,
                },
                "id_stability": {
                    "id_type": "customer_name (string)",
                    "id_is_unique": True,
                    "id_is_stable": "Only if customer name never changes in LeafLink",
                    "sample_ids": [c["id"] for c in customers[:3]],
                },
                "freshness": {
                    "newest_customer_name": newest_customer["name"] if newest_customer else None,
                    "newest_customer_latest_update": newest_customer["latest_db_updated_at"] if newest_customer else None,
                    "oldest_customer_name": oldest_customer["name"] if oldest_customer else None,
                    "oldest_customer_latest_update": oldest_customer["latest_db_updated_at"] if oldest_customer else None,
                },
            },
        }
    
    except Exception as exc:
        logger.error("[CRMDiagnostic] error brand=%s error=%s", brand, str(exc)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(exc)[:500],
        }


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
    Fast CRM dashboard with key metrics and recent data.

    Returns:
    - total_orders: Total orders in system
    - total_revenue: Total revenue (dollars)
    - unique_customers: Count of unique customers
    - top_customers: Top 10 customers by spend
    - recent_orders: Last 50 orders
    - synced_at: Timestamp

    Performance: <500ms
    """
    try:
        logger.info("[CRM] dashboard_requested")

        # 1. Get aggregate stats (single query)
        stats_result = await db.execute(
            select(
                func.count(func.distinct(Order.customer_name)).label("unique_customers"),
                func.count(Order.id).label("total_orders"),
                func.sum(Order.amount).label("total_revenue_cents"),
            )
            .where(Order.customer_name != None)
        )
        stats_row = stats_result.fetchone()

        unique_customers = stats_row.unique_customers or 0
        total_orders = stats_row.total_orders or 0
        total_revenue = float(stats_row.total_revenue_cents or 0) / 100.0

        # 2. Get top 10 customers by spend
        top_customers_result = await db.execute(
            select(
                Order.customer_name,
                func.count(Order.id).label("order_count"),
                func.sum(Order.amount).label("total_spend_cents"),
                func.max(Order.external_updated_at).label("last_order_at"),
            )
            .where(Order.customer_name != None)
            .group_by(Order.customer_name)
            .order_by(func.sum(Order.amount).desc())
            .limit(10)
        )

        top_customers = []
        for row in top_customers_result:
            top_customers.append({
                "name": row.customer_name,
                "order_count": row.order_count or 0,
                "total_spend": float(row.total_spend_cents or 0) / 100.0,
                "last_order_at": row.last_order_at.isoformat() if row.last_order_at else None,
            })

        # 3. Get recent 50 orders
        recent_orders_result = await db.execute(
            select(Order)
            .order_by(Order.external_updated_at.desc())
            .limit(50)
        )

        recent_orders = []
        for order in recent_orders_result.scalars().all():
            recent_orders.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "customer_name": order.customer_name,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0) / 100.0,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        synced_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "[CRM] dashboard_complete customers=%s orders=%s revenue=%s",
            unique_customers,
            total_orders,
            total_revenue,
        )

        return make_json_safe({
            "ok": True,
            "stats": {
                "total_orders": total_orders,
                "total_revenue": total_revenue,
                "unique_customers": unique_customers,
            },
            "top_customers": top_customers,
            "recent_orders": recent_orders,
            "synced_at": synced_at,
        })

    except Exception as e:
        logger.error("[CRM] dashboard_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "stats": {
                "total_orders": 0,
                "total_revenue": 0.0,
                "unique_customers": 0,
            },
            "top_customers": [],
            "recent_orders": [],
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/orders")
async def crm_orders(
    limit: int = Query(50, ge=10, le=500, description="Records per page (default 50, max 500)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    customer_name: Optional[str] = Query(None, description="Filter by customer name"),
    status: Optional[str] = Query(None, description="Filter by order status"),
    date_from: Optional[str] = Query(None, description="Filter orders from date (ISO format)"),
    date_to: Optional[str] = Query(None, description="Filter orders to date (ISO format)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated orders with optional filtering.

    Supports:
    - Cursor-based pagination
    - Filter by customer_name
    - Filter by status
    - Filter by date range

    Returns: orders array + next_cursor (if more results)
    """
    try:
        logger.info(
            "[CRM] orders_requested limit=%s customer=%s status=%s",
            limit,
            customer_name,
            status,
        )

        # Build query with filters
        query = select(Order)

        if customer_name:
            query = query.where(Order.customer_name.ilike(f"%{customer_name}%"))

        if status:
            query = query.where(Order.status == status)

        if date_from:
            try:
                from_dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
                query = query.where(Order.external_created_at >= from_dt)
            except Exception:
                pass

        if date_to:
            try:
                to_dt = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
                query = query.where(Order.external_created_at <= to_dt)
            except Exception:
                pass

        # Cursor-based pagination
        if cursor:
            try:
                cursor_id = base64.b64decode(cursor).decode()
                query = query.where(Order.id > cursor_id)
            except Exception:
                pass

        # Fetch limit + 1 to detect if there are more results
        query = query.order_by(Order.id).limit(limit + 1)

        result = await db.execute(query)
        orders_list = result.scalars().all()

        has_more = len(orders_list) > limit
        if has_more:
            orders_list = orders_list[:limit]

        orders = []
        for order in orders_list:
            orders.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "customer_name": order.customer_name,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0) / 100.0,
                "item_count": order.item_count or 0,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1]["id"]).encode()).decode()

        logger.info("[CRM] orders_returned count=%s has_more=%s", len(orders), has_more)

        return make_json_safe({
            "ok": True,
            "orders": orders,
            "count": len(orders),
            "has_more": has_more,
            "next_cursor": next_cursor,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        logger.error("[CRM] orders_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "orders": [],
            "count": 0,
            "has_more": False,
            "next_cursor": None,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/full-data")
async def crm_full_data(
    limit: int = Query(20000, ge=100, le=50000, description="Max records (default 20k, max 50k)"),
    db: AsyncSession = Depends(get_db),
):
    """
    DEBUG/INTERNAL ONLY - Get complete CRM dataset.

    Not for production frontend use. Use /crm/dashboard for summaries
    and /crm/orders for paginated data instead.

    Useful for:
    - Data exports
    - Analytics
    - Internal debugging
    - Batch operations
    """
    try:
        logger.info("[CRM] full_data_requested limit=%s", limit)

        result = await db.execute(
            select(Order)
            .order_by(Order.external_updated_at.desc())
            .limit(limit)
        )
        orders_list = result.scalars().all()

        orders = []
        for order in orders_list:
            orders.append({
                "id": order.id,
                "external_id": order.external_order_id,
                "customer_name": order.customer_name,
                "order_number": order.order_number,
                "status": order.status,
                "amount": float(order.amount or 0) / 100.0,
                "item_count": order.item_count or 0,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.external_updated_at.isoformat() if order.external_updated_at else None,
            })

        logger.info("[CRM] full_data_returned count=%s", len(orders))

        return make_json_safe({
            "ok": True,
            "orders": orders,
            "count": len(orders),
            "limit": limit,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        logger.error("[CRM] full_data_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "orders": [],
            "count": 0,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }


@router.get("/customers")
async def crm_customers(
    limit: int = Query(50, ge=10, le=500, description="Records per page (default 50, max 500)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor"),
    search: Optional[str] = Query(None, description="Search customer name"),
    sort_by: Optional[str] = Query("spend", description="Sort by: spend, orders, recent"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated customers with aggregated stats.

    Returns per-customer:
    - name
    - order_count
    - total_spend (dollars)
    - last_order_at

    Supports:
    - Cursor pagination
    - Name search
    - Sort by spend, order count, or recency
    """
    try:
        logger.info("[CRM] customers_requested limit=%s search=%s sort=%s", limit, search, sort_by)

        # Build query
        query = select(
            Order.customer_name,
            func.count(Order.id).label("order_count"),
            func.sum(Order.amount).label("total_spend_cents"),
            func.max(Order.external_updated_at).label("last_order_at"),
        ).where(Order.customer_name != None).group_by(Order.customer_name)

        if search:
            query = query.where(Order.customer_name.ilike(f"%{search}%"))

        # Sort
        if sort_by == "orders":
            query = query.order_by(func.count(Order.id).desc())
        elif sort_by == "recent":
            query = query.order_by(func.max(Order.external_updated_at).desc())
        else:  # spend (default)
            query = query.order_by(func.sum(Order.amount).desc())

        # Cursor pagination
        if cursor:
            try:
                cursor_name = base64.b64decode(cursor).decode()
                query = query.where(Order.customer_name > cursor_name)
            except Exception:
                pass

        query = query.limit(limit + 1)

        result = await db.execute(query)
        customers_list = result.fetchall()

        has_more = len(customers_list) > limit
        if has_more:
            customers_list = customers_list[:limit]

        customers = []
        for row in customers_list:
            customers.append({
                "name": row.customer_name,
                "order_count": row.order_count or 0,
                "total_spend": float(row.total_spend_cents or 0) / 100.0,
                "last_order_at": row.last_order_at.isoformat() if row.last_order_at else None,
            })

        next_cursor = None
        if has_more and customers:
            next_cursor = base64.b64encode(customers[-1]["name"].encode()).decode()

        logger.info("[CRM] customers_returned count=%s has_more=%s", len(customers), has_more)

        return make_json_safe({
            "ok": True,
            "customers": customers,
            "count": len(customers),
            "has_more": has_more,
            "next_cursor": next_cursor,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        logger.error("[CRM] customers_failed error=%s", str(e)[:500], exc_info=True)
        return {
            "ok": False,
            "error": str(e)[:500],
            "customers": [],
            "count": 0,
            "has_more": False,
            "next_cursor": None,
            "synced_at": datetime.now(timezone.utc).isoformat(),
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
                "source": "empty",
                "data_source": "empty",
                "synced_at": datetime.now(timezone.utc).isoformat(),
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

        synced_at = datetime.now(timezone.utc).isoformat()

        return make_json_safe({
            "ok": True,
            "source": "live",
            "data_source": "live",
            "synced_at": synced_at,
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
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
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

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if orders_data else "empty"

        logger.info("[CRM] orders_query count=%s customer_id=%s", len(orders_data), customer_id)

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "customer_id": customer_id,
            "orders": orders_data,
            "count": len(orders_data),
        })

    except Exception as e:
        logger.error("crm: customer_orders_failed customer_id=%s error=%s", customer_id, e)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "customer_id": customer_id,
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

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if activity else "empty"

        logger.info("crm: customer_activity_complete customer_id=%s count=%s", customer_id, len(activity))

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "customer_id": customer_id,
            "activity": activity,
            "count": len(activity),
        })

    except Exception as e:
        logger.error("crm: customer_activity_failed customer_id=%s error=%s", customer_id, e)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "customer_id": customer_id,
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

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if orders_data else "empty"

        logger.info("[CRM] orders_query count=%s source=%s", len(orders_data), source)

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "orders": orders_data,
            "count": len(orders_data),
        })

    except Exception as e:
        logger.error("crm: recent_orders_failed error=%s", e)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
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

        synced_at = datetime.now(timezone.utc).isoformat()
        source = "live" if brands else "empty"

        logger.info("crm: sync_status_complete brands=%s", len(brands))

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": synced_at,
            "brands": brands,
            "count": len(brands),
        })

    except Exception as e:
        logger.error("crm: sync_status_failed error=%s", e)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "brands": [],
            "count": 0,
        }


@router.get("/sync")
async def crm_sync(
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return records updated after this time"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of records to return (default 500, max 1000)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all CRM data (customers and recent orders) for incremental sync.

    Combines customer and order data in a single sync call for efficiency.

    Query params:
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z")
    - cursor: Pagination cursor from previous response
    - limit: Number of records (1-1000, default 500)

    Response includes:
    - customers: Array of customer objects (derived from orders)
    - orders: Array of order objects
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more records exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    try:
        logger.info(
            "crm: sync_requested updated_after=%s cursor=%s limit=%s",
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
        )

        # Parse updated_after if provided
        updated_after_dt = None
        if updated_after:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
                logger.info("crm: sync_filter updated_after=%s", updated_after_dt.isoformat())
            except ValueError as e:
                logger.error("crm: sync_invalid_timestamp error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Decode cursor if provided
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
                logger.info("crm: sync_cursor_decoded cursor_id=%s", cursor_id)
            except Exception as e:
                logger.error("crm: sync_invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Fetch orders (orders are the source of truth for both customers and orders)
        order_query = select(Order)
        if updated_after_dt:
            order_query = order_query.where(Order.updated_at >= updated_after_dt)
        if cursor_id is not None:
            order_query = order_query.where(Order.id > cursor_id)
        order_query = order_query.order_by(Order.updated_at.asc(), Order.id.asc())
        order_query = order_query.limit(limit + 1)

        order_result = await db.execute(order_query)
        orders = order_result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # Build order response
        orders_data = []
        for order in orders:
            order_dict = {
                "id": order.id,
                "external_id": order.external_order_id,
                "order_number": order.order_number,
                "customer_name": order.customer_name,
                "amount": float(order.amount) if order.amount else 0,
                "item_count": order.item_count,
                "unit_count": order.unit_count,
                "status": order.status,
                "review_status": order.review_status,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
            }
            orders_data.append(order_dict)

        # Derive unique customers from the fetched orders
        seen_customers: dict[str, dict] = {}
        for order in orders:
            name = order.customer_name
            if not name or name in seen_customers:
                continue
            seen_customers[name] = {
                "id": name,
                "name": name,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
            }
        customers_data = list(seen_customers.values())

        # Determine next cursor
        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1].id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        source = "live" if (orders_data or customers_data) else "empty"

        logger.info(
            "[CRM] dashboard_complete customers=%s orders=%s source=%s",
            len(customers_data),
            len(orders_data),
            source,
        )

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": server_time,
            "customers": customers_data,
            "orders": orders_data,
            "count": len(orders_data),
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("crm: sync_failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "customers": [],
            "orders": [],
            "count": 0,
        }


@router.get("/customers/sync")
async def crm_customers_sync(
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return customers updated after this time"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of customers to return (default 500, max 1000)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get customers for incremental sync with cursor-based pagination.

    Customers are derived from orders since there is no separate customer table.

    Query params:
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z")
    - cursor: Pagination cursor from previous response
    - limit: Number of customers (1-1000, default 500)

    Response includes:
    - customers: Array of customer objects
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more customers exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    try:
        logger.info(
            "crm: customers_sync_requested updated_after=%s cursor=%s limit=%s",
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
        )

        # Parse updated_after if provided
        updated_after_dt = None
        if updated_after:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
            except ValueError as e:
                logger.error("crm: customers_sync_invalid_timestamp error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Decode cursor if provided
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
            except Exception as e:
                logger.error("crm: customers_sync_invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Query orders to derive customer data
        query = select(Order)
        if updated_after_dt:
            query = query.where(Order.updated_at >= updated_after_dt)
        if cursor_id is not None:
            query = query.where(Order.id > cursor_id)
        query = query.order_by(Order.updated_at.asc(), Order.id.asc())
        query = query.limit(limit + 1)

        result = await db.execute(query)
        orders = result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # Derive unique customers with aggregated stats
        customers_data = []
        seen_names: set[str] = set()
        for order in orders:
            name = order.customer_name
            if not name or name in seen_names:
                continue
            seen_names.add(name)

            order_count_result = await db.execute(
                select(func.count(Order.id)).where(Order.customer_name == name)
            )
            order_count = order_count_result.scalar() or 0

            total_spend_result = await db.execute(
                select(func.sum(Order.amount)).where(Order.customer_name == name)
            )
            total_spend = float(total_spend_result.scalar() or 0)

            last_order_result = await db.execute(
                select(Order.external_created_at)
                .where(Order.customer_name == name)
                .order_by(Order.external_created_at.desc())
                .limit(1)
            )
            last_order_at = last_order_result.scalar()

            customers_data.append({
                "id": name,
                "name": name,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
                "order_count": order_count,
                "total_spend": total_spend,
                "last_order_at": last_order_at.isoformat() if last_order_at else None,
            })

        # Determine next cursor
        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1].id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        source = "live" if customers_data else "empty"

        logger.info(
            "[CRM] customers_query count=%s has_more=%s source=%s",
            len(customers_data),
            has_more,
            source,
        )

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": server_time,
            "count": len(customers_data),
            "customers": customers_data,
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("crm: customers_sync_failed error=%s", e, exc_info=True)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "count": 0,
            "customers": [],
        }


@router.get("/recent-orders/sync")
async def crm_recent_orders_sync(
    updated_after: Optional[str] = Query(None, description="ISO timestamp - only return orders updated after this time"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    limit: int = Query(500, ge=1, le=1000, description="Number of orders to return (default 500, max 1000)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get recent orders for incremental sync with cursor-based pagination.

    Query params:
    - updated_after: ISO timestamp (e.g., "2026-04-25T12:30:00Z")
    - cursor: Pagination cursor from previous response
    - limit: Number of orders (1-1000, default 500)

    Response includes:
    - orders: Array of order objects
    - next_cursor: Cursor for next page (if has_more=true)
    - has_more: Whether more orders exist
    - server_time: Current server time
    - sync_version: Sync protocol version
    - last_synced_at: Timestamp of this sync
    """
    try:
        logger.info(
            "crm: recent_orders_sync_requested updated_after=%s cursor=%s limit=%s",
            updated_after,
            cursor[:8] + "..." if cursor else None,
            limit,
        )

        # Parse updated_after if provided
        updated_after_dt = None
        if updated_after:
            try:
                updated_after_dt = datetime.fromisoformat(updated_after.replace("Z", "+00:00"))
            except ValueError as e:
                logger.error("crm: recent_orders_sync_invalid_timestamp error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid updated_after timestamp format. Use ISO 8601 (e.g., 2026-04-25T12:30:00Z)",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Decode cursor if provided
        cursor_id = None
        if cursor:
            try:
                cursor_id = int(base64.b64decode(cursor).decode("utf-8"))
            except Exception as e:
                logger.error("crm: recent_orders_sync_invalid_cursor error=%s", e)
                return {
                    "ok": False,
                    "error": "Invalid cursor format",
                    "source": "error",
                    "data_source": "error",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }

        # Build query
        query = select(Order)
        if updated_after_dt:
            query = query.where(Order.updated_at >= updated_after_dt)
        if cursor_id is not None:
            query = query.where(Order.id > cursor_id)
        query = query.order_by(Order.updated_at.asc(), Order.id.asc())
        query = query.limit(limit + 1)

        result = await db.execute(query)
        orders = result.scalars().all()

        # Determine if there are more results
        has_more = len(orders) > limit
        if has_more:
            orders = orders[:limit]

        # Build response
        orders_data = []
        for order in orders:
            order_dict = {
                "id": order.id,
                "external_id": order.external_order_id,
                "order_number": order.order_number,
                "customer_name": order.customer_name,
                "amount": float(order.amount) if order.amount else 0,
                "item_count": order.item_count,
                "unit_count": order.unit_count,
                "status": order.status,
                "review_status": order.review_status,
                "created_at": order.external_created_at.isoformat() if order.external_created_at else None,
                "updated_at": order.updated_at.isoformat() if order.updated_at else None,
            }
            orders_data.append(order_dict)

        # Determine next cursor
        next_cursor = None
        if has_more and orders:
            next_cursor = base64.b64encode(str(orders[-1].id).encode()).decode()

        server_time = datetime.now(timezone.utc).isoformat()

        source = "live" if orders_data else "empty"

        logger.info(
            "[CRM] orders_query count=%s has_more=%s source=%s",
            len(orders_data),
            has_more,
            source,
        )

        return make_json_safe({
            "ok": True,
            "source": source,
            "data_source": source,
            "synced_at": server_time,
            "count": len(orders_data),
            "orders": orders_data,
            "next_cursor": next_cursor if has_more else None,
            "has_more": has_more,
            "server_time": server_time,
            "sync_version": 1,
            "last_synced_at": server_time,
        })

    except Exception as e:
        logger.error("crm: recent_orders_sync_failed error=%s", e, exc_info=True)
        # Never return empty array on error
        return {
            "ok": False,
            "error": str(e),
            "source": "error",
            "data_source": "error",
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "count": 0,
            "orders": [],
        }
