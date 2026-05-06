import logging
import os
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order, SyncRun
from models.sync_health import DeadLetterLineItem, SyncHealth

logger = logging.getLogger("opsyn-backend")
router = APIRouter(prefix="/debug", tags=["debug"])


@router.get("/db-check")
async def debug_db_check(
    brand: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Debug endpoint to diagnose database connection and tenant credentials."""
    logger.info("[DB] debug_db_check_request brand=%s", brand)

    # Parse DATABASE_URL
    database_url = os.getenv("DATABASE_URL", "")
    db_host = "unknown"
    db_port = 5432
    db_name = "unknown"

    if database_url:
        try:
            parsed = urlparse(database_url)
            db_host = parsed.hostname or "unknown"
            db_port = parsed.port or 5432
            db_name = parsed.path.lstrip("/") or "unknown"
        except Exception as exc:
            logger.error("[DB] parse_error error=%s", exc)

    # Count total credentials
    total_count = None
    try:
        count_result = await db.execute(
            select(func.count(BrandAPICredential.id))
        )
        total_count = count_result.scalar_one() or 0
    except Exception as exc:
        logger.error("[DB] count_error error=%s", exc)

    # Check for specific brand if provided
    if brand:
        logger.info("[DB] debug_db_check_request brand=%s", brand)

        # Use shared resolver
        from services.credential_resolver import resolve_brand_credential

        cred_row = await resolve_brand_credential(db, brand, "leaflink")

        if cred_row:
            credential_id = cred_row[0]
            credential_brand = cred_row[1]
            credential_integration = cred_row[2]
            company_id = cred_row[3]
            api_key = cred_row[4]
            is_active = cred_row[5]
            sync_status = cred_row[6]
            last_synced_page = cred_row[7]

            credential_exists = True
            api_key_len = len(api_key) if api_key else None
            resolver_match_found = True
        else:
            credential_exists = False
            credential_id = None
            company_id = None
            api_key_len = None
            is_active = None
            sync_status = None
            last_synced_page = None
            resolver_match_found = False

        return {
            "ok": True,
            "database_name": db_name,
            "brand_id": brand,
            "credential_exists": credential_exists,
            "credential_id": credential_id,
            "company_id": company_id,
            "api_key_len": api_key_len,
            "is_active": is_active,
            "sync_status": sync_status,
            "last_synced_page": last_synced_page,
            "brand_api_credentials_count": total_count,
            "resolver_match_found": resolver_match_found,
            "credential_source": "db_only",
            "env_checked": False,
        }
    else:
        # Return general database info
        return {
            "ok": True,
            "database_url_host": db_host,
            "database_url_port": db_port,
            "database_name": db_name,
            "brand_api_credentials_count": total_count,
            "credential_source": "db_only",
            "env_checked": False,
        }


@router.get("/brand-api-credentials")
async def debug_brand_api_credentials(
    db: AsyncSession = Depends(get_db),
):
    """Debug endpoint to inspect all brand_api_credentials rows with exact values."""

    logger.info("[DEBUG] brand_api_credentials_dump_request")

    try:
        result = await db.execute(
            text("""
                SELECT
                    id,
                    brand_id,
                    LENGTH(brand_id) AS brand_id_length,
                    quote_literal(brand_id) AS quoted_brand_id,
                    integration_name,
                    LENGTH(integration_name) AS integration_name_length,
                    quote_literal(integration_name) AS quoted_integration_name,
                    company_id,
                    is_active,
                    sync_status,
                    last_synced_page,
                    LENGTH(api_key) AS api_key_len,
                    created_at,
                    updated_at
                FROM brand_api_credentials
                ORDER BY created_at DESC
            """)
        )

        rows = result.fetchall()

        credentials = []
        for row in rows:
            credentials.append({
                "id": row[0],
                "brand_id": row[1],
                "brand_id_length": row[2],
                "quoted_brand_id": row[3],
                "integration_name": row[4],
                "integration_name_length": row[5],
                "quoted_integration_name": row[6],
                "company_id": row[7],
                "is_active": row[8],
                "sync_status": row[9],
                "last_synced_page": row[10],
                "api_key_len": row[11],
                "created_at": row[12],
                "updated_at": row[13],
            })

        logger.info("[DEBUG] brand_api_credentials_dump total_rows=%s", len(credentials))

        return {
            "ok": True,
            "total_credentials": len(credentials),
            "credentials": credentials,
        }

    except Exception as exc:
        logger.error("[DEBUG] brand_api_credentials_dump_error error=%s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/orders/brand/{brand_id}")
async def debug_brand_orders(
    brand_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Diagnostic endpoint to trace order visibility issues for a brand.

    Returns comprehensive order and credential information without secrets.
    Use this to diagnose why orders are not appearing in the brand app.
    """
    logger.info("[DEBUG] debug_brand_orders_request brand_id=%s", brand_id)

    try:
        # Get credential (no API key in response)
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
            )
        )
        cred = cred_result.scalar_one_or_none()

        # Get total order count for brand
        total_result = await db.execute(
            select(func.count()).select_from(Order).where(Order.brand_id == brand_id)
        )
        total_orders = total_result.scalar() or 0

        # Get latest 10 orders
        latest_result = await db.execute(
            select(Order)
            .where(Order.brand_id == brand_id)
            .order_by(Order.synced_at.desc())
            .limit(10)
        )
        latest_orders = latest_result.scalars().all()

        # Get null field stats
        null_stats = {}
        for field in ["external_order_id", "org_id", "brand_id", "synced_at", "external_created_at"]:
            null_result = await db.execute(
                select(func.count()).select_from(Order).where(
                    Order.brand_id == brand_id,
                    getattr(Order, field).is_(None),
                )
            )
            null_stats[f"null_{field}"] = null_result.scalar() or 0

        # Get latest sync run
        sync_run_result = await db.execute(
            select(SyncRun)
            .where(SyncRun.brand_id == brand_id)
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
        latest_sync_run = sync_run_result.scalar_one_or_none()

        # Get sync health
        health_result = await db.execute(
            select(SyncHealth).where(SyncHealth.brand_id == brand_id)
        )
        sync_health = health_result.scalar_one_or_none()

        # Get dead letters
        dead_letter_result = await db.execute(
            select(DeadLetterLineItem)
            .where(DeadLetterLineItem.brand_id == brand_id)
            .order_by(DeadLetterLineItem.last_failed_at.desc())
            .limit(5)
        )
        dead_letters = dead_letter_result.scalars().all()

        logger.info(
            "[DEBUG] debug_brand_orders_result brand_id=%s total_orders=%s cred_found=%s",
            brand_id,
            total_orders,
            cred is not None,
        )

        return {
            "ok": True,
            "brand_id": brand_id,
            "credential": {
                "brand_id": cred.brand_id if cred else None,
                "org_id": cred.org_id if cred else None,
                "company_id": cred.company_id if cred else None,
                "auth_scheme": cred.auth_scheme if cred else None,
                "base_url": cred.base_url[:50] if cred and cred.base_url else None,
                "integration_name": cred.integration_name if cred else None,
                "is_active": cred.is_active if cred else None,
            },
            "order_stats": {
                "total_orders": total_orders,
                "null_field_stats": null_stats,
            },
            "latest_orders": [
                {
                    "id": o.id,
                    "external_order_id": o.external_order_id,
                    "brand_id": o.brand_id,
                    "org_id": o.org_id,
                    "customer_name": o.customer_name,
                    "status": o.status,
                    "synced_at": o.synced_at.isoformat() if o.synced_at else None,
                    "external_created_at": o.external_created_at.isoformat() if o.external_created_at else None,
                    "external_updated_at": o.external_updated_at.isoformat() if o.external_updated_at else None,
                }
                for o in latest_orders
            ],
            "latest_sync_run": {
                "id": latest_sync_run.id if latest_sync_run else None,
                "status": latest_sync_run.status if latest_sync_run else None,
                "started_at": latest_sync_run.started_at.isoformat() if latest_sync_run and latest_sync_run.started_at else None,
                "completed_at": latest_sync_run.completed_at.isoformat() if latest_sync_run and latest_sync_run.completed_at else None,
                "last_error": latest_sync_run.last_error if latest_sync_run else None,
            },
            "sync_health": {
                "last_successful_sync_at": sync_health.last_successful_sync_at.isoformat() if sync_health and sync_health.last_successful_sync_at else None,
                "consecutive_failures": sync_health.consecutive_failures if sync_health else None,
                "total_orders_synced": sync_health.total_orders_synced if sync_health else None,
                "last_error": sync_health.last_error if sync_health else None,
            },
            "dead_letters": [
                {
                    "id": dl.id,
                    "external_order_id": dl.external_order_id,
                    "sku": dl.sku,
                    "failure_reason": dl.failure_reason,
                    "failure_count": dl.failure_count,
                    "last_failed_at": dl.last_failed_at.isoformat() if dl.last_failed_at else None,
                }
                for dl in dead_letters
            ],
        }

    except Exception as exc:
        logger.error("[DEBUG] debug_brand_orders_error brand_id=%s error=%s", brand_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))
