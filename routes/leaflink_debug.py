"""
LeafLink debug endpoint — returns full connectivity and configuration diagnostics.
GET /leaflink/debug
"""
import logging
import os
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential, Order
from services.leaflink_client import (
    DEFAULT_LEAFLINK_BASE_URL,
    DEFAULT_LEAFLINK_COMPANY_ID,
    MOCK_MODE,
    LeafLinkClient,
)

logger = logging.getLogger("leaflink_debug")
router = APIRouter()


def _env_status(value: str) -> str:
    return "set" if value else "not_set"


@router.get("/leaflink/debug")
async def debug_leaflink(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    logger.info("leaflink: debug_endpoint called")

    # ── Environment variable status ──────────────────────────────────────────
    api_key_raw = os.getenv("LEAFLINK_API_KEY", "")
    company_id_raw = os.getenv("LEAFLINK_COMPANY_ID", "")
    base_url_raw = os.getenv("LEAFLINK_BASE_URL", "")
    user_key_raw = os.getenv("LEAFLINK_USER_KEY", "")
    vendor_key_raw = os.getenv("LEAFLINK_VENDOR_KEY", "")
    mock_mode_raw = os.getenv("MOCK_MODE", "")

    environment_vars = {
        "LEAFLINK_API_KEY": _env_status(api_key_raw),
        "LEAFLINK_COMPANY_ID": _env_status(company_id_raw),
        "LEAFLINK_BASE_URL": _env_status(base_url_raw),
        "LEAFLINK_USER_KEY": _env_status(user_key_raw),
        "LEAFLINK_VENDOR_KEY": _env_status(vendor_key_raw),
        "MOCK_MODE": mock_mode_raw if mock_mode_raw else "not_set",
    }

    credentials_loaded = bool(api_key_raw and company_id_raw and base_url_raw)
    logger.info(
        "leaflink: debug credentials_loaded=%s api_key_set=%s company_id_set=%s base_url_set=%s",
        credentials_loaded,
        bool(api_key_raw),
        bool(company_id_raw),
        bool(base_url_raw),
    )

    # ── Try to create client and call API ────────────────────────────────────
    api_connected = False
    credentials_valid = False
    last_error: str | None = None

    try:
        client = LeafLinkClient()
        credentials_valid = True
        logger.info("leaflink: debug client_created base_url=%s company_id=%s", client.base_url, client.company_id)

        try:
            payload = client.list_orders(page=1, page_size=1)
            api_connected = True
            logger.info("leaflink: debug api_connected=true payload_type=%s", type(payload).__name__)
        except Exception as api_exc:
            last_error = str(api_exc)
            logger.error("leaflink: debug api_call_failed error=%s", api_exc)

    except Exception as client_exc:
        last_error = str(client_exc)
        logger.error("leaflink: debug client_init_failed error=%s", client_exc)

    # ── Database: count orders ───────────────────────────────────────────────
    orders_in_db = 0
    try:
        count_result = await db.execute(select(func.count()).select_from(Order))
        orders_in_db = count_result.scalar_one() or 0
        logger.info("leaflink: debug orders_in_db=%s", orders_in_db)
    except Exception as db_exc:
        logger.error("leaflink: debug db_count_failed error=%s", db_exc)

    # ── Last sync attempt from BrandAPICredential ────────────────────────────
    last_sync_attempt: str | None = None
    last_sync_status = "never"
    db_last_error: str | None = None

    try:
        cred_result = await db.execute(
            select(BrandAPICredential)
            .where(BrandAPICredential.integration_name == "leaflink")
            .order_by(BrandAPICredential.last_sync_at.desc().nullslast())
            .limit(1)
        )
        cred = cred_result.scalar_one_or_none()

        if cred:
            if cred.last_sync_at:
                last_sync_attempt = cred.last_sync_at.isoformat()
                last_sync_status = cred.sync_status or "unknown"
            db_last_error = cred.last_error
            logger.info(
                "leaflink: debug last_sync_at=%s sync_status=%s",
                last_sync_attempt,
                last_sync_status,
            )
        else:
            logger.info("leaflink: debug no BrandAPICredential found for leaflink")
    except Exception as cred_exc:
        logger.error("leaflink: debug cred_lookup_failed error=%s", cred_exc)

    combined_last_error = last_error or db_last_error

    result = {
        "ok": True,
        "api_connected": api_connected,
        "credentials_loaded": credentials_loaded,
        "credentials_valid": credentials_valid,
        "base_url": DEFAULT_LEAFLINK_BASE_URL or base_url_raw or None,
        "company_id": DEFAULT_LEAFLINK_COMPANY_ID or company_id_raw or None,
        "orders_in_db": orders_in_db,
        "last_sync_attempt": last_sync_attempt,
        "last_sync_status": last_sync_status,
        "last_error": combined_last_error,
        "mock_mode_enabled": MOCK_MODE,
        "environment_vars": environment_vars,
    }

    logger.info(
        "leaflink: debug_complete api_connected=%s credentials_loaded=%s orders_in_db=%s mock_mode=%s",
        api_connected,
        credentials_loaded,
        orders_in_db,
        MOCK_MODE,
    )

    return result
