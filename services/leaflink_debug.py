"""
LeafLink debug service.

Provides connectivity checks and diagnostic information for the LeafLink
integration without exposing API keys or secrets.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential, Order

logger = logging.getLogger("leaflink_debug")

LEAFLINK_API_KEY = os.getenv("LEAFLINK_API_KEY", "").strip()
LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://app.leaflink.com/api/v2").strip().rstrip("/")
MOCK_MODE = os.getenv("MOCK_MODE", "false").strip().lower() == "true"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def check_api_connectivity(db: AsyncSession) -> dict[str, Any]:
    """
    Attempt to fetch one page of orders from the LeafLink API.

    Returns a dict with keys:
        ok            – True if the request succeeded
        api_connected – True if the API returned a valid response
        orders_count  – Number of orders returned on the first page
        error         – Error message if the request failed, else None
    """
    from services.leaflink_client import LeafLinkClient  # local import to avoid circular deps

    if not LEAFLINK_API_KEY or not LEAFLINK_COMPANY_ID:
        logger.info("leaflink_debug: API key or company_id not configured, skipping connectivity check")
        return {
            "ok": False,
            "api_connected": False,
            "orders_count": 0,
            "error": "LEAFLINK_API_KEY or LEAFLINK_COMPANY_ID not configured",
        }

    try:
        logger.info(
            "leaflink_debug: checking API connectivity for company_id=%s base_url=%s",
            LEAFLINK_COMPANY_ID,
            LEAFLINK_BASE_URL,
        )
        client = LeafLinkClient(
            api_key=LEAFLINK_API_KEY,
            company_id=LEAFLINK_COMPANY_ID,
            base_url=LEAFLINK_BASE_URL,
        )
        payload = client.list_orders(page=1, page_size=1)

        if isinstance(payload, list):
            count = len(payload)
        elif isinstance(payload, dict):
            results = (
                payload.get("results")
                or payload.get("data")
                or payload.get("orders")
                or []
            )
            count = len(results) if isinstance(results, list) else 0
        else:
            count = 0

        logger.info("leaflink_debug: API connectivity OK, orders_count=%d", count)
        return {
            "ok": True,
            "api_connected": True,
            "orders_count": count,
            "error": None,
        }

    except Exception as exc:
        logger.error("leaflink_debug: API connectivity check failed: %s", str(exc))
        return {
            "ok": False,
            "api_connected": False,
            "orders_count": 0,
            "error": str(exc),
        }


async def get_debug_info(db: AsyncSession) -> dict[str, Any]:
    """
    Return full diagnostic information about the LeafLink integration.

    Checks:
    - Whether environment credentials are loaded
    - Whether the API is reachable
    - How many orders are stored in the database
    - Whether mock mode is active
    """
    credentials_loaded = bool(LEAFLINK_API_KEY and LEAFLINK_COMPANY_ID)
    api_key_present = bool(LEAFLINK_API_KEY)

    logger.info(
        "leaflink_debug: get_debug_info credentials_loaded=%s mock_mode=%s",
        credentials_loaded,
        MOCK_MODE,
    )

    # Count orders in the database
    db_orders_count = 0
    db_error: str | None = None
    try:
        count_result = await db.execute(select(func.count()).select_from(Order))
        db_orders_count = count_result.scalar_one() or 0
        logger.info("leaflink_debug: database orders_count=%d", db_orders_count)
    except Exception as exc:
        db_error = str(exc)
        logger.error("leaflink_debug: database count failed: %s", db_error)

    # Check API connectivity
    connectivity = await check_api_connectivity(db)

    last_error = connectivity.get("error") or db_error

    return {
        "ok": connectivity["ok"] or db_orders_count > 0,
        "api_connected": connectivity["api_connected"],
        "orders_count": db_orders_count,
        "using_mock_data": MOCK_MODE,
        "last_error": last_error,
        "credentials_loaded": credentials_loaded,
        "api_key_present": api_key_present,
        "company_id": LEAFLINK_COMPANY_ID or None,
        "base_url": LEAFLINK_BASE_URL,
        "timestamp": _utc_now_iso(),
    }
