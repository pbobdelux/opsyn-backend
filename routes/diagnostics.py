"""
LeafLink diagnostic endpoint — returns raw API response without any transformation.

GET /diagnostics/leaflink/orders-raw?brand_id=<brand_id>

Use this to inspect exactly what LeafLink returns for a given brand's credentials,
including the final URL, HTTP status, query params, pagination fields, and raw body.
No data is upserted or transformed — purely observational.
"""
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import BrandAPICredential
from services.leaflink_client import LeafLinkClient

logger = logging.getLogger("diagnostics")

router = APIRouter(prefix="/diagnostics", tags=["diagnostics"])

# Thread pool for running synchronous LeafLink HTTP calls off the event loop
_diag_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="leaflink-diag")


def _call_list_orders_raw(client: LeafLinkClient, limit: int = 10) -> dict[str, Any]:
    """
    Call LeafLinkClient.list_orders() with no date filters and return a structured
    diagnostic dict containing the raw response, status, params, and pagination info.

    This runs synchronously inside a thread pool executor so it doesn't block the
    async event loop.
    """
    # Build the final URL the same way list_orders() does, for reporting
    _orders_path = "orders-received/"
    _final_url = f"{client.base_url.rstrip('/')}/{_orders_path}"
    _params_sent = {"limit": limit, "offset": 0}

    logger.info(
        "[LEAFLINK_DIAGNOSTIC] calling_list_orders brand_id=%s final_url=%s params=%s",
        client.brand_id,
        _final_url,
        _params_sent,
    )

    try:
        # Call with skip_date_filters=True so we get the raw unfiltered response
        data = client.list_orders(
            limit=limit,
            offset=0,
            skip_date_filters=True,
        )
    except RuntimeError as exc:
        # list_orders raises RuntimeError on non-200 responses
        return {
            "ok": False,
            "final_url": _final_url,
            "params": _params_sent,
            "error": str(exc)[:500],
            "response_body": None,
        }

    # Determine response type and extract pagination fields
    if isinstance(data, list):
        response_type = "list"
        parsed_count = len(data)
        pagination = {"count": None, "next": None, "previous": None}
        response_body = data
    elif isinstance(data, dict):
        response_type = "dict"
        results = (
            data.get("results")
            or data.get("data")
            or data.get("orders")
            or []
        )
        parsed_count = len(results) if isinstance(results, list) else 0
        pagination = {
            "count": data.get("count"),
            "next": data.get("next"),
            "previous": data.get("previous"),
        }
        response_body = data
    else:
        response_type = "other"
        parsed_count = 0
        pagination = {"count": None, "next": None, "previous": None}
        response_body = str(data)

    # Build a text preview of the response body (first 1000 chars of JSON repr)
    import json as _json
    try:
        _preview = _json.dumps(response_body, default=str)[:1000]
    except Exception:
        _preview = str(response_body)[:1000]

    return {
        "ok": True,
        "final_url": _final_url,
        "params": _params_sent,
        "response_type": response_type,
        "response_preview": _preview,
        "parsed_count": parsed_count,
        "pagination": pagination,
        "response_body": response_body,
    }


@router.get("/leaflink/orders-raw")
async def diagnostic_leaflink_orders_raw(
    brand_id: str = Query(..., description="Brand ID to load LeafLink credentials for"),
    limit: int = Query(10, ge=1, le=100, description="Max orders to fetch (default 10)"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Fetch raw LeafLink orders for a brand WITHOUT any transformation or DB upsert.

    Returns the exact response from LeafLink including:
    - The final URL called
    - HTTP status code (logged; errors surface as ok=false)
    - Query parameters sent
    - Response type (dict | list | other)
    - First 1000 chars of the response body
    - Pagination fields (count, next, previous)
    - Full response body

    Date filters are intentionally omitted so the call tests the broadest possible
    query — if this returns orders but the sync returns zero, date filters are the
    likely culprit.
    """
    logger.info(
        "[LEAFLINK_DIAGNOSTIC] request brand_id=%s limit=%s",
        brand_id,
        limit,
    )

    # Load the active LeafLink credential for this brand
    result = await db.execute(
        select(BrandAPICredential).where(
            BrandAPICredential.brand_id == brand_id,
            BrandAPICredential.integration_name == "leaflink",
            BrandAPICredential.is_active == True,
        )
    )
    cred = result.scalar_one_or_none()

    if cred is None:
        logger.warning(
            "[LEAFLINK_DIAGNOSTIC] no_credential_found brand_id=%s",
            brand_id,
        )
        raise HTTPException(
            status_code=404,
            detail=f"No active LeafLink credential found for brand_id={brand_id}",
        )

    logger.info(
        "[LEAFLINK_DIAGNOSTIC] credential_found brand_id=%s company_id=%s"
        " base_url=%s auth_scheme=%s",
        brand_id,
        cred.company_id or "none",
        cred.base_url or "none",
        cred.auth_scheme or "Token (default)",
    )

    # Instantiate the client — this validates the credential and logs base_url/auth_scheme
    try:
        client = LeafLinkClient(
            api_key=cred.api_key,
            company_id=cred.company_id,
            brand_id=brand_id,
            base_url=cred.base_url,
            auth_scheme=cred.auth_scheme or "Token",
        )
    except Exception as client_exc:
        logger.error(
            "[LEAFLINK_DIAGNOSTIC] client_init_failed brand_id=%s error=%s",
            brand_id,
            str(client_exc)[:300],
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialise LeafLink client: {str(client_exc)[:300]}",
        )

    # Run the synchronous HTTP call in a thread pool so we don't block the event loop
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        diag_result = await loop.run_in_executor(
            _diag_executor,
            lambda: _call_list_orders_raw(client, limit=limit),
        )
    except Exception as exc:
        logger.error(
            "[LEAFLINK_DIAGNOSTIC] executor_error brand_id=%s error=%s",
            brand_id,
            str(exc)[:300],
        )
        raise HTTPException(
            status_code=500,
            detail=f"Diagnostic call failed: {str(exc)[:300]}",
        )

    _endpoint_called = diag_result.get("final_url", "unknown")
    _status = "ok" if diag_result.get("ok") else "error"

    logger.info(
        "[LEAFLINK_DIAGNOSTIC] brand_id=%s endpoint_called=%s status=%s"
        " parsed_count=%s response_type=%s",
        brand_id,
        _endpoint_called,
        _status,
        diag_result.get("parsed_count", 0),
        diag_result.get("response_type", "unknown"),
    )

    return {
        "ok": diag_result.get("ok", False),
        "brand_id": brand_id,
        "company_id": cred.company_id,
        "final_url": diag_result.get("final_url"),
        "params": diag_result.get("params"),
        "response_type": diag_result.get("response_type"),
        "response_preview": diag_result.get("response_preview"),
        "parsed_count": diag_result.get("parsed_count", 0),
        "pagination": diag_result.get("pagination"),
        "response_body": diag_result.get("response_body"),
        "error": diag_result.get("error"),
    }
