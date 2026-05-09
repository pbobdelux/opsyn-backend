"""
Webhook tenant resolution service.

Resolves the brand/org tenant from an inbound LeafLink webhook payload
using multiple strategies in priority order:

  1. leaflink_company_id field in brand_api_credentials (new secure field)
  2. company_id field in brand_api_credentials (legacy field)
  3. seller_id from payload
  4. brand_id from X-LEAFLINK-BRAND-ID header

Returns a structured result dict with resolution status, brand_id, org_id,
credential, resolution method used, and any error details.

This module is intentionally free of global state and env-var dependencies —
all tenant resolution is driven by per-brand database records.
"""

import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential

logger = logging.getLogger("webhook_tenant_resolver")


async def resolve_tenant_multi_source(
    payload: dict,
    headers: dict,
    db: AsyncSession,
) -> dict[str, Any]:
    """
    Resolve the brand/org tenant from a webhook payload using multiple strategies.

    Tries resolution strategies in priority order, stopping at the first
    unambiguous match. Handles ambiguous cases (multiple brands match) by
    returning status='ambiguous' with all matching brand_ids.

    Args:
        payload: Parsed JSON payload from the webhook request.
        headers: HTTP request headers dict (case-insensitive lookup handled internally).
        db: Active async database session.

    Returns:
        {
            "ok": bool,
            "brand_id": str | None,
            "org_id": str | None,
            "credential": BrandAPICredential | None,
            "resolution_method": str,   # which strategy succeeded
            "error": str | None,
            "status_code": int,         # 200 ok, 202 unresolved, 409 ambiguous, 500 error
            "all_matching_brand_ids": list[str],  # populated on ambiguous
        }
    """
    # Normalize headers to lowercase keys for case-insensitive lookup
    normalized_headers = {k.lower(): v for k, v in (headers or {}).items()}

    # Extract candidate identifiers from payload and headers
    company_id_from_payload = _safe_str(payload.get("company_id"))
    seller_id_from_payload = _safe_str(payload.get("seller_id"))
    brand_id_from_header = _safe_str(normalized_headers.get("x-leaflink-brand-id"))

    # Also check nested data fields
    data = payload.get("data") or {}
    if isinstance(data, dict):
        if not company_id_from_payload:
            company_id_from_payload = _safe_str(data.get("company_id"))
        if not seller_id_from_payload:
            seller_id_from_payload = _safe_str(data.get("seller_id"))

    logger.info(
        "[WebhookTenantResolver] resolution_attempt company_id=%s seller_id=%s brand_id_header=%s",
        company_id_from_payload,
        seller_id_from_payload,
        brand_id_from_header,
    )

    # ------------------------------------------------------------------ #
    # Strategy 1: Match on leaflink_company_id (new secure field)         #
    # ------------------------------------------------------------------ #
    if company_id_from_payload:
        result = await _resolve_by_leaflink_company_id(db, company_id_from_payload)
        if result["ok"] or result["status_code"] == 409:
            result["resolution_method"] = "leaflink_company_id"
            return result
        if result["status_code"] != 404:
            # Unexpected error — propagate
            result["resolution_method"] = "leaflink_company_id"
            return result

    # ------------------------------------------------------------------ #
    # Strategy 2: Match on legacy company_id field                        #
    # ------------------------------------------------------------------ #
    if company_id_from_payload:
        result = await _resolve_by_company_id(db, company_id_from_payload)
        if result["ok"] or result["status_code"] == 409:
            result["resolution_method"] = "company_id"
            return result
        if result["status_code"] != 404:
            result["resolution_method"] = "company_id"
            return result

    # ------------------------------------------------------------------ #
    # Strategy 3: Match on seller_id from payload                         #
    # ------------------------------------------------------------------ #
    if seller_id_from_payload:
        result = await _resolve_by_seller_id(db, seller_id_from_payload)
        if result["ok"] or result["status_code"] == 409:
            result["resolution_method"] = "seller_id"
            return result
        if result["status_code"] != 404:
            result["resolution_method"] = "seller_id"
            return result

    # ------------------------------------------------------------------ #
    # Strategy 4: Match on brand_id from X-LEAFLINK-BRAND-ID header       #
    # ------------------------------------------------------------------ #
    if brand_id_from_header:
        result = await _resolve_by_brand_id_header(db, brand_id_from_header)
        if result["ok"] or result["status_code"] == 409:
            result["resolution_method"] = "x_leaflink_brand_id_header"
            return result
        if result["status_code"] != 404:
            result["resolution_method"] = "x_leaflink_brand_id_header"
            return result

    # ------------------------------------------------------------------ #
    # All strategies exhausted — tenant unresolved                        #
    # ------------------------------------------------------------------ #
    logger.warning(
        "[WebhookTenantResolver] tenant_unresolved company_id=%s seller_id=%s brand_id_header=%s"
        " payload_keys=%s",
        company_id_from_payload,
        seller_id_from_payload,
        brand_id_from_header,
        list(payload.keys())[:10],
    )

    return {
        "ok": False,
        "brand_id": None,
        "org_id": None,
        "credential": None,
        "resolution_method": "none",
        "error": (
            f"Could not resolve tenant from payload. "
            f"company_id={company_id_from_payload!r} "
            f"seller_id={seller_id_from_payload!r} "
            f"brand_id_header={brand_id_from_header!r}"
        ),
        "status_code": 202,  # 202 Accepted — store and retry later
        "all_matching_brand_ids": [],
    }


# ---------------------------------------------------------------------------
# Private resolution helpers
# ---------------------------------------------------------------------------

async def _resolve_by_leaflink_company_id(
    db: AsyncSession,
    company_id: str,
) -> dict[str, Any]:
    """Resolve tenant by matching leaflink_company_id column (new secure field)."""
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.leaflink_company_id == company_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
                BrandAPICredential.webhook_enabled == True,
            )
        )
        rows = result.scalars().all()
    except Exception as exc:
        logger.error(
            "[WebhookTenantResolver] leaflink_company_id_lookup_error company_id=%s error=%s",
            company_id,
            exc,
        )
        return _error_result(f"Database error during leaflink_company_id lookup: {exc}", 500)

    return _evaluate_rows(rows, f"leaflink_company_id={company_id}")


async def _resolve_by_company_id(
    db: AsyncSession,
    company_id: str,
) -> dict[str, Any]:
    """Resolve tenant by matching legacy company_id column."""
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.company_id == company_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        rows = result.scalars().all()
    except Exception as exc:
        logger.error(
            "[WebhookTenantResolver] company_id_lookup_error company_id=%s error=%s",
            company_id,
            exc,
        )
        return _error_result(f"Database error during company_id lookup: {exc}", 500)

    return _evaluate_rows(rows, f"company_id={company_id}")


async def _resolve_by_seller_id(
    db: AsyncSession,
    seller_id: str,
) -> dict[str, Any]:
    """Resolve tenant by matching seller_id against company_id or leaflink_company_id."""
    try:
        # seller_id in LeafLink payloads often corresponds to the company_id
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
                # Match against either company_id or leaflink_company_id
                (
                    (BrandAPICredential.company_id == seller_id) |
                    (BrandAPICredential.leaflink_company_id == seller_id)
                ),
            )
        )
        rows = result.scalars().all()
    except Exception as exc:
        logger.error(
            "[WebhookTenantResolver] seller_id_lookup_error seller_id=%s error=%s",
            seller_id,
            exc,
        )
        return _error_result(f"Database error during seller_id lookup: {exc}", 500)

    return _evaluate_rows(rows, f"seller_id={seller_id}")


async def _resolve_by_brand_id_header(
    db: AsyncSession,
    brand_id: str,
) -> dict[str, Any]:
    """Resolve tenant by matching brand_id from X-LEAFLINK-BRAND-ID header."""
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.brand_id == brand_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        rows = result.scalars().all()
    except Exception as exc:
        logger.error(
            "[WebhookTenantResolver] brand_id_header_lookup_error brand_id=%s error=%s",
            brand_id,
            exc,
        )
        return _error_result(f"Database error during brand_id header lookup: {exc}", 500)

    return _evaluate_rows(rows, f"brand_id_header={brand_id}")


def _evaluate_rows(
    rows: list[BrandAPICredential],
    context: str,
) -> dict[str, Any]:
    """
    Evaluate a list of credential rows and return the appropriate resolution result.

    - 0 rows → not found (404)
    - 1 row  → resolved (200)
    - 2+ rows → ambiguous (409)
    """
    if not rows:
        return {
            "ok": False,
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "resolution_method": "unknown",
            "error": f"No active credential found for {context}",
            "status_code": 404,
            "all_matching_brand_ids": [],
        }

    if len(rows) > 1:
        brand_ids = [r.brand_id for r in rows]
        logger.error(
            "[WebhookTenantResolver] ambiguous_tenant context=%s brand_ids=%s",
            context,
            brand_ids,
        )
        return {
            "ok": False,
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "resolution_method": "unknown",
            "error": f"Ambiguous tenant: {context} matches multiple brands: {brand_ids}",
            "status_code": 409,
            "all_matching_brand_ids": brand_ids,
        }

    cred = rows[0]
    logger.info(
        "[WebhookTenantResolver] tenant_resolved context=%s brand_id=%s org_id=%s",
        context,
        cred.brand_id,
        cred.org_id,
    )
    return {
        "ok": True,
        "brand_id": cred.brand_id,
        "org_id": cred.org_id,
        "credential": cred,
        "resolution_method": "unknown",  # caller sets this
        "error": None,
        "status_code": 200,
        "all_matching_brand_ids": [cred.brand_id],
    }


def _error_result(error: str, status_code: int) -> dict[str, Any]:
    return {
        "ok": False,
        "brand_id": None,
        "org_id": None,
        "credential": None,
        "resolution_method": "unknown",
        "error": error,
        "status_code": status_code,
        "all_matching_brand_ids": [],
    }


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None
