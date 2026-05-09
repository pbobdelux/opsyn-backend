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
    # Normalize headers to lowercase for case-insensitive lookup
    normalized_headers = {k.lower(): v for k, v in (headers or {}).items()}

    # Extract candidate identifiers from payload
    company_id = _safe_str(payload.get("company_id"))
    seller_id = _safe_str(payload.get("seller_id") or payload.get("seller"))
    header_brand_id = normalized_headers.get("x-leaflink-brand-id")

    logger.info(
        "[TenantResolver] resolving tenant company_id=%s seller_id=%s header_brand_id=%s",
        company_id,
        seller_id,
        header_brand_id,
    )

    # ------------------------------------------------------------------ #
    # Strategy 1: leaflink_company_id field (new secure field)            #
    # ------------------------------------------------------------------ #
    if company_id:
        result = await _resolve_by_leaflink_company_id(db, company_id)
        if result["ok"]:
            logger.info(
                "[TenantResolver] resolved via leaflink_company_id brand_id=%s",
                result["brand_id"],
            )
            result["resolution_method"] = "leaflink_company_id"
            return result
        if result["status_code"] == 409:
            # Ambiguous — return immediately
            result["resolution_method"] = "leaflink_company_id"
            return result

    # ------------------------------------------------------------------ #
    # Strategy 2: company_id field (legacy field)                         #
    # ------------------------------------------------------------------ #
    if company_id:
        result = await _resolve_by_company_id(db, company_id)
        if result["ok"]:
            logger.info(
                "[TenantResolver] resolved via company_id brand_id=%s",
                result["brand_id"],
            )
            result["resolution_method"] = "company_id"
            return result
        if result["status_code"] == 409:
            result["resolution_method"] = "company_id"
            return result

    # ------------------------------------------------------------------ #
    # Strategy 3: seller_id from payload                                  #
    # ------------------------------------------------------------------ #
    if seller_id:
        result = await _resolve_by_seller_id(db, seller_id)
        if result["ok"]:
            logger.info(
                "[TenantResolver] resolved via seller_id brand_id=%s",
                result["brand_id"],
            )
            result["resolution_method"] = "seller_id"
            return result
        if result["status_code"] == 409:
            result["resolution_method"] = "seller_id"
            return result

    # ------------------------------------------------------------------ #
    # Strategy 4: X-LEAFLINK-BRAND-ID header                             #
    # ------------------------------------------------------------------ #
    if header_brand_id:
        result = await _resolve_by_brand_id_header(db, header_brand_id)
        if result["ok"]:
            logger.info(
                "[TenantResolver] resolved via X-LEAFLINK-BRAND-ID header brand_id=%s",
                result["brand_id"],
            )
            result["resolution_method"] = "x_leaflink_brand_id_header"
            return result

    # ------------------------------------------------------------------ #
    # Unresolved — no strategy succeeded                                  #
    # ------------------------------------------------------------------ #
    logger.warning(
        "[TenantResolver] tenant_unresolved company_id=%s seller_id=%s header_brand_id=%s",
        company_id,
        seller_id,
        header_brand_id,
    )
    return {
        "ok": False,
        "brand_id": None,
        "org_id": None,
        "credential": None,
        "resolution_method": "none",
        "error": "Could not resolve tenant from payload or headers",
        "status_code": 202,  # Unresolved — store event, return 202
        "all_matching_brand_ids": [],
    }


async def _resolve_by_leaflink_company_id(db: AsyncSession, company_id: str) -> dict[str, Any]:
    """Resolve tenant by leaflink_company_id column (new secure field)."""
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.leaflink_company_id == company_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        rows = result.scalars().all()
    except AttributeError:
        # leaflink_company_id column may not exist yet (pre-migration)
        return _not_found_result()
    except Exception as exc:
        logger.error("[TenantResolver] leaflink_company_id lookup error: %s", exc)
        return _error_result(str(exc))

    return _process_rows(rows, company_id, "leaflink_company_id")


async def _resolve_by_company_id(db: AsyncSession, company_id: str) -> dict[str, Any]:
    """Resolve tenant by legacy company_id column."""
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
        logger.error("[TenantResolver] company_id lookup error: %s", exc)
        return _error_result(str(exc))

    return _process_rows(rows, company_id, "company_id")


async def _resolve_by_seller_id(db: AsyncSession, seller_id: str) -> dict[str, Any]:
    """Resolve tenant by seller_id (tries both company_id and leaflink_company_id)."""
    try:
        result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.company_id == seller_id,
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
        )
        rows = result.scalars().all()
    except Exception as exc:
        logger.error("[TenantResolver] seller_id lookup error: %s", exc)
        return _error_result(str(exc))

    return _process_rows(rows, seller_id, "seller_id")


async def _resolve_by_brand_id_header(db: AsyncSession, brand_id: str) -> dict[str, Any]:
    """Resolve tenant by X-LEAFLINK-BRAND-ID header value."""
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
        logger.error("[TenantResolver] brand_id header lookup error: %s", exc)
        return _error_result(str(exc))

    return _process_rows(rows, brand_id, "x_leaflink_brand_id_header")


def _process_rows(rows: list, identifier: str, strategy: str) -> dict[str, Any]:
    """Process query rows into a resolution result."""
    if not rows:
        return _not_found_result()

    if len(rows) > 1:
        brand_ids = [r.brand_id for r in rows]
        logger.warning(
            "[TenantResolver] ambiguous_match strategy=%s identifier=%s brand_ids=%s",
            strategy,
            identifier,
            brand_ids,
        )
        return {
            "ok": False,
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "resolution_method": strategy,
            "error": f"Ambiguous: {identifier} maps to multiple brands: {brand_ids}",
            "status_code": 409,
            "all_matching_brand_ids": brand_ids,
        }

    cred = rows[0]
    return {
        "ok": True,
        "brand_id": cred.brand_id,
        "org_id": cred.org_id,
        "credential": cred,
        "resolution_method": strategy,
        "error": None,
        "status_code": 200,
        "all_matching_brand_ids": [cred.brand_id],
    }


def _not_found_result() -> dict[str, Any]:
    return {
        "ok": False,
        "brand_id": None,
        "org_id": None,
        "credential": None,
        "resolution_method": "none",
        "error": None,
        "status_code": 202,
        "all_matching_brand_ids": [],
    }


def _error_result(error: str) -> dict[str, Any]:
    return {
        "ok": False,
        "brand_id": None,
        "org_id": None,
        "credential": None,
        "resolution_method": "none",
        "error": error,
        "status_code": 500,
        "all_matching_brand_ids": [],
    }


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_val = str(value).strip()
    return text_val if text_val else None
