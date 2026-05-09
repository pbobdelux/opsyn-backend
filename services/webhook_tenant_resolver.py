"""
Webhook tenant resolver — resolves brand_id + org_id from inbound webhook payloads.

LeafLink webhooks do not include a brand_id directly.  We resolve the tenant by
matching the company_id (from the payload or X-LeafLink-Company-Id header) against
the leaflink_company_id column in brand_api_credentials.

Resolution strategy (tried in order):
  1. X-LeafLink-Company-Id header (most explicit)
  2. payload.company_id
  3. payload.seller_id
  4. payload.leaflink_company_id

Outcomes:
  resolved   — exactly one active credential matched
  unresolved — no credential matched
  ambiguous  — multiple credentials matched (data integrity issue)
"""

import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import BrandAPICredential

logger = logging.getLogger("webhook_tenant_resolver")


async def resolve_tenant_from_webhook(
    db: AsyncSession,
    payload: dict[str, Any],
    company_id_header: Optional[str] = None,
) -> dict[str, Any]:
    """
    Resolve brand_id + org_id from an inbound LeafLink webhook payload.

    Tries multiple fields in priority order to find a company_id, then
    queries brand_api_credentials for a matching active credential.

    Args:
        db:                 Active async database session.
        payload:            Parsed JSON payload from the webhook request.
        company_id_header:  Value of the X-LeafLink-Company-Id header (optional).

    Returns a dict with the following keys:
        ok (bool)                   — True if resolved unambiguously
        status (str)                — "resolved" | "unresolved" | "ambiguous"
        brand_id (str | None)       — resolved brand UUID
        org_id (str | None)         — resolved org UUID
        credential (BrandAPICredential | None)
        error (str | None)          — human-readable error message
        candidates (list[str])      — brand_ids when ambiguous
        resolved_company_id (str | None) — the company_id that matched
    """
    # ------------------------------------------------------------------
    # Step 1: Extract candidate company_id values in priority order
    # ------------------------------------------------------------------
    candidate_ids: list[str] = []

    def _add(val: Any) -> None:
        s = str(val).strip() if val is not None else ""
        if s and s not in candidate_ids:
            candidate_ids.append(s)

    _add(company_id_header)
    _add(payload.get("company_id"))
    _add(payload.get("seller_id"))
    _add(payload.get("leaflink_company_id"))

    # Also check nested data dict
    data = payload.get("data") or {}
    if isinstance(data, dict):
        _add(data.get("company_id"))
        _add(data.get("seller_id"))

    if not candidate_ids:
        logger.warning("[WEBHOOK_TENANT_UNRESOLVED] reason=no_company_id_in_payload")
        return {
            "ok": False,
            "status": "unresolved",
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "error": "No company_id found in payload or headers",
            "candidates": [],
            "resolved_company_id": None,
        }

    # ------------------------------------------------------------------
    # Step 2: Query credentials for each candidate until we find a match
    # ------------------------------------------------------------------
    for company_id in candidate_ids:
        try:
            result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.leaflink_company_id == company_id,
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            rows = result.scalars().all()
        except Exception as exc:
            logger.error(
                "[WEBHOOK_TENANT_RESOLVER] db_error company_id=%s error=%s",
                company_id,
                str(exc)[:300],
            )
            continue

        if not rows:
            # Also try the legacy company_id column as a fallback
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
                    "[WEBHOOK_TENANT_RESOLVER] db_error_fallback company_id=%s error=%s",
                    company_id,
                    str(exc)[:300],
                )
                continue

        if not rows:
            continue  # Try next candidate

        if len(rows) == 1:
            cred = rows[0]
            logger.info(
                "[WEBHOOK_TENANT_RESOLVED] company_id=%s brand_id=%s org_id=%s",
                company_id,
                cred.brand_id,
                cred.org_id,
            )
            return {
                "ok": True,
                "status": "resolved",
                "brand_id": cred.brand_id,
                "org_id": cred.org_id,
                "credential": cred,
                "error": None,
                "candidates": [cred.brand_id],
                "resolved_company_id": company_id,
            }

        # Multiple matches — ambiguous
        brand_ids = [r.brand_id for r in rows]
        logger.warning(
            "[WEBHOOK_TENANT_AMBIGUOUS] company_id=%s brand_ids=%s",
            company_id,
            brand_ids,
        )
        return {
            "ok": False,
            "status": "ambiguous",
            "brand_id": None,
            "org_id": None,
            "credential": None,
            "error": (
                f"company_id {company_id!r} matches multiple brands: {brand_ids}. "
                "Set a unique leaflink_company_id per brand via the webhook-config endpoint."
            ),
            "candidates": brand_ids,
            "resolved_company_id": company_id,
        }

    # No candidate matched any credential
    logger.warning(
        "[WEBHOOK_TENANT_UNRESOLVED] candidates_tried=%s",
        candidate_ids,
    )
    return {
        "ok": False,
        "status": "unresolved",
        "brand_id": None,
        "org_id": None,
        "credential": None,
        "error": (
            f"No active LeafLink credential found for company_id(s): {candidate_ids}. "
            "Configure leaflink_company_id via the webhook-config endpoint."
        ),
        "candidates": [],
        "resolved_company_id": None,
    }
