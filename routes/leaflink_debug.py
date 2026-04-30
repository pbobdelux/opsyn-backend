"""
LeafLink debug endpoint — returns full connectivity and configuration diagnostics.
GET /leaflink/debug
GET /leaflink/auth-debug
"""
import logging
import os
from typing import Any, Optional

import requests

from fastapi import APIRouter, Depends, Header, HTTPException
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

# DEFAULT_LEAFLINK_API_KEY was removed — credentials are now loaded from DB per brand

logger = logging.getLogger("leaflink_debug")
router = APIRouter()


def _env_status(value: str) -> str:
    return "set" if value else "not_set"


@router.get("/debug")
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
    credentials_source: str | None = None
    last_error: str | None = None
    api_error: str | None = None

    # Look up the first active LeafLink credential from the database,
    # mirroring the same auth path used by the working sync flow.
    db_cred_for_api = None
    try:
        cred_api_result = await db.execute(
            select(BrandAPICredential)
            .where(
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
            .order_by(BrandAPICredential.last_sync_at.desc().nullslast())
            .limit(1)
        )
        db_cred_for_api = cred_api_result.scalar_one_or_none()
    except Exception as cred_lookup_exc:
        logger.error("leaflink: debug cred_api_lookup_failed error=%s", cred_lookup_exc)

    if db_cred_for_api is not None:
        credentials_source = "database"
        logger.info(
            "leaflink: debug using_db_credentials brand_id=%s company_id=%s",
            db_cred_for_api.brand_id,
            db_cred_for_api.company_id,
        )
        logger.info(
            "[OrdersAPI] brand_id=%s company_id=%s",
            db_cred_for_api.brand_id,
            db_cred_for_api.company_id,
        )
        try:
            client = LeafLinkClient(
                api_key=db_cred_for_api.api_key,
                company_id=db_cred_for_api.company_id,
                brand_id=db_cred_for_api.brand_id,
            )
            credentials_valid = True
            logger.info(
                "leaflink: debug client_created base_url=%s company_id=%s",
                client.base_url,
                client.company_id,
            )

            try:
                payload = client.list_orders(page=1, page_size=1)
                api_connected = True
                logger.info("leaflink: debug api_connected=true payload_type=%s", type(payload).__name__)
            except Exception as api_exc:
                api_error = str(api_exc)
                logger.error("leaflink: debug api_call_failed error=%s", api_exc)

        except Exception as client_exc:
            api_error = str(client_exc)
            logger.error("leaflink: debug client_init_failed error=%s", client_exc)
    else:
        # No DB credentials found — cannot connect without per-brand credentials.
        credentials_source = "missing"
        logger.warning("leaflink: debug no_db_credentials_found — cannot connect without per-brand credentials")
        api_error = "No active LeafLink credentials found in database. Use POST /integrations/leaflink/connect to add credentials."

    # ── Database: count orders ───────────────────────────────────────────────
    orders_in_db = 0
    try:
        count_result = await db.execute(select(func.count()).select_from(Order))
        orders_in_db = count_result.scalar_one() or 0
        logger.info("leaflink: debug orders_in_db=%s", orders_in_db)
    except Exception as db_exc:
        logger.error("leaflink: debug db_count_failed error=%s", db_exc)

    # ── Determine connection_status and apply fallback logic ─────────────────
    # - live:               API call succeeded
    # - healthy_from_sync:  API call failed but DB has orders (don't surface error)
    # - error:              API call failed and DB is empty
    if api_connected:
        connection_status = "live"
    elif orders_in_db > 0:
        api_connected = True
        connection_status = "healthy_from_sync"
        logger.info(
            "leaflink: debug fallback_triggered orders_in_db=%s api_error=%s",
            orders_in_db,
            api_error,
        )
    else:
        connection_status = "error"
        last_error = api_error

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

    # Only surface last_error when connection_status is "error" (i.e. no fallback
    # succeeded). For "live" and "healthy_from_sync" states last_error is null so
    # callers don't see transient API failures as actionable problems.
    # db_last_error is only used when we have no runtime error to report.
    if last_error is None and connection_status == "error":
        last_error = db_last_error

    # ── Sync freshness metrics ──────────────────────────────────────────────
    orders_with_line_items = 0
    orders_without_line_items = 0
    total_line_items = 0
    newest_order_date = None
    oldest_order_date = None

    try:
        # Count orders with/without line items
        all_orders_result = await db.execute(select(Order))
        all_orders = all_orders_result.scalars().all()

        for order in all_orders:
            line_items = order.line_items_json
            if isinstance(line_items, list) and len(line_items) > 0:
                orders_with_line_items += 1
                total_line_items += len(line_items)
            else:
                orders_without_line_items += 1

            # Track date range
            if order.external_updated_at:
                if newest_order_date is None or order.external_updated_at > newest_order_date:
                    newest_order_date = order.external_updated_at
                if oldest_order_date is None or order.external_updated_at < oldest_order_date:
                    oldest_order_date = order.external_updated_at

        logger.info(
            "leaflink: debug orders_with_items=%s without_items=%s total_items=%s",
            orders_with_line_items,
            orders_without_line_items,
            total_line_items,
        )
    except Exception as metrics_exc:
        logger.error("leaflink: debug metrics_failed error=%s", metrics_exc)

    # Resolve brand_id from the active credential used for the API check
    _debug_brand_id = db_cred_for_api.brand_id if db_cred_for_api else None
    _debug_company_id = db_cred_for_api.company_id if db_cred_for_api else (DEFAULT_LEAFLINK_COMPANY_ID or company_id_raw or None)

    result = {
        "ok": True,
        "api_connected": api_connected,
        "credentials_loaded": credentials_loaded,
        "credentials_valid": credentials_valid,
        "credentials_source": credentials_source,
        "brand_id": _debug_brand_id,
        "base_url": DEFAULT_LEAFLINK_BASE_URL or base_url_raw or None,
        "company_id": _debug_company_id,
        "orders_in_db": orders_in_db,
        "orders_with_line_items": orders_with_line_items,
        "orders_without_line_items": orders_without_line_items,
        "total_line_items": total_line_items,
        "newest_order_date": newest_order_date.isoformat() if newest_order_date else None,
        "oldest_order_date": oldest_order_date.isoformat() if oldest_order_date else None,
        "connection_status": connection_status,
        "last_sync_attempt": last_sync_attempt,
        "last_sync_status": last_sync_status,
        "last_error": last_error,
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


@router.get("/auth-debug")
async def debug_leaflink_auth(
    x_opsyn_secret: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Safe debug endpoint for LeafLink authentication.
    Requires X-OPSYN-SECRET header.

    Returns credential metadata (no full secrets) and tests authentication.
    """
    expected = os.getenv("OPSYN_SYNC_SECRET")
    if not expected or x_opsyn_secret != expected:
        logger.warning("leaflink: auth_debug unauthorized attempt")
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info("leaflink: auth_debug endpoint called")

    try:
        # Query the first active LeafLink credential
        cred_result = await db.execute(
            select(BrandAPICredential).where(
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            ).limit(1)
        )
        cred = cred_result.scalar_one_or_none()

        if not cred:
            logger.warning("leaflink: auth_debug no active credentials found")
            return {
                "ok": True,
                "credential_found": False,
                "error": "No active LeafLink credentials found",
            }

        # Extract and clean the API key
        raw_api_key = cred.api_key or ""
        api_key_length = len(raw_api_key)

        # Check for whitespace issues
        has_whitespace = any(c.isspace() for c in raw_api_key)
        has_newlines = "\n" in raw_api_key or "\r" in raw_api_key

        # Check if it already starts with "Token "
        starts_with_token = raw_api_key.startswith("Token ")

        # Clean the key: strip whitespace and remove "Token " prefix if present
        clean_api_key = raw_api_key.strip()
        if clean_api_key.startswith("Token "):
            clean_api_key = clean_api_key[6:].strip()

        # Get safe prefix/suffix
        api_key_prefix = clean_api_key[:6] if clean_api_key else "NONE"
        api_key_suffix = clean_api_key[-4:] if len(clean_api_key) >= 4 else "NONE"

        logger.info(
            "leaflink: auth_debug credential_found=true brand_id=%s company_id=%s key_length=%s key_prefix=%s key_suffix=%s has_whitespace=%s has_newlines=%s starts_with_token=%s",
            cred.brand_id,
            cred.company_id,
            api_key_length,
            api_key_prefix,
            api_key_suffix,
            has_whitespace,
            has_newlines,
            starts_with_token,
        )

        # Test multiple order endpoint paths with Token auth to find which one works
        base_url = DEFAULT_LEAFLINK_BASE_URL

        headers = {
            "Authorization": f"Token {clean_api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "opsyn",
        }

        endpoint_tests = [
            {
                "endpoint": "/companies/",
                "description": "List companies (baseline - should work)",
                "params": {"limit": 1},
            },
            {
                "endpoint": "/orders/",
                "description": "List orders",
                "params": {"limit": 1},
            },
            {
                "endpoint": "/orders-received/",
                "description": "List orders received",
                "params": {"limit": 1},
            },
            {
                "endpoint": "/purchases/",
                "description": "List purchases",
                "params": {"limit": 1},
            },
            {
                "endpoint": "/sales-orders/",
                "description": "List sales orders",
                "params": {"limit": 1},
            },
        ]

        endpoint_results = []
        for endpoint_config in endpoint_tests:
            endpoint = endpoint_config["endpoint"]
            description = endpoint_config["description"]
            params = endpoint_config["params"]

            try:
                url = f"{base_url}{endpoint}"

                logger.info(
                    "leaflink: auth_debug testing endpoint=%s description=%s",
                    endpoint,
                    description,
                )

                resp = requests.get(url, headers=headers, params=params, timeout=10)

                # Get sample data if successful
                sample = None
                if resp.ok:
                    try:
                        data = resp.json()
                        if isinstance(data, list) and len(data) > 0:
                            sample = f"Got {len(data)} items"
                        elif isinstance(data, dict):
                            if "results" in data:
                                sample = f"Got {len(data.get('results', []))} results"
                            elif "data" in data:
                                sample = f"Got {len(data.get('data', []))} data items"
                            else:
                                sample = f"Got response with keys: {list(data.keys())[:3]}"
                    except Exception:
                        sample = "Response OK but couldn't parse JSON"

                endpoint_result = {
                    "endpoint": endpoint,
                    "description": description,
                    "status": resp.status_code,
                    "error": None if resp.ok else resp.text[:200],
                    "sample": sample,
                }

                logger.info(
                    "leaflink: auth_debug endpoint_result endpoint=%s status=%s sample=%s",
                    endpoint,
                    resp.status_code,
                    sample,
                )

                endpoint_results.append(endpoint_result)

            except Exception as test_exc:
                endpoint_result = {
                    "endpoint": endpoint,
                    "description": description,
                    "status": None,
                    "error": str(test_exc)[:200],
                    "sample": None,
                }

                logger.error(
                    "leaflink: auth_debug endpoint_test_failed endpoint=%s error=%s",
                    endpoint,
                    test_exc,
                )

                endpoint_results.append(endpoint_result)

        # Analyse results
        working_order_endpoints = [
            r for r in endpoint_results
            if r["status"] == 200 and "/orders" in r["endpoint"]
        ]

        working_companies = [
            r for r in endpoint_results
            if r["status"] == 200 and r["endpoint"] == "/companies/"
        ]

        blocked_endpoints = [
            r["endpoint"] for r in endpoint_results
            if r["status"] == 403
        ]

        if working_order_endpoints:
            return {
                "ok": True,
                "credential_found": True,
                "brand_id": cred.brand_id,
                "company_id": cred.company_id,
                "api_key_length": api_key_length,
                "api_key_prefix": api_key_prefix,
                "api_key_suffix": api_key_suffix,
                "base_url": base_url,
                "working_order_endpoint": working_order_endpoints[0]["endpoint"],
                "endpoint_results": endpoint_results,
                "created_at": cred.created_at.isoformat() if cred.created_at else None,
                "updated_at": cred.updated_at.isoformat() if cred.updated_at else None,
                "last_sync_at": cred.last_sync_at.isoformat() if cred.last_sync_at else None,
                "sync_status": cred.sync_status,
            }
        elif working_companies:
            return {
                "ok": False,
                "credential_found": True,
                "reason": "LeafLink token works for companies but not order endpoints",
                "working_endpoint": "/companies/",
                "blocked_endpoints": blocked_endpoints,
                "endpoint_results": endpoint_results,
            }
        else:
            return {
                "ok": False,
                "credential_found": True,
                "reason": "LeafLink token doesn't work for any endpoint",
                "endpoint_results": endpoint_results,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("leaflink: auth_debug failed error=%s", e, exc_info=True)
        return {
            "ok": False,
            "error": str(e),
        }


@router.get("/raw-orders")
async def raw_orders_leaflink(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    """
    Debug endpoint: calls LeafLink orders-received/ directly and returns the
    raw API response with no normalization.  Use this to determine whether
    LeafLink is returning 0 orders or whether the issue is in parsing/filtering.

    GET /leaflink/raw-orders
    """
    # ── Load first active LeafLink credential (same query as /orders/sync) ──
    try:
        cred_result = await db.execute(
            select(BrandAPICredential)
            .where(
                BrandAPICredential.integration_name == "leaflink",
                BrandAPICredential.is_active == True,
            )
            .order_by(BrandAPICredential.last_sync_at.desc().nullslast())
            .limit(1)
        )
        cred = cred_result.scalar_one_or_none()
    except Exception as cred_exc:
        logger.error("[LeafLinkRaw] cred_lookup_failed error=%s", cred_exc)
        return {"ok": False, "error": f"DB credential lookup failed: {cred_exc}"}

    if cred is None:
        logger.warning("[LeafLinkRaw] no active credentials found")
        return {
            "ok": False,
            "error": "No active LeafLink credentials found in database. Use POST /integrations/leaflink/connect to add credentials.",
        }

    company_id = cred.company_id
    logger.info("[LeafLinkRaw] request company_id=%s", company_id)

    # ── Build client and call orders-received/ ───────────────────────────────
    try:
        client = LeafLinkClient(
            api_key=cred.api_key,
            company_id=company_id,
            brand_id=cred.brand_id,
        )
    except Exception as client_exc:
        logger.error("[LeafLinkRaw] client_init_failed error=%s", client_exc)
        return {"ok": False, "error": f"LeafLink client init failed: {client_exc}"}

    api_url = f"{client.base_url}/orders-received/"

    try:
        raw = client.list_orders(page=1, page_size=100)
    except Exception as api_exc:
        logger.error("[LeafLinkRaw] api_call_failed error=%s", api_exc)
        return {
            "ok": False,
            "company_id": company_id,
            "api_url": api_url,
            "base_url": client.base_url,
            "error": str(api_exc),
        }

    # ── Inspect the raw response ─────────────────────────────────────────────
    response_type = type(raw).__name__
    logger.info("[LeafLinkRaw] response_type=%s", response_type)

    # Determine result list, count, and pagination info regardless of shape
    if isinstance(raw, list):
        results_list = raw
        count = len(raw)
        next_url = None
        total_pages = 1
    elif isinstance(raw, dict):
        results_list = (
            raw.get("results")
            or raw.get("data")
            or raw.get("orders")
            or []
        )
        count = raw.get("count", len(results_list))
        next_url = raw.get("next")
        # Estimate total pages from count + page_size (100)
        if isinstance(count, int) and count > 0:
            total_pages = (count + 99) // 100
        else:
            total_pages = 1
    else:
        results_list = []
        count = 0
        next_url = None
        total_pages = 0

    logger.info("[LeafLinkRaw] response_count=%s", count)

    first_2 = results_list[:2] if isinstance(results_list, list) else []

    if first_2:
        first_order_id = first_2[0].get("id") if isinstance(first_2[0], dict) else None
        logger.info("[LeafLinkRaw] first_order_id=%s", first_order_id)
    else:
        logger.info("[LeafLinkRaw] first_order_id=none (empty results)")

    return {
        "ok": True,
        "company_id": company_id,
        "api_url": api_url,
        "base_url": client.base_url,
        "response_type": response_type,
        "count": count,
        "total_pages": total_pages,
        "has_next": bool(next_url),
        "next_url": next_url,
        "first_2_orders": first_2,
    }


@router.get("/auth-test")
async def leaflink_auth_test(
    brand: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Test LeafLink authentication (DB credentials only)."""
    from database import AsyncSessionLocal
    from routes.orders import _load_leaflink_credential

    logger.info("[CredentialResolver] auth_test_request brand=%s", brand)

    # Load credential from DB only
    try:
        cred = await _load_leaflink_credential(db, brand)
    except ValueError as val_exc:
        logger.error("[CredentialResolver] auth_test_credential_error error=%s", val_exc)
        return {
            "ok": False,
            "status_code": None,
            "credential_found": False,
            "credential_source": "db",
            "env_checked": False,
            "env_ignored": True,
            "error": str(val_exc),
        }

    if not cred:
        logger.error("[CredentialResolver] auth_test_no_credential brand=%s", brand)
        return {
            "ok": False,
            "status_code": None,
            "credential_found": False,
            "credential_source": "db",
            "env_checked": False,
            "env_ignored": True,
            "error": "DB credential not found",
        }

    api_key = (cred.api_key or "").strip()
    company_id = (cred.company_id or "").strip()
    brand_id = cred.brand_id

    logger.info(
        "[CredentialResolver] auth_test_credential brand=%s credential_id=%s key_len=%s company_id=%s",
        brand_id,
        cred.id,
        len(api_key),
        company_id,
    )

    # Try auth schemes in order
    schemes_to_try = ["Bearer", "Token", "Raw"]
    attempts = []
    successful_scheme = None

    for scheme in schemes_to_try:
        try:
            logger.info("[CredentialResolver] attempt scheme=%s brand=%s", scheme, brand_id)

            client = LeafLinkClient(
                api_key=api_key,
                company_id=company_id,
                brand_id=brand_id,
                auth_scheme=scheme,
            )

            # Call lightweight endpoint to test auth
            resp = client._get_raw("orders-received/", params={"page": 1, "page_size": 1})
            status_code = resp.status_code

            logger.info(
                "[CredentialResolver] attempt scheme=%s status=%s brand=%s",
                scheme,
                status_code,
                brand_id,
            )

            attempts.append({
                "scheme": scheme,
                "status_code": status_code,
            })

            if status_code == 200:
                successful_scheme = scheme
                logger.info(
                    "[CredentialResolver] selected_scheme=%s brand=%s",
                    scheme,
                    brand_id,
                )
                break

        except Exception as exc:
            logger.error(
                "[CredentialResolver] attempt_error scheme=%s error=%s brand=%s",
                scheme,
                exc,
                brand_id,
            )
            attempts.append({
                "scheme": scheme,
                "status_code": None,
                "error": str(exc),
            })

    if successful_scheme:
        # Save detected scheme to database
        try:
            if AsyncSessionLocal is not None:
                async with AsyncSessionLocal() as save_sess:
                    async with save_sess.begin():
                        save_result = await save_sess.execute(
                            select(BrandAPICredential).where(
                                BrandAPICredential.id == cred.id,
                            )
                        )
                        save_cred = save_result.scalar_one_or_none()
                        if save_cred:
                            save_cred.auth_scheme = successful_scheme
                            logger.info(
                                "[CredentialResolver] saved_scheme scheme=%s credential_id=%s",
                                successful_scheme,
                                cred.id,
                            )
        except Exception as save_exc:
            logger.error(
                "[CredentialResolver] save_scheme_error error=%s",
                save_exc,
            )

        logger.info(
            "[CredentialResolver] auth_test_success scheme=%s brand=%s",
            successful_scheme,
            brand_id,
        )
        return {
            "ok": True,
            "status_code": 200,
            "credential_found": True,
            "credential_source": "db",
            "credential_id": cred.id,
            "brand_id": brand_id,
            "company_id": company_id,
            "api_key_prefix": api_key[:6] if api_key else "MISSING",
            "api_key_len": len(api_key),
            "env_checked": False,
            "env_ignored": True,
            "successful_auth_scheme": successful_scheme,
            "attempts": attempts,
            "error": None,
        }
    else:
        logger.error(
            "[CredentialResolver] auth_test_failed all_schemes_failed brand=%s",
            brand_id,
        )
        return {
            "ok": False,
            "status_code": attempts[-1].get("status_code") if attempts else None,
            "credential_found": True,
            "credential_source": "db",
            "credential_id": cred.id,
            "brand_id": brand_id,
            "company_id": company_id,
            "api_key_prefix": api_key[:6] if api_key else "MISSING",
            "api_key_len": len(api_key),
            "env_checked": False,
            "env_ignored": True,
            "successful_auth_scheme": None,
            "attempts": attempts,
            "error": "All auth schemes failed",
        }
