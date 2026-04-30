import logging
import os
from typing import Any, Dict, List, Literal, Optional

import requests

AuthScheme = Literal["Bearer", "Token", "Raw"]

logger = logging.getLogger("leaflink_client")

DEFAULT_LEAFLINK_BASE_URL = os.getenv("LEAFLINK_BASE_URL", "https://www.leaflink.com/api/v2").strip().rstrip("/")
DEFAULT_LEAFLINK_COMPANY_ID = os.getenv("LEAFLINK_COMPANY_ID", "").strip()
LEAFLINK_API_VERSION = os.getenv("LEAFLINK_API_VERSION", "").strip()
LEAFLINK_USER_AGENT = os.getenv("LEAFLINK_USER_AGENT", "opsyn-backend").strip()
MOCK_MODE = os.getenv("MOCK_MODE", "false").lower() == "true"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default

    if isinstance(value, bool):
        return default

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value.replace(",", "").replace("$", "").strip())
        except ValueError:
            return default

    if isinstance(value, dict):
        for key in ("amount", "value", "total", "price"):
            if key in value:
                return _safe_float(value.get(key), default)

    return default


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default

    if isinstance(value, bool):
        return default

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        try:
            return int(float(value.replace(",", "").strip()))
        except ValueError:
            return default

    if isinstance(value, dict):
        for key in ("amount", "value", "quantity", "qty"):
            if key in value:
                return _safe_int(value.get(key), default)

    return default


def _first_non_empty(*values: Any) -> Any:
    for v in values:
        if v is not None and v != "":
            return v
    return None


class LeafLinkClient:
    def __init__(
        self,
        api_key: str,
        base_url: Optional[str] = None,
        company_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        auth_scheme: Optional[AuthScheme] = None,
    ) -> None:
        self.brand_id = brand_id or "unknown"
        self.base_url = (base_url or DEFAULT_LEAFLINK_BASE_URL).strip().rstrip("/")

        # Clean the API key: strip whitespace and remove "Token " prefix if present
        # api_key is required — no env var fallback (multi-tenant: each brand has its own key)
        _raw_api_key = api_key or ""
        self.api_key = _raw_api_key.strip()
        _had_whitespace = self.api_key != _raw_api_key  # True if we stripped something
        if self.api_key.startswith("Token "):
            logger.warning(
                "leaflink: client_init brand=%s api_key starts with 'Token ' prefix, removing it",
                self.brand_id,
            )
            self.api_key = self.api_key[6:].strip()

        self.company_id = str(company_id or DEFAULT_LEAFLINK_COMPANY_ID).strip()

        if not self.base_url:
            logger.warning("leaflink: client_init brand=%s missing base_url", self.brand_id)
            raise ValueError("Missing LEAFLINK_BASE_URL")
        if not self.api_key:
            logger.warning("leaflink: client_init brand=%s missing api_key", self.brand_id)
            raise ValueError("Missing api_key — LeafLink credentials must be provided per brand (not from env vars)")
        if not self.company_id:
            logger.warning("leaflink: client_init brand=%s missing company_id", self.brand_id)
            raise ValueError("Missing LEAFLINK_COMPANY_ID")

        # CRITICAL: Validate API key length
        if len(self.api_key) > 50:
            logger.error(
                "[LeafLinkAuth] INVALID_KEY_LENGTH brand=%s len=%s — REJECTING",
                self.brand_id,
                len(self.api_key),
            )
            raise ValueError(
                f"Invalid LeafLink API key length: {len(self.api_key)} (expected ~40)"
            )

        if len(self.api_key) < 20:
            logger.error(
                "[LeafLinkAuth] INVALID_KEY_LENGTH brand=%s len=%s — TOO SHORT",
                self.brand_id,
                len(self.api_key),
            )
            raise ValueError(
                f"Invalid LeafLink API key length: {len(self.api_key)} (expected ~40)"
            )

        # Determine auth scheme
        if auth_scheme:
            self.auth_scheme: str = auth_scheme
            logger.info(
                "[LeafLinkAuth] using_explicit_scheme scheme=%s",
                auth_scheme,
            )
        else:
            env_scheme = os.getenv("LEAFLINK_AUTH_SCHEME", "auto").lower()
            if env_scheme in ("bearer", "token", "raw"):
                self.auth_scheme = env_scheme.capitalize() if env_scheme != "raw" else "Raw"
                logger.info(
                    "[LeafLinkAuth] using_env_scheme scheme=%s",
                    self.auth_scheme,
                )
            else:
                # Default to Bearer (most common for modern APIs)
                self.auth_scheme = "Bearer"
                logger.info(
                    "[LeafLinkAuth] using_default_scheme scheme=Bearer",
                )

        self.session = requests.Session()
        # Base session headers — Authorization header is built per-request from
        # the instance api_key so each brand uses its own credential.
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": LEAFLINK_USER_AGENT,
        })

        if LEAFLINK_API_VERSION:
            self.session.headers["LeafLink-Version"] = LEAFLINK_API_VERSION

        if not self.api_key:
            logger.error("[LeafLinkAuth] MISSING api_key brand=%s", self.brand_id)
        if not self.company_id:
            logger.error("[LeafLinkAuth] MISSING company_id brand=%s", self.brand_id)

    def _get_auth_header(self) -> dict:
        """Build Authorization header based on configured scheme.

        Always returns a valid dict with Authorization key.
        """
        if not self.api_key:
            logger.error("[LeafLinkAuth] MISSING api_key in _get_auth_header")
            return {"Authorization": ""}  # Fallback to empty (will fail, but won't crash)

        if self.auth_scheme == "Bearer":
            return {"Authorization": f"Bearer {self.api_key}"}
        elif self.auth_scheme == "Token":
            return {"Authorization": f"Token {self.api_key}"}
        elif self.auth_scheme == "Raw":
            return {"Authorization": self.api_key}
        else:
            # Fallback to Bearer
            logger.warning(
                "[LeafLinkAuth] unknown_scheme=%s falling_back_to_Bearer",
                self.auth_scheme,
            )
            return {"Authorization": f"Bearer {self.api_key}"}

    def _validate_request_headers(self, headers: dict) -> bool:
        """Validate that Authorization header is present and non-empty."""
        if "Authorization" not in headers:
            logger.error("[LeafLinkAuth] MISSING Authorization header in request")
            return False

        auth_value = headers.get("Authorization", "")
        if not auth_value or auth_value.isspace():
            logger.error("[LeafLinkAuth] EMPTY Authorization header in request")
            return False

        logger.debug(
            "[LeafLinkAuth] valid_auth_header scheme=%s prefix=%s",
            self.auth_scheme,
            auth_value[:10] if auth_value else "MISSING",
        )
        return True

    def _get_raw(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Make GET request with explicit Authorization header."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        safe_params = {k: v for k, v in (params or {}).items() if k not in ("api_key", "token", "secret")}

        def _do_request() -> requests.Response:
            # Build headers with explicit Authorization
            headers = {
                **self._get_auth_header(),
                "Content-Type": "application/json",
            }

            if not self._validate_request_headers(headers):
                logger.error("[LeafLinkAuth] invalid_headers aborting_request path=%s", path)
                raise ValueError("Invalid Authorization header")

            logger.info(
                "[LeafLinkAuth] request_start path=%s scheme=%s api_key_len=%s",
                path,
                self.auth_scheme,
                len(self.api_key),
            )

            try:
                return self.session.get(
                    url,
                    params=params,
                    timeout=45,
                    headers=headers,  # EXPLICIT headers on every request
                )
            except Exception as exc:
                logger.error(
                    "[LeafLinkAuth] request_error path=%s error=%s",
                    path,
                    exc,
                )
                raise

        resp = _do_request()

        logger.info(
            "[LeafLinkSync] page_response status=%s path=%s",
            resp.status_code,
            path,
        )

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" not in content_type.lower():
            logger.warning(
                "leaflink: API response is not JSON url=%s status=%s content_type=%s body_preview=%s",
                url,
                resp.status_code,
                content_type,
                resp.text[:200],
            )
        if not resp.ok:
            if resp.status_code == 403:
                logger.error(
                    "[LeafLinkAuth] 403_invalid_token endpoint=%s scheme=%s api_key_prefix=%s api_key_len=%s brand=%s",
                    url,
                    self.auth_scheme,
                    self.api_key[:6] if self.api_key else "MISSING",
                    len(self.api_key),
                    self.brand_id,
                )
            if resp.status_code in (401, 403):
                logger.error(
                    "[LeafLinkAuth] auth_failed brand=%s status=%s reason=%s url=%s",
                    self.brand_id,
                    resp.status_code,
                    resp.text[:200],
                    url,
                )
            else:
                logger.error(
                    "leaflink: API HTTP error brand=%s url=%s status=%s body=%s",
                    self.brand_id,
                    url,
                    resp.status_code,
                    resp.text[:500],
                )
        return resp

    def _get_raw_url(self, url: str) -> requests.Response:
        """Make GET request to full URL with explicit Authorization header."""
        # Build headers with explicit Authorization
        headers = {
            **self._get_auth_header(),
            "Content-Type": "application/json",
        }

        if not self._validate_request_headers(headers):
            logger.error("[LeafLinkAuth] invalid_headers aborting_request url=%s", url)
            raise ValueError("Invalid Authorization header")

        try:
            resp = self.session.get(
                url,
                params=None,
                timeout=45,
                headers=headers,  # EXPLICIT headers on every request
            )
        except Exception as exc:
            logger.error(
                "[LeafLinkAuth] request_error url=%s error=%s",
                url,
                exc,
            )
            raise

        if resp.status_code == 403:
            logger.error(
                "[LeafLinkAuth] 403_invalid_token url=%s scheme=%s prefix=%s len=%s",
                url,
                self.auth_scheme,
                self.api_key[:6] if self.api_key else "MISSING",
                len(self.api_key),
            )

        if resp.status_code in (401, 403):
            logger.error(
                "[LeafLinkAuth] pagination_auth_failed status=%s body=%s url=%s",
                resp.status_code,
                resp.text[:200],
                url,
            )

        return resp

    def _post_raw(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Make POST request with explicit Authorization header."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        safe_params = {k: v for k, v in (params or {}).items() if k not in ("api_key", "token", "secret")}

        logger.debug(
            "[LeafLinkAuth] request_start method=POST path=%s",
            path,
        )

        # Build headers with explicit Authorization
        headers = {
            **self._get_auth_header(),
            "Content-Type": "application/json",
        }

        if not self._validate_request_headers(headers):
            logger.error("[LeafLinkAuth] invalid_headers aborting_request method=POST path=%s", path)
            raise ValueError("Invalid Authorization header")

        logger.debug(
            "[LeafLinkAuth] sending_request method=POST url=%s has_auth_header=true scheme=%s",
            url,
            self.auth_scheme,
        )

        try:
            return self.session.post(
                url,
                params=safe_params,
                json=json_data,
                timeout=45,
                headers=headers,  # EXPLICIT headers on every request
            )
        except Exception as exc:
            logger.error(
                "[LeafLinkAuth] request_error method=POST path=%s error=%s",
                path,
                exc,
            )
            raise

    def list_orders(
        self,
        page: int = 1,
        page_size: int = 100,
        status: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {
            "page": page,
            "page_size": page_size,
            "include_children": "line_items,customer,sales_reps",
        }
        if status:
            params["status"] = status

        try:
            resp = self._get_raw("orders-received/", params=params)
        except Exception as exc:
            logger.error(
                "leaflink: list_orders_request_failed brand=%s error=%s",
                self.brand_id,
                exc,
            )
            raise

        content_type = resp.headers.get("Content-Type", "")

        if resp.ok and "application/json" in content_type.lower():
            data = resp.json()
            return data

        if resp.status_code in (401, 403):
            logger.error(
                "[LeafLinkAuth] list_orders_auth_failed brand=%s status=%s body=%s",
                self.brand_id,
                resp.status_code,
                resp.text[:500],
            )
        else:
            logger.error(
                "leaflink: list_orders failed brand=%s status=%s content_type=%s body_preview=%s",
                self.brand_id,
                resp.status_code,
                content_type,
                resp.text[:220],
            )
        raise RuntimeError(
            f"LeafLink API error: status={resp.status_code} content_type={content_type} body={resp.text[:220]}"
        )

    def _extract_customer_name(self, raw: Dict[str, Any]) -> str:
        buyer = raw.get("buyer") if isinstance(raw.get("buyer"), dict) else {}
        customer = raw.get("customer") if isinstance(raw.get("customer"), dict) else {}
        dispensary = raw.get("dispensary") if isinstance(raw.get("dispensary"), dict) else {}
        retailer = raw.get("retailer") if isinstance(raw.get("retailer"), dict) else {}
        store = raw.get("store") if isinstance(raw.get("store"), dict) else {}
        corporate_address = raw.get("corporate_address") if isinstance(raw.get("corporate_address"), dict) else {}
        delivery_address = raw.get("delivery_address") if isinstance(raw.get("delivery_address"), dict) else {}

        name = _first_non_empty(
            raw.get("customer_name"),
            raw.get("buyer_name"),
            raw.get("dispensary_name"),
            raw.get("retailer_name"),
            buyer.get("display_name"),
            buyer.get("name"),
            buyer.get("business_name"),
            customer.get("display_name"),
            customer.get("name"),
            customer.get("business_name"),
            dispensary.get("display_name"),
            dispensary.get("name"),
            retailer.get("display_name"),
            retailer.get("name"),
            store.get("display_name"),
            store.get("name"),
            corporate_address.get("name"),
            delivery_address.get("name"),
            corporate_address.get("city"),
        )

        return str(name) if name else "Unknown Customer"

    def _extract_line_items(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Try multiple field names for line items (LeafLink API variations)
        candidate = (
            raw.get("line_items")
            or raw.get("items")
            or raw.get("order_items")
            or raw.get("products")
            or raw.get("ordered_items")
            or raw.get("line_item_list")
            or raw.get("order_lines")
            or []
        )

        if not isinstance(candidate, list):
            return []

        normalized: List[Dict[str, Any]] = []

        for item in candidate:
            if not isinstance(item, dict):
                continue

            product = item.get("product") if isinstance(item.get("product"), dict) else {}
            frozen_product = (
                item.get("frozen_data", {}).get("product")
                if isinstance(item.get("frozen_data"), dict) and isinstance(item.get("frozen_data", {}).get("product"), dict)
                else {}
            )

            quantity = _safe_int(
                _first_non_empty(
                    item.get("quantity"),
                    item.get("qty"),
                    item.get("units"),
                    item.get("unit_count"),
                    item.get("bulk_units"),
                    item.get("bulk_units_decimal"),
                    item.get("ordered_quantity"),
                ),
                0,
            )

            sale_price = _safe_float(item.get("sale_price"), 0.0)
            ordered_unit_price = _safe_float(item.get("ordered_unit_price"), 0.0)
            fallback_price = _safe_float(
                _first_non_empty(
                    item.get("unit_price"),
                    item.get("price"),
                    item.get("wholesale_price"),
                    item.get("retail_price"),
                    item.get("suggested_wholesale_price"),
                ),
                0.0,
            )

            unit_price = sale_price if sale_price > 0 else (ordered_unit_price if ordered_unit_price > 0 else fallback_price)

            line_total_raw = _first_non_empty(
                item.get("line_total"),
                item.get("total_price"),
                item.get("total"),
                item.get("extended_total"),
            )

            if line_total_raw is None:
                line_total = round(unit_price * quantity, 2)
            else:
                line_total = _safe_float(line_total_raw, 0.0)

            normalized.append({
                "external_id": str(_first_non_empty(item.get("id"), item.get("uuid"), item.get("sku")) or ""),
                "sku": str(_first_non_empty(item.get("sku"), product.get("sku"), frozen_product.get("sku")) or ""),
                "name": str(
                    _first_non_empty(
                        item.get("name"),
                        item.get("product_name"),
                        product.get("name"),
                        frozen_product.get("name"),
                    ) or "Unknown Item"
                ),
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
                "raw_payload": item,
            })

        return normalized

    def _extract_total_amount(self, raw: Dict[str, Any], line_items: List[Dict[str, Any]]) -> float:
        direct = _first_non_empty(
            raw.get("total_amount"),
            raw.get("total"),
            raw.get("grand_total"),
            raw.get("order_total"),
            raw.get("subtotal"),
            raw.get("amount"),
            raw.get("payment_balance"),
        )

        direct_value = _safe_float(direct, 0.0)
        if direct_value > 0:
            return round(direct_value, 2)

        computed = round(sum(float(li.get("line_total") or 0.0) for li in line_items), 2)
        return computed

    def _normalize_order(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        line_items = self._extract_line_items(raw)
        item_count = len(line_items)
        unit_count = sum(_safe_int(li.get("quantity"), 0) for li in line_items)

        external_id = str(_first_non_empty(
            raw.get("id"),
            raw.get("uuid"),
            raw.get("number"),
            raw.get("short_id"),
            raw.get("external_id"),
        ) or "")

        order_number = str(_first_non_empty(
            raw.get("order_short_number"),
            raw.get("short_id"),
            raw.get("number"),
            raw.get("order_number"),
            raw.get("display_id"),
            raw.get("id"),
        ) or external_id)

        status = str(_first_non_empty(
            raw.get("classification"),
            raw.get("status"),
            raw.get("state"),
            raw.get("fulfillment_status"),
            "unknown",
        )).strip().lower()

        submitted_at = _first_non_empty(
            raw.get("submitted_at"),
            raw.get("created_at"),
            raw.get("created"),
        )

        updated_at = _first_non_empty(
            raw.get("modified"),
            raw.get("updated_at"),
            raw.get("submitted_at"),
            raw.get("created_at"),
        )

        return {
            "external_id": external_id,
            "order_number": order_number,
            "customer_name": self._extract_customer_name(raw),
            "status": status,
            "currency": _first_non_empty(raw.get("currency"), "USD"),
            "submitted_at": submitted_at,
            "created_at": submitted_at,
            "updated_at": updated_at,
            "total_amount": self._extract_total_amount(raw, line_items),
            "item_count": item_count,
            "unit_count": unit_count,
            "line_items": line_items,
            "raw_payload": raw,
        }

    def fetch_orders_page_range(
        self,
        start_page: int = 1,
        num_pages: int = 3,
        page_size: int = 100,
        normalize: bool = True,
        brand: str = "unknown",
        resume_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch a bounded range of pages from LeafLink, starting at ``start_page``.

        Designed for paginated incremental sync: fetch a small batch of pages
        quickly, return the ``next_url`` so the caller can resume later.

        Args:
            start_page: Logical page number to start from (used for logging).
            num_pages: Maximum number of pages to fetch in this call.
            page_size: Orders per page (passed to LeafLink as ``page_size``).
            normalize: When ``True``, run each raw order through ``_normalize_order``.
            brand: Brand slug used only for structured log messages.
            resume_url: If provided, resume pagination from this absolute URL
                        instead of fetching page 1 from scratch.

        Returns:
            A dict with keys:
                ``orders``            – list of order dicts for this batch
                ``pages_fetched``     – number of pages retrieved in this call
                ``next_url``          – absolute URL for the next page, or ``None``
                ``next_page``         – logical page number for the next batch
                ``total_count``       – total order count reported by LeafLink (first page only)
                ``total_pages``       – estimated total pages (total_count / page_size, rounded up)
        """
        all_orders: List[Dict[str, Any]] = []
        pages_fetched = 0
        next_url: Optional[str] = resume_url
        total_count: Optional[int] = None
        current_page = start_page

        try:
            while pages_fetched < num_pages:
                if next_url:
                    resp = self._get_raw_url(next_url)
                    if resp.status_code == 403:
                        logger.error(
                            "[LeafLinkAuth] 403_invalid_token next_url=%s scheme=%s",
                            next_url,
                            self.auth_scheme,
                        )
                    if resp.status_code in (401, 403):
                        raise RuntimeError(
                            f"LeafLink pagination auth failed: status={resp.status_code} body={resp.text[:220]}"
                        )
                    if not resp.ok:
                        raise RuntimeError(
                            f"LeafLink API error: status={resp.status_code} body={resp.text[:220]}"
                        )
                    payload = resp.json()
                else:
                    payload = self.list_orders(page=current_page, page_size=page_size)

                if isinstance(payload, list):
                    results = payload
                    next_url = None
                elif isinstance(payload, dict):
                    results = (
                        payload.get("results")
                        or payload.get("data")
                        or payload.get("orders")
                        or []
                    )
                    next_url = payload.get("next")
                    # Capture total count from first page — try multiple API response formats
                    if total_count is None:
                        if "meta" in payload and isinstance(payload["meta"], dict) and "total" in payload["meta"]:
                            total_count = payload["meta"]["total"]
                        elif "count" in payload:
                            total_count = payload.get("count")
                        elif "total" in payload:
                            total_count = payload.get("total")
                else:
                    raise RuntimeError(f"Unexpected LeafLink response type: {type(payload).__name__}")

                if not isinstance(results, list):
                    raise RuntimeError(f"Unexpected LeafLink results type: {type(results).__name__}")

                if not results:
                    logger.info(
                        "[LeafLink] page_range page=%s empty_results — stopping",
                        current_page,
                    )
                    next_url = None
                    break

                if normalize:
                    for raw in results:
                        if isinstance(raw, dict):
                            all_orders.append(self._normalize_order(raw))
                else:
                    for raw in results:
                        if isinstance(raw, dict):
                            all_orders.append(raw)

                pages_fetched += 1

                if not next_url:
                    break

                current_page += 1

        except Exception as exc:
            logger.error(
                "[LeafLink] fetch_page_range_error brand=%s start_page=%s error=%s",
                brand,
                start_page,
                exc,
            )
            raise

        # LeafLink uses cursor-based pagination — total_pages cannot be reliably
        # calculated from the API response. Leave it as None so callers do not
        # display a misleading "4 / 1" style progress indicator.
        total_pages: Optional[int] = None

        next_page = start_page + pages_fetched if next_url else None

        return {
            "orders": all_orders,
            "pages_fetched": pages_fetched,
            "next_url": next_url,
            "next_page": next_page,
            "total_count": total_count,
            "total_pages": total_pages,
        }

    def fetch_recent_orders(
        self,
        max_pages: Optional[int] = None,
        normalize: bool = False,
        brand: str = "unknown",
    ) -> Dict[str, Any]:
        """Fetch all orders from LeafLink, paginating until no ``next`` URL is returned.

        Args:
            max_pages: Maximum number of pages to fetch. ``None`` (default) means
                unlimited — fetch every page until LeafLink returns no ``next`` URL.
                Pass an integer to cap the number of pages (e.g. for testing).
            normalize: When ``True``, run each raw order through ``_normalize_order``.
            brand: Brand slug used only for structured log messages.

        Returns:
            A dict with keys:
                ``orders``       – list of order dicts
                ``pages_fetched`` – number of pages retrieved
        """
        effective_max = max_pages if max_pages is not None else 10_000

        all_orders: List[Dict[str, Any]] = []
        pages_fetched = 0

        try:
            page = 1
            next_url: Optional[str] = None

            while page <= effective_max:
                pages_fetched += 1

                if next_url:
                    # Use the full ``next`` URL returned by LeafLink directly.
                    # _get_raw_url() explicitly attaches the Authorization header so
                    # that pagination requests are authenticated the same way as page 1.
                    resp = self._get_raw_url(next_url)
                    if resp.status_code in (401, 403):
                        logger.error(
                            "[LeafLinkAuth] pagination_auth_failed page=%s status=%s body=%s",
                            page,
                            resp.status_code,
                            resp.text[:200],
                        )
                        raise RuntimeError(
                            f"LeafLink pagination auth failed: status={resp.status_code} body={resp.text[:220]}"
                        )
                    if not resp.ok:
                        logger.error(
                            "[LeafLinkSync] page=%s http_error status=%s body=%s",
                            page,
                            resp.status_code,
                            resp.text[:500],
                        )
                        raise RuntimeError(
                            f"LeafLink API error: status={resp.status_code} body={resp.text[:220]}"
                        )
                    payload = resp.json()
                else:
                    payload = self.list_orders(page=page, page_size=100)

                if isinstance(payload, list):
                    results = payload
                    next_url = None
                elif isinstance(payload, dict):
                    results = (
                        payload.get("results")
                        or payload.get("data")
                        or payload.get("orders")
                        or []
                    )
                    next_url = payload.get("next")
                else:
                    raise RuntimeError(f"Unexpected LeafLink response type: {type(payload).__name__}")

                if not isinstance(results, list):
                    raise RuntimeError(f"Unexpected LeafLink results type: {type(results).__name__}")

                if not results:
                    break

                if normalize:
                    for raw in results:
                        if isinstance(raw, dict):
                            all_orders.append(self._normalize_order(raw))
                else:
                    for raw in results:
                        if isinstance(raw, dict):
                            all_orders.append(raw)

                if not next_url:
                    break

                page += 1

        except Exception as exc:
            logger.error(
                "[LeafLinkSync] API call failed brand=%s error=%s mock_mode=%s",
                brand,
                exc,
                MOCK_MODE,
            )
            if MOCK_MODE:
                logger.error(
                    "[LeafLinkSync] API failed and MOCK_MODE=true — refusing to return mock data when live data is expected"
                )
                logger.error("[LeafLinkSync] error=%s", exc)
                logger.error("[LeafLinkSync] returning_mock=true (blocked — raising instead)")
                raise  # Re-raise the original exception instead of returning mock
            logger.info("[LeafLinkSync] returning_mock=false")
            raise

        return {"orders": all_orders, "pages_fetched": pages_fetched}