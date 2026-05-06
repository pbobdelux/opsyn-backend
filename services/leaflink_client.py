import logging
import os
import random
import socket
import time
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

AuthScheme = Literal["Bearer", "Token", "Raw", "Api-Key"]

logger = logging.getLogger("leaflink_client")

LEAFLINK_API_VERSION = os.getenv("LEAFLINK_API_VERSION", "").strip()
LEAFLINK_USER_AGENT = os.getenv("LEAFLINK_USER_AGENT", "opsyn-backend").strip()
MOCK_MODE = os.getenv("MOCK_MODE", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Canonical endpoint constants — single source of truth
# ---------------------------------------------------------------------------

LEAFLINK_CANONICAL_BASE_URL = "https://www.leaflink.com/api/v2"
LEAFLINK_ORDERS_ENDPOINT = "orders-received/"
LEAFLINK_ORDERS_FULL_URL = f"{LEAFLINK_CANONICAL_BASE_URL}/{LEAFLINK_ORDERS_ENDPOINT}"

# Log the canonical endpoint at module import time so it appears in every deploy log.
logger.info(
    "[LEAFLINK_FINAL_URL] final_url=%s",
    LEAFLINK_ORDERS_FULL_URL,
)


def validate_leaflink_endpoint_url(url: str) -> tuple[bool, str]:
    """Validate that a LeafLink orders endpoint URL is correctly formed.

    Returns (is_valid, reason).  reason is empty string when valid.

    Checks:
    - URL must not contain 'orders-receive' without the trailing 'd'
    - URL must end with 'orders-received/' (trailing slash required)
    """
    import re as _re

    if _re.search(r"orders-receive(?!d)", url):
        return False, "missing_final_d"

    if "orders-received" in url and not url.rstrip("?").endswith("orders-received/"):
        # Allow query strings — strip them before checking trailing slash
        path_part = url.split("?")[0]
        if not path_part.endswith("orders-received/"):
            return False, "missing_trailing_slash"

    return True, ""


# Retry configuration for transient HTTP errors and network failures
_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
_MAX_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF_BASE = 1.0   # seconds (doubles each attempt: 1s, 2s, 4s)
_REQUEST_TIMEOUT = 30       # seconds per HTTP request

# Network-level errors that are always retryable (DNS, connection reset, timeout)
_RETRYABLE_NETWORK_EXCEPTIONS = (RequestsConnectionError, RequestsTimeout, OSError)

# ---------------------------------------------------------------------------
# Module-level singleton session with connection pooling
# ---------------------------------------------------------------------------

_GLOBAL_SESSION: Optional[requests.Session] = None


def _get_global_session() -> requests.Session:
    """Get or create the global requests.Session with connection pooling."""
    global _GLOBAL_SESSION
    if _GLOBAL_SESSION is None:
        _GLOBAL_SESSION = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=50,
            max_retries=0,  # Custom retry logic in _get_raw, _get_raw_url, _post_raw
        )
        _GLOBAL_SESSION.mount("http://", adapter)
        _GLOBAL_SESSION.mount("https://", adapter)
    return _GLOBAL_SESSION


# ---------------------------------------------------------------------------
# DNS failure cooldown
# ---------------------------------------------------------------------------

_last_dns_failure_time: Optional[float] = None
_DNS_COOLDOWN_SECONDS = 60


def _is_in_dns_cooldown() -> bool:
    """Check if we're in DNS failure cooldown period."""
    global _last_dns_failure_time
    if _last_dns_failure_time is None:
        return False
    elapsed = time.time() - _last_dns_failure_time
    return elapsed < _DNS_COOLDOWN_SECONDS


def _record_dns_failure() -> None:
    """Record a DNS failure and start cooldown."""
    global _last_dns_failure_time
    _last_dns_failure_time = time.time()


def _get_dns_cooldown_remaining() -> float:
    """Get remaining cooldown time in seconds."""
    global _last_dns_failure_time
    if _last_dns_failure_time is None:
        return 0.0
    elapsed = time.time() - _last_dns_failure_time
    return max(0.0, _DNS_COOLDOWN_SECONDS - elapsed)


# ---------------------------------------------------------------------------
# Health check cache
# ---------------------------------------------------------------------------

_last_health_check_time: Optional[float] = None
_last_health_check_ok: bool = False
_HEALTH_CHECK_CACHE_SECONDS = 30


def _check_leaflink_health(base_url: str) -> bool:
    """Check if LeafLink API is reachable. Cache result for 30 seconds."""
    global _last_health_check_time, _last_health_check_ok

    now = time.time()
    if _last_health_check_time is not None:
        elapsed = now - _last_health_check_time
        if elapsed < _HEALTH_CHECK_CACHE_SECONDS:
            return _last_health_check_ok

    try:
        session = _get_global_session()
        resp = session.get(
            f"{base_url.rstrip('/')}/",
            timeout=5,
            allow_redirects=False,
        )
        ok = resp.status_code < 400
        _last_health_check_time = now
        _last_health_check_ok = ok

        if ok:
            logger.info("[LEAFLINK_HEALTH_CHECK_OK] status=%s", resp.status_code)
        else:
            logger.warning("[LEAFLINK_HEALTH_CHECK_FAILED] status=%s", resp.status_code)

        return ok
    except Exception as exc:
        logger.warning("[LEAFLINK_HEALTH_CHECK_FAILED] error=%s", type(exc).__name__)
        _last_health_check_time = now
        _last_health_check_ok = False
        return False


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

_consecutive_request_failures: int = 0
_circuit_breaker_state: str = "closed"  # closed, open, half_open
_circuit_breaker_open_time: Optional[float] = None
_CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
_CIRCUIT_BREAKER_COOLDOWN_SECONDS = 120


def _check_circuit_breaker() -> bool:
    """Check if circuit breaker allows requests. Return True if requests should proceed."""
    global _circuit_breaker_state, _circuit_breaker_open_time

    if _circuit_breaker_state == "closed":
        return True

    if _circuit_breaker_state == "open":
        if _circuit_breaker_open_time is None:
            return False
        elapsed = time.time() - _circuit_breaker_open_time
        if elapsed >= _CIRCUIT_BREAKER_COOLDOWN_SECONDS:
            _circuit_breaker_state = "half_open"
            logger.warning("[LEAFLINK_CIRCUIT_HALF_OPEN] attempting_recovery")
            return True
        remaining = _CIRCUIT_BREAKER_COOLDOWN_SECONDS - elapsed
        logger.warning(
            "[LEAFLINK_CIRCUIT_OPEN] cooldown_active remaining=%.1fs",
            remaining,
        )
        return False

    # half_open — allow one probe request
    return True


def _record_request_success() -> None:
    """Record a successful request. Reset circuit breaker if needed."""
    global _consecutive_request_failures, _circuit_breaker_state

    if _circuit_breaker_state == "half_open":
        logger.info("[LEAFLINK_CIRCUIT_CLOSED] recovered")

    _consecutive_request_failures = 0
    _circuit_breaker_state = "closed"


def _record_request_failure() -> None:
    """Record a request failure. Open circuit breaker if threshold reached."""
    global _consecutive_request_failures, _circuit_breaker_state, _circuit_breaker_open_time

    _consecutive_request_failures += 1

    if _circuit_breaker_state == "half_open":
        _circuit_breaker_state = "open"
        _circuit_breaker_open_time = time.time()
        logger.error("[LEAFLINK_CIRCUIT_OPEN] recovery_failed")
    elif _consecutive_request_failures >= _CIRCUIT_BREAKER_FAILURE_THRESHOLD:
        _circuit_breaker_state = "open"
        _circuit_breaker_open_time = time.time()
        logger.error(
            "[LEAFLINK_CIRCUIT_OPEN] failures=%s cooldown=120s",
            _consecutive_request_failures,
        )


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

        # Canonical LeafLink API base URL
        _CANONICAL_BASE_URL = "https://www.leaflink.com/api/v2"

        # Normalize and validate base_url
        if not base_url or base_url.strip() == "":
            # No base_url provided — use canonical
            self.base_url = _CANONICAL_BASE_URL
            logger.info(
                "[LEAFLINK_BASE_URL_CANONICALIZED] brand_id=%s original=none final=%s reason=empty_base_url",
                self.brand_id,
                self.base_url,
            )
        elif "marketplace.leaflink.com" in base_url.lower():
            # Broken marketplace.leaflink.com URL — rewrite to canonical
            self.base_url = _CANONICAL_BASE_URL
            logger.warning(
                "[LEAFLINK_BASE_URL_CANONICALIZED] brand_id=%s original=%s final=%s reason=marketplace_rewrite",
                self.brand_id,
                base_url[:50],
                self.base_url,
            )
        else:
            # Use provided base_url (assume it's valid)
            self.base_url = base_url.strip().rstrip("/")
            logger.info(
                "[LEAFLINK_BASE_URL_CANONICALIZED] brand_id=%s original=%s final=%s reason=credential_provided",
                self.brand_id,
                base_url[:50],
                self.base_url,
            )

        # Verify base_url is valid
        if not self.base_url.startswith("https://"):
            logger.error(
                "[LEAFLINK_BASE_URL_INVALID] brand_id=%s base_url=%s reason=not_https",
                self.brand_id,
                self.base_url[:50],
            )
            raise ValueError(f"LeafLink base_url must start with https://: {self.base_url}")

        if "leaflink.com" not in self.base_url.lower():
            logger.error(
                "[LEAFLINK_BASE_URL_INVALID] brand_id=%s base_url=%s reason=not_leaflink_domain",
                self.brand_id,
                self.base_url[:50],
            )
            raise ValueError(f"LeafLink base_url must contain leaflink.com: {self.base_url}")

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

        # company_id is stored in DB for metadata/tenant resolution only.
        # It is NOT used in the API path — the correct endpoint is /orders-received/
        # without any company scoping in the URL.
        self.company_id = str(company_id or "").strip()

        if not self.api_key:
            logger.warning("leaflink: client_init brand=%s missing api_key", self.brand_id)
            raise ValueError("Missing api_key — LeafLink credentials must be provided per brand (not from env vars)")

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

        # Determine auth scheme — LeafLink requires Token format
        if auth_scheme:
            self.auth_scheme: str = auth_scheme
            logger.info(
                "[LeafLinkAuth] using_explicit_scheme scheme=%s",
                auth_scheme,
            )
        else:
            # Default to Token (correct LeafLink auth scheme)
            self.auth_scheme = "Token"
            logger.info(
                "[LeafLinkAuth] using_default_scheme scheme=Token",
            )

        # ---------------------------------------------------------------------------
        # Hard-fail guards — reject stale credential values before any HTTP call
        # ---------------------------------------------------------------------------
        if "marketplace.leaflink.com" in self.base_url.lower():
            logger.error(
                "[LEAFLINK_BASE_URL_INVALID] brand_id=%s base_url=%s reason=marketplace_domain_rejected",
                self.brand_id,
                self.base_url[:80],
            )
            raise ValueError(
                f"[LEAFLINK_BASE_URL_INVALID] base_url contains deprecated marketplace domain: {self.base_url}"
            )

        if self.auth_scheme == "Api-Key":
            logger.error(
                "[LEAFLINK_AUTH_SCHEME_INVALID] brand_id=%s auth_scheme=%s reason=api_key_deprecated",
                self.brand_id,
                self.auth_scheme,
            )
            raise ValueError(
                "[LEAFLINK_AUTH_SCHEME_INVALID] auth_scheme 'Api-Key' is deprecated; use 'Token'"
            )

        # Log the final resolved values for every client instantiation
        logger.info(
            "[LEAFLINK_BASE_URL_FINAL] brand_id=%s base_url=%s",
            self.brand_id,
            self.base_url,
        )
        logger.info(
            "[LEAFLINK_AUTH_SCHEME_FINAL] brand_id=%s auth_scheme=%s",
            self.brand_id,
            self.auth_scheme,
        )

        self.session = _get_global_session()
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
        """Build Authorization header using auth_scheme from DB (default: Token)."""
        if not self.api_key:
            logger.error("[LeafLinkAuth] MISSING api_key in _get_auth_header")
            return {"Authorization": ""}

        # Use auth_scheme from DB, default to Token
        scheme = self.auth_scheme or "Token"
        auth_header = f"{scheme} {self.api_key}"
        logger.info("[LeafLinkAuth] building_header scheme=%s api_key_len=%s", scheme, len(self.api_key))
        return {"Authorization": auth_header}

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
        """Make GET request with explicit Authorization header and retry logic."""
        url = f"{self.base_url}/{path.lstrip('/')}"

        # Defensive: ensure no marketplace.leaflink.com in final URL
        if "marketplace.leaflink.com" in url.lower():
            logger.error(
                "[LEAFLINK_FINAL_URL_ERROR] final_url=%s reason=marketplace_in_final_url",
                url[:100],
            )
            raise ValueError(f"LeafLink URL contains broken marketplace domain: {url}")

        # Guardrail: detect truncated "orders-receive" (missing final "d") in the URL.
        # This catches typos like /orders-receive or /companies/{id}/orders-receive
        # before they cause a 404 from the LeafLink API.
        import re as _re
        if _re.search(r"orders-receive(?!d)", url):
            logger.error(
                "[LEAFLINK_ENDPOINT_VALIDATION] endpoint_valid=false reason=missing_final_d final_url=%s",
                url[:150],
            )
            raise ValueError(
                f"LeafLink endpoint URL contains 'orders-receive' without the final 'd'. "
                f"The correct endpoint is /orders-received/ (with trailing slash). URL: {url}"
            )
        else:
            logger.info(
                "[LEAFLINK_ENDPOINT_VALIDATION] endpoint_valid=true final_url=%s",
                url[:150],
            )

        logger.info(
            "[LEAFLINK_FINAL_URL] final_url=%s method=GET endpoint=%s",
            url,
            path,
        )

        # Build headers with explicit Authorization
        headers = {
            **self._get_auth_header(),
            "Content-Type": "application/json",
        }

        if not self._validate_request_headers(headers):
            logger.error("[LeafLinkAuth] invalid_headers aborting_request path=%s", path)
            raise ValueError("Invalid Authorization header")

        # DNS cooldown check — skip if we recently had a DNS failure
        if _is_in_dns_cooldown():
            remaining = _get_dns_cooldown_remaining()
            logger.warning(
                "[LEAFLINK_DNS_COOLDOWN] cooldown_active remaining=%.1fs next_retry_at=%s",
                remaining,
                datetime.fromtimestamp(time.time() + remaining).isoformat(),
            )
            raise RuntimeError(f"DNS cooldown active, retry in {remaining:.1f}s")

        # Circuit breaker check
        if not _check_circuit_breaker():
            raise RuntimeError("Circuit breaker is open, requests paused")

        last_exc: Optional[Exception] = None
        for attempt in range(1, _MAX_RETRY_ATTEMPTS + 1):
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    timeout=_REQUEST_TIMEOUT,
                    headers=headers,
                )
            except socket.gaierror as exc:
                # DNS failure — transient, don't dump traceback
                _record_dns_failure()
                _record_request_failure()
                logger.error(
                    "[LEAFLINK_DNS_FAILURE] error=%s cooldown_started duration=60s",
                    str(exc)[:200],
                )
                last_exc = exc
                continue
            except (RequestsConnectionError, RequestsTimeout) as exc:
                # Check for DNS-related connection errors
                exc_str = str(exc)
                if "Name or service not known" in exc_str or "NameResolutionError" in exc_str or "getaddrinfo" in exc_str:
                    _record_dns_failure()
                    logger.error(
                        "[LEAFLINK_DNS_FAILURE] error=%s cooldown_started duration=60s",
                        type(exc).__name__,
                    )
                last_exc = exc
                _record_request_failure()
                if attempt < _MAX_RETRY_ATTEMPTS:
                    backoff = _RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                    jitter = random.uniform(0, backoff * 0.1)
                    backoff_total = round(backoff + jitter, 2)
                    logger.warning(
                        "[LEAFLINK_REQUEST_RETRY] method=GET endpoint=%s error=%s attempt=%s backoff=%ss",
                        path, type(exc).__name__, attempt, backoff_total,
                    )
                    time.sleep(backoff_total)
                else:
                    logger.error(
                        "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s error=%s max_retries_exceeded",
                        path, type(exc).__name__,
                    )
                continue
            except OSError as exc:
                last_exc = exc
                _record_request_failure()
                if attempt < _MAX_RETRY_ATTEMPTS:
                    backoff = _RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                    jitter = random.uniform(0, backoff * 0.1)
                    backoff_total = round(backoff + jitter, 2)
                    logger.warning(
                        "[LEAFLINK_REQUEST_RETRY] method=GET endpoint=%s error=%s attempt=%s backoff=%ss",
                        path, type(exc).__name__, attempt, backoff_total,
                    )
                    time.sleep(backoff_total)
                else:
                    logger.error(
                        "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s error=%s max_retries_exceeded",
                        path, type(exc).__name__,
                    )
                continue
            except Exception as exc:
                logger.exception(
                    "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s unexpected_error=%s",
                    path, type(exc).__name__,
                )
                _record_request_failure()
                last_exc = exc
                continue

            content_type = resp.headers.get("Content-Type", "")
            if "application/json" not in content_type.lower():
                logger.warning(
                    "leaflink: API response is not JSON full_url=%s status=%s content_type=%s body_preview=%s",
                    url, resp.status_code, content_type, resp.text[:200],
                )

            if not resp.ok:
                if resp.status_code == 404:
                    logger.error(
                        "[LeafLinkAuth] 404_not_found full_url=%s scheme=%s brand=%s",
                        url, self.auth_scheme, self.brand_id,
                    )
                elif resp.status_code == 403:
                    logger.error(
                        "[LeafLinkAuth] 403_invalid_token full_url=%s scheme=%s api_key_len=%s brand=%s",
                        url, self.auth_scheme, len(self.api_key), self.brand_id,
                    )
                if resp.status_code in (401, 403):
                    logger.error(
                        "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s status=%s",
                        path, resp.status_code,
                    )
                    _record_request_failure()
                    # Do NOT retry auth failures
                    return resp
                elif resp.status_code in _RETRY_STATUS_CODES and attempt < _MAX_RETRY_ATTEMPTS:
                    backoff = _RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                    jitter = random.uniform(0, backoff * 0.1)
                    backoff_total = round(backoff + jitter, 2)
                    logger.warning(
                        "[LEAFLINK_REQUEST_RETRY] method=GET endpoint=%s status=%s attempt=%s backoff=%ss",
                        path, resp.status_code, attempt, backoff_total,
                    )
                    time.sleep(backoff_total)
                    continue
                else:
                    logger.error(
                        "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s status=%s",
                        path, resp.status_code,
                    )
                    _record_request_failure()
            else:
                _record_request_success()
                logger.info(
                    "[LEAFLINK_REQUEST_SUCCESS] method=GET endpoint=%s status=%s",
                    path, resp.status_code,
                )
                # [LEAFLINK_RESPONSE_META] Log detailed response metadata for diagnostics
                try:
                    _meta_data = resp.json()
                    _response_count = _meta_data.get("count") if isinstance(_meta_data, dict) else None
                    _next = _meta_data.get("next") if isinstance(_meta_data, dict) else None
                    _previous = _meta_data.get("previous") if isinstance(_meta_data, dict) else None
                    _results = _meta_data.get("results") if isinstance(_meta_data, dict) else (_meta_data if isinstance(_meta_data, list) else [])
                    _results_length = len(_results) if isinstance(_results, list) else 0
                    _top_level_keys = list(_meta_data.keys()) if isinstance(_meta_data, dict) else []

                    logger.info(
                        "[LEAFLINK_RESPONSE_META] endpoint=%s status=%s response_count=%s next=%s previous=%s results_length=%s top_level_keys=%s company_id=%s",
                        path,
                        resp.status_code,
                        _response_count,
                        "present" if _next else "none",
                        "present" if _previous else "none",
                        _results_length,
                        _top_level_keys,
                        self.company_id,
                    )

                    # [LEAFLINK_EMPTY_RESULT] Explicitly log when API returns 200 but zero results
                    if _results_length == 0 and _response_count == 0:
                        logger.warning(
                            "[LEAFLINK_EMPTY_RESULT] endpoint=%s status=%s response_count=%s results_length=%s company_id=%s",
                            path,
                            resp.status_code,
                            _response_count,
                            _results_length,
                            self.company_id,
                        )

                    # [LEAFLINK_FIRST_OBJECT] Log structure of first result when results exist
                    if isinstance(_results, list) and len(_results) > 0:
                        _first = _results[0]
                        if isinstance(_first, dict):
                            _order_id = _first.get("id") or _first.get("external_id") or _first.get("order_id") or "unknown"
                            _seller = _first.get("seller") or _first.get("seller_company") or _first.get("from_company") or "unknown"
                            _buyer = _first.get("buyer") or _first.get("buyer_company") or _first.get("to_company") or "unknown"
                            _source_company = _first.get("source_company_id") or _first.get("from_company_id") or "unknown"
                            _destination_company = _first.get("destination_company_id") or _first.get("to_company_id") or "unknown"
                            _first_keys = list(_first.keys())

                            logger.info(
                                "[LEAFLINK_FIRST_OBJECT] order_id=%s seller=%s buyer=%s source_company=%s destination_company=%s top_level_keys=%s",
                                _order_id,
                                _seller,
                                _buyer,
                                _source_company,
                                _destination_company,
                                _first_keys,
                            )
                except Exception as _meta_exc:
                    logger.warning("[LEAFLINK_RESPONSE_META] failed to parse response: %s", str(_meta_exc)[:200])

            return resp

        # All attempts exhausted via exception path
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"LeafLink request failed after {_MAX_RETRY_ATTEMPTS} attempts: {url}")

    def _get_raw_url(self, url: str) -> requests.Response:
        """Make GET request to full URL with explicit Authorization header and retry logic."""
        # Defensive: ensure no marketplace.leaflink.com in final URL
        if "marketplace.leaflink.com" in url.lower():
            logger.error(
                "[LEAFLINK_FINAL_URL_ERROR] final_url=%s reason=marketplace_in_final_url",
                url[:100],
            )
            raise ValueError(f"LeafLink URL contains broken marketplace domain: {url}")

        logger.info(
            "[LEAFLINK_FINAL_URL] final_url=%s method=GET endpoint=full_url",
            url,
        )

        # Build headers with explicit Authorization
        headers = {
            **self._get_auth_header(),
            "Content-Type": "application/json",
        }

        if not self._validate_request_headers(headers):
            logger.error("[LeafLinkAuth] invalid_headers aborting_request full_url=%s", url)
            raise ValueError("Invalid Authorization header")

        # DNS cooldown check — skip if we recently had a DNS failure
        if _is_in_dns_cooldown():
            remaining = _get_dns_cooldown_remaining()
            logger.warning(
                "[LEAFLINK_DNS_COOLDOWN] cooldown_active remaining=%.1fs next_retry_at=%s",
                remaining,
                datetime.fromtimestamp(time.time() + remaining).isoformat(),
            )
            raise RuntimeError(f"DNS cooldown active, retry in {remaining:.1f}s")

        # Circuit breaker check
        if not _check_circuit_breaker():
            raise RuntimeError("Circuit breaker is open, requests paused")

        last_exc: Optional[Exception] = None
        for attempt in range(1, _MAX_RETRY_ATTEMPTS + 1):
            try:
                resp = self.session.get(
                    url,
                    params=None,
                    timeout=_REQUEST_TIMEOUT,
                    headers=headers,
                )
            except socket.gaierror as exc:
                # DNS failure — transient, don't dump traceback
                _record_dns_failure()
                _record_request_failure()
                logger.error(
                    "[LEAFLINK_DNS_FAILURE] error=%s cooldown_started duration=60s",
                    str(exc)[:200],
                )
                last_exc = exc
                continue
            except (RequestsConnectionError, RequestsTimeout) as exc:
                # Check for DNS-related connection errors
                exc_str = str(exc)
                if "Name or service not known" in exc_str or "NameResolutionError" in exc_str or "getaddrinfo" in exc_str:
                    _record_dns_failure()
                    logger.error(
                        "[LEAFLINK_DNS_FAILURE] error=%s cooldown_started duration=60s",
                        type(exc).__name__,
                    )
                last_exc = exc
                _record_request_failure()
                if attempt < _MAX_RETRY_ATTEMPTS:
                    backoff = _RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                    jitter = random.uniform(0, backoff * 0.1)
                    backoff_total = round(backoff + jitter, 2)
                    logger.warning(
                        "[LEAFLINK_REQUEST_RETRY] method=GET endpoint=%s error=%s attempt=%s backoff=%ss",
                        url, type(exc).__name__, attempt, backoff_total,
                    )
                    time.sleep(backoff_total)
                else:
                    logger.error(
                        "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s error=%s max_retries_exceeded",
                        url, type(exc).__name__,
                    )
                continue
            except OSError as exc:
                last_exc = exc
                _record_request_failure()
                if attempt < _MAX_RETRY_ATTEMPTS:
                    backoff = _RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                    jitter = random.uniform(0, backoff * 0.1)
                    backoff_total = round(backoff + jitter, 2)
                    logger.warning(
                        "[LEAFLINK_REQUEST_RETRY] method=GET endpoint=%s error=%s attempt=%s backoff=%ss",
                        url, type(exc).__name__, attempt, backoff_total,
                    )
                    time.sleep(backoff_total)
                else:
                    logger.error(
                        "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s error=%s max_retries_exceeded",
                        url, type(exc).__name__,
                    )
                continue
            except Exception as exc:
                logger.exception(
                    "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s unexpected_error=%s",
                    url, type(exc).__name__,
                )
                _record_request_failure()
                last_exc = exc
                continue

            if resp.status_code == 404:
                logger.error(
                    "[LeafLinkAuth] 404_not_found full_url=%s scheme=%s",
                    url, self.auth_scheme,
                )
            elif resp.status_code == 403:
                logger.error(
                    "[LeafLinkAuth] 403_invalid_token full_url=%s scheme=%s api_key_len=%s",
                    url, self.auth_scheme, len(self.api_key),
                )

            if resp.status_code in (401, 403):
                logger.error(
                    "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s status=%s",
                    url, resp.status_code,
                )
                _record_request_failure()
                # Do NOT retry auth failures
                return resp
            elif not resp.ok and resp.status_code in _RETRY_STATUS_CODES and attempt < _MAX_RETRY_ATTEMPTS:
                backoff = _RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                jitter = random.uniform(0, backoff * 0.1)
                backoff_total = round(backoff + jitter, 2)
                logger.warning(
                    "[LEAFLINK_REQUEST_RETRY] method=GET endpoint=%s status=%s attempt=%s backoff=%ss",
                    url, resp.status_code, attempt, backoff_total,
                )
                time.sleep(backoff_total)
                continue
            elif resp.ok:
                _record_request_success()
                logger.info(
                    "[LEAFLINK_REQUEST_SUCCESS] method=GET endpoint=%s status=%s",
                    url, resp.status_code,
                )
            else:
                _record_request_failure()
                logger.error(
                    "[LEAFLINK_REQUEST_FAILED] method=GET endpoint=%s status=%s",
                    url, resp.status_code,
                )

            return resp

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"LeafLink pagination request failed after {_MAX_RETRY_ATTEMPTS} attempts: {url}")

    def _post_raw(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Make POST request with explicit Authorization header."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        safe_params = {k: v for k, v in (params or {}).items() if k not in ("api_key", "token", "secret")}

        # Defensive: ensure no marketplace.leaflink.com in final URL
        if "marketplace.leaflink.com" in url.lower():
            logger.error(
                "[LEAFLINK_FINAL_URL_ERROR] final_url=%s reason=marketplace_in_final_url",
                url[:100],
            )
            raise ValueError(f"LeafLink URL contains broken marketplace domain: {url}")

        logger.info(
            "[LEAFLINK_FINAL_URL] final_url=%s method=POST endpoint=%s",
            url,
            path,
        )

        # Build headers with explicit Authorization
        headers = {
            **self._get_auth_header(),
            "Content-Type": "application/json",
        }

        if not self._validate_request_headers(headers):
            logger.error("[LeafLinkAuth] invalid_headers aborting_request method=POST full_url=%s", url)
            raise ValueError("Invalid Authorization header")

        # DNS cooldown check — skip if we recently had a DNS failure
        if _is_in_dns_cooldown():
            remaining = _get_dns_cooldown_remaining()
            logger.warning(
                "[LEAFLINK_DNS_COOLDOWN] cooldown_active remaining=%.1fs next_retry_at=%s",
                remaining,
                datetime.fromtimestamp(time.time() + remaining).isoformat(),
            )
            raise RuntimeError(f"DNS cooldown active, retry in {remaining:.1f}s")

        # Circuit breaker check
        if not _check_circuit_breaker():
            raise RuntimeError("Circuit breaker is open, requests paused")

        try:
            resp = self.session.post(
                url,
                params=safe_params,
                json=json_data,
                timeout=_REQUEST_TIMEOUT,
                headers=headers,  # EXPLICIT headers on every request
            )
            if resp.ok:
                _record_request_success()
                logger.info(
                    "[LEAFLINK_REQUEST_SUCCESS] method=POST endpoint=%s status=%s",
                    path, resp.status_code,
                )
            else:
                _record_request_failure()
                logger.error(
                    "[LEAFLINK_REQUEST_FAILED] method=POST endpoint=%s status=%s",
                    path, resp.status_code,
                )
            return resp
        except socket.gaierror as exc:
            _record_dns_failure()
            _record_request_failure()
            logger.error(
                "[LEAFLINK_DNS_FAILURE] error=%s cooldown_started duration=60s",
                str(exc)[:200],
            )
            raise
        except (RequestsConnectionError, RequestsTimeout) as exc:
            exc_str = str(exc)
            if "Name or service not known" in exc_str or "NameResolutionError" in exc_str or "getaddrinfo" in exc_str:
                _record_dns_failure()
                logger.error(
                    "[LEAFLINK_DNS_FAILURE] error=%s cooldown_started duration=60s",
                    type(exc).__name__,
                )
            _record_request_failure()
            logger.warning(
                "[LEAFLINK_REQUEST_FAILED] method=POST endpoint=%s error=%s",
                path, type(exc).__name__,
            )
            raise
        except Exception as exc:
            _record_request_failure()
            logger.exception(
                "[LEAFLINK_REQUEST_FAILED] method=POST endpoint=%s unexpected_error=%s",
                path, type(exc).__name__,
            )
            raise

    def list_orders(
        self,
        limit: int = 100,
        offset: int = 0,
        modified__gte: Optional[str] = None,
        modified__lte: Optional[str] = None,
        status: Optional[str] = None,
        skip_date_filters: bool = False,
    ) -> Any:
        # ---------------------------------------------------------------------------
        # ENDPOINT CHOICE — LeafLink v2 API docs
        # ---------------------------------------------------------------------------
        # The correct endpoint is /orders-received/ WITHOUT any company scoping.
        # Using /companies/{company_id}/orders-received/ is NOT the right path for
        # the v2 API — it returns 404 or an empty result set for most accounts.
        # company_id is stored in DB for tenant resolution only; it is NOT used here.
        # ---------------------------------------------------------------------------
        _orders_path = "orders-received/"

        # Build the final URL for validation logging BEFORE making the request
        _final_url = f"{self.base_url.rstrip('/')}/{_orders_path}"

        # Log endpoint choice with reasoning
        _company_scoping = "yes" if "/companies/" in _final_url else "no"
        logger.info(
            "[LEAFLINK_ENDPOINT_CHOICE] endpoint=%s reason=v2_api_no_company_scoping_required"
            " company_id=%s company_scoping=%s",
            _final_url,
            self.company_id or "none",
            _company_scoping,
        )

        # Log company_id usage
        logger.info(
            "[LEAFLINK_REQUEST] company_id=%s company_scoping=%s",
            self.company_id or "none",
            _company_scoping,
        )

        # Hard-fail: reject any URL that contains the truncated form (missing final "d")
        import re as _re_lo
        if _re_lo.search(r"orders-receive(?!d)", _final_url):
            logger.error(
                "[LEAFLINK_FINAL_URL_INVALID] brand_id=%s final_url=%s"
                " reason=orders_receive_missing_d",
                self.brand_id,
                _final_url,
            )
            raise ValueError(
                f"[LEAFLINK_FINAL_URL_INVALID] orders endpoint URL contains 'orders-receive' "
                f"without the final 'd'. Correct endpoint is /orders-received/. URL: {_final_url}"
            )

        # Log the final URL being called
        logger.info(
            "[LEAFLINK_REQUEST] final_url=%s brand_id=%s",
            _final_url,
            self.brand_id,
        )

        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }

        # ---------------------------------------------------------------------------
        # Date filter handling — disabled on first call to test without filters.
        # If date filters are the cause of zero results, removing them will return
        # orders and confirm the filter range is the problem.
        # ---------------------------------------------------------------------------
        if skip_date_filters:
            # Testing without date filters — helps isolate whether filters cause zero results
            logger.info(
                "[LEAFLINK_REQUEST] date_filters=none (testing without filters)"
                " brand_id=%s",
                self.brand_id,
            )
        else:
            if modified__gte:
                params["modified__gte"] = modified__gte
            if modified__lte:
                params["modified__lte"] = modified__lte
            logger.info(
                "[LEAFLINK_REQUEST] date_filters=modified__gte=%s modified__lte=%s brand_id=%s",
                modified__gte or "none",
                modified__lte or "none",
                self.brand_id,
            )

        if status:
            params["status"] = status

        # Log all query parameters being sent
        logger.info(
            "[LEAFLINK_REQUEST] params=%s brand_id=%s",
            params,
            self.brand_id,
        )

        try:
            resp = self._get_raw(_orders_path, params=params)
        except Exception as exc:
            logger.error(
                "leaflink: list_orders_request_failed brand=%s error=%s",
                self.brand_id,
                str(exc)[:200],
            )
            raise

        content_type = resp.headers.get("Content-Type", "")

        # Log HTTP status code
        logger.info(
            "[LEAFLINK_RESPONSE] status_code=%s brand_id=%s",
            resp.status_code,
            self.brand_id,
        )

        # Log response preview (first 1000 chars)
        _resp_preview = resp.text[:1000] if resp.text else ""
        logger.info(
            "[LEAFLINK_RESPONSE] response_preview=%s brand_id=%s",
            _resp_preview,
            self.brand_id,
        )

        if resp.ok and "application/json" in content_type.lower():
            data = resp.json()

            # Log response type
            if isinstance(data, list):
                _resp_type = "list"
            elif isinstance(data, dict):
                _resp_type = "dict"
            else:
                _resp_type = "other"
            logger.info(
                "[LEAFLINK_RESPONSE] response_type=%s brand_id=%s",
                _resp_type,
                self.brand_id,
            )

            # If response is a dict, log pagination fields
            if isinstance(data, dict):
                _count = data.get("count")
                _next = data.get("next")
                _previous = data.get("previous")
                logger.info(
                    "[LEAFLINK_RESPONSE] count=%s next=%s previous=%s brand_id=%s",
                    _count,
                    _next or "none",
                    _previous or "none",
                    self.brand_id,
                )

            return data

        if resp.status_code in (401, 403):
            logger.error(
                "[LeafLinkAuth] list_orders_auth_failed brand=%s status=%s body=%s",
                self.brand_id,
                resp.status_code,
                resp.text[:200],
            )
        else:
            logger.error(
                "leaflink: list_orders failed brand=%s status=%s content_type=%s body_preview=%s",
                self.brand_id,
                resp.status_code,
                content_type,
                resp.text[:200],
            )
        raise RuntimeError(
            f"LeafLink API error: status={resp.status_code} content_type={content_type} body={resp.text[:200]}"
        )

    def test_endpoint_comparison(self) -> None:
        """Test both /orders-received/ and /orders/ endpoints for comparison.

        Call this once per sync if primary endpoint returns zero results.
        Helps diagnose whether the endpoint type is correct.
        """
        logger.info("[LEAFLINK_ENDPOINT_COMPARE] starting_comparison company_id=%s", self.company_id)

        # Test /companies/{company_id}/orders-received/ (or unscoped /orders-received/)
        try:
            resp1 = self._get_raw("orders-received/", params={"limit": 1, "offset": 0})
            data1 = resp1.json() if resp1.ok else {}
            count1 = data1.get("count") if isinstance(data1, dict) else None
            results1 = data1.get("results") if isinstance(data1, dict) else []
            results_length1 = len(results1) if isinstance(results1, list) else 0
            status1 = resp1.status_code
        except Exception as exc:
            logger.warning("[LEAFLINK_ENDPOINT_COMPARE] orders-received_test_failed: %s", str(exc)[:200])
            count1 = None
            results_length1 = 0
            status1 = "error"

        # Test /orders/ endpoint
        try:
            resp2 = self._get_raw("orders/", params={"limit": 1, "offset": 0})
            data2 = resp2.json() if resp2.ok else {}
            count2 = data2.get("count") if isinstance(data2, dict) else None
            results2 = data2.get("results") if isinstance(data2, dict) else []
            results_length2 = len(results2) if isinstance(results2, list) else 0
            status2 = resp2.status_code
        except Exception as exc:
            logger.warning("[LEAFLINK_ENDPOINT_COMPARE] orders_test_failed: %s", str(exc)[:200])
            count2 = None
            results_length2 = 0
            status2 = "error"

        logger.info(
            "[LEAFLINK_ENDPOINT_COMPARE] company_id=%s endpoint_orders_received_count=%s endpoint_orders_received_results=%s endpoint_orders_received_status=%s endpoint_orders_count=%s endpoint_orders_results=%s endpoint_orders_status=%s",
            self.company_id,
            count1,
            results_length1,
            status1,
            count2,
            results_length2,
            status2,
        )

    def test_unscoped_endpoint(self) -> None:
        """Test unscoped /api/v2/orders-received/ endpoint for diagnostics.

        Only call if explicitly enabled. Helps determine if company_id scoping is the issue.
        """
        logger.info("[LEAFLINK_UNSCOPED_TEST] starting_unscoped_test company_id=%s", self.company_id)

        try:
            # Build unscoped URL directly (strip any company-scoped path from base_url)
            # base_url is typically https://marketplace.leaflink.com/api/v2/companies/{id}
            # We want https://marketplace.leaflink.com/api/v2/orders-received/
            _base = self.base_url.rstrip("/")
            # If base_url already contains /companies/, strip back to the api/v2 root
            if "/companies/" in _base:
                _api_root = _base[:_base.index("/companies/")]
            else:
                _api_root = _base
            unscoped_url = f"{_api_root}/orders-received/"

            headers = {
                **self._get_auth_header(),
                "Content-Type": "application/json",
            }

            resp = self.session.get(
                unscoped_url,
                params={"limit": 1, "offset": 0},
                timeout=30,
                headers=headers,
            )

            data = resp.json() if resp.ok else {}
            count = data.get("count") if isinstance(data, dict) else None
            results = data.get("results") if isinstance(data, dict) else []
            results_length = len(results) if isinstance(results, list) else 0

            logger.info(
                "[LEAFLINK_UNSCOPED_TEST] endpoint=%s/orders-received/ status=%s count=%s results_length=%s",
                _api_root,
                resp.status_code,
                count,
                results_length,
            )
        except Exception as exc:
            logger.warning("[LEAFLINK_UNSCOPED_TEST] failed: %s", str(exc)[:200])

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
        # [SYNC_FETCH_ORDERS_ENTRY] Log entry into fetch_orders_page_range
        logger.info(
            "[SYNC_FETCH_ORDERS_ENTRY] fetch_orders_page_range called brand=%s start_page=%s"
            " num_pages=%s page_size=%s resume_url=%s",
            self.brand_id,
            start_page,
            num_pages,
            page_size,
            "present" if resume_url else "none",
        )

        _health_ok = _check_leaflink_health(self.base_url)
        logger.info(
            "[SYNC_HEALTH_CHECK] brand=%s base_url=%s health_ok=%s",
            self.brand_id,
            self.base_url,
            _health_ok,
        )
        if not _health_ok:
            logger.error(
                "[LEAFLINK_SYNC_SKIPPED] health_check_failed brand=%s base_url=%s"
                " — returning empty orders list, this may cause fetched_count=0",
                self.brand_id,
                self.base_url,
            )
            return {"orders": [], "pages_fetched": 0, "next_url": None, "next_page": None, "total_count": None, "total_pages": None}

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
                    # Use limit/offset pagination — offset is derived from the logical
                    # page number so that resume_url and fresh-start paths are consistent.
                    _offset = (current_page - 1) * page_size
                    payload = self.list_orders(limit=page_size, offset=_offset)

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

                # [SYNC_PAGE_FETCHED] Log raw results count before normalization
                logger.info(
                    "[SYNC_PAGE_FETCHED] page=%s raw_count=%s normalize=%s brand=%s",
                    current_page,
                    len(results),
                    normalize,
                    self.brand_id,
                )

                if normalize:
                    for raw in results:
                        if isinstance(raw, dict):
                            all_orders.append(self._normalize_order(raw))
                else:
                    for raw in results:
                        if isinstance(raw, dict):
                            all_orders.append(raw)

                # [SYNC_PAGE_FETCHED] Log parsed count after normalization
                logger.info(
                    "[SYNC_PAGE_FETCHED] page=%s parsed_count=%s total_so_far=%s brand=%s",
                    current_page,
                    len(results),
                    len(all_orders),
                    self.brand_id,
                )

                # [LEAFLINK_ORDER_SAMPLE] Log a sample of orders from this page
                if results:
                    _sample_count = min(3, len(results))
                    _sample_orders = results[:_sample_count]
                    _order_ids = [o.get("id") or o.get("external_id") or "unknown" for o in _sample_orders]
                    _customer_names = [o.get("customer_name") or "unknown" for o in _sample_orders]
                    _created_ats = [o.get("created_at") or o.get("external_created_at") or o.get("created") or "unknown" for o in _sample_orders]
                    _updated_ats = [o.get("updated_at") or o.get("external_updated_at") or o.get("modified") or "unknown" for o in _sample_orders]
                    _company_ids = [o.get("company_id") or o.get("source") or "unknown" for o in _sample_orders]
                    logger.info(
                        "[LEAFLINK_ORDER_SAMPLE] count=%s order_ids=%s customer_names=%s external_created_at=%s external_updated_at=%s company_ids=%s",
                        len(results),
                        _order_ids,
                        _customer_names,
                        _created_ats,
                        _updated_ats,
                        _company_ids,
                    )

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

        # [SYNC_FETCH_ORDERS_RESULT] Log final result of fetch_orders_page_range
        logger.info(
            "[SYNC_FETCH_ORDERS_RESULT] brand=%s orders_count=%s pages_fetched=%s"
            " has_next=%s next_page=%s total_count=%s",
            self.brand_id,
            len(all_orders),
            pages_fetched,
            "true" if next_url else "false",
            next_page,
            total_count,
        )

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
        limit: int = 100,
        modified__gte: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch all orders from LeafLink using limit/offset pagination.

        Uses the correct endpoint: /orders-received/ (no company_id in path).
        Paginates via limit/offset until no ``next`` URL is returned by the API
        or ``max_pages`` is reached.

        Args:
            max_pages: Maximum number of pages to fetch. ``None`` (default) means
                unlimited — fetch every page until LeafLink returns no ``next`` URL.
                Pass an integer to cap the number of pages (e.g. for testing).
            normalize: When ``True``, run each raw order through ``_normalize_order``.
            brand: Brand slug used only for structured log messages.
            limit: Number of orders per page (default 100).
            modified__gte: Optional ISO datetime string to filter orders modified
                after this timestamp (e.g. for incremental sync).

        Returns:
            A dict with keys:
                ``orders``       – list of order dicts
                ``pages_fetched`` – number of pages retrieved
        """
        if not _check_leaflink_health(self.base_url):
            logger.warning("[LEAFLINK_SYNC_SKIPPED] health_check_failed brand=%s", self.brand_id)
            return {"orders": [], "pages_fetched": 0}

        effective_max = max_pages if max_pages is not None else 10_000

        all_orders: List[Dict[str, Any]] = []
        pages_fetched = 0

        try:
            offset = 0
            next_url: Optional[str] = None

            while pages_fetched < effective_max:
                if next_url:
                    # Follow the ``next`` URL returned by LeafLink directly.
                    # _get_raw_url() explicitly attaches the Authorization header so
                    # that pagination requests are authenticated the same way as page 1.
                    resp = self._get_raw_url(next_url)
                    if resp.status_code in (401, 403):
                        logger.error(
                            "[LeafLinkAuth] pagination_auth_failed offset=%s status=%s body=%s",
                            offset,
                            resp.status_code,
                            resp.text[:200],
                        )
                        raise RuntimeError(
                            f"LeafLink pagination auth failed: status={resp.status_code} body={resp.text[:220]}"
                        )
                    if not resp.ok:
                        logger.error(
                            "[LeafLinkSync] offset=%s http_error status=%s body=%s",
                            offset,
                            resp.status_code,
                            resp.text[:200],
                        )
                        raise RuntimeError(
                            f"LeafLink API error: status={resp.status_code} body={resp.text[:200]}"
                        )
                    payload = resp.json()
                else:
                    # First page or explicit offset-based fetch
                    payload = self.list_orders(
                        limit=limit,
                        offset=offset,
                        modified__gte=modified__gte,
                    )

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

                pages_fetched += 1

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

                # [LEAFLINK_ORDER_SAMPLE] Log a sample of orders from this page
                if results:
                    _sample_count = min(3, len(results))
                    _sample_orders = results[:_sample_count]
                    _order_ids = [o.get("id") or o.get("external_id") or "unknown" for o in _sample_orders]
                    _customer_names = [o.get("customer_name") or "unknown" for o in _sample_orders]
                    _created_ats = [o.get("created_at") or o.get("external_created_at") or o.get("created") or "unknown" for o in _sample_orders]
                    _updated_ats = [o.get("updated_at") or o.get("external_updated_at") or o.get("modified") or "unknown" for o in _sample_orders]
                    _company_ids = [o.get("company_id") or o.get("source") or "unknown" for o in _sample_orders]
                    logger.info(
                        "[LEAFLINK_ORDER_SAMPLE] count=%s order_ids=%s customer_names=%s external_created_at=%s external_updated_at=%s company_ids=%s",
                        len(results),
                        _order_ids,
                        _customer_names,
                        _created_ats,
                        _updated_ats,
                        _company_ids,
                    )

                if not next_url:
                    break

                offset += limit

        except Exception as exc:
            logger.error(
                "[LeafLinkSync] API call failed brand=%s error=%s mock_mode=%s",
                brand,
                str(exc)[:200],
                MOCK_MODE,
            )
            raise

        logger.info(
            "[LeafLink] pagination_complete total_orders=%s pages_fetched=%s brand=%s",
            len(all_orders),
            pages_fetched,
            brand,
        )
        return {"orders": all_orders, "pages_fetched": pages_fetched}