"""
LeafLink authentication management.

Supports two auth modes:
  - "api_key"  – static API key sent as "Token {key}" (default)
  - "oauth"    – OAuth2 Bearer token with automatic refresh

Module-level state is intentionally process-scoped so that a refreshed token
is immediately available to all subsequent requests without a DB round-trip.

Safety: secrets are NEVER written to logs.  Only key prefixes, lengths, and
boolean presence flags are logged.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger("leaflink_auth")

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_auth_mode: str = "api_key"
_last_success_at: Optional[datetime] = None
_last_error: Optional[str] = None
_oauth_access_token: Optional[str] = None
_oauth_token_expires_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_env(key: str) -> str:
    """Return stripped env var value or empty string."""
    return os.getenv(key, "").strip()


def _mask(value: str, prefix_len: int = 6) -> str:
    """Return a safe representation of a secret for logging."""
    if not value:
        return "NONE"
    if len(value) <= prefix_len:
        return "***"
    return f"{value[:prefix_len]}***"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_leaflink_credentials() -> dict:
    """
    Validate that all required env vars are present at startup.

    Returns:
        {
            "valid": bool,
            "auth_mode": "api_key" | "oauth",
            "missing_vars": [str, ...]
        }

    Never logs actual secret values.
    """
    global _auth_mode

    auth_mode = _get_env("LEAFLINK_AUTH_MODE") or "api_key"
    _auth_mode = auth_mode

    missing: list[str] = []

    # Always required
    for var in ("LEAFLINK_API_KEY", "LEAFLINK_BASE_URL", "LEAFLINK_COMPANY_ID"):
        if not _get_env(var):
            missing.append(var)

    # OAuth-specific requirements
    if auth_mode == "oauth":
        for var in ("LEAFLINK_CLIENT_ID", "LEAFLINK_CLIENT_SECRET", "LEAFLINK_REFRESH_TOKEN"):
            if not _get_env(var):
                missing.append(var)

    valid = len(missing) == 0

    if missing:
        logger.warning(
            "[LeafLinkAuth] missing_credentials vars=%s",
            ",".join(missing),
        )
    else:
        api_key = _get_env("LEAFLINK_API_KEY")
        logger.info(
            "[LeafLinkAuth] credentials_valid auth_mode=%s key_prefix=%s",
            auth_mode,
            _mask(api_key),
        )

    return {
        "valid": valid,
        "auth_mode": auth_mode,
        "missing_vars": missing,
    }


def get_auth_headers() -> dict:
    """
    Return the correct Authorization header for the current auth mode.

    For api_key mode:  {"Authorization": "Token {key}"}
    For oauth mode:    {"Authorization": "Bearer {access_token}"}

    Never logs actual token values.
    """
    global _auth_mode, _oauth_access_token

    auth_mode = _auth_mode or _get_env("LEAFLINK_AUTH_MODE") or "api_key"

    if auth_mode == "oauth":
        token = _oauth_access_token or _get_env("LEAFLINK_ACCESS_TOKEN")
        logger.info(
            "[LeafLinkAuth] auth_present=%s auth_mode=oauth token_prefix=%s",
            bool(token),
            _mask(token) if token else "NONE",
        )
        return {"Authorization": f"Bearer {token}"} if token else {}

    # Default: api_key mode
    api_key = _get_env("LEAFLINK_API_KEY")
    logger.info(
        "[LeafLinkAuth] auth_present=%s auth_mode=api_key key_prefix=%s",
        bool(api_key),
        _mask(api_key),
    )
    return {"Authorization": f"Token {api_key}"} if api_key else {}


def handle_auth_failure(status_code: int, response_body: str) -> bool:
    """
    Handle a 401 or 403 response from the LeafLink API.

    If OAuth mode is active, attempts a token refresh and returns True on
    success.  For api_key mode (or if refresh fails) returns False.

    Args:
        status_code:   HTTP status code received (401 or 403).
        response_body: Raw response text for diagnostic logging.

    Returns:
        True  – auth was recovered (caller should retry the request).
        False – auth failure is unrecoverable with current credentials.
    """
    global _last_error

    reason = response_body[:200] if response_body else "no_body"
    logger.error(
        "[LeafLinkAuth] auth_failed status=%s reason=%s",
        status_code,
        reason,
    )
    _last_error = f"HTTP {status_code}: {reason}"

    if _auth_mode == "oauth":
        logger.info("[LeafLinkAuth] attempting token refresh after auth failure")
        return refresh_oauth_token()

    logger.warning(
        "[LeafLinkAuth] auth_mode=api_key — no automatic recovery possible status=%s",
        status_code,
    )
    return False


def refresh_oauth_token() -> bool:
    """
    Attempt to obtain a new OAuth access token using the stored refresh token.

    Reads LEAFLINK_REFRESH_TOKEN, LEAFLINK_CLIENT_ID, LEAFLINK_CLIENT_SECRET,
    and LEAFLINK_TOKEN_ENDPOINT from the environment.

    Updates _oauth_access_token and _oauth_token_expires_at in-memory on
    success.

    Returns:
        True  – new token obtained and stored.
        False – refresh failed.
    """
    global _oauth_access_token, _oauth_token_expires_at, _last_error

    logger.info("[LeafLinkAuth] token_refresh_start")

    refresh_token = _get_env("LEAFLINK_REFRESH_TOKEN")
    client_id = _get_env("LEAFLINK_CLIENT_ID")
    client_secret = _get_env("LEAFLINK_CLIENT_SECRET")
    token_endpoint = _get_env("LEAFLINK_TOKEN_ENDPOINT") or "https://www.leaflink.com/api/oauth/token"

    if not refresh_token or not client_id or not client_secret:
        missing = [
            v for v, val in (
                ("LEAFLINK_REFRESH_TOKEN", refresh_token),
                ("LEAFLINK_CLIENT_ID", client_id),
                ("LEAFLINK_CLIENT_SECRET", client_secret),
            ) if not val
        ]
        reason = f"missing_vars={','.join(missing)}"
        logger.error("[LeafLinkAuth] token_refresh_failed reason=%s", reason)
        _last_error = f"token_refresh_failed: {reason}"
        return False

    try:
        resp = requests.post(
            token_endpoint,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
    except Exception as exc:
        reason = f"request_error={exc}"
        logger.error("[LeafLinkAuth] token_refresh_failed reason=%s", reason)
        _last_error = f"token_refresh_failed: {reason}"
        return False

    if not resp.ok:
        reason = f"http_{resp.status_code} body={resp.text[:200]}"
        logger.error("[LeafLinkAuth] token_refresh_failed reason=%s", reason)
        _last_error = f"token_refresh_failed: {reason}"
        return False

    try:
        data = resp.json()
    except Exception as exc:
        reason = f"json_parse_error={exc}"
        logger.error("[LeafLinkAuth] token_refresh_failed reason=%s", reason)
        _last_error = f"token_refresh_failed: {reason}"
        return False

    new_token = data.get("access_token")
    if not new_token:
        reason = "no_access_token_in_response"
        logger.error("[LeafLinkAuth] token_refresh_failed reason=%s", reason)
        _last_error = f"token_refresh_failed: {reason}"
        return False

    _oauth_access_token = new_token

    expires_in = data.get("expires_in")
    if expires_in:
        from datetime import timedelta
        _oauth_token_expires_at = _utc_now() + timedelta(seconds=int(expires_in))

    logger.info(
        "[LeafLinkAuth] token_refresh_success new_token_prefix=%s expires_at=%s",
        _mask(new_token),
        _oauth_token_expires_at.isoformat() if _oauth_token_expires_at else "unknown",
    )
    return True


def record_success() -> None:
    """Record a successful authenticated request (called by the HTTP client)."""
    global _last_success_at, _last_error
    _last_success_at = _utc_now()
    _last_error = None


def record_error(message: str) -> None:
    """Record a failed request (called by the HTTP client)."""
    global _last_error
    _last_error = message


def get_auth_health() -> dict:
    """
    Return a health snapshot for the LeafLink auth subsystem.

    Performs a lightweight test request to verify that credentials are
    currently accepted by the API.

    Returns:
        {
            "auth_present": bool,
            "auth_mode": str,
            "last_success_at": ISO timestamp | None,
            "last_error": str | None,
            "can_fetch_orders": bool,
        }

    Never exposes secret values.
    """
    global _last_success_at, _last_error, _auth_mode

    auth_mode = _auth_mode or _get_env("LEAFLINK_AUTH_MODE") or "api_key"
    api_key = _get_env("LEAFLINK_API_KEY")
    base_url = _get_env("LEAFLINK_BASE_URL") or "https://www.leaflink.com/api/v2"

    auth_present = bool(api_key) if auth_mode == "api_key" else bool(
        _oauth_access_token or _get_env("LEAFLINK_ACCESS_TOKEN")
    )

    # Attempt a lightweight test request to confirm the token is accepted
    can_fetch_orders = False
    test_error: Optional[str] = None

    if auth_present:
        try:
            headers = get_auth_headers()
            headers["Content-Type"] = "application/json"
            test_url = f"{base_url.rstrip('/')}/orders-received/"
            resp = requests.get(
                test_url,
                headers=headers,
                params={"page": 1, "page_size": 1},
                timeout=15,
            )
            if resp.ok:
                can_fetch_orders = True
                record_success()
                logger.info("[LeafLinkAuth] health_check_ok status=%s", resp.status_code)
            elif resp.status_code in (401, 403):
                test_error = f"HTTP {resp.status_code}: authentication rejected"
                record_error(test_error)
                logger.warning(
                    "[LeafLinkAuth] health_check_auth_rejected status=%s",
                    resp.status_code,
                )
            else:
                # Non-auth error (e.g. 500, 429) — credentials may still be valid
                can_fetch_orders = False
                test_error = f"HTTP {resp.status_code}: {resp.text[:100]}"
                logger.warning(
                    "[LeafLinkAuth] health_check_non_auth_error status=%s",
                    resp.status_code,
                )
        except Exception as exc:
            test_error = f"request_error: {exc}"
            logger.error("[LeafLinkAuth] health_check_request_failed error=%s", exc)

    effective_last_error = test_error or _last_error

    return {
        "auth_present": auth_present,
        "auth_mode": auth_mode,
        "last_success_at": _last_success_at.isoformat() if _last_success_at else None,
        "last_error": effective_last_error,
        "can_fetch_orders": can_fetch_orders,
    }
