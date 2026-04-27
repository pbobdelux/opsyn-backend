"""
Watchdog inbound authentication.

Validates the `Authorization: Bearer <secret>` header on watchdog-protected
endpoints.  The expected secret is read from OPSYN_WATCHDOG_SECRET.

Usage (FastAPI dependency):

    from auth.watchdog_auth import require_watchdog_auth

    @router.get("/orders/sync/{brand_id}/status")
    async def status(brand_id: str, _=Depends(require_watchdog_auth)):
        ...
"""

import logging
import os

from fastapi import Header, HTTPException, Request

logger = logging.getLogger("watchdog")


async def require_watchdog_auth(
    request: Request,
    authorization: str | None = Header(None, alias="Authorization"),
) -> None:
    """FastAPI dependency that enforces Bearer token authentication.

    Raises HTTP 401 when the header is missing or the token does not match
    OPSYN_WATCHDOG_SECRET.  The secret is never written to logs.
    """
    expected = os.getenv("OPSYN_WATCHDOG_SECRET", "")

    success = False
    if authorization and authorization.startswith("Bearer "):
        token = authorization[len("Bearer "):]
        if expected and token == expected:
            success = True

    # Extract brand_id from path params for logging (best-effort).
    brand_id = request.path_params.get("brand_id", "unknown")

    logger.info(
        "[Watchdog] inbound_auth success=%s brand=%s",
        str(success).lower(),
        brand_id,
    )

    if not success:
        raise HTTPException(status_code=401, detail="Unauthorized")
