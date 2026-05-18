"""
Request timing middleware.

Logs structured markers for every request:
  [BACKEND_REQUEST_START] method=GET path=/api/mobile/orders
  [BACKEND_RESPONSE_SENT] method=GET path=/api/mobile/orders status=200 elapsed_ms=1234

Skips logging for /ping and / to avoid noise from health probes.
"""

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("opsyn-backend.timing")

# Paths that are too noisy to log (health probes, root ping)
_SKIP_PATHS = frozenset({"/ping", "/"})


class TimingMiddleware(BaseHTTPMiddleware):
    """Log request start and response sent with elapsed time in milliseconds."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        method = request.method

        if path not in _SKIP_PATHS:
            logger.info(
                "[BACKEND_REQUEST_START] method=%s path=%s",
                method,
                path,
            )

        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = round((time.monotonic() - start) * 1000)

        if path not in _SKIP_PATHS:
            logger.info(
                "[BACKEND_RESPONSE_SENT] method=%s path=%s status=%s elapsed_ms=%d",
                method,
                path,
                response.status_code,
                elapsed_ms,
            )

        return response
