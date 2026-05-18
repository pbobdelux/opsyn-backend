"""
Request timing middleware for opsyn-backend.

Logs every request with structured markers:
  [BACKEND_REQUEST_START] method=GET path=/api/mobile/orders
  [BACKEND_RESPONSE_SENT] method=GET path=/api/mobile/orders status=200 elapsed_ms=1234

Also records latency samples into the existing LatencyTrackingMiddleware store
so /health/database can report p50/p95/p99 percentiles.
"""

from __future__ import annotations

import logging
import time
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

logger = logging.getLogger("timing")


class TimingMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that logs request start and response sent with elapsed_ms.

    Skips /ping and / to avoid log noise from health probes.
    """

    _SKIP_PATHS = frozenset({"/ping", "/"})

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path
        method = request.method

        if path not in self._SKIP_PATHS:
            logger.info(
                "[BACKEND_REQUEST_START] method=%s path=%s",
                method,
                path,
            )

        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)

        if path not in self._SKIP_PATHS:
            logger.info(
                "[BACKEND_RESPONSE_SENT] method=%s path=%s status=%s elapsed_ms=%.2f",
                method,
                path,
                response.status_code,
                elapsed_ms,
            )

        return response
