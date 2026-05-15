"""
Latency tracking middleware for request-level performance observability.

Tracks p50, p95, p99 latency percentiles for all HTTP requests.
Exposes metrics via get_latency_percentiles() for health endpoints.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Callable, Dict, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

logger = logging.getLogger("latency_tracking")

# ---------------------------------------------------------------------------
# In-memory latency store — rolling window of the last N samples
# ---------------------------------------------------------------------------

_MAX_SAMPLES = 1000  # keep last 1000 request latencies

# Separate buckets per endpoint group for granular reporting
_latency_samples: deque[float] = deque(maxlen=_MAX_SAMPLES)
_db_latency_samples: deque[float] = deque(maxlen=_MAX_SAMPLES)


def record_request_latency(latency_ms: float) -> None:
    """Record a request latency sample (milliseconds)."""
    _latency_samples.append(latency_ms)


def record_db_latency(latency_ms: float) -> None:
    """Record a database query latency sample (milliseconds)."""
    _db_latency_samples.append(latency_ms)


def _percentile(samples: list[float], p: float) -> Optional[float]:
    """Compute the p-th percentile of a sorted list of samples."""
    if not samples:
        return None
    sorted_samples = sorted(samples)
    idx = int(len(sorted_samples) * p / 100)
    idx = min(idx, len(sorted_samples) - 1)
    return round(sorted_samples[idx], 2)


def get_latency_percentiles() -> Dict[str, Optional[float]]:
    """Return p50/p95/p99 API request latency percentiles in milliseconds."""
    samples = list(_latency_samples)
    return {
        "p50_ms": _percentile(samples, 50),
        "p95_ms": _percentile(samples, 95),
        "p99_ms": _percentile(samples, 99),
        "sample_count": len(samples),
    }


def get_db_latency_percentiles() -> Dict[str, Optional[float]]:
    """Return p50/p95/p99 DB query latency percentiles in milliseconds."""
    samples = list(_db_latency_samples)
    return {
        "p50_ms": _percentile(samples, 50),
        "p95_ms": _percentile(samples, 95),
        "p99_ms": _percentile(samples, 99),
        "sample_count": len(samples),
    }


# ---------------------------------------------------------------------------
# Starlette middleware
# ---------------------------------------------------------------------------

class LatencyTrackingMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that measures wall-clock latency for every HTTP request
    and records it in the in-memory rolling window.

    Skips health/ping endpoints to avoid polluting latency stats with
    high-frequency liveness probes.
    """

    _SKIP_PATHS = frozenset({"/ping", "/", "/health", "/health/"})

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = (time.monotonic() - start) * 1000

        path = request.url.path
        if path not in self._SKIP_PATHS:
            record_request_latency(elapsed_ms)
            logger.debug(
                "[LATENCY_TRACKING] method=%s path=%s status=%s latency_ms=%.2f",
                request.method,
                path,
                response.status_code,
                elapsed_ms,
            )

        return response
