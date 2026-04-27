"""
Pydantic models for Opsyn Watchdog webhook payloads and status responses.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class SyncMetadata(BaseModel):
    """Metadata describing the current state of a LeafLink sync operation."""

    total_fetched_from_leaflink: Optional[int] = None
    total_in_database: Optional[int] = None
    percent_complete: Optional[float] = None
    latest_order_date: Optional[str] = None
    oldest_order_date: Optional[str] = None
    sync_error: Optional[str] = None


class WatchdogEventPayload(BaseModel):
    """Inbound webhook payload sent by Opsyn Watchdog (or outbound to Watchdog)."""

    event_type: str
    brand_id: str
    timestamp: str
    sync_metadata: Optional[SyncMetadata] = None
