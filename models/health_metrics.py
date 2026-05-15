"""
Health metrics data models for structured observability.

Provides Pydantic models (not ORM) for health endpoint responses and
an ErrorTaxonomy enum for structured error categorization.
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Error Taxonomy
# ---------------------------------------------------------------------------

class ErrorTaxonomy(str, enum.Enum):
    """Canonical error categories for structured error classification."""
    datetime_error = "datetime_error"
    uuid_mismatch = "uuid_mismatch"
    api_timeout = "api_timeout"
    db_write_failure = "db_write_failure"
    duplicate_key = "duplicate_key"
    invalid_payload = "invalid_payload"
    missing_brand_id = "missing_brand_id"
    auth_failure = "auth_failure"
    network_failure = "network_failure"
    unknown = "unknown"


# ---------------------------------------------------------------------------
# Per-run metrics
# ---------------------------------------------------------------------------

class SyncRunMetrics(BaseModel):
    """Detailed metrics for a single sync run."""
    run_id: int
    brand_id: str
    status: str
    mode: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    pages_synced: int = 0
    total_pages: Optional[int] = None
    orders_fetched: int = 0
    orders_inserted: int = 0
    orders_updated: int = 0
    line_items_inserted: int = 0
    line_items_updated: int = 0
    dead_letters_created: int = 0
    failure_reason: Optional[str] = None
    error_type: Optional[str] = None
    error_context: Optional[Dict[str, Any]] = None
    is_stalled: bool = False


# ---------------------------------------------------------------------------
# Aggregate sync health
# ---------------------------------------------------------------------------

class SyncHealthMetrics(BaseModel):
    """Aggregate sync health across all brands."""
    active_runs: int = 0
    stalled_runs: int = 0
    orphaned_runs: int = 0
    successful_24h: int = 0
    failed_24h: int = 0
    avg_duration_seconds: Optional[float] = None
    orders_processed_24h: int = 0
    line_items_processed_24h: int = 0
    dead_letters_created_24h: int = 0
    retry_queue_size: int = 0
    last_successful_sync_at: Optional[str] = None
    active_run_details: List[Dict[str, Any]] = Field(default_factory=list)
    stalled_run_details: List[Dict[str, Any]] = Field(default_factory=list)
    orphaned_run_details: List[Dict[str, Any]] = Field(default_factory=list)
    failed_run_details: List[Dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Database health
# ---------------------------------------------------------------------------

class DatabaseHealthMetrics(BaseModel):
    """Database connectivity and performance metrics."""
    connected: bool = False
    query_latency_p50_ms: Optional[float] = None
    query_latency_p95_ms: Optional[float] = None
    query_latency_p99_ms: Optional[float] = None
    error_count_24h: int = 0
    last_successful_query_at: Optional[str] = None
    connection_pool_status: str = "unknown"
    current_migration_version: Optional[str] = None
    pending_migrations: int = 0


# ---------------------------------------------------------------------------
# Migration health
# ---------------------------------------------------------------------------

class MigrationHealthMetrics(BaseModel):
    """Migration runner health and history."""
    last_run_at: Optional[str] = None
    current_version: Optional[str] = None
    status: str = "unknown"  # completed | in_progress | failed | unknown
    failed_migrations: List[str] = Field(default_factory=list)
    skipped_migrations: List[str] = Field(default_factory=list)
    validation_warnings: List[str] = Field(default_factory=list)
    failed_count: int = 0
    skipped_count: int = 0
    warning_count: int = 0
