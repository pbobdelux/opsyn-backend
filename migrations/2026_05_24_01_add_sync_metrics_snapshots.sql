-- Migration: 2026_05_24_01_add_sync_metrics_snapshots
-- Creates the sync_metrics_snapshots table for caching sync state.
--
-- Purpose:
--   Endpoints like /sync-metrics and /sync-status previously performed
--   full DB scans and dead-letter aggregations inline on every request,
--   causing 502 errors under load. This table stores a lightweight
--   snapshot of the current sync state that is updated periodically
--   during sync (not on every request). Endpoints read cached values
--   instead of recomputing them.
--
-- Update cadence: after each batch of records during sync.
-- Read cadence: every /sync-metrics, /sync-status, /runtime-health request.
--
-- [SYNC_METRICS_SNAPSHOTS_ADDED]

CREATE TABLE IF NOT EXISTS sync_metrics_snapshots (
    id                      SERIAL PRIMARY KEY,
    brand_id                VARCHAR(120) NOT NULL,
    sync_run_id             VARCHAR(120),

    -- Order counts (cached from orders table)
    total_local_orders      INTEGER,
    total_ok                INTEGER,
    total_partial           INTEGER,
    total_failed            INTEGER,

    -- Dead-letter count (cached from sync_dead_letters)
    dead_letter_count       INTEGER,

    -- Failure category breakdown (JSONB map of category -> count)
    count_by_failure_category JSONB,

    -- Sync progress
    pages_processed         INTEGER,
    records_processed       INTEGER,

    -- Performance metrics
    sync_rate               FLOAT,          -- records per second
    estimated_completion    TIMESTAMP WITH TIME ZONE,

    -- Timestamps
    last_successful_sync_at TIMESTAMP WITH TIME ZONE,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE (brand_id, sync_run_id)
);

-- Index for fast lookup by brand_id (most common query pattern)
CREATE INDEX IF NOT EXISTS ix_sync_metrics_snapshots_brand_id
    ON sync_metrics_snapshots (brand_id);

-- Index for finding the latest snapshot per brand
CREATE INDEX IF NOT EXISTS ix_sync_metrics_snapshots_brand_updated
    ON sync_metrics_snapshots (brand_id, updated_at DESC);

COMMENT ON TABLE sync_metrics_snapshots IS
    'Cached sync metrics snapshot — updated during sync, read by metrics endpoints. '
    'Prevents full DB scans on every /sync-metrics or /sync-status request.';

COMMENT ON COLUMN sync_metrics_snapshots.brand_id IS 'Brand UUID (tenant identifier)';
COMMENT ON COLUMN sync_metrics_snapshots.sync_run_id IS 'Current or last SyncRun ID';
COMMENT ON COLUMN sync_metrics_snapshots.total_local_orders IS 'Cached COUNT(*) of orders for this brand';
COMMENT ON COLUMN sync_metrics_snapshots.total_ok IS 'Cached count of orders with sync_status=ok';
COMMENT ON COLUMN sync_metrics_snapshots.total_partial IS 'Cached count of orders with sync_status=partial';
COMMENT ON COLUMN sync_metrics_snapshots.total_failed IS 'Cached count of orders with sync_status=failed';
COMMENT ON COLUMN sync_metrics_snapshots.dead_letter_count IS 'Cached count of unresolved dead-letter entries';
COMMENT ON COLUMN sync_metrics_snapshots.count_by_failure_category IS 'JSONB map: failure_category -> count';
COMMENT ON COLUMN sync_metrics_snapshots.pages_processed IS 'Pages fetched in current/last sync run';
COMMENT ON COLUMN sync_metrics_snapshots.records_processed IS 'Orders loaded in current/last sync run';
COMMENT ON COLUMN sync_metrics_snapshots.sync_rate IS 'Records per second (rolling average)';
COMMENT ON COLUMN sync_metrics_snapshots.estimated_completion IS 'Estimated UTC timestamp when sync will complete';
COMMENT ON COLUMN sync_metrics_snapshots.last_successful_sync_at IS 'UTC timestamp of last successful sync completion';
COMMENT ON COLUMN sync_metrics_snapshots.updated_at IS 'When this snapshot was last written';
