-- Migration: Add per-order sync health fields to orders table
-- These columns track the sync health status of individual orders,
-- enabling accurate "Needs Review" queue counts and per-order sync diagnostics.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS sync_health_status VARCHAR(20) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS sync_health_missing_fields JSONB DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS sync_health_last_error TEXT DEFAULT NULL;

-- Index for efficient needs_review queue queries
CREATE INDEX IF NOT EXISTS ix_orders_sync_health_status
    ON orders (brand_id, sync_health_status)
    WHERE sync_health_status IS NOT NULL;

COMMENT ON COLUMN orders.sync_health_status IS 'Per-order sync health: ok | partial | failed';
COMMENT ON COLUMN orders.sync_health_missing_fields IS 'List of missing required fields for this order (JSONB array)';
COMMENT ON COLUMN orders.sync_health_last_error IS 'Last sync error message for this order';
