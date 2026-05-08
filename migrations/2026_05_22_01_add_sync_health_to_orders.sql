-- Migration: add sync_health JSONB column to orders table
-- This column stores per-order sync health state:
--   { "status": "ok"|"partial"|"failed", "missing_fields": [], "last_synced_at": ..., "last_error": ... }

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS sync_health JSONB DEFAULT NULL;

-- Index for querying orders by sync health status
CREATE INDEX IF NOT EXISTS ix_orders_sync_health_status
    ON orders ((sync_health->>'status'))
    WHERE sync_health IS NOT NULL;

COMMENT ON COLUMN orders.sync_health IS
    'Per-order sync health: {"status": "ok"|"partial"|"failed", "missing_fields": [], "last_synced_at": ..., "last_error": null}';
