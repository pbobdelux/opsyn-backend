-- Add missing columns to orders table for sync tracking
-- These columns are required by the current sync_leaflink_background_continuous() logic
--
-- LEGACY NOTE: sync_run_id is added here as INTEGER because sync_runs.id was INTEGER
-- at the time this migration was written (2026-05-03).  Later migrations
-- (2026_05_28_02_migrate_sync_runs_to_uuid.sql and
--  2026_05_30_01_reconcile_uuid_integer_mismatches.sql) convert sync_runs.id and
-- orders.sync_run_id to UUID.  This migration is kept as-is for historical accuracy;
-- the type upgrade is handled by the reconciliation migration.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_run_id INTEGER;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS review_status VARCHAR(50);

-- Create index on sync_run_id for efficient filtering
CREATE INDEX IF NOT EXISTS idx_orders_sync_run_id ON orders(sync_run_id);
