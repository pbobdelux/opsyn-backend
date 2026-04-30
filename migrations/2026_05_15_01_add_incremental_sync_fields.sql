-- Migration: 2026_05_15_01_add_incremental_sync_fields
-- Adds fields required for incremental sync support to the sync_runs table.
--
-- New columns:
--   last_successful_sync_at  TIMESTAMPTZ  — timestamp of the last successfully
--                                           completed sync run; used as the lower
--                                           bound (updated_at__gte) when fetching
--                                           orders in incremental mode.
--
-- The `mode` column (full | incremental) was already present in the original
-- CREATE TABLE statement (2026_05_12_01_add_sync_runs_table.sql), so it is
-- guarded with ADD COLUMN IF NOT EXISTS for safety on any environment that may
-- have been created without it.

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS mode VARCHAR(50) NOT NULL DEFAULT 'full',
    ADD COLUMN IF NOT EXISTS last_successful_sync_at TIMESTAMPTZ;

COMMENT ON COLUMN sync_runs.last_successful_sync_at IS
    'Timestamp of the last successfully completed sync run. '
    'Used as the lower bound (updated_at__gte) for incremental LeafLink fetches.';
