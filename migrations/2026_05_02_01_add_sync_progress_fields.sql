-- Migration: 2026_05_02_01_add_sync_progress_fields
-- Adds paginated background-sync progress tracking columns to
-- brand_api_credentials so that sync state survives service restarts.
--
-- New columns:
--   last_synced_page      INTEGER  — last LeafLink page number successfully synced (0 = not started)
--   total_pages_available INTEGER  — total pages reported by the LeafLink API for this brand
--
-- Existing columns used for sync state (no schema change needed):
--   sync_status   VARCHAR(50)  — "idle" | "syncing" | "paused" | "error"
--   last_sync_at  TIMESTAMPTZ  — timestamp of last successful page sync
--   last_error    TEXT         — error message if sync failed

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS last_synced_page      INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_pages_available INTEGER;

-- Index to quickly find credentials with interrupted syncs on startup
CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_sync_status
    ON brand_api_credentials (sync_status)
    WHERE sync_status = 'syncing';
