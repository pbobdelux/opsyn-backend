-- Migration: 2026_05_01_01_add_credential_sync_pagination
-- Adds pagination state columns to brand_api_credentials to support
-- incremental background sync (Phase 2) for large LeafLink datasets.
--
-- last_synced_page:     The last LeafLink page number successfully synced.
--                       Updated by the background sync task after each page.
-- total_pages_estimate: Estimated total pages from the LeafLink API count field.
--                       Derived as ceil(total_count / 100) on first sync.
-- sync_in_progress:     True while a background sync task is running.
--                       Set to True when Phase 1 triggers background sync,
--                       set back to False when background sync completes.

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS last_synced_page INTEGER NOT NULL DEFAULT 1;

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS total_pages_estimate INTEGER;

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS sync_in_progress BOOLEAN NOT NULL DEFAULT FALSE;
