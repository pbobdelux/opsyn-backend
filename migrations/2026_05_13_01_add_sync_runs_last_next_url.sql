-- Migration: 2026_05_13_01_add_sync_runs_last_next_url
-- Adds last_next_url column to sync_runs for LeafLink cursor-based pagination resume.
--
-- last_next_url stores the full LeafLink "next" cursor URL returned by the API
-- after each page. On worker restart, the sync resumes from this URL instead of
-- starting from page 1, preventing duplicate work and ensuring full pagination.

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS last_next_url TEXT;

COMMENT ON COLUMN sync_runs.last_next_url IS 'LeafLink next cursor URL for pagination resume — persisted after every page';
