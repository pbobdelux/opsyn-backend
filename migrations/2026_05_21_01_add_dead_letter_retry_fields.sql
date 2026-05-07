-- Migration: 2026_05_21_01_add_dead_letter_retry_fields
-- Adds last_retry_at column to sync_dead_letters for self-healing reprocessing.
-- Also adds last_error_at to sync_health for watchdog status reporting.

ALTER TABLE sync_dead_letters
    ADD COLUMN IF NOT EXISTS last_retry_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE sync_health
    ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE sync_health
    ADD COLUMN IF NOT EXISTS orders_fetched_last_run INTEGER NOT NULL DEFAULT 0;

ALTER TABLE sync_health
    ADD COLUMN IF NOT EXISTS orders_written_last_run INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_sync_dead_letters_last_retry
    ON sync_dead_letters (brand_id, last_retry_at DESC)
    WHERE resolved_at IS NULL;
