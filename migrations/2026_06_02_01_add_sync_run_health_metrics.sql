-- Migration: 2026_06_02_01_add_sync_run_health_metrics
-- Adds detailed per-run metrics columns to sync_runs for structured observability.
--
-- New columns:
--   orders_fetched       INTEGER  — total orders fetched from LeafLink API this run
--   orders_inserted      INTEGER  — orders newly inserted into the DB this run
--   orders_updated       INTEGER  — orders updated (upserted) in the DB this run
--   line_items_inserted  INTEGER  — order line items newly inserted this run
--   line_items_updated   INTEGER  — order line items updated this run
--   dead_letters_created INTEGER  — dead-letter records created this run
--   failure_reason       TEXT     — human-readable failure reason
--   error_type           VARCHAR(50) — ErrorTaxonomy enum value for primary failure
--   error_context        JSONB    — structured error context dict

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS orders_fetched      INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS orders_inserted     INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS orders_updated      INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS line_items_inserted INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS line_items_updated  INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dead_letters_created INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS failure_reason      TEXT,
    ADD COLUMN IF NOT EXISTS error_type          VARCHAR(50),
    ADD COLUMN IF NOT EXISTS error_context       JSONB;

-- Index for querying runs by error type (useful for error taxonomy analysis)
CREATE INDEX IF NOT EXISTS ix_sync_runs_error_type
    ON sync_runs (error_type)
    WHERE error_type IS NOT NULL;
