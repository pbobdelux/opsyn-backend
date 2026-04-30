-- Migration: 2026_05_12_01_add_sync_runs_table
-- Creates the sync_runs table for authoritative per-brand sync lifecycle tracking.
--
-- Each sync operation (full or incremental) creates one SyncRun row.
-- Only one run per brand may be in an active state (queued | syncing) at a time.
-- Completed/failed/stalled runs are preserved for audit.
--
-- Columns:
--   id                      SERIAL PK
--   brand_id                VARCHAR(255) — brand slug / ID
--   integration_name        VARCHAR(50)  — always 'leaflink' for now
--   status                  VARCHAR(20)  — queued | syncing | completed | stalled | failed
--   mode                    VARCHAR(20)  — full | incremental
--   pages_synced            INTEGER      — pages successfully fetched and upserted
--   total_pages             INTEGER      — total pages reported by LeafLink (NULL until known)
--   orders_loaded_this_run  INTEGER      — orders upserted during this run
--   total_orders_available  INTEGER      — total orders reported by LeafLink (NULL until known)
--   current_cursor          TEXT         — current LeafLink pagination cursor
--   current_page            INTEGER      — current page number
--   last_successful_cursor  TEXT         — last cursor that produced orders
--   last_successful_page    INTEGER      — last page that produced orders
--   started_at              TIMESTAMPTZ  — when the run was created
--   last_progress_at        TIMESTAMPTZ  — last time pages_synced was incremented
--   completed_at            TIMESTAMPTZ  — when the run reached completed/failed/stalled
--   last_error              TEXT         — last error message
--   error_count             INTEGER      — number of errors encountered
--   stalled_reason          VARCHAR(255) — reason for stall (e.g. cursor_loop_detected)
--   worker_id               VARCHAR(100) — identifier of the worker handling this run
--   last_heartbeat_at       TIMESTAMPTZ  — last worker heartbeat
--   created_at              TIMESTAMPTZ
--   updated_at              TIMESTAMPTZ

CREATE TABLE IF NOT EXISTS sync_runs (
    id                      SERIAL PRIMARY KEY,
    brand_id                VARCHAR(255) NOT NULL,
    integration_name        VARCHAR(50)  NOT NULL DEFAULT 'leaflink',
    status                  VARCHAR(20)  NOT NULL DEFAULT 'queued',
    mode                    VARCHAR(20)  NOT NULL DEFAULT 'incremental',
    pages_synced            INTEGER      NOT NULL DEFAULT 0,
    total_pages             INTEGER,
    orders_loaded_this_run  INTEGER      NOT NULL DEFAULT 0,
    total_orders_available  INTEGER,
    current_cursor          TEXT,
    current_page            INTEGER      DEFAULT 1,
    last_successful_cursor  TEXT,
    last_successful_page    INTEGER,
    started_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_progress_at        TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    last_error              TEXT,
    error_count             INTEGER      NOT NULL DEFAULT 0,
    stalled_reason          VARCHAR(255),
    worker_id               VARCHAR(100),
    last_heartbeat_at       TIMESTAMPTZ,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Composite index for fast active-run lookup per brand
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_status
    ON sync_runs (brand_id, status);

-- Index for chronological queries per brand
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_started
    ON sync_runs (brand_id, started_at DESC);

-- Add sync_run_id FK to orders table if not already present
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS sync_run_id INTEGER REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS ix_orders_sync_run_id
    ON orders (sync_run_id)
    WHERE sync_run_id IS NOT NULL;
