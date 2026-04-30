-- Migration: 2026_05_10_01_add_sync_requests_table
-- Creates the sync_requests queue table used by the opsyn-sync-worker service.
--
-- The web process enqueues a row here after Phase 1 completes; the dedicated
-- worker polls this table, processes the background sync, and updates status.
--
-- Columns:
--   id                     SERIAL PK
--   brand_id               VARCHAR(120) — brand slug / ID
--   status                 VARCHAR(50)  — pending | processing | complete | error
--   start_page             INTEGER      — first LeafLink page the worker should fetch
--   total_pages            INTEGER      — total pages reported by LeafLink (may be NULL)
--   total_orders_available INTEGER      — total orders reported by LeafLink (may be NULL)
--   error                  TEXT         — error message if status = 'error'
--   created_at             TIMESTAMPTZ  — when the request was enqueued
--   started_at             TIMESTAMPTZ  — when the worker picked it up
--   completed_at           TIMESTAMPTZ  — when the worker finished

CREATE TABLE IF NOT EXISTS sync_requests (
    id                     SERIAL PRIMARY KEY,
    brand_id               VARCHAR(120) NOT NULL,
    status                 VARCHAR(50)  NOT NULL DEFAULT 'pending',
    start_page             INTEGER      NOT NULL DEFAULT 1,
    total_pages            INTEGER,
    total_orders_available INTEGER,
    error                  TEXT,
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    started_at             TIMESTAMPTZ,
    completed_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_sync_requests_brand_id
    ON sync_requests (brand_id);

CREATE INDEX IF NOT EXISTS ix_sync_requests_status
    ON sync_requests (status)
    WHERE status = 'pending';
