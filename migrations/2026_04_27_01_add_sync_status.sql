-- Migration: 2026_04_27_01_add_sync_status
-- Adds the sync_status table used by the Opsyn Watchdog integration.
-- One row per brand_id tracks the current sync lifecycle state so that
-- Twin can poll GET /orders/sync/{brand_id}/status for real-time progress.

CREATE TABLE IF NOT EXISTS sync_status (
    id                      SERIAL PRIMARY KEY,
    brand_id                VARCHAR(120) NOT NULL,
    status                  VARCHAR(50)  NOT NULL DEFAULT 'connecting',
    total_fetched           INTEGER      NOT NULL DEFAULT 0,
    total_in_database       INTEGER      NOT NULL DEFAULT 0,
    percent_complete        INTEGER      NOT NULL DEFAULT 0,
    latest_order_date       TIMESTAMP WITH TIME ZONE,
    oldest_order_date       TIMESTAMP WITH TIME ZONE,
    sync_error              TEXT,
    last_progress_timestamp TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_sync_status_brand_id UNIQUE (brand_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_status_brand_id ON sync_status(brand_id);
