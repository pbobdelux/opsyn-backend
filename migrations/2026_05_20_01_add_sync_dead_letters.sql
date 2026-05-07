-- Migration: 2026_05_20_01_add_sync_dead_letters
-- Creates the sync_dead_letters table for the resilient ingestion pipeline.
--
-- sync_dead_letters: captures full raw LeafLink payloads for orders that
-- could not be processed (header insert failure or unrecoverable transform
-- error). Records are preserved for later reprocessing without blocking the
-- sync loop.
--
-- Columns:
--   source        — integration name (e.g. 'leaflink')
--   brand_id      — brand UUID
--   org_id        — org UUID (nullable)
--   external_id   — LeafLink order PK (nullable)
--   order_number  — human-readable order number (nullable)
--   raw_payload   — full JSON payload from LeafLink
--   error_stage   — where the failure occurred (e.g. 'header_insert', 'line_item_transform')
--   error_message — exception message
--   retry_count   — number of reprocess attempts (default 0)
--   created_at    — when the dead-letter record was written
--   resolved_at   — when the record was successfully reprocessed (nullable)
--
-- [SYNC_DEAD_LETTER_TABLE_CREATED]

CREATE TABLE IF NOT EXISTS sync_dead_letters (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(120) NOT NULL DEFAULT 'leaflink',
    brand_id        UUID NOT NULL,
    org_id          UUID,
    external_id     VARCHAR(255),
    order_number    VARCHAR(255),
    raw_payload     JSONB NOT NULL,
    error_stage     VARCHAR(120) NOT NULL,
    error_message   TEXT NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS ix_sync_dead_letters_brand_id
    ON sync_dead_letters (brand_id);

CREATE INDEX IF NOT EXISTS ix_sync_dead_letters_source_brand
    ON sync_dead_letters (source, brand_id);

CREATE INDEX IF NOT EXISTS ix_sync_dead_letters_external_id
    ON sync_dead_letters (external_id)
    WHERE external_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_sync_dead_letters_unresolved
    ON sync_dead_letters (brand_id, created_at DESC)
    WHERE resolved_at IS NULL;
