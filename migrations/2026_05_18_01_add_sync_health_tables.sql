-- Migration: 2026_05_18_01_add_sync_health_tables
-- Creates sync_health and dead_letter_line_items tables,
-- and adds a unique index on order_lines for idempotent upserts.
--
-- sync_health: one row per brand, tracks last successful/attempted sync,
--   consecutive failures, and running totals.
--
-- dead_letter_line_items: line items that failed after MAX_RETRIES.
--   Allows admin inspection and manual replay without blocking the sync loop.
--
-- order_lines unique index: (order_id, sku, product_name) enables
--   INSERT ... ON CONFLICT DO UPDATE so retries never create duplicates.

-- -------------------------------------------------------------------------
-- sync_health table
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_health (
    id                      SERIAL PRIMARY KEY,
    brand_id                VARCHAR(120) NOT NULL,
    last_successful_sync_at TIMESTAMP WITH TIME ZONE,
    last_attempted_sync_at  TIMESTAMP WITH TIME ZONE,
    last_error              TEXT,
    consecutive_failures    INTEGER NOT NULL DEFAULT 0,
    last_page_synced        INTEGER,
    total_orders_synced     INTEGER NOT NULL DEFAULT 0,
    total_line_items_synced INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_sync_health_brand_id UNIQUE (brand_id)
);

CREATE INDEX IF NOT EXISTS ix_sync_health_brand_id ON sync_health (brand_id);

-- -------------------------------------------------------------------------
-- dead_letter_line_items table
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dead_letter_line_items (
    id                  SERIAL PRIMARY KEY,
    brand_id            VARCHAR(120) NOT NULL,
    external_order_id   VARCHAR(120) NOT NULL,
    order_id            INTEGER REFERENCES orders(id) ON DELETE SET NULL,
    sku                 VARCHAR(255),
    product_name        VARCHAR(255),
    raw_payload         JSONB,
    failure_reason      TEXT,
    failure_count       INTEGER NOT NULL DEFAULT 1,
    last_failed_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dead_letter_brand_order_sku UNIQUE (brand_id, external_order_id, sku)
);

CREATE INDEX IF NOT EXISTS ix_dead_letter_brand_external_order
    ON dead_letter_line_items (brand_id, external_order_id);

-- -------------------------------------------------------------------------
-- Unique index on order_lines for idempotent upserts
-- Allows INSERT ... ON CONFLICT (order_id, sku, product_name) DO UPDATE
--
-- Step 1: Remove duplicate rows, keeping the most recently updated one.
--         This is required before adding a UNIQUE index.
-- Step 2: Create the unique index (IF NOT EXISTS makes this idempotent).
--
-- NOTE: The partial WHERE clause (sku IS NOT NULL AND product_name IS NOT NULL)
-- ensures rows with NULL sku or product_name are excluded from the constraint
-- so they can still be inserted without conflict.
-- -------------------------------------------------------------------------
DELETE FROM order_lines
WHERE id NOT IN (
    SELECT DISTINCT ON (order_id, sku, product_name) id::bigint
    FROM order_lines
    ORDER BY order_id, sku, product_name, updated_at DESC NULLS LAST, id DESC
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_order_line_identity
    ON order_lines (order_id, sku, product_name)
    WHERE sku IS NOT NULL AND product_name IS NOT NULL;
