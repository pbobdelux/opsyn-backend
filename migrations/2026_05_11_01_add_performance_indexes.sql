-- Migration: 2026_05_11_01_add_performance_indexes
-- Adds composite and single-column indexes to support 100k+ order queries.
--
-- Indexes already in place (not repeated here):
--   ix_orders_brand_id          — orders (brand_id)              [2026_05_01]
--   ix_orders_order_number      — orders (order_number)          [2026_05_01]
--   ix_brand_api_credentials_sync_status
--                               — brand_api_credentials (sync_status)
--                                 WHERE sync_status = 'syncing'  [2026_05_02]

-- orders table: composite indexes for brand-scoped pagination queries
CREATE INDEX IF NOT EXISTS ix_orders_brand_updated_at
    ON orders (brand_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_orders_brand_created_at
    ON orders (brand_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_orders_brand_external_updated_at
    ON orders (brand_id, external_updated_at DESC);

-- orders table: single-column indexes for upsert lookups and incremental sync
CREATE INDEX IF NOT EXISTS ix_orders_external_order_id
    ON orders (external_order_id);

CREATE INDEX IF NOT EXISTS ix_orders_last_synced_at
    ON orders (last_synced_at);

-- order_lines table: FK lookup and SKU search
CREATE INDEX IF NOT EXISTS ix_order_lines_order_id
    ON order_lines (order_id);

CREATE INDEX IF NOT EXISTS ix_order_lines_sku
    ON order_lines (sku);

-- brand_api_credentials table: lookup credentials by brand
CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_brand_id
    ON brand_api_credentials (brand_id);
