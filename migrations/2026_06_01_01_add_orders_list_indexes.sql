-- Migration: 2026_06_01_01_add_orders_list_indexes
-- Adds indexes required for fast paginated, filtered /orders queries.
--
-- These indexes ensure that brand-scoped ORDER BY + WHERE queries on the
-- orders table use index scans instead of sequential scans, keeping
-- response times under 2 seconds even for brands with 100k+ orders.
--
-- Existing indexes (not repeated here):
--   ix_orders_brand_id          — orders (brand_id)              [2026_05_01]
--   ix_orders_brand_created_at  — orders (brand_id, created_at)  [2026_05_11]
--   ix_orders_brand_updated_at  — orders (brand_id, updated_at)  [2026_05_11]

-- Primary pagination index: brand + created_at DESC (most common sort)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_brand_created
    ON orders (brand_id, created_at DESC);

-- Alternative pagination index: brand + external_created_at DESC
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_brand_external_created
    ON orders (brand_id, external_created_at DESC);

-- Status filtering index
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_brand_status
    ON orders (brand_id, status);

-- Mapping/sync status filtering index
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_brand_mapping_status
    ON orders (brand_id, sync_status);

-- Customer name search index (for ILIKE prefix searches)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_customer_name
    ON orders (customer_name);

-- Order number search index (for ILIKE prefix searches)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_order_number_search
    ON orders (order_number);
