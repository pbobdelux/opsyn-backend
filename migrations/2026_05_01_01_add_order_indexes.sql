-- Migration: 2026_05_01_01_add_order_indexes
-- Adds indexes on orders.brand_id and orders.order_number to support fast
-- upsert lookups and brand-scoped queries.
-- The unique constraint on (brand_id, external_order_id) already exists as
-- uq_brand_external_order and is the primary key for upsert performance.

CREATE INDEX IF NOT EXISTS ix_orders_brand_id
    ON orders (brand_id);

CREATE INDEX IF NOT EXISTS ix_orders_order_number
    ON orders (order_number);
