-- Migration: 2026_06_01_01_add_sync_metrics_view_and_identity_columns
-- Adds sync_metrics view and identity audit columns to sync_health.
-- Part of the upsert identity + reconciliation validation fix.

-- ---------------------------------------------------------------------------
-- 1. Add identity audit columns to sync_health
--    These track null/unknown/duplicate external_order_id counts per brand.
-- ---------------------------------------------------------------------------
ALTER TABLE sync_health
    ADD COLUMN IF NOT EXISTS null_external_order_id_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unknown_external_order_id_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duplicate_order_number_count INTEGER DEFAULT 0;

-- ---------------------------------------------------------------------------
-- 2. Create sync_metrics view
--    Provides per-brand order counts without requiring a full table scan
--    at query time. Used by health endpoints and reconciliation validator.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW sync_metrics AS
SELECT
    brand_id,
    COUNT(*)                                                        AS total_orders,
    COUNT(CASE WHEN external_order_id IS NULL THEN 1 END)           AS null_external_id,
    COUNT(CASE WHEN external_order_id = 'unknown' THEN 1 END)       AS unknown_external_id,
    COUNT(CASE WHEN status != 'cancelled' THEN 1 END)               AS visible_orders,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END)                AS cancelled_orders,
    MAX(created_at)                                                 AS latest_order_created_at,
    MAX(synced_at)                                                  AS latest_synced_at
FROM orders
GROUP BY brand_id;

-- ---------------------------------------------------------------------------
-- 3. Add unique index on (brand_id, order_number) to support the order_number
--    fallback conflict target in the upsert identity logic.
--    Only created if it does not already exist.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE tablename = 'orders'
          AND indexname = 'uq_brand_order_number'
    ) THEN
        CREATE UNIQUE INDEX uq_brand_order_number
            ON orders (brand_id, order_number)
            WHERE order_number IS NOT NULL;
    END IF;
END
$$;
