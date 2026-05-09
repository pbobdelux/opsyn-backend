-- ============================================================================
-- Migration: 2026_05_28_03_migrate_orders_to_uuid
-- ============================================================================
-- Migrates orders.id from SERIAL/BIGSERIAL (INTEGER) to UUID.
--
-- PREREQUISITE: 2026_05_28_02_migrate_sync_runs_to_uuid.sql must have run
-- successfully. orders.sync_run_id is already UUID after that migration.
--
-- This migration also updates the FK columns in tables that reference orders.id:
--   - order_lines.order_id (INTEGER → UUID)
--   - dead_letter_line_items.order_id (INTEGER → UUID)
--
-- Strategy:
--   1. Add UUID column to orders
--   2. Backfill UUID column for all existing rows
--   3. Add UUID FK columns to order_lines and dead_letter_line_items
--   4. Populate new FK columns via join on old INTEGER id
--   5. Verify integrity
--   6. Swap orders PK: drop INTEGER id, rename id_uuid → id
--   7. Swap order_lines FK: drop order_id, rename order_id_uuid → order_id
--   8. Swap dead_letter_line_items FK: same pattern
--   9. Recreate indexes and constraints
--
-- NOTE: This migration does NOT migrate order_lines.id or
-- dead_letter_line_items.id (their own PKs). Those are handled in migration 04.
--
-- Safe to re-run: ADD COLUMN IF NOT EXISTS guards all additive steps.
-- Wrapped in a single transaction: rolls back automatically on any error.
-- ============================================================================

BEGIN;

-- ============================================================================
-- STEP 1: Add UUID column to orders
-- ============================================================================

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

-- ============================================================================
-- STEP 2: Backfill UUID column for all existing rows
-- ============================================================================

UPDATE orders
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

-- ============================================================================
-- STEP 3: Add UUID FK columns to referencing tables
-- ============================================================================

ALTER TABLE order_lines
    ADD COLUMN IF NOT EXISTS order_id_uuid UUID;

ALTER TABLE dead_letter_line_items
    ADD COLUMN IF NOT EXISTS order_id_uuid UUID;

-- ============================================================================
-- STEP 4: Populate new FK columns via join on old INTEGER id
-- ============================================================================

UPDATE order_lines ol
SET order_id_uuid = o.id_uuid
FROM orders o
WHERE o.id = ol.order_id;

UPDATE dead_letter_line_items dl
SET order_id_uuid = o.id_uuid
FROM orders o
WHERE o.id = dl.order_id;

-- ============================================================================
-- STEP 5: Verify data integrity
-- ============================================================================

DO $$
DECLARE
    null_count INTEGER;
BEGIN
    -- All orders rows must have a UUID id
    SELECT COUNT(*) INTO null_count
    FROM orders WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] orders has % rows with NULL id_uuid — aborting',
            null_count;
    END IF;

    -- All order_lines rows must have a resolved order_id_uuid
    -- (order_id is NOT NULL in the schema)
    SELECT COUNT(*) INTO null_count
    FROM order_lines
    WHERE order_id IS NOT NULL AND order_id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] order_lines has % rows with unresolved order_id_uuid — aborting',
            null_count;
    END IF;

    -- dead_letter_line_items.order_id is nullable — only check non-null rows
    SELECT COUNT(*) INTO null_count
    FROM dead_letter_line_items
    WHERE order_id IS NOT NULL AND order_id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] dead_letter_line_items has % rows with unresolved order_id_uuid — aborting',
            null_count;
    END IF;

    RAISE NOTICE '[INTEGRITY_OK] orders, order_lines, dead_letter_line_items UUID columns verified — proceeding';
END $$;

-- ============================================================================
-- STEP 6: Swap orders PK from INTEGER to UUID
-- ============================================================================

-- Drop the primary key (CASCADE drops all FK constraints referencing orders.id)
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_pkey CASCADE;

-- Drop the old INTEGER id column
ALTER TABLE orders DROP COLUMN IF EXISTS id;

-- Rename the UUID column to id
ALTER TABLE orders RENAME COLUMN id_uuid TO id;

-- Recreate primary key
ALTER TABLE orders ADD PRIMARY KEY (id);

-- Set default for new rows
ALTER TABLE orders ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- STEP 7: Swap order_lines.order_id FK from INTEGER to UUID
-- ============================================================================

-- Drop the old INTEGER FK column (FK constraint was dropped by CASCADE above)
ALTER TABLE order_lines DROP COLUMN IF EXISTS order_id;

-- Rename the UUID FK column
ALTER TABLE order_lines RENAME COLUMN order_id_uuid TO order_id;

-- Enforce NOT NULL (order_id was NOT NULL in original schema)
ALTER TABLE order_lines ALTER COLUMN order_id SET NOT NULL;

-- Recreate FK constraint
ALTER TABLE order_lines
    ADD CONSTRAINT fk_order_lines_order_id
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE;

-- ============================================================================
-- STEP 8: Swap dead_letter_line_items.order_id FK from INTEGER to UUID
-- ============================================================================

-- Drop the old INTEGER FK column
ALTER TABLE dead_letter_line_items DROP COLUMN IF EXISTS order_id;

-- Rename the UUID FK column
ALTER TABLE dead_letter_line_items RENAME COLUMN order_id_uuid TO order_id;

-- order_id is nullable in dead_letter_line_items — do not set NOT NULL

-- Recreate FK constraint
ALTER TABLE dead_letter_line_items
    ADD CONSTRAINT fk_dead_letter_line_items_order_id
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL;

-- ============================================================================
-- STEP 9: Recreate indexes
-- ============================================================================

-- orders indexes
CREATE INDEX IF NOT EXISTS ix_orders_brand_id
    ON orders (brand_id);

CREATE INDEX IF NOT EXISTS ix_orders_order_number
    ON orders (order_number);

CREATE INDEX IF NOT EXISTS ix_orders_external_order_id
    ON orders (external_order_id);

CREATE INDEX IF NOT EXISTS ix_orders_org_id
    ON orders (org_id);

CREATE INDEX IF NOT EXISTS ix_orders_org_brand
    ON orders (org_id, brand_id);

CREATE INDEX IF NOT EXISTS ix_orders_assigned_driver
    ON orders (assigned_driver_id);

CREATE INDEX IF NOT EXISTS ix_orders_delivery_status
    ON orders (delivery_status);

CREATE INDEX IF NOT EXISTS ix_orders_payment_status
    ON orders (payment_status);

CREATE INDEX IF NOT EXISTS ix_orders_route_id
    ON orders (route_id);

CREATE INDEX IF NOT EXISTS ix_orders_brand_updated_at
    ON orders (brand_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_orders_brand_created_at
    ON orders (brand_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_orders_sync_health_status
    ON orders (brand_id, sync_health_status)
    WHERE sync_health_status IS NOT NULL;

-- order_lines indexes
CREATE INDEX IF NOT EXISTS ix_order_lines_order_id
    ON order_lines (order_id);

CREATE INDEX IF NOT EXISTS ix_order_lines_sku
    ON order_lines (sku);

-- Recreate the unique index for idempotent upserts (from migration 2026_05_18)
CREATE UNIQUE INDEX IF NOT EXISTS uq_order_line_identity
    ON order_lines (order_id, sku, product_name)
    WHERE sku IS NOT NULL AND product_name IS NOT NULL;

-- dead_letter_line_items indexes
CREATE INDEX IF NOT EXISTS ix_dead_letter_brand_external_order
    ON dead_letter_line_items (brand_id, external_order_id);

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

COMMIT;

-- Post-migration verification:
--
-- SELECT data_type, udt_name FROM information_schema.columns
-- WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'id';
-- Expected: data_type = 'uuid'
--
-- SELECT COUNT(*) FROM orders WHERE id IS NULL;       -- must be 0
-- SELECT COUNT(*) FROM order_lines WHERE id IS NULL;  -- must be 0 (own PK not yet migrated)
--
-- SELECT data_type FROM information_schema.columns
-- WHERE table_schema = 'public' AND table_name = 'order_lines' AND column_name = 'order_id';
-- Expected: data_type = 'uuid'
--
-- SELECT COUNT(*) FROM order_lines ol
-- LEFT JOIN orders o ON o.id = ol.order_id
-- WHERE ol.order_id IS NOT NULL AND o.id IS NULL;
-- Expected: 0 (no orphaned order_lines)
