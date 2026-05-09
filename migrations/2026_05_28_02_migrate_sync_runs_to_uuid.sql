-- ============================================================================
-- Migration: 2026_05_28_02_migrate_sync_runs_to_uuid
-- ============================================================================
-- Migrates sync_runs.id from SERIAL (INTEGER) to UUID.
--
-- sync_runs is migrated first in the INTEGER→UUID sequence because
-- orders.sync_run_id is a FK that references sync_runs.id. Migrating
-- sync_runs first allows the orders FK to be updated in the same pass.
--
-- Dependency order:
--   sync_runs (this migration)
--   → orders (migration 03)
--   → order_lines, dead_letter_line_items (migration 04)
--
-- Strategy:
--   1. Add UUID column to sync_runs
--   2. Backfill UUID column for all existing rows
--   3. Add UUID FK column to orders (orders.sync_run_id_uuid)
--   4. Populate orders.sync_run_id_uuid from sync_runs.id_uuid
--   5. Verify integrity
--   6. Swap sync_runs PK: drop INTEGER id, rename id_uuid → id
--   7. Swap orders FK: drop sync_run_id, rename sync_run_id_uuid → sync_run_id
--   8. Recreate indexes
--
-- Safe to re-run: ADD COLUMN IF NOT EXISTS guards all additive steps.
-- Wrapped in a single transaction: rolls back automatically on any error.
-- ============================================================================

BEGIN;

-- ============================================================================
-- STEP 1: Add UUID column to sync_runs
-- ============================================================================

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

-- ============================================================================
-- STEP 2: Backfill UUID column for all existing rows
-- ============================================================================

UPDATE sync_runs
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

-- ============================================================================
-- STEP 3: Add UUID FK column to orders
-- ============================================================================

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS sync_run_id_uuid UUID;

-- ============================================================================
-- STEP 4: Populate orders.sync_run_id_uuid from sync_runs.id_uuid
-- ============================================================================

UPDATE orders o
SET sync_run_id_uuid = r.id_uuid
FROM sync_runs r
WHERE r.id = o.sync_run_id;

-- ============================================================================
-- STEP 5: Verify data integrity
-- ============================================================================

DO $$
DECLARE
    null_count INTEGER;
BEGIN
    -- All sync_runs rows must have a UUID id
    SELECT COUNT(*) INTO null_count
    FROM sync_runs WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] sync_runs has % rows with NULL id_uuid — aborting',
            null_count;
    END IF;

    -- All orders that had a sync_run_id must have a resolved sync_run_id_uuid
    SELECT COUNT(*) INTO null_count
    FROM orders
    WHERE sync_run_id IS NOT NULL AND sync_run_id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] orders has % rows with sync_run_id but NULL sync_run_id_uuid — aborting',
            null_count;
    END IF;

    RAISE NOTICE '[INTEGRITY_OK] sync_runs and orders UUID columns verified — proceeding with column swap';
END $$;

-- ============================================================================
-- STEP 6: Swap sync_runs PK from INTEGER to UUID
-- ============================================================================

-- Drop the primary key (CASCADE drops any FK constraints referencing sync_runs.id)
ALTER TABLE sync_runs DROP CONSTRAINT IF EXISTS sync_runs_pkey CASCADE;

-- Drop the old INTEGER id column
ALTER TABLE sync_runs DROP COLUMN IF EXISTS id;

-- Rename the UUID column to id
ALTER TABLE sync_runs RENAME COLUMN id_uuid TO id;

-- Recreate primary key
ALTER TABLE sync_runs ADD PRIMARY KEY (id);

-- Set default for new rows
ALTER TABLE sync_runs ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- STEP 7: Swap orders.sync_run_id FK from INTEGER to UUID
-- ============================================================================

-- Drop the old INTEGER FK column (constraint was already dropped by CASCADE above)
ALTER TABLE orders DROP COLUMN IF EXISTS sync_run_id;

-- Rename the UUID FK column
ALTER TABLE orders RENAME COLUMN sync_run_id_uuid TO sync_run_id;

-- Recreate FK constraint
ALTER TABLE orders
    ADD CONSTRAINT fk_orders_sync_run_id
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id);

-- ============================================================================
-- STEP 8: Recreate indexes
-- ============================================================================

-- sync_runs indexes
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_id
    ON sync_runs (brand_id);

CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_status
    ON sync_runs (brand_id, status);

CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_started
    ON sync_runs (brand_id, started_at DESC);

-- orders.sync_run_id index
CREATE INDEX IF NOT EXISTS ix_orders_sync_run_id
    ON orders (sync_run_id)
    WHERE sync_run_id IS NOT NULL;

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

COMMIT;

-- Post-migration verification:
--
-- SELECT data_type, udt_name FROM information_schema.columns
-- WHERE table_schema = 'public' AND table_name = 'sync_runs' AND column_name = 'id';
-- Expected: data_type = 'uuid'
--
-- SELECT COUNT(*) FROM sync_runs WHERE id IS NULL;  -- must be 0
--
-- SELECT data_type FROM information_schema.columns
-- WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'sync_run_id';
-- Expected: data_type = 'uuid'
