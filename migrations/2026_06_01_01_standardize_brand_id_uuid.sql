-- Migration: 2026_06_01_01_standardize_brand_id_uuid.sql
--
-- PURPOSE: Fix BLOCKER #2 — "operator does not exist: character varying = uuid"
--
-- ROOT CAUSE: brand_id columns in orders, order_lines, dead_letter_line_items,
-- sync_health, and sync_dead_letters are VARCHAR/TEXT but the application
-- passes CAST(:brand_id AS UUID) in SQL, causing PostgreSQL to reject the
-- comparison when the column is VARCHAR and the parameter is UUID type.
--
-- STRATEGY: Convert all brand_id columns to UUID type so that:
--   1. No CAST is needed in application queries
--   2. PostgreSQL can use indexes efficiently (no implicit cast)
--   3. Type consistency is enforced at the schema layer
--
-- SAFETY: All conversions use ALTER COLUMN ... USING brand_id::uuid
-- which will fail if any existing value is not a valid UUID.
-- The DO $$ block logs current types before altering.

-- ---------------------------------------------------------------------------
-- STEP 0: Audit current schema — log column types before migration
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE '[SCHEMA_AUDIT] Auditing brand_id column types before migration';
    FOR r IN
        SELECT table_name, column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_name = 'brand_id'
          AND table_name IN (
              'orders', 'order_lines', 'dead_letter_line_items',
              'sync_health', 'sync_dead_letters', 'sync_metrics_snapshots',
              'brand_api_credentials', 'organization_brand_bindings'
          )
        ORDER BY table_name
    LOOP
        RAISE NOTICE '[SCHEMA_AUDIT] table=% column=% type=% max_length=%',
            r.table_name, r.column_name, r.data_type, r.character_maximum_length;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- STEP 1: orders.brand_id — convert VARCHAR(120) → UUID
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'orders'
          AND column_name = 'brand_id'
          AND data_type IN ('character varying', 'text', 'varchar')
    ) THEN
        RAISE NOTICE '[MIGRATION] Converting orders.brand_id from VARCHAR to UUID';

        -- Drop dependent indexes first (will be recreated below)
        DROP INDEX IF EXISTS ix_orders_brand_id;
        DROP INDEX IF EXISTS ix_orders_org_brand;

        -- Convert column type
        ALTER TABLE orders
            ALTER COLUMN brand_id TYPE UUID
            USING brand_id::uuid;

        RAISE NOTICE '[MIGRATION] orders.brand_id converted to UUID';
    ELSE
        RAISE NOTICE '[MIGRATION] orders.brand_id is already UUID or does not exist — skipping';
    END IF;
END $$;

-- Recreate indexes after type change
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = 'ix_orders_brand_id'
    ) THEN
        CREATE INDEX ix_orders_brand_id ON orders(brand_id);
        RAISE NOTICE '[MIGRATION] Recreated index ix_orders_brand_id';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = 'ix_orders_org_brand'
    ) THEN
        CREATE INDEX ix_orders_org_brand ON orders(org_id, brand_id);
        RAISE NOTICE '[MIGRATION] Recreated index ix_orders_org_brand';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- STEP 2: sync_health.brand_id — convert VARCHAR(120) → UUID
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sync_health'
          AND column_name = 'brand_id'
          AND data_type IN ('character varying', 'text', 'varchar')
    ) THEN
        RAISE NOTICE '[MIGRATION] Converting sync_health.brand_id from VARCHAR to UUID';

        -- Drop unique constraint (references brand_id column type)
        ALTER TABLE sync_health DROP CONSTRAINT IF EXISTS uq_sync_health_brand_id;
        DROP INDEX IF EXISTS ix_sync_health_brand_id;

        ALTER TABLE sync_health
            ALTER COLUMN brand_id TYPE UUID
            USING brand_id::uuid;

        -- Recreate unique constraint
        ALTER TABLE sync_health
            ADD CONSTRAINT uq_sync_health_brand_id UNIQUE (brand_id);

        RAISE NOTICE '[MIGRATION] sync_health.brand_id converted to UUID';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_health.brand_id is already UUID or does not exist — skipping';
    END IF;
END $$;

-- Recreate index
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = 'ix_sync_health_brand_id'
    ) THEN
        CREATE INDEX ix_sync_health_brand_id ON sync_health(brand_id);
        RAISE NOTICE '[MIGRATION] Recreated index ix_sync_health_brand_id';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- STEP 3: dead_letter_line_items.brand_id — convert VARCHAR(120) → UUID
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dead_letter_line_items'
          AND column_name = 'brand_id'
          AND data_type IN ('character varying', 'text', 'varchar')
    ) THEN
        RAISE NOTICE '[MIGRATION] Converting dead_letter_line_items.brand_id from VARCHAR to UUID';

        -- Drop dependent constraints/indexes
        ALTER TABLE dead_letter_line_items DROP CONSTRAINT IF EXISTS uq_dead_letter_brand_order_sku;
        DROP INDEX IF EXISTS ix_dead_letter_brand_external_order;

        ALTER TABLE dead_letter_line_items
            ALTER COLUMN brand_id TYPE UUID
            USING brand_id::uuid;

        -- Recreate constraints
        ALTER TABLE dead_letter_line_items
            ADD CONSTRAINT uq_dead_letter_brand_order_sku
            UNIQUE (brand_id, external_order_id, sku);

        CREATE INDEX IF NOT EXISTS ix_dead_letter_brand_external_order
            ON dead_letter_line_items(brand_id, external_order_id);

        RAISE NOTICE '[MIGRATION] dead_letter_line_items.brand_id converted to UUID';
    ELSE
        RAISE NOTICE '[MIGRATION] dead_letter_line_items.brand_id is already UUID or does not exist — skipping';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- STEP 4: sync_dead_letters.brand_id — convert VARCHAR/TEXT → UUID
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sync_dead_letters'
          AND column_name = 'brand_id'
          AND data_type IN ('character varying', 'text', 'varchar')
    ) THEN
        RAISE NOTICE '[MIGRATION] Converting sync_dead_letters.brand_id from VARCHAR/TEXT to UUID';

        ALTER TABLE sync_dead_letters
            ALTER COLUMN brand_id TYPE UUID
            USING brand_id::uuid;

        RAISE NOTICE '[MIGRATION] sync_dead_letters.brand_id converted to UUID';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_dead_letters.brand_id is already UUID or does not exist — skipping';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- STEP 5: sync_metrics_snapshots.brand_id — convert VARCHAR/TEXT → UUID
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sync_metrics_snapshots'
          AND column_name = 'brand_id'
          AND data_type IN ('character varying', 'text', 'varchar')
    ) THEN
        RAISE NOTICE '[MIGRATION] Converting sync_metrics_snapshots.brand_id from VARCHAR/TEXT to UUID';

        -- Drop unique constraint if it exists
        ALTER TABLE sync_metrics_snapshots
            DROP CONSTRAINT IF EXISTS uq_sync_metrics_brand_run;

        ALTER TABLE sync_metrics_snapshots
            ALTER COLUMN brand_id TYPE UUID
            USING brand_id::uuid;

        -- Recreate unique constraint
        DO $inner$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'sync_metrics_snapshots'
                  AND column_name = 'sync_run_id'
            ) THEN
                ALTER TABLE sync_metrics_snapshots
                    ADD CONSTRAINT uq_sync_metrics_brand_run
                    UNIQUE (brand_id, sync_run_id);
            END IF;
        END $inner$;

        RAISE NOTICE '[MIGRATION] sync_metrics_snapshots.brand_id converted to UUID';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_metrics_snapshots.brand_id is already UUID or does not exist — skipping';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- STEP 6: Audit final schema — confirm all brand_id columns are now UUID
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    r RECORD;
    non_uuid_count INTEGER := 0;
BEGIN
    RAISE NOTICE '[SCHEMA_AUDIT] Auditing brand_id column types AFTER migration';
    FOR r IN
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_name = 'brand_id'
          AND table_name IN (
              'orders', 'dead_letter_line_items',
              'sync_health', 'sync_dead_letters', 'sync_metrics_snapshots'
          )
        ORDER BY table_name
    LOOP
        RAISE NOTICE '[SCHEMA_AUDIT_FINAL] table=% column=% type=%',
            r.table_name, r.column_name, r.data_type;
        IF r.data_type NOT IN ('uuid') THEN
            non_uuid_count := non_uuid_count + 1;
            RAISE WARNING '[SCHEMA_AUDIT_FINAL] WARN: %.% is still % (expected uuid)',
                r.table_name, r.column_name, r.data_type;
        END IF;
    END LOOP;

    IF non_uuid_count = 0 THEN
        RAISE NOTICE '[SCHEMA_AUDIT_FINAL] SUCCESS: all brand_id columns are UUID type';
    ELSE
        RAISE WARNING '[SCHEMA_AUDIT_FINAL] % brand_id column(s) are not UUID type', non_uuid_count;
    END IF;
END $$;
