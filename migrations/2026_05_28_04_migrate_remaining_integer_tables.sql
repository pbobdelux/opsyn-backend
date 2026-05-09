-- ============================================================================
-- Migration: 2026_05_28_04_migrate_remaining_integer_tables
-- ============================================================================
-- Migrates all remaining INTEGER-PK tables to UUID.
--
-- PREREQUISITES:
--   - 2026_05_28_02_migrate_sync_runs_to_uuid.sql (sync_runs now UUID)
--   - 2026_05_28_03_migrate_orders_to_uuid.sql (orders now UUID;
--     order_lines.order_id and dead_letter_line_items.order_id now UUID FKs)
--
-- Tables migrated in this script (own PKs only — FKs already handled above):
--   1. brand_api_credentials  (no FK dependents)
--   2. tenant_credentials     (no FK dependents)
--   3. sync_requests          (no FK dependents)
--   4. sync_health            (no FK dependents)
--   5. sync_dead_letters      (no FK dependents)
--   6. sync_metrics_snapshots (no FK dependents)
--   7. organization_brand_bindings (no FK dependents)
--   8. order_lines            (own PK only; FK to orders already UUID)
--   9. dead_letter_line_items (own PK only; FK to orders already UUID)
--
-- Each table follows the same pattern:
--   a. ADD COLUMN id_uuid UUID DEFAULT gen_random_uuid()
--   b. UPDATE ... SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL
--   c. Integrity check
--   d. DROP CONSTRAINT pkey CASCADE
--   e. DROP COLUMN id
--   f. RENAME COLUMN id_uuid TO id
--   g. ADD PRIMARY KEY (id)
--   h. ALTER COLUMN id SET DEFAULT gen_random_uuid()
--
-- Safe to re-run: ADD COLUMN IF NOT EXISTS guards all additive steps.
-- Wrapped in a single transaction: rolls back automatically on any error.
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. brand_api_credentials
-- ============================================================================

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE brand_api_credentials
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM brand_api_credentials WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] brand_api_credentials has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] brand_api_credentials UUID column verified';
END $$;

ALTER TABLE brand_api_credentials DROP CONSTRAINT IF EXISTS brand_api_credentials_pkey CASCADE;
ALTER TABLE brand_api_credentials DROP COLUMN IF EXISTS id;
ALTER TABLE brand_api_credentials RENAME COLUMN id_uuid TO id;
ALTER TABLE brand_api_credentials ADD PRIMARY KEY (id);
ALTER TABLE brand_api_credentials ALTER COLUMN id SET DEFAULT gen_random_uuid();

CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_brand_id
    ON brand_api_credentials (brand_id);

CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_integration
    ON brand_api_credentials (integration_name);

-- ============================================================================
-- 2. tenant_credentials
-- ============================================================================

ALTER TABLE tenant_credentials
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE tenant_credentials
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM tenant_credentials WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] tenant_credentials has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] tenant_credentials UUID column verified';
END $$;

ALTER TABLE tenant_credentials DROP CONSTRAINT IF EXISTS tenant_credentials_pkey CASCADE;
ALTER TABLE tenant_credentials DROP COLUMN IF EXISTS id;
ALTER TABLE tenant_credentials RENAME COLUMN id_uuid TO id;
ALTER TABLE tenant_credentials ADD PRIMARY KEY (id);
ALTER TABLE tenant_credentials ALTER COLUMN id SET DEFAULT gen_random_uuid();

CREATE INDEX IF NOT EXISTS ix_tenant_credentials_org_id
    ON tenant_credentials (org_id);

-- ============================================================================
-- 3. sync_requests
-- ============================================================================

ALTER TABLE sync_requests
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE sync_requests
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM sync_requests WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] sync_requests has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] sync_requests UUID column verified';
END $$;

ALTER TABLE sync_requests DROP CONSTRAINT IF EXISTS sync_requests_pkey CASCADE;
ALTER TABLE sync_requests DROP COLUMN IF EXISTS id;
ALTER TABLE sync_requests RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_requests ADD PRIMARY KEY (id);
ALTER TABLE sync_requests ALTER COLUMN id SET DEFAULT gen_random_uuid();

CREATE INDEX IF NOT EXISTS ix_sync_requests_brand_id
    ON sync_requests (brand_id);

CREATE INDEX IF NOT EXISTS ix_sync_requests_status
    ON sync_requests (status)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS ix_sync_requests_org_id
    ON sync_requests (org_id);

-- ============================================================================
-- 4. sync_health
-- ============================================================================

ALTER TABLE sync_health
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE sync_health
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM sync_health WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] sync_health has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] sync_health UUID column verified';
END $$;

ALTER TABLE sync_health DROP CONSTRAINT IF EXISTS sync_health_pkey CASCADE;
ALTER TABLE sync_health DROP COLUMN IF EXISTS id;
ALTER TABLE sync_health RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_health ADD PRIMARY KEY (id);
ALTER TABLE sync_health ALTER COLUMN id SET DEFAULT gen_random_uuid();

CREATE INDEX IF NOT EXISTS ix_sync_health_brand_id
    ON sync_health (brand_id);

-- ============================================================================
-- 5. sync_dead_letters
-- ============================================================================

ALTER TABLE sync_dead_letters
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE sync_dead_letters
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM sync_dead_letters WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] sync_dead_letters has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] sync_dead_letters UUID column verified';
END $$;

ALTER TABLE sync_dead_letters DROP CONSTRAINT IF EXISTS sync_dead_letters_pkey CASCADE;
ALTER TABLE sync_dead_letters DROP COLUMN IF EXISTS id;
ALTER TABLE sync_dead_letters RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_dead_letters ADD PRIMARY KEY (id);
ALTER TABLE sync_dead_letters ALTER COLUMN id SET DEFAULT gen_random_uuid();

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

-- ============================================================================
-- 6. sync_metrics_snapshots
-- ============================================================================

ALTER TABLE sync_metrics_snapshots
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE sync_metrics_snapshots
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM sync_metrics_snapshots WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] sync_metrics_snapshots has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] sync_metrics_snapshots UUID column verified';
END $$;

ALTER TABLE sync_metrics_snapshots DROP CONSTRAINT IF EXISTS sync_metrics_snapshots_pkey CASCADE;
ALTER TABLE sync_metrics_snapshots DROP COLUMN IF EXISTS id;
ALTER TABLE sync_metrics_snapshots RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_metrics_snapshots ADD PRIMARY KEY (id);
ALTER TABLE sync_metrics_snapshots ALTER COLUMN id SET DEFAULT gen_random_uuid();

CREATE INDEX IF NOT EXISTS ix_sync_metrics_snapshots_brand_id
    ON sync_metrics_snapshots (brand_id);

CREATE INDEX IF NOT EXISTS ix_sync_metrics_snapshots_brand_updated
    ON sync_metrics_snapshots (brand_id, updated_at DESC);

-- ============================================================================
-- 7. organization_brand_bindings
-- ============================================================================

ALTER TABLE organization_brand_bindings
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE organization_brand_bindings
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM organization_brand_bindings WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] organization_brand_bindings has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] organization_brand_bindings UUID column verified';
END $$;

ALTER TABLE organization_brand_bindings DROP CONSTRAINT IF EXISTS organization_brand_bindings_pkey CASCADE;
ALTER TABLE organization_brand_bindings DROP COLUMN IF EXISTS id;
ALTER TABLE organization_brand_bindings RENAME COLUMN id_uuid TO id;
ALTER TABLE organization_brand_bindings ADD PRIMARY KEY (id);
ALTER TABLE organization_brand_bindings ALTER COLUMN id SET DEFAULT gen_random_uuid();

CREATE INDEX IF NOT EXISTS ix_org_brand_bindings_org_id
    ON organization_brand_bindings (org_id);

CREATE INDEX IF NOT EXISTS ix_org_brand_bindings_brand_id
    ON organization_brand_bindings (brand_id);

-- ============================================================================
-- 8. order_lines (own PK only — FK to orders already UUID from migration 03)
-- ============================================================================

ALTER TABLE order_lines
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE order_lines
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM order_lines WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] order_lines has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] order_lines UUID column verified';
END $$;

ALTER TABLE order_lines DROP CONSTRAINT IF EXISTS order_lines_pkey CASCADE;
ALTER TABLE order_lines DROP COLUMN IF EXISTS id;
ALTER TABLE order_lines RENAME COLUMN id_uuid TO id;
ALTER TABLE order_lines ADD PRIMARY KEY (id);
ALTER TABLE order_lines ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- 9. dead_letter_line_items (own PK only — FK to orders already UUID from migration 03)
-- ============================================================================

ALTER TABLE dead_letter_line_items
    ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

UPDATE dead_letter_line_items
SET id_uuid = gen_random_uuid()
WHERE id_uuid IS NULL;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM dead_letter_line_items WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION '[INTEGRITY_FAIL] dead_letter_line_items has NULL id_uuid rows';
    END IF;
    RAISE NOTICE '[INTEGRITY_OK] dead_letter_line_items UUID column verified';
END $$;

ALTER TABLE dead_letter_line_items DROP CONSTRAINT IF EXISTS dead_letter_line_items_pkey CASCADE;
ALTER TABLE dead_letter_line_items DROP COLUMN IF EXISTS id;
ALTER TABLE dead_letter_line_items RENAME COLUMN id_uuid TO id;
ALTER TABLE dead_letter_line_items ADD PRIMARY KEY (id);
ALTER TABLE dead_letter_line_items ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

COMMIT;

-- Post-migration verification:
--
-- SELECT table_name, data_type, udt_name
-- FROM information_schema.columns
-- WHERE table_schema = 'public'
--   AND column_name = 'id'
--   AND table_name IN (
--       'brand_api_credentials', 'tenant_credentials', 'sync_requests',
--       'sync_health', 'sync_dead_letters', 'sync_metrics_snapshots',
--       'organization_brand_bindings', 'order_lines', 'dead_letter_line_items'
--   )
-- ORDER BY table_name;
-- Expected: all rows show data_type = 'uuid'
--
-- SELECT
--     'brand_api_credentials' AS t, COUNT(*) FROM brand_api_credentials WHERE id IS NULL
-- UNION ALL SELECT 'tenant_credentials', COUNT(*) FROM tenant_credentials WHERE id IS NULL
-- UNION ALL SELECT 'sync_requests', COUNT(*) FROM sync_requests WHERE id IS NULL
-- UNION ALL SELECT 'sync_health', COUNT(*) FROM sync_health WHERE id IS NULL
-- UNION ALL SELECT 'sync_dead_letters', COUNT(*) FROM sync_dead_letters WHERE id IS NULL
-- UNION ALL SELECT 'sync_metrics_snapshots', COUNT(*) FROM sync_metrics_snapshots WHERE id IS NULL
-- UNION ALL SELECT 'organization_brand_bindings', COUNT(*) FROM organization_brand_bindings WHERE id IS NULL
-- UNION ALL SELECT 'order_lines', COUNT(*) FROM order_lines WHERE id IS NULL
-- UNION ALL SELECT 'dead_letter_line_items', COUNT(*) FROM dead_letter_line_items WHERE id IS NULL;
-- Expected: all counts = 0
