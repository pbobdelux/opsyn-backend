-- ============================================================================
-- Migration: 2026_05_30_01_reconcile_uuid_integer_mismatches
-- ============================================================================
-- Purpose: Audit and reconcile any remaining UUID/INTEGER type mismatches
-- that may exist if the 001/002/003 migrations ran before
-- 2026_05_21_02_add_drivers_routes_stops.sql established UUID PKs on
-- drivers, routes, and route_stops.
--
-- Strategy:
--   - All steps are guarded with IF NOT EXISTS / conditional DO blocks.
--   - The migration is safe to re-run on a fully-migrated schema (no-ops).
--   - Wrapped in a single transaction; rolls back automatically on error.
--
-- Dependency order (must have run before this migration):
--   2026_05_04_02_create_full_schema_aws_rds.sql
--   2026_05_21_02_add_drivers_routes_stops.sql
--   001_create_route_events_table.sql
--   002_create_driver_locations_table.sql
--   003_create_driver_route_history_table.sql
--   2026_05_28_02_migrate_sync_runs_to_uuid.sql
--   2026_05_28_03_migrate_orders_to_uuid.sql
--   2026_05_28_04_migrate_remaining_integer_tables.sql
-- ============================================================================

BEGIN;

-- ============================================================================
-- STEP 1: Audit current schema for UUID/INTEGER mismatches
-- ============================================================================

DO $$
DECLARE
    drivers_pk_type   TEXT;
    routes_pk_type    TEXT;
    orders_pk_type    TEXT;
BEGIN
    -- Check drivers.id type
    SELECT data_type INTO drivers_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'drivers'
      AND column_name  = 'id';

    -- Check routes.id type
    SELECT data_type INTO routes_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'routes'
      AND column_name  = 'id';

    -- Check orders.id type
    SELECT data_type INTO orders_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'orders'
      AND column_name  = 'id';

    RAISE NOTICE '[AUDIT] drivers.id type  = %', COALESCE(drivers_pk_type, 'TABLE_NOT_FOUND');
    RAISE NOTICE '[AUDIT] routes.id type   = %', COALESCE(routes_pk_type,  'TABLE_NOT_FOUND');
    RAISE NOTICE '[AUDIT] orders.id type   = %', COALESCE(orders_pk_type,  'TABLE_NOT_FOUND');

    IF drivers_pk_type IS NOT NULL AND drivers_pk_type <> 'uuid' THEN
        RAISE WARNING '[MISMATCH] drivers.id is % — expected uuid', drivers_pk_type;
    END IF;

    IF routes_pk_type IS NOT NULL AND routes_pk_type <> 'uuid' THEN
        RAISE WARNING '[MISMATCH] routes.id is % — expected uuid', routes_pk_type;
    END IF;

    IF orders_pk_type IS NOT NULL AND orders_pk_type <> 'uuid' THEN
        RAISE WARNING '[MISMATCH] orders.id is % — expected uuid', orders_pk_type;
    END IF;
END $$;

-- ============================================================================
-- STEP 2: Ensure route_events table exists with correct UUID FK types
-- ============================================================================
-- If 001_create_route_events_table.sql failed (because routes did not exist
-- yet), create it now that routes is guaranteed to be present.

CREATE TABLE IF NOT EXISTS route_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id      UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id        UUID NOT NULL,
    event_type    VARCHAR(50) NOT NULL,
    actor_type    VARCHAR(20) NOT NULL,
    actor_id      UUID NOT NULL,
    event_metadata JSONB DEFAULT '{}',
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT ck_route_events_event_type CHECK (event_type IN (
        'published', 'driver_assigned', 'stop_status_changed',
        'collection_recorded', 'stops_reordered', 'route_completed'
    )),
    CONSTRAINT ck_route_events_actor_type CHECK (actor_type IN ('admin', 'driver'))
);

CREATE INDEX IF NOT EXISTS ix_route_events_route_id  ON route_events(route_id);
CREATE INDEX IF NOT EXISTS ix_route_events_org_id    ON route_events(org_id);
CREATE INDEX IF NOT EXISTS ix_route_events_created_at ON route_events(created_at);

-- ============================================================================
-- STEP 3: Ensure driver_locations table exists with correct UUID FK types
-- ============================================================================
-- If 002_create_driver_locations_table.sql failed (because drivers/routes did
-- not exist yet), create it now.

CREATE TABLE IF NOT EXISTS driver_locations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id       UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    org_id          UUID NOT NULL,
    route_id        UUID REFERENCES routes(id) ON DELETE SET NULL,
    latitude        NUMERIC(10,7) NOT NULL,
    longitude       NUMERIC(10,7) NOT NULL,
    accuracy_meters NUMERIC(8,2),
    speed_mph       NUMERIC(6,2),
    heading         NUMERIC(5,2),
    altitude_meters NUMERIC(8,2),
    battery_percent INTEGER,
    is_moving       BOOLEAN DEFAULT false,
    source          VARCHAR(20) DEFAULT 'gps'
                        CHECK (source IN ('gps', 'network', 'manual')),
    recorded_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_driver_locations_driver_recorded
    ON driver_locations(driver_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_driver_locations_route
    ON driver_locations(route_id);
CREATE INDEX IF NOT EXISTS ix_driver_locations_org_time
    ON driver_locations(org_id, recorded_at DESC);

-- ============================================================================
-- STEP 4: Ensure driver_route_history table exists with correct UUID FK types
-- ============================================================================
-- If 003_create_driver_route_history_table.sql failed (because drivers/routes
-- did not exist yet), create it now.

CREATE TABLE IF NOT EXISTS driver_route_history (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id        UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    route_id         UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id           UUID NOT NULL,
    event_type       VARCHAR(30) NOT NULL CHECK (event_type IN (
        'route_started', 'stop_arrived', 'stop_departed', 'stop_completed',
        'stop_failed', 'stop_skipped', 'route_completed', 'route_paused',
        'route_resumed', 'break_started', 'break_ended', 'deviation_detected'
    )),
    stop_id          UUID REFERENCES route_stops(id) ON DELETE SET NULL,
    latitude         NUMERIC(10,7),
    longitude        NUMERIC(10,7),
    address_snapshot TEXT,
    event_metadata   JSONB DEFAULT '{}',
    notes            TEXT,
    recorded_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_driver_route_history_route
    ON driver_route_history(route_id, recorded_at ASC);
CREATE INDEX IF NOT EXISTS ix_driver_route_history_driver
    ON driver_route_history(driver_id, recorded_at DESC);

-- ============================================================================
-- STEP 5: Verify FK type consistency for all three tables
-- ============================================================================

DO $$
DECLARE
    fail_count INTEGER := 0;

    -- route_events.route_id
    re_route_id_type   TEXT;
    -- driver_locations.driver_id
    dl_driver_id_type  TEXT;
    dl_route_id_type   TEXT;
    -- driver_route_history.driver_id / route_id
    drh_driver_id_type TEXT;
    drh_route_id_type  TEXT;

    -- Referenced PK types
    drivers_pk_type    TEXT;
    routes_pk_type     TEXT;
BEGIN
    -- Fetch FK column types
    SELECT data_type INTO re_route_id_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'route_events'
      AND column_name = 'route_id';

    SELECT data_type INTO dl_driver_id_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'driver_locations'
      AND column_name = 'driver_id';

    SELECT data_type INTO dl_route_id_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'driver_locations'
      AND column_name = 'route_id';

    SELECT data_type INTO drh_driver_id_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'driver_route_history'
      AND column_name = 'driver_id';

    SELECT data_type INTO drh_route_id_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'driver_route_history'
      AND column_name = 'route_id';

    -- Fetch PK types
    SELECT data_type INTO drivers_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'drivers'
      AND column_name = 'id';

    SELECT data_type INTO routes_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'routes'
      AND column_name = 'id';

    -- Validate
    IF re_route_id_type IS DISTINCT FROM routes_pk_type THEN
        RAISE WARNING '[FK_MISMATCH] route_events.route_id=% but routes.id=%',
            re_route_id_type, routes_pk_type;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[FK_OK] route_events.route_id matches routes.id (%)', routes_pk_type;
    END IF;

    IF dl_driver_id_type IS DISTINCT FROM drivers_pk_type THEN
        RAISE WARNING '[FK_MISMATCH] driver_locations.driver_id=% but drivers.id=%',
            dl_driver_id_type, drivers_pk_type;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[FK_OK] driver_locations.driver_id matches drivers.id (%)', drivers_pk_type;
    END IF;

    IF dl_route_id_type IS NOT NULL AND dl_route_id_type IS DISTINCT FROM routes_pk_type THEN
        RAISE WARNING '[FK_MISMATCH] driver_locations.route_id=% but routes.id=%',
            dl_route_id_type, routes_pk_type;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[FK_OK] driver_locations.route_id matches routes.id (%)', routes_pk_type;
    END IF;

    IF drh_driver_id_type IS DISTINCT FROM drivers_pk_type THEN
        RAISE WARNING '[FK_MISMATCH] driver_route_history.driver_id=% but drivers.id=%',
            drh_driver_id_type, drivers_pk_type;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[FK_OK] driver_route_history.driver_id matches drivers.id (%)', drivers_pk_type;
    END IF;

    IF drh_route_id_type IS DISTINCT FROM routes_pk_type THEN
        RAISE WARNING '[FK_MISMATCH] driver_route_history.route_id=% but routes.id=%',
            drh_route_id_type, routes_pk_type;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[FK_OK] driver_route_history.route_id matches routes.id (%)', routes_pk_type;
    END IF;

    IF fail_count = 0 THEN
        RAISE NOTICE '[RECONCILE_OK] All FK types match their referenced PKs — schema is consistent';
    ELSE
        RAISE EXCEPTION '[RECONCILE_FAIL] % FK type mismatch(es) detected — review warnings above',
            fail_count;
    END IF;
END $$;

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

COMMIT;

-- Post-migration verification (run manually as needed):
--
-- SELECT table_name, column_name, data_type
-- FROM information_schema.columns
-- WHERE table_schema = 'public'
--   AND table_name IN ('route_events', 'driver_locations', 'driver_route_history')
--   AND column_name IN ('id', 'route_id', 'driver_id')
-- ORDER BY table_name, column_name;
-- Expected: all rows show data_type = 'uuid'
