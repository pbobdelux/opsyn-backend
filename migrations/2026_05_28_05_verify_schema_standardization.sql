-- ============================================================================
-- Verification Script: 2026_05_28_05_verify_schema_standardization
-- ============================================================================
-- Run this script after all Phase 1 and Phase 2 migrations complete to
-- confirm the schema is fully standardized to UUID primary keys.
--
-- This script is READ-ONLY — it makes no changes to the database.
-- All checks raise a NOTICE on success or an EXCEPTION on failure.
-- ============================================================================

DO $$
DECLARE
    rec         RECORD;
    null_count  INTEGER;
    fail_count  INTEGER := 0;
BEGIN
    RAISE NOTICE '=== SCHEMA STANDARDIZATION VERIFICATION ===';
    RAISE NOTICE 'Running at: %', NOW();
    RAISE NOTICE '';

    -- ========================================================================
    -- CHECK 1: All target tables have UUID primary keys
    -- ========================================================================
    RAISE NOTICE '--- CHECK 1: Primary key types ---';

    FOR rec IN
        SELECT
            t.table_name,
            c.data_type,
            c.udt_name
        FROM information_schema.tables t
        JOIN information_schema.columns c
            ON t.table_name = c.table_name AND t.table_schema = c.table_schema
        JOIN information_schema.table_constraints tc
            ON tc.table_name = t.table_name AND tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = t.table_schema
        JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_name = t.table_name
            AND kcu.column_name = c.column_name
        WHERE t.table_schema = 'public'
          AND t.table_name IN (
              'orders', 'order_lines', 'brand_api_credentials',
              'tenant_credentials', 'sync_runs', 'sync_requests',
              'sync_health', 'dead_letter_line_items', 'sync_dead_letters',
              'sync_metrics_snapshots', 'organization_brand_bindings',
              'drivers', 'routes', 'route_stops', 'route_events',
              'driver_locations', 'driver_route_history',
              'assistant_sessions', 'assistant_messages',
              'assistant_pending_actions', 'assistant_audit_logs',
              'leaflink_webhook_events'
          )
        ORDER BY t.table_name
    LOOP
        IF rec.udt_name NOT IN ('uuid', 'varchar') THEN
            RAISE WARNING '[FAIL] %.id has type % (expected uuid or varchar)', rec.table_name, rec.udt_name;
            fail_count := fail_count + 1;
        ELSE
            RAISE NOTICE '[OK]   %.id type = %', rec.table_name, rec.udt_name;
        END IF;
    END LOOP;

    -- ========================================================================
    -- CHECK 2: No NULL primary keys in any table
    -- ========================================================================
    RAISE NOTICE '';
    RAISE NOTICE '--- CHECK 2: NULL primary key check ---';

    FOR rec IN
        SELECT unnest(ARRAY[
            'orders', 'order_lines', 'brand_api_credentials',
            'tenant_credentials', 'sync_runs', 'sync_requests',
            'sync_health', 'dead_letter_line_items', 'sync_dead_letters',
            'sync_metrics_snapshots', 'organization_brand_bindings',
            'drivers', 'routes', 'route_stops',
            'assistant_sessions', 'assistant_messages',
            'assistant_pending_actions', 'assistant_audit_logs'
        ]) AS table_name
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM %I WHERE id IS NULL', rec.table_name)
        INTO null_count;

        IF null_count > 0 THEN
            RAISE WARNING '[FAIL] %.id has % NULL rows', rec.table_name, null_count;
            fail_count := fail_count + 1;
        ELSE
            RAISE NOTICE '[OK]   %.id has no NULL rows', rec.table_name;
        END IF;
    END LOOP;

    -- ========================================================================
    -- CHECK 3: FK integrity — no orphaned rows
    -- ========================================================================
    RAISE NOTICE '';
    RAISE NOTICE '--- CHECK 3: FK integrity ---';

    -- order_lines → orders
    SELECT COUNT(*) INTO null_count
    FROM order_lines ol
    LEFT JOIN orders o ON o.id = ol.order_id
    WHERE ol.order_id IS NOT NULL AND o.id IS NULL;
    IF null_count > 0 THEN
        RAISE WARNING '[FAIL] order_lines has % orphaned rows (order_id not in orders)', null_count;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[OK]   order_lines.order_id FK integrity verified';
    END IF;

    -- orders → sync_runs
    SELECT COUNT(*) INTO null_count
    FROM orders o
    LEFT JOIN sync_runs r ON r.id = o.sync_run_id
    WHERE o.sync_run_id IS NOT NULL AND r.id IS NULL;
    IF null_count > 0 THEN
        RAISE WARNING '[FAIL] orders has % rows with invalid sync_run_id', null_count;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[OK]   orders.sync_run_id FK integrity verified';
    END IF;

    -- assistant_messages → assistant_sessions
    SELECT COUNT(*) INTO null_count
    FROM assistant_messages m
    LEFT JOIN assistant_sessions s ON s.id = m.session_id
    WHERE m.session_id IS NOT NULL AND s.id IS NULL;
    IF null_count > 0 THEN
        RAISE WARNING '[FAIL] assistant_messages has % orphaned rows (session_id not in assistant_sessions)', null_count;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[OK]   assistant_messages.session_id FK integrity verified';
    END IF;

    -- assistant_pending_actions → assistant_sessions
    SELECT COUNT(*) INTO null_count
    FROM assistant_pending_actions p
    LEFT JOIN assistant_sessions s ON s.id = p.session_id
    WHERE p.session_id IS NOT NULL AND s.id IS NULL;
    IF null_count > 0 THEN
        RAISE WARNING '[FAIL] assistant_pending_actions has % orphaned rows', null_count;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[OK]   assistant_pending_actions.session_id FK integrity verified';
    END IF;

    -- assistant_audit_logs → assistant_sessions
    SELECT COUNT(*) INTO null_count
    FROM assistant_audit_logs a
    LEFT JOIN assistant_sessions s ON s.id = a.session_id
    WHERE a.session_id IS NOT NULL AND s.id IS NULL;
    IF null_count > 0 THEN
        RAISE WARNING '[FAIL] assistant_audit_logs has % orphaned rows', null_count;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[OK]   assistant_audit_logs.session_id FK integrity verified';
    END IF;

    -- route_stops → routes
    SELECT COUNT(*) INTO null_count
    FROM route_stops rs
    LEFT JOIN routes r ON r.id = rs.route_id
    WHERE rs.route_id IS NOT NULL AND r.id IS NULL;
    IF null_count > 0 THEN
        RAISE WARNING '[FAIL] route_stops has % orphaned rows (route_id not in routes)', null_count;
        fail_count := fail_count + 1;
    ELSE
        RAISE NOTICE '[OK]   route_stops.route_id FK integrity verified';
    END IF;

    -- ========================================================================
    -- CHECK 4: UUID default is set on all PK columns
    -- ========================================================================
    RAISE NOTICE '';
    RAISE NOTICE '--- CHECK 4: UUID default on PK columns ---';

    FOR rec IN
        SELECT table_name, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_name = 'id'
          AND table_name IN (
              'orders', 'order_lines', 'brand_api_credentials',
              'tenant_credentials', 'sync_runs', 'sync_requests',
              'sync_health', 'dead_letter_line_items', 'sync_dead_letters',
              'sync_metrics_snapshots', 'organization_brand_bindings',
              'drivers', 'routes', 'route_stops', 'route_events',
              'driver_locations', 'driver_route_history'
          )
        ORDER BY table_name
    LOOP
        IF rec.column_default IS NULL OR rec.column_default NOT LIKE '%gen_random_uuid%' THEN
            RAISE WARNING '[WARN]  %.id has no gen_random_uuid() default (default=%)',
                rec.table_name, COALESCE(rec.column_default, 'NULL');
        ELSE
            RAISE NOTICE '[OK]   %.id default = gen_random_uuid()', rec.table_name;
        END IF;
    END LOOP;

    -- ========================================================================
    -- SUMMARY
    -- ========================================================================
    RAISE NOTICE '';
    RAISE NOTICE '=== VERIFICATION SUMMARY ===';
    IF fail_count = 0 THEN
        RAISE NOTICE '[PASS] All checks passed — schema standardization complete';
    ELSE
        RAISE EXCEPTION '[FAIL] % check(s) failed — review warnings above', fail_count;
    END IF;
END $$;

-- ============================================================================
-- Additional diagnostic queries (run manually as needed)
-- ============================================================================

-- Full PK type inventory
SELECT
    t.table_name,
    c.column_name,
    c.data_type,
    c.udt_name,
    c.column_default
FROM information_schema.tables t
JOIN information_schema.columns c
    ON t.table_name = c.table_name AND t.table_schema = c.table_schema
JOIN information_schema.table_constraints tc
    ON tc.table_name = t.table_name AND tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = t.table_schema
JOIN information_schema.key_column_usage kcu
    ON kcu.constraint_name = tc.constraint_name
    AND kcu.table_name = t.table_name
    AND kcu.column_name = c.column_name
WHERE t.table_schema = 'public'
ORDER BY t.table_name;

-- Row counts for all migrated tables
SELECT
    'orders'                    AS table_name, COUNT(*) AS rows FROM orders
UNION ALL SELECT 'order_lines',                COUNT(*) FROM order_lines
UNION ALL SELECT 'brand_api_credentials',      COUNT(*) FROM brand_api_credentials
UNION ALL SELECT 'tenant_credentials',         COUNT(*) FROM tenant_credentials
UNION ALL SELECT 'sync_runs',                  COUNT(*) FROM sync_runs
UNION ALL SELECT 'sync_requests',              COUNT(*) FROM sync_requests
UNION ALL SELECT 'sync_health',                COUNT(*) FROM sync_health
UNION ALL SELECT 'dead_letter_line_items',     COUNT(*) FROM dead_letter_line_items
UNION ALL SELECT 'sync_dead_letters',          COUNT(*) FROM sync_dead_letters
UNION ALL SELECT 'sync_metrics_snapshots',     COUNT(*) FROM sync_metrics_snapshots
UNION ALL SELECT 'organization_brand_bindings',COUNT(*) FROM organization_brand_bindings
UNION ALL SELECT 'drivers',                    COUNT(*) FROM drivers
UNION ALL SELECT 'routes',                     COUNT(*) FROM routes
UNION ALL SELECT 'route_stops',                COUNT(*) FROM route_stops
UNION ALL SELECT 'assistant_sessions',         COUNT(*) FROM assistant_sessions
UNION ALL SELECT 'assistant_messages',         COUNT(*) FROM assistant_messages
UNION ALL SELECT 'assistant_pending_actions',  COUNT(*) FROM assistant_pending_actions
UNION ALL SELECT 'assistant_audit_logs',       COUNT(*) FROM assistant_audit_logs
ORDER BY table_name;
