-- Migration: 2026_05_15_02_convert_timestamps_to_timestamptz
-- Converts all TIMESTAMP WITHOUT TIME ZONE columns to TIMESTAMPTZ across
-- all affected tables.  PostgreSQL interprets the existing naive values as
-- UTC when the USING clause specifies AT TIME ZONE 'UTC', so no data is lost.
--
-- This migration fixes asyncpg errors of the form:
--   can't subtract offset-naive and offset-aware datetimes
--   invalid input for query argument $N: datetime.datetime(... tzinfo=UTC)
--
-- All ORM models already declare DateTime(timezone=True) and all new
-- migrations use TIMESTAMPTZ.  This migration brings the production schema
-- into alignment with the ORM and eliminates the type mismatch at the
-- asyncpg driver layer.
--
-- Each ALTER TABLE block is wrapped in a DO $$ ... $$ guard that checks
-- whether the column exists AND is currently TIMESTAMP WITHOUT TIME ZONE
-- before attempting the conversion, making the migration fully idempotent.

-- -------------------------------------------------------------------------
-- orders
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'orders'
          AND column_name  = 'external_created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN external_created_at TYPE TIMESTAMPTZ
            USING external_created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] orders.external_created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] orders.external_created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'orders'
          AND column_name  = 'external_updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN external_updated_at TYPE TIMESTAMPTZ
            USING external_updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] orders.external_updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] orders.external_updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'orders'
          AND column_name  = 'synced_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN synced_at TYPE TIMESTAMPTZ
            USING synced_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] orders.synced_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] orders.synced_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'orders'
          AND column_name  = 'last_synced_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN last_synced_at TYPE TIMESTAMPTZ
            USING last_synced_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] orders.last_synced_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] orders.last_synced_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'orders'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] orders.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] orders.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'orders'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] orders.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] orders.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- order_lines
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'order_lines'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE order_lines
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] order_lines.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] order_lines.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'order_lines'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE order_lines
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] order_lines.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] order_lines.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- sync_health
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_health'
          AND column_name  = 'last_successful_sync_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_health
            ALTER COLUMN last_successful_sync_at TYPE TIMESTAMPTZ
            USING last_successful_sync_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_health.last_successful_sync_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_health.last_successful_sync_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_health'
          AND column_name  = 'last_attempted_sync_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_health
            ALTER COLUMN last_attempted_sync_at TYPE TIMESTAMPTZ
            USING last_attempted_sync_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_health.last_attempted_sync_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_health.last_attempted_sync_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_health'
          AND column_name  = 'last_error_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_health
            ALTER COLUMN last_error_at TYPE TIMESTAMPTZ
            USING last_error_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_health.last_error_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_health.last_error_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_health'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_health
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_health.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_health.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_health'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_health
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_health.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_health.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- dead_letter_line_items
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'dead_letter_line_items'
          AND column_name  = 'last_failed_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE dead_letter_line_items
            ALTER COLUMN last_failed_at TYPE TIMESTAMPTZ
            USING last_failed_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] dead_letter_line_items.last_failed_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] dead_letter_line_items.last_failed_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'dead_letter_line_items'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE dead_letter_line_items
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] dead_letter_line_items.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] dead_letter_line_items.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- sync_runs
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_runs'
          AND column_name  = 'started_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_runs
            ALTER COLUMN started_at TYPE TIMESTAMPTZ
            USING started_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_runs.started_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_runs.started_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_runs'
          AND column_name  = 'last_progress_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_runs
            ALTER COLUMN last_progress_at TYPE TIMESTAMPTZ
            USING last_progress_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_runs.last_progress_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_runs.last_progress_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_runs'
          AND column_name  = 'completed_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_runs
            ALTER COLUMN completed_at TYPE TIMESTAMPTZ
            USING completed_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_runs.completed_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_runs.completed_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_runs'
          AND column_name  = 'last_heartbeat_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_runs
            ALTER COLUMN last_heartbeat_at TYPE TIMESTAMPTZ
            USING last_heartbeat_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_runs.last_heartbeat_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_runs.last_heartbeat_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_runs'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_runs
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_runs.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_runs.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_runs'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_runs
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_runs.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_runs.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- brand_api_credentials
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'brand_api_credentials'
          AND column_name  = 'last_sync_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE brand_api_credentials
            ALTER COLUMN last_sync_at TYPE TIMESTAMPTZ
            USING last_sync_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] brand_api_credentials.last_sync_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] brand_api_credentials.last_sync_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'brand_api_credentials'
          AND column_name  = 'last_checked_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE brand_api_credentials
            ALTER COLUMN last_checked_at TYPE TIMESTAMPTZ
            USING last_checked_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] brand_api_credentials.last_checked_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] brand_api_credentials.last_checked_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'brand_api_credentials'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE brand_api_credentials
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] brand_api_credentials.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] brand_api_credentials.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'brand_api_credentials'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE brand_api_credentials
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] brand_api_credentials.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] brand_api_credentials.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- sync_requests
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_requests'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_requests
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_requests.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_requests.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_requests'
          AND column_name  = 'started_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_requests
            ALTER COLUMN started_at TYPE TIMESTAMPTZ
            USING started_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_requests.started_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_requests.started_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'sync_requests'
          AND column_name  = 'completed_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE sync_requests
            ALTER COLUMN completed_at TYPE TIMESTAMPTZ
            USING completed_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] sync_requests.completed_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] sync_requests.completed_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- routes  (the dispatch routes table — ORM model: Route)
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'routes'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE routes
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] routes.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] routes.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'routes'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE routes
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] routes.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] routes.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- route_stops
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'route_stops'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE route_stops
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] route_stops.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] route_stops.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'route_stops'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE route_stops
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] route_stops.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] route_stops.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- assistant_sessions
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'assistant_sessions'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE assistant_sessions
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] assistant_sessions.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] assistant_sessions.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'assistant_sessions'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE assistant_sessions
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] assistant_sessions.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] assistant_sessions.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- assistant_messages
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'assistant_messages'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE assistant_messages
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] assistant_messages.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] assistant_messages.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- assistant_pending_actions
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'assistant_pending_actions'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE assistant_pending_actions
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] assistant_pending_actions.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] assistant_pending_actions.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'assistant_pending_actions'
          AND column_name  = 'updated_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE assistant_pending_actions
            ALTER COLUMN updated_at TYPE TIMESTAMPTZ
            USING updated_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] assistant_pending_actions.updated_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] assistant_pending_actions.updated_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- assistant_audit_logs
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'assistant_audit_logs'
          AND column_name  = 'created_at'
          AND data_type    = 'timestamp without time zone'
    ) THEN
        ALTER TABLE assistant_audit_logs
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'UTC';
        RAISE NOTICE '[MIGRATION] assistant_audit_logs.created_at converted to TIMESTAMPTZ';
    ELSE
        RAISE NOTICE '[MIGRATION] assistant_audit_logs.created_at already TIMESTAMPTZ or does not exist — skipping';
    END IF;
END $$;
