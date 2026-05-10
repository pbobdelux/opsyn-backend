-- ============================================================================
-- Migration Helpers: migration_helpers.sql
-- ============================================================================
-- Reusable PL/pgSQL helper functions for safe, idempotent schema migrations.
--
-- Each function guards against the most common migration failure modes:
--   - Operating on a table that doesn't exist yet
--   - Referencing a column that was never added (or whose earlier ADD COLUMN
--     migration failed silently)
--   - Re-creating a constraint or index that already exists
--
-- Usage: source this file (or \i it) before running migrations that need
-- these helpers, or call the functions directly from DO $$ ... $$ blocks.
--
-- All functions emit [MIGRATION] NOTICE lines so their actions are always
-- visible in the PostgreSQL log and in the migration runner output.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- safe_add_column(p_table_name, p_column_name, p_column_type)
--
-- Adds a column to a table only if the column does not already exist.
-- p_column_type is passed verbatim to EXECUTE, so it can include modifiers
-- such as "VARCHAR(36) NOT NULL DEFAULT ''" or "JSONB NOT NULL DEFAULT '{}'".
--
-- Example:
--   SELECT safe_add_column('orders', 'org_id', 'VARCHAR(120)');
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION safe_add_column(
    p_table_name  TEXT,
    p_column_name TEXT,
    p_column_type TEXT
) RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = p_table_name
          AND column_name  = p_column_name
    ) THEN
        EXECUTE format(
            'ALTER TABLE %I ADD COLUMN %I %s',
            p_table_name, p_column_name, p_column_type
        );
        RAISE NOTICE '[MIGRATION] Added column %.%', p_table_name, p_column_name;
    ELSE
        RAISE NOTICE '[MIGRATION] Column %.% already exists — skipping', p_table_name, p_column_name;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------------
-- safe_add_unique_constraint(p_table_name, p_constraint_name, p_column_name)
--
-- Adds a UNIQUE constraint to a single column only after verifying:
--   1. The table exists in the public schema.
--   2. The column exists on that table.
--   3. The constraint does not already exist.
--
-- Example:
--   SELECT safe_add_unique_constraint(
--       'assistant_pending_actions',
--       'assistant_pending_actions_confirmation_id_key',
--       'confirmation_id'
--   );
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION safe_add_unique_constraint(
    p_table_name      TEXT,
    p_constraint_name TEXT,
    p_column_name     TEXT
) RETURNS VOID AS $$
BEGIN
    -- Verify table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name   = p_table_name
    ) THEN
        RAISE NOTICE '[MIGRATION] Table % does not exist — skipping constraint %',
            p_table_name, p_constraint_name;
        RETURN;
    END IF;

    -- Verify column exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = p_table_name
          AND column_name  = p_column_name
    ) THEN
        RAISE NOTICE '[MIGRATION] Column %.% does not exist — skipping constraint %',
            p_table_name, p_column_name, p_constraint_name;
        RETURN;
    END IF;

    -- Verify constraint doesn't already exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema   = 'public'
          AND table_name     = p_table_name
          AND constraint_name = p_constraint_name
    ) THEN
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I UNIQUE (%I)',
            p_table_name, p_constraint_name, p_column_name
        );
        RAISE NOTICE '[MIGRATION] Created constraint %.%', p_table_name, p_constraint_name;
    ELSE
        RAISE NOTICE '[MIGRATION] Constraint %.% already exists — skipping', p_table_name, p_constraint_name;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------------
-- safe_add_check_constraint(p_table_name, p_constraint_name, p_check_expr)
--
-- Adds a CHECK constraint only after verifying the table exists and the
-- constraint does not already exist.  p_check_expr is the raw SQL expression
-- that goes inside CHECK ( ... ).
--
-- Example:
--   SELECT safe_add_check_constraint(
--       'orders',
--       'ck_orders_delivery_status',
--       'delivery_status IN (''pending'',''assigned'',''delivered'')'
--   );
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION safe_add_check_constraint(
    p_table_name      TEXT,
    p_constraint_name TEXT,
    p_check_expr      TEXT
) RETURNS VOID AS $$
BEGIN
    -- Verify table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name   = p_table_name
    ) THEN
        RAISE NOTICE '[MIGRATION] Table % does not exist — skipping constraint %',
            p_table_name, p_constraint_name;
        RETURN;
    END IF;

    -- Verify constraint doesn't already exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema   = 'public'
          AND table_name     = p_table_name
          AND constraint_name = p_constraint_name
    ) THEN
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I CHECK (%s)',
            p_table_name, p_constraint_name, p_check_expr
        );
        RAISE NOTICE '[MIGRATION] Created constraint %.%', p_table_name, p_constraint_name;
    ELSE
        RAISE NOTICE '[MIGRATION] Constraint %.% already exists — skipping', p_table_name, p_constraint_name;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------------
-- safe_create_index(p_index_name, p_table_name, p_column_name)
--
-- Creates a plain (non-unique) index on a single column only after verifying:
--   1. The table exists in the public schema.
--   2. The column exists on that table.
--   3. The index does not already exist.
--
-- For multi-column or partial indexes, use CREATE INDEX IF NOT EXISTS directly
-- (PostgreSQL's native guard is sufficient when the column set is known to
-- exist, which should be verified by the surrounding DO block).
--
-- Example:
--   SELECT safe_create_index(
--       'ix_assistant_pending_actions_org_id',
--       'assistant_pending_actions',
--       'org_id'
--   );
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION safe_create_index(
    p_index_name  TEXT,
    p_table_name  TEXT,
    p_column_name TEXT
) RETURNS VOID AS $$
BEGIN
    -- Verify table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name   = p_table_name
    ) THEN
        RAISE NOTICE '[MIGRATION] Table % does not exist — skipping index %',
            p_table_name, p_index_name;
        RETURN;
    END IF;

    -- Verify column exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = p_table_name
          AND column_name  = p_column_name
    ) THEN
        RAISE NOTICE '[MIGRATION] Column %.% does not exist — skipping index %',
            p_table_name, p_column_name, p_index_name;
        RETURN;
    END IF;

    -- Verify index doesn't already exist
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname  = p_index_name
    ) THEN
        EXECUTE format(
            'CREATE INDEX %I ON %I (%I)',
            p_index_name, p_table_name, p_column_name
        );
        RAISE NOTICE '[MIGRATION] Created index %', p_index_name;
    ELSE
        RAISE NOTICE '[MIGRATION] Index % already exists — skipping', p_index_name;
    END IF;
END;
$$ LANGUAGE plpgsql;
