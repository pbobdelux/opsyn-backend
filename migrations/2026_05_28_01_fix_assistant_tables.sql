-- ============================================================================
-- Migration: 2026_05_28_01_fix_assistant_tables
-- ============================================================================
-- Aligns assistant_* tables with the ORM models in assistant_models.py.
--
-- Root cause:
--   The tables were created by 2026_05_04_02_create_full_schema_aws_rds.sql
--   with SERIAL (INTEGER) primary keys and a minimal column set.
--   The ORM models in models/assistant_models.py define String(36) (UUID-as-
--   string) primary keys and a significantly richer column schema.
--
--   Any ORM query against these tables fails with:
--     "operator does not exist: integer = character varying"
--   Any INSERT/SELECT for missing columns fails with:
--     "UndefinedColumnError: column X does not exist"
--
-- Strategy:
--   1. Add all missing columns to existing tables (non-destructive)
--   2. Add new VARCHAR(36) id column alongside existing INTEGER id
--   3. Populate new id column with gen_random_uuid()::text
--   4. Update all FK columns to reference new UUID values
--   5. Verify data integrity before any destructive step
--   6. Drop old INTEGER id and FK columns, rename UUID columns
--   7. Recreate constraints and indexes
--
-- Safe to re-run: all ADD COLUMN statements use IF NOT EXISTS.
-- Wrapped in a single transaction: rolls back automatically on any error.
-- ============================================================================

BEGIN;

-- ============================================================================
-- STEP 1: Add missing columns to assistant_sessions
-- ============================================================================
-- The original migration created: id, org_id, user_id, session_key, context,
-- is_active, created_at, updated_at.
-- The ORM model expects: id, org_id, user_id, app_context, device_id,
-- created_at, updated_at, metadata_json.

ALTER TABLE assistant_sessions
    ADD COLUMN IF NOT EXISTS app_context   VARCHAR(80),
    ADD COLUMN IF NOT EXISTS device_id     VARCHAR(120),
    ADD COLUMN IF NOT EXISTS metadata_json JSONB;

-- ============================================================================
-- STEP 2: Add missing columns to assistant_messages
-- ============================================================================
-- The original migration created: id, session_id, role, content, metadata,
-- created_at.
-- The ORM model expects: id, session_id, role, content, input_type, created_at.

ALTER TABLE assistant_messages
    ADD COLUMN IF NOT EXISTS input_type VARCHAR(20) NOT NULL DEFAULT 'text';

-- ============================================================================
-- STEP 3: Add missing columns to assistant_pending_actions
-- ============================================================================
-- The original migration created: id, session_id, action_type, action_data,
-- status, created_at, updated_at.
-- The ORM model expects: id, confirmation_id, session_id, org_id, user_id,
-- action_name, payload_json, risk_level, status, created_at, expires_at,
-- executed_at, error_message.

ALTER TABLE assistant_pending_actions
    ADD COLUMN IF NOT EXISTS confirmation_id VARCHAR(36),
    ADD COLUMN IF NOT EXISTS org_id          VARCHAR(120),
    ADD COLUMN IF NOT EXISTS user_id         VARCHAR(120),
    ADD COLUMN IF NOT EXISTS action_name     VARCHAR(120),
    ADD COLUMN IF NOT EXISTS payload_json    JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS risk_level      VARCHAR(20) NOT NULL DEFAULT 'medium',
    ADD COLUMN IF NOT EXISTS expires_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS executed_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS error_message   TEXT;

-- Populate confirmation_id for any existing rows
UPDATE assistant_pending_actions
SET confirmation_id = gen_random_uuid()::text
WHERE confirmation_id IS NULL;

-- Add unique constraint on confirmation_id (only if not already present)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'assistant_pending_actions_confirmation_id_key'
          AND conrelid = 'assistant_pending_actions'::regclass
    ) THEN
        ALTER TABLE assistant_pending_actions
            ADD CONSTRAINT assistant_pending_actions_confirmation_id_key
            UNIQUE (confirmation_id);
    END IF;
END $$;

-- ============================================================================
-- STEP 4: Add missing columns to assistant_audit_logs
-- ============================================================================
-- The original migration created: id, session_id, action, details, created_at.
-- The ORM model expects: id, org_id, user_id, session_id, action_name,
-- risk_level, request_json, result_json, status, error_message, created_at.

ALTER TABLE assistant_audit_logs
    ADD COLUMN IF NOT EXISTS org_id        VARCHAR(120),
    ADD COLUMN IF NOT EXISTS user_id       VARCHAR(120),
    ADD COLUMN IF NOT EXISTS action_name   VARCHAR(120),
    ADD COLUMN IF NOT EXISTS risk_level    VARCHAR(20) NOT NULL DEFAULT 'medium',
    ADD COLUMN IF NOT EXISTS request_json  JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS result_json   JSONB,
    ADD COLUMN IF NOT EXISTS status        VARCHAR(20),
    ADD COLUMN IF NOT EXISTS error_message TEXT;

-- ============================================================================
-- STEP 5: Add new VARCHAR(36) id columns alongside existing INTEGER id columns
-- ============================================================================

ALTER TABLE assistant_sessions
    ADD COLUMN IF NOT EXISTS id_uuid VARCHAR(36) DEFAULT gen_random_uuid()::text;

ALTER TABLE assistant_messages
    ADD COLUMN IF NOT EXISTS id_uuid VARCHAR(36) DEFAULT gen_random_uuid()::text;

ALTER TABLE assistant_pending_actions
    ADD COLUMN IF NOT EXISTS id_uuid VARCHAR(36) DEFAULT gen_random_uuid()::text;

ALTER TABLE assistant_audit_logs
    ADD COLUMN IF NOT EXISTS id_uuid VARCHAR(36) DEFAULT gen_random_uuid()::text;

-- ============================================================================
-- STEP 6: Populate UUID id columns for all existing rows
-- ============================================================================

UPDATE assistant_sessions
SET id_uuid = gen_random_uuid()::text
WHERE id_uuid IS NULL;

UPDATE assistant_messages
SET id_uuid = gen_random_uuid()::text
WHERE id_uuid IS NULL;

UPDATE assistant_pending_actions
SET id_uuid = gen_random_uuid()::text
WHERE id_uuid IS NULL;

UPDATE assistant_audit_logs
SET id_uuid = gen_random_uuid()::text
WHERE id_uuid IS NULL;

-- ============================================================================
-- STEP 7: Add new VARCHAR(36) FK columns for session_id references
-- ============================================================================

ALTER TABLE assistant_messages
    ADD COLUMN IF NOT EXISTS session_id_uuid VARCHAR(36);

ALTER TABLE assistant_pending_actions
    ADD COLUMN IF NOT EXISTS session_id_uuid VARCHAR(36);

ALTER TABLE assistant_audit_logs
    ADD COLUMN IF NOT EXISTS session_id_uuid VARCHAR(36);

-- ============================================================================
-- STEP 8: Populate new FK columns by joining on old INTEGER id
-- ============================================================================

UPDATE assistant_messages m
SET session_id_uuid = s.id_uuid
FROM assistant_sessions s
WHERE s.id = m.session_id;

UPDATE assistant_pending_actions p
SET session_id_uuid = s.id_uuid
FROM assistant_sessions s
WHERE s.id = p.session_id;

UPDATE assistant_audit_logs a
SET session_id_uuid = s.id_uuid
FROM assistant_sessions s
WHERE s.id = a.session_id;

-- ============================================================================
-- STEP 9: Verify data integrity before any destructive operations
-- ============================================================================

DO $$
DECLARE
    null_count INTEGER;
BEGIN
    -- Verify all sessions have a UUID id
    SELECT COUNT(*) INTO null_count
    FROM assistant_sessions WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] assistant_sessions has % rows with NULL id_uuid — aborting',
            null_count;
    END IF;

    -- Verify all messages have a UUID id
    SELECT COUNT(*) INTO null_count
    FROM assistant_messages WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] assistant_messages has % rows with NULL id_uuid — aborting',
            null_count;
    END IF;

    -- Verify all messages with a session_id have a resolved session_id_uuid
    SELECT COUNT(*) INTO null_count
    FROM assistant_messages
    WHERE session_id IS NOT NULL AND session_id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] assistant_messages has % rows with unresolved session_id_uuid — aborting',
            null_count;
    END IF;

    -- Verify all pending_actions have a UUID id
    SELECT COUNT(*) INTO null_count
    FROM assistant_pending_actions WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] assistant_pending_actions has % rows with NULL id_uuid — aborting',
            null_count;
    END IF;

    -- Verify all pending_actions with a session_id have a resolved session_id_uuid
    SELECT COUNT(*) INTO null_count
    FROM assistant_pending_actions
    WHERE session_id IS NOT NULL AND session_id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] assistant_pending_actions has % rows with unresolved session_id_uuid — aborting',
            null_count;
    END IF;

    -- Verify all audit_logs have a UUID id
    SELECT COUNT(*) INTO null_count
    FROM assistant_audit_logs WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION
            '[INTEGRITY_FAIL] assistant_audit_logs has % rows with NULL id_uuid — aborting',
            null_count;
    END IF;

    RAISE NOTICE '[INTEGRITY_OK] All UUID columns populated — proceeding with column swap';
END $$;

-- ============================================================================
-- STEP 10: Swap assistant_sessions PK from INTEGER to VARCHAR(36)
-- ============================================================================

-- Drop the primary key constraint (CASCADE drops dependent FK constraints)
ALTER TABLE assistant_sessions DROP CONSTRAINT IF EXISTS assistant_sessions_pkey CASCADE;

-- Drop the old INTEGER id column
ALTER TABLE assistant_sessions DROP COLUMN IF EXISTS id;

-- Rename the UUID column to id
ALTER TABLE assistant_sessions RENAME COLUMN id_uuid TO id;

-- Recreate primary key
ALTER TABLE assistant_sessions ADD PRIMARY KEY (id);

-- ============================================================================
-- STEP 11: Swap assistant_messages PK and FK
-- ============================================================================

ALTER TABLE assistant_messages DROP CONSTRAINT IF EXISTS assistant_messages_pkey CASCADE;
ALTER TABLE assistant_messages DROP COLUMN IF EXISTS id;
ALTER TABLE assistant_messages DROP COLUMN IF EXISTS session_id;
ALTER TABLE assistant_messages RENAME COLUMN id_uuid TO id;
ALTER TABLE assistant_messages RENAME COLUMN session_id_uuid TO session_id;
ALTER TABLE assistant_messages ADD PRIMARY KEY (id);
ALTER TABLE assistant_messages ALTER COLUMN session_id SET NOT NULL;
ALTER TABLE assistant_messages
    ADD CONSTRAINT fk_assistant_messages_session
    FOREIGN KEY (session_id) REFERENCES assistant_sessions(id) ON DELETE CASCADE;

-- ============================================================================
-- STEP 12: Swap assistant_pending_actions PK and FK
-- ============================================================================

ALTER TABLE assistant_pending_actions DROP CONSTRAINT IF EXISTS assistant_pending_actions_pkey CASCADE;
ALTER TABLE assistant_pending_actions DROP COLUMN IF EXISTS id;
ALTER TABLE assistant_pending_actions DROP COLUMN IF EXISTS session_id;
ALTER TABLE assistant_pending_actions RENAME COLUMN id_uuid TO id;
ALTER TABLE assistant_pending_actions RENAME COLUMN session_id_uuid TO session_id;
ALTER TABLE assistant_pending_actions ADD PRIMARY KEY (id);
ALTER TABLE assistant_pending_actions ALTER COLUMN session_id SET NOT NULL;
ALTER TABLE assistant_pending_actions
    ADD CONSTRAINT fk_assistant_pending_actions_session
    FOREIGN KEY (session_id) REFERENCES assistant_sessions(id) ON DELETE CASCADE;

-- ============================================================================
-- STEP 13: Swap assistant_audit_logs PK and FK
-- ============================================================================

ALTER TABLE assistant_audit_logs DROP CONSTRAINT IF EXISTS assistant_audit_logs_pkey CASCADE;
ALTER TABLE assistant_audit_logs DROP COLUMN IF EXISTS id;
ALTER TABLE assistant_audit_logs DROP COLUMN IF EXISTS session_id;
ALTER TABLE assistant_audit_logs RENAME COLUMN id_uuid TO id;
ALTER TABLE assistant_audit_logs RENAME COLUMN session_id_uuid TO session_id;
ALTER TABLE assistant_audit_logs ADD PRIMARY KEY (id);
-- session_id is nullable in audit_logs (ON DELETE SET NULL)
ALTER TABLE assistant_audit_logs
    ADD CONSTRAINT fk_assistant_audit_logs_session
    FOREIGN KEY (session_id) REFERENCES assistant_sessions(id) ON DELETE SET NULL;

-- ============================================================================
-- STEP 14: Recreate indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_assistant_sessions_org_id
    ON assistant_sessions (org_id);

CREATE INDEX IF NOT EXISTS ix_assistant_sessions_session_key
    ON assistant_sessions (session_key);

CREATE INDEX IF NOT EXISTS ix_assistant_messages_session_id
    ON assistant_messages (session_id);

CREATE INDEX IF NOT EXISTS ix_assistant_pending_actions_session_id
    ON assistant_pending_actions (session_id);

CREATE INDEX IF NOT EXISTS ix_assistant_pending_actions_org_id
    ON assistant_pending_actions (org_id);

CREATE UNIQUE INDEX IF NOT EXISTS ix_assistant_pending_actions_confirmation_id
    ON assistant_pending_actions (confirmation_id)
    WHERE confirmation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_assistant_audit_logs_session_id
    ON assistant_audit_logs (session_id);

CREATE INDEX IF NOT EXISTS ix_assistant_audit_logs_org_id
    ON assistant_audit_logs (org_id);

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

COMMIT;

-- Post-migration verification (run manually to confirm success):
--
-- SELECT table_name, column_name, data_type, udt_name
-- FROM information_schema.columns
-- WHERE table_schema = 'public'
--   AND table_name IN (
--       'assistant_sessions', 'assistant_messages',
--       'assistant_pending_actions', 'assistant_audit_logs'
--   )
--   AND column_name = 'id'
-- ORDER BY table_name;
-- Expected: data_type = 'character varying', udt_name = 'varchar' for all rows
--
-- SELECT COUNT(*) FROM assistant_sessions WHERE id IS NULL;  -- must be 0
-- SELECT COUNT(*) FROM assistant_messages WHERE id IS NULL;  -- must be 0
-- SELECT id, LENGTH(id) FROM assistant_sessions LIMIT 3;    -- must be 36 chars
