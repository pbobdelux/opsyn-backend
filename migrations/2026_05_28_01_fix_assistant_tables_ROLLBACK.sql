-- ============================================================================
-- ROLLBACK: 2026_05_28_01_fix_assistant_tables
-- ============================================================================
-- WARNING: This rollback is DESTRUCTIVE. It drops and recreates the assistant
-- tables from scratch using the original SERIAL (INTEGER) PK schema.
--
-- Only safe to run if:
--   1. The forward migration was applied
--   2. The application has NOT written any new rows using UUID PKs
--   3. You have verified that no production data will be lost
--
-- If the application has already written UUID-keyed rows, restore from the
-- RDS snapshot taken before the migration instead of running this script.
-- ============================================================================

BEGIN;

-- Drop all assistant tables (CASCADE removes FK constraints automatically)
DROP TABLE IF EXISTS assistant_audit_logs CASCADE;
DROP TABLE IF EXISTS assistant_pending_actions CASCADE;
DROP TABLE IF EXISTS assistant_messages CASCADE;
DROP TABLE IF EXISTS assistant_sessions CASCADE;

-- Recreate from original 2026_05_04_02 schema

CREATE TABLE assistant_sessions (
    id          SERIAL PRIMARY KEY,
    org_id      VARCHAR(120) NOT NULL,
    user_id     VARCHAR(120),
    session_key VARCHAR(255),
    context     JSONB,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE assistant_sessions
    ADD COLUMN IF NOT EXISTS session_key VARCHAR(255);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'assistant_sessions_session_key_key'
          AND conrelid = 'assistant_sessions'::regclass
    ) THEN
        ALTER TABLE assistant_sessions
            ADD CONSTRAINT assistant_sessions_session_key_key UNIQUE (session_key);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_assistant_sessions_org_id
    ON assistant_sessions (org_id);

CREATE INDEX IF NOT EXISTS ix_assistant_sessions_session_key
    ON assistant_sessions (session_key);

CREATE TABLE assistant_messages (
    id         SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    role       VARCHAR(50) NOT NULL,
    content    TEXT NOT NULL,
    metadata   JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_assistant_messages_session_id
    ON assistant_messages (session_id);

CREATE TABLE assistant_pending_actions (
    id          SERIAL PRIMARY KEY,
    session_id  INTEGER NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    action_type VARCHAR(100) NOT NULL,
    action_data JSONB,
    status      VARCHAR(50) DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_assistant_pending_actions_session_id
    ON assistant_pending_actions (session_id);

CREATE TABLE assistant_audit_logs (
    id         SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES assistant_sessions(id) ON DELETE SET NULL,
    action     VARCHAR(255) NOT NULL,
    details    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_assistant_audit_logs_session_id
    ON assistant_audit_logs (session_id);

COMMIT;
