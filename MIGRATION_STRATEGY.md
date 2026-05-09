# Migration Strategy — Schema Standardization to UUID Primary Keys

**Date:** 2026-05-28  
**Service:** opsyn-backend (FastAPI + SQLAlchemy + asyncpg)  
**Database:** AWS RDS PostgreSQL  
**Goal:** Standardize all primary keys to UUID, resolve all model/DB mismatches, preserve all production data, minimize downtime

---

## Overview

This document describes the phased migration strategy to resolve the schema inconsistencies documented in `SCHEMA_AUDIT.md`. The strategy is organized into four phases, each delivered as a separate PR. Phases are ordered by urgency: Phase 1 fixes active failures first, then progressively migrates legacy INTEGER tables to UUID.

**Guiding principles:**
- Every migration is idempotent (safe to re-run)
- No destructive operations without a verified backup
- All column renames use the add-populate-verify-drop pattern
- FK constraints are updated before old columns are dropped
- Downtime is bounded to index creation on large tables (use `CREATE INDEX CONCURRENTLY`)

---

## Phase 1: Fix Active Failures — Assistant Tables (Immediate)

**Priority:** 🔴 P0 — These tables are broken today.  
**Downtime:** None (additive changes only until final rename)  
**Risk:** Low — assistant tables are new and likely have minimal production data

### Problem

The `assistant_sessions`, `assistant_messages`, `assistant_pending_actions`, and `assistant_audit_logs` tables were created by `2026_05_04_02_create_full_schema_aws_rds.sql` with `SERIAL` (INTEGER) primary keys and a minimal column set. The ORM models in `models/assistant_models.py` define `String(36)` (UUID-as-string) primary keys and a significantly richer column schema. Any ORM query against these tables will fail with `operator does not exist: integer = character varying`.

Additionally, the ORM models define columns that do not exist in the database at all (`app_context`, `device_id`, `metadata_json`, `input_type`, `confirmation_id`, `action_name`, `payload_json`, `risk_level`, `expires_at`, `executed_at`, `error_message`, `org_id`, `user_id`, `request_json`, `result_json`, `status`). Any INSERT or SELECT that touches these columns will fail with `UndefinedColumnError`.

### Decision: Migrate DB to Match ORM

The ORM models represent the intended design. The database schema is the legacy artifact. We migrate the database to match the models rather than rolling back the models.

### Migration Script: `migrations/2026_05_28_01_fix_assistant_tables.sql`

```sql
-- ============================================================================
-- Migration: 2026_05_28_01_fix_assistant_tables
-- Aligns assistant_* tables with the ORM models in assistant_models.py.
--
-- Problem: DB has SERIAL (INTEGER) PKs; ORM expects String(36) (UUID-as-string).
--          DB is missing ~20 columns that the ORM defines.
--
-- Strategy:
--   1. Add all missing columns to existing tables (non-destructive)
--   2. Add new UUID id column alongside existing INTEGER id
--   3. Populate UUID id column with gen_random_uuid()::text
--   4. Update all FK columns to reference new UUID values
--   5. Verify data integrity
--   6. Drop old INTEGER id and FK columns, rename UUID columns
--   7. Recreate constraints and indexes
--
-- Safe to re-run: all statements use IF NOT EXISTS / DO $$ guards.
-- ============================================================================

BEGIN;

-- ============================================================================
-- STEP 1: Add missing columns to assistant_sessions
-- ============================================================================

ALTER TABLE assistant_sessions
    ADD COLUMN IF NOT EXISTS app_context    VARCHAR(80),
    ADD COLUMN IF NOT EXISTS device_id      VARCHAR(120),
    ADD COLUMN IF NOT EXISTS metadata_json  JSONB;

-- Remove columns that exist in DB but not in ORM (safe to keep, just unused)
-- session_key: exists in DB migration but not in ORM model — keep for now
-- context: exists in DB migration but not in ORM model — keep for now
-- is_active: exists in DB migration but not in ORM model — keep for now

-- ============================================================================
-- STEP 2: Add missing columns to assistant_messages
-- ============================================================================

ALTER TABLE assistant_messages
    ADD COLUMN IF NOT EXISTS input_type VARCHAR(20) NOT NULL DEFAULT 'text';

-- ============================================================================
-- STEP 3: Add missing columns to assistant_pending_actions
-- ============================================================================

ALTER TABLE assistant_pending_actions
    ADD COLUMN IF NOT EXISTS confirmation_id VARCHAR(36) UNIQUE,
    ADD COLUMN IF NOT EXISTS org_id          VARCHAR(120),
    ADD COLUMN IF NOT EXISTS user_id         VARCHAR(120),
    ADD COLUMN IF NOT EXISTS action_name     VARCHAR(120),
    ADD COLUMN IF NOT EXISTS payload_json    JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS risk_level      VARCHAR(20) NOT NULL DEFAULT 'medium',
    ADD COLUMN IF NOT EXISTS expires_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS executed_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS error_message   TEXT;

-- Populate confirmation_id for existing rows
UPDATE assistant_pending_actions
SET confirmation_id = gen_random_uuid()::text
WHERE confirmation_id IS NULL;

-- ============================================================================
-- STEP 4: Add missing columns to assistant_audit_logs
-- ============================================================================

ALTER TABLE assistant_audit_logs
    ADD COLUMN IF NOT EXISTS org_id       VARCHAR(120),
    ADD COLUMN IF NOT EXISTS user_id      VARCHAR(120),
    ADD COLUMN IF NOT EXISTS action_name  VARCHAR(120),
    ADD COLUMN IF NOT EXISTS risk_level   VARCHAR(20) NOT NULL DEFAULT 'medium',
    ADD COLUMN IF NOT EXISTS request_json JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS result_json  JSONB,
    ADD COLUMN IF NOT EXISTS status       VARCHAR(20),
    ADD COLUMN IF NOT EXISTS error_message TEXT;

-- ============================================================================
-- STEP 5: Add new UUID id columns alongside existing INTEGER id columns
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
-- STEP 7: Add new UUID FK columns for session_id references
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
-- STEP 9: Verify data integrity before dropping old columns
-- ============================================================================

DO $$
DECLARE
    null_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO null_count FROM assistant_sessions WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION 'assistant_sessions has % rows with NULL id_uuid', null_count;
    END IF;

    SELECT COUNT(*) INTO null_count FROM assistant_messages WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION 'assistant_messages has % rows with NULL id_uuid', null_count;
    END IF;

    SELECT COUNT(*) INTO null_count
    FROM assistant_messages
    WHERE session_id IS NOT NULL AND session_id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION 'assistant_messages has % rows with unresolved session_id_uuid', null_count;
    END IF;

    SELECT COUNT(*) INTO null_count FROM assistant_pending_actions WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION 'assistant_pending_actions has % rows with NULL id_uuid', null_count;
    END IF;

    SELECT COUNT(*) INTO null_count FROM assistant_audit_logs WHERE id_uuid IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION 'assistant_audit_logs has % rows with NULL id_uuid', null_count;
    END IF;

    RAISE NOTICE 'Data integrity check passed — all UUID columns populated';
END $$;

-- ============================================================================
-- STEP 10: Drop old INTEGER PKs and FK columns, rename UUID columns
-- ============================================================================

-- assistant_sessions: drop old PK, rename id_uuid to id
ALTER TABLE assistant_sessions DROP CONSTRAINT IF EXISTS assistant_sessions_pkey CASCADE;
ALTER TABLE assistant_sessions DROP COLUMN IF EXISTS id;
ALTER TABLE assistant_sessions RENAME COLUMN id_uuid TO id;
ALTER TABLE assistant_sessions ADD PRIMARY KEY (id);

-- assistant_messages: drop old PK and FK, rename UUID columns
ALTER TABLE assistant_messages DROP CONSTRAINT IF EXISTS assistant_messages_pkey CASCADE;
ALTER TABLE assistant_messages DROP CONSTRAINT IF EXISTS assistant_messages_session_id_fkey CASCADE;
ALTER TABLE assistant_messages DROP COLUMN IF EXISTS id;
ALTER TABLE assistant_messages DROP COLUMN IF EXISTS session_id;
ALTER TABLE assistant_messages RENAME COLUMN id_uuid TO id;
ALTER TABLE assistant_messages RENAME COLUMN session_id_uuid TO session_id;
ALTER TABLE assistant_messages ADD PRIMARY KEY (id);
ALTER TABLE assistant_messages ALTER COLUMN session_id SET NOT NULL;
ALTER TABLE assistant_messages ADD CONSTRAINT fk_assistant_messages_session
    FOREIGN KEY (session_id) REFERENCES assistant_sessions(id) ON DELETE CASCADE;

-- assistant_pending_actions: drop old PK and FK, rename UUID columns
ALTER TABLE assistant_pending_actions DROP CONSTRAINT IF EXISTS assistant_pending_actions_pkey CASCADE;
ALTER TABLE assistant_pending_actions DROP CONSTRAINT IF EXISTS assistant_pending_actions_session_id_fkey CASCADE;
ALTER TABLE assistant_pending_actions DROP COLUMN IF EXISTS id;
ALTER TABLE assistant_pending_actions DROP COLUMN IF EXISTS session_id;
ALTER TABLE assistant_pending_actions RENAME COLUMN id_uuid TO id;
ALTER TABLE assistant_pending_actions RENAME COLUMN session_id_uuid TO session_id;
ALTER TABLE assistant_pending_actions ADD PRIMARY KEY (id);
ALTER TABLE assistant_pending_actions ALTER COLUMN session_id SET NOT NULL;
ALTER TABLE assistant_pending_actions ADD CONSTRAINT fk_assistant_pending_actions_session
    FOREIGN KEY (session_id) REFERENCES assistant_sessions(id) ON DELETE CASCADE;

-- assistant_audit_logs: drop old PK and FK, rename UUID columns
ALTER TABLE assistant_audit_logs DROP CONSTRAINT IF EXISTS assistant_audit_logs_pkey CASCADE;
ALTER TABLE assistant_audit_logs DROP CONSTRAINT IF EXISTS assistant_audit_logs_session_id_fkey CASCADE;
ALTER TABLE assistant_audit_logs DROP COLUMN IF EXISTS id;
ALTER TABLE assistant_audit_logs DROP COLUMN IF EXISTS session_id;
ALTER TABLE assistant_audit_logs RENAME COLUMN id_uuid TO id;
ALTER TABLE assistant_audit_logs RENAME COLUMN session_id_uuid TO session_id;
ALTER TABLE assistant_audit_logs ADD PRIMARY KEY (id);
ALTER TABLE assistant_audit_logs ADD CONSTRAINT fk_assistant_audit_logs_session
    FOREIGN KEY (session_id) REFERENCES assistant_sessions(id) ON DELETE SET NULL;

-- ============================================================================
-- STEP 11: Recreate indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_assistant_sessions_org_id
    ON assistant_sessions (org_id);

CREATE INDEX IF NOT EXISTS ix_assistant_messages_session_id
    ON assistant_messages (session_id);

CREATE INDEX IF NOT EXISTS ix_assistant_pending_actions_session_id
    ON assistant_pending_actions (session_id);

CREATE INDEX IF NOT EXISTS ix_assistant_pending_actions_org_id
    ON assistant_pending_actions (org_id);

CREATE UNIQUE INDEX IF NOT EXISTS ix_assistant_pending_actions_confirmation_id
    ON assistant_pending_actions (confirmation_id);

CREATE INDEX IF NOT EXISTS ix_assistant_audit_logs_session_id
    ON assistant_audit_logs (session_id);

CREATE INDEX IF NOT EXISTS ix_assistant_audit_logs_org_id
    ON assistant_audit_logs (org_id);

COMMIT;
```

### Rollback Script

```sql
-- ROLLBACK: 2026_05_28_01_fix_assistant_tables
-- Only safe to run if the migration was applied but the application has not
-- written any new rows using UUID PKs yet.
-- WARNING: This will destroy any data written after the migration.

BEGIN;

-- Drop the migrated tables entirely and recreate from original schema
DROP TABLE IF EXISTS assistant_audit_logs CASCADE;
DROP TABLE IF EXISTS assistant_pending_actions CASCADE;
DROP TABLE IF EXISTS assistant_messages CASCADE;
DROP TABLE IF EXISTS assistant_sessions CASCADE;

CREATE TABLE assistant_sessions (
    id SERIAL PRIMARY KEY,
    org_id VARCHAR(120) NOT NULL,
    user_id VARCHAR(120),
    session_key VARCHAR(255) UNIQUE,
    context JSONB,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE assistant_messages (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE assistant_pending_actions (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    action_type VARCHAR(100) NOT NULL,
    action_data JSONB,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE assistant_audit_logs (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES assistant_sessions(id) ON DELETE SET NULL,
    action VARCHAR(255) NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
```

---

## Phase 2: Migrate Legacy INTEGER Tables to UUID

**Priority:** 🟡 P2 — No active failures, but required for long-term consistency  
**Downtime:** Minimal — index creation uses CONCURRENTLY; table locks are brief  
**Risk:** Medium — these tables have production data; backup required before running

### Tables to Migrate (in dependency order)

The migration order must respect FK dependencies. Tables with no FK dependencies go first; tables that reference them go after.

**Dependency graph:**
```
sync_runs ← orders ← order_lines
                    ← dead_letter_line_items
brand_api_credentials (no FKs)
tenant_credentials (no FKs)
sync_requests (no FKs)
sync_health (no FKs)
sync_dead_letters (no FKs)
sync_metrics_snapshots (no FKs)
organization_brand_bindings (no FKs)
```

### Migration Order

1. `sync_runs` (referenced by `orders.sync_run_id`)
2. `brand_api_credentials` (no dependents)
3. `tenant_credentials` (no dependents)
4. `sync_requests` (no dependents)
5. `sync_health` (no dependents)
6. `sync_dead_letters` (no dependents)
7. `sync_metrics_snapshots` (no dependents)
8. `organization_brand_bindings` (no dependents)
9. `orders` (references `sync_runs`; referenced by `order_lines`, `dead_letter_line_items`)
10. `order_lines` (references `orders`)
11. `dead_letter_line_items` (references `orders`)

### Generic Migration Pattern

For each table, the pattern is:

```sql
-- Phase A: Add UUID column (non-blocking, no lock)
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

-- Phase B: Backfill existing rows (UPDATE in batches for large tables)
UPDATE {table} SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;

-- Phase C: Create index on new UUID column (CONCURRENTLY — no table lock)
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_{table}_id_uuid ON {table}(id_uuid);

-- Phase D: Update referencing tables (add new FK column, populate, verify)
-- (see FK update pattern below)

-- Phase E: Verify integrity
SELECT COUNT(*) FROM {table} WHERE id_uuid IS NULL;  -- must be 0

-- Phase F: Swap columns (requires brief table lock)
BEGIN;
ALTER TABLE {table} DROP CONSTRAINT {table}_pkey CASCADE;
ALTER TABLE {table} DROP COLUMN id;
ALTER TABLE {table} RENAME COLUMN id_uuid TO id;
ALTER TABLE {table} ADD PRIMARY KEY (id);
ALTER TABLE {table} ALTER COLUMN id SET DEFAULT gen_random_uuid();
COMMIT;
```

### FK Update Pattern

For each table that has a FK to a table being migrated:

```sql
-- 1. Add new UUID FK column
ALTER TABLE {referencing_table}
    ADD COLUMN IF NOT EXISTS {fk_column}_uuid UUID;

-- 2. Populate from old INTEGER FK via join
UPDATE {referencing_table} r
SET {fk_column}_uuid = p.id_uuid
FROM {referenced_table} p
WHERE p.id = r.{fk_column};

-- 3. Verify no NULLs (for NOT NULL FKs)
SELECT COUNT(*) FROM {referencing_table}
WHERE {fk_column} IS NOT NULL AND {fk_column}_uuid IS NULL;
-- Must be 0

-- 4. Add FK constraint on new column
ALTER TABLE {referencing_table}
    ADD CONSTRAINT fk_{referencing_table}_{fk_column}
    FOREIGN KEY ({fk_column}_uuid) REFERENCES {referenced_table}(id_uuid);

-- 5. Drop old FK column and rename new column
ALTER TABLE {referencing_table} DROP COLUMN {fk_column};
ALTER TABLE {referencing_table} RENAME COLUMN {fk_column}_uuid TO {fk_column};
```

### Migration Script: `migrations/2026_05_28_02_migrate_sync_runs_to_uuid.sql`

```sql
-- ============================================================================
-- Migration: 2026_05_28_02_migrate_sync_runs_to_uuid
-- Migrates sync_runs.id from SERIAL (INTEGER) to UUID.
-- sync_runs is migrated first because orders.sync_run_id references it.
-- ============================================================================

BEGIN;

-- Step 1: Add UUID column
ALTER TABLE sync_runs ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

-- Step 2: Backfill
UPDATE sync_runs SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;

-- Step 3: Verify
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM sync_runs WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'sync_runs has rows with NULL id_uuid';
    END IF;
END $$;

-- Step 4: Add UUID FK column to orders
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_run_id_uuid UUID;

-- Step 5: Populate orders.sync_run_id_uuid
UPDATE orders o
SET sync_run_id_uuid = r.id_uuid
FROM sync_runs r
WHERE r.id = o.sync_run_id;

-- Step 6: Swap sync_runs PK
ALTER TABLE sync_runs DROP CONSTRAINT sync_runs_pkey CASCADE;
ALTER TABLE sync_runs DROP COLUMN id;
ALTER TABLE sync_runs RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_runs ADD PRIMARY KEY (id);
ALTER TABLE sync_runs ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Step 7: Swap orders.sync_run_id FK
ALTER TABLE orders DROP COLUMN sync_run_id;
ALTER TABLE orders RENAME COLUMN sync_run_id_uuid TO sync_run_id;
ALTER TABLE orders ADD CONSTRAINT fk_orders_sync_run_id
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id);

-- Step 8: Recreate indexes
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_status ON sync_runs(brand_id, status);
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_started ON sync_runs(brand_id, started_at);
CREATE INDEX IF NOT EXISTS ix_orders_sync_run_id ON orders(sync_run_id) WHERE sync_run_id IS NOT NULL;

COMMIT;
```

### Migration Script: `migrations/2026_05_28_03_migrate_orders_to_uuid.sql`

```sql
-- ============================================================================
-- Migration: 2026_05_28_03_migrate_orders_to_uuid
-- Migrates orders.id from SERIAL/BIGSERIAL (INTEGER) to UUID.
-- Must run AFTER 2026_05_28_02 (sync_runs already UUID).
-- ============================================================================

BEGIN;

-- Step 1: Add UUID column to orders
ALTER TABLE orders ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();

-- Step 2: Backfill
UPDATE orders SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;

-- Step 3: Add UUID FK columns to referencing tables
ALTER TABLE order_lines ADD COLUMN IF NOT EXISTS order_id_uuid UUID;
ALTER TABLE dead_letter_line_items ADD COLUMN IF NOT EXISTS order_id_uuid UUID;

-- Step 4: Populate FK columns
UPDATE order_lines ol
SET order_id_uuid = o.id_uuid
FROM orders o
WHERE o.id = ol.order_id;

UPDATE dead_letter_line_items dl
SET order_id_uuid = o.id_uuid
FROM orders o
WHERE o.id = dl.order_id;

-- Step 5: Verify
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM orders WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'orders has rows with NULL id_uuid';
    END IF;
    IF (SELECT COUNT(*) FROM order_lines WHERE order_id IS NOT NULL AND order_id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'order_lines has rows with unresolved order_id_uuid';
    END IF;
END $$;

-- Step 6: Swap orders PK
ALTER TABLE orders DROP CONSTRAINT orders_pkey CASCADE;
ALTER TABLE orders DROP COLUMN id;
ALTER TABLE orders RENAME COLUMN id_uuid TO id;
ALTER TABLE orders ADD PRIMARY KEY (id);
ALTER TABLE orders ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Step 7: Swap order_lines FK
ALTER TABLE order_lines DROP CONSTRAINT order_lines_order_id_fkey CASCADE;
ALTER TABLE order_lines DROP COLUMN order_id;
ALTER TABLE order_lines RENAME COLUMN order_id_uuid TO order_id;
ALTER TABLE order_lines ALTER COLUMN order_id SET NOT NULL;
ALTER TABLE order_lines ADD CONSTRAINT fk_order_lines_order_id
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE;

-- Step 8: Swap dead_letter_line_items FK
ALTER TABLE dead_letter_line_items DROP CONSTRAINT IF EXISTS dead_letter_line_items_order_id_fkey CASCADE;
ALTER TABLE dead_letter_line_items DROP COLUMN order_id;
ALTER TABLE dead_letter_line_items RENAME COLUMN order_id_uuid TO order_id;
ALTER TABLE dead_letter_line_items ADD CONSTRAINT fk_dead_letter_order_id
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL;

-- Step 9: Recreate indexes
CREATE INDEX IF NOT EXISTS ix_orders_brand_id ON orders(brand_id);
CREATE INDEX IF NOT EXISTS ix_orders_order_number ON orders(order_number);
CREATE INDEX IF NOT EXISTS ix_orders_external_order_id ON orders(external_order_id);
CREATE INDEX IF NOT EXISTS ix_order_lines_order_id ON order_lines(order_id);

COMMIT;
```

### Migration Script: `migrations/2026_05_28_04_migrate_remaining_integer_tables.sql`

```sql
-- ============================================================================
-- Migration: 2026_05_28_04_migrate_remaining_integer_tables
-- Migrates all remaining INTEGER-PK tables to UUID.
-- Tables: brand_api_credentials, tenant_credentials, sync_requests,
--         sync_health, sync_dead_letters, sync_metrics_snapshots,
--         organization_brand_bindings, order_lines, dead_letter_line_items
--
-- order_lines and dead_letter_line_items are included here (their own PKs,
-- not their FKs which were handled in migration 03).
-- ============================================================================

BEGIN;

-- ============================================================================
-- brand_api_credentials
-- ============================================================================
ALTER TABLE brand_api_credentials ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE brand_api_credentials SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM brand_api_credentials WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'brand_api_credentials has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE brand_api_credentials DROP CONSTRAINT brand_api_credentials_pkey CASCADE;
ALTER TABLE brand_api_credentials DROP COLUMN id;
ALTER TABLE brand_api_credentials RENAME COLUMN id_uuid TO id;
ALTER TABLE brand_api_credentials ADD PRIMARY KEY (id);
ALTER TABLE brand_api_credentials ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- tenant_credentials
-- ============================================================================
ALTER TABLE tenant_credentials ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE tenant_credentials SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM tenant_credentials WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'tenant_credentials has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE tenant_credentials DROP CONSTRAINT tenant_credentials_pkey CASCADE;
ALTER TABLE tenant_credentials DROP COLUMN id;
ALTER TABLE tenant_credentials RENAME COLUMN id_uuid TO id;
ALTER TABLE tenant_credentials ADD PRIMARY KEY (id);
ALTER TABLE tenant_credentials ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- sync_requests
-- ============================================================================
ALTER TABLE sync_requests ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE sync_requests SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM sync_requests WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'sync_requests has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE sync_requests DROP CONSTRAINT sync_requests_pkey CASCADE;
ALTER TABLE sync_requests DROP COLUMN id;
ALTER TABLE sync_requests RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_requests ADD PRIMARY KEY (id);
ALTER TABLE sync_requests ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- sync_health
-- ============================================================================
ALTER TABLE sync_health ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE sync_health SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM sync_health WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'sync_health has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE sync_health DROP CONSTRAINT sync_health_pkey CASCADE;
ALTER TABLE sync_health DROP COLUMN id;
ALTER TABLE sync_health RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_health ADD PRIMARY KEY (id);
ALTER TABLE sync_health ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- sync_dead_letters
-- ============================================================================
ALTER TABLE sync_dead_letters ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE sync_dead_letters SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM sync_dead_letters WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'sync_dead_letters has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE sync_dead_letters DROP CONSTRAINT sync_dead_letters_pkey CASCADE;
ALTER TABLE sync_dead_letters DROP COLUMN id;
ALTER TABLE sync_dead_letters RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_dead_letters ADD PRIMARY KEY (id);
ALTER TABLE sync_dead_letters ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- sync_metrics_snapshots
-- ============================================================================
ALTER TABLE sync_metrics_snapshots ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE sync_metrics_snapshots SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM sync_metrics_snapshots WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'sync_metrics_snapshots has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE sync_metrics_snapshots DROP CONSTRAINT sync_metrics_snapshots_pkey CASCADE;
ALTER TABLE sync_metrics_snapshots DROP COLUMN id;
ALTER TABLE sync_metrics_snapshots RENAME COLUMN id_uuid TO id;
ALTER TABLE sync_metrics_snapshots ADD PRIMARY KEY (id);
ALTER TABLE sync_metrics_snapshots ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- organization_brand_bindings
-- ============================================================================
ALTER TABLE organization_brand_bindings ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE organization_brand_bindings SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM organization_brand_bindings WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'organization_brand_bindings has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE organization_brand_bindings DROP CONSTRAINT organization_brand_bindings_pkey CASCADE;
ALTER TABLE organization_brand_bindings DROP COLUMN id;
ALTER TABLE organization_brand_bindings RENAME COLUMN id_uuid TO id;
ALTER TABLE organization_brand_bindings ADD PRIMARY KEY (id);
ALTER TABLE organization_brand_bindings ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- order_lines (own PK only — FK to orders already handled in migration 03)
-- ============================================================================
ALTER TABLE order_lines ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE order_lines SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM order_lines WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'order_lines has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE order_lines DROP CONSTRAINT order_lines_pkey CASCADE;
ALTER TABLE order_lines DROP COLUMN id;
ALTER TABLE order_lines RENAME COLUMN id_uuid TO id;
ALTER TABLE order_lines ADD PRIMARY KEY (id);
ALTER TABLE order_lines ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ============================================================================
-- dead_letter_line_items (own PK only — FK to orders already handled in migration 03)
-- ============================================================================
ALTER TABLE dead_letter_line_items ADD COLUMN IF NOT EXISTS id_uuid UUID DEFAULT gen_random_uuid();
UPDATE dead_letter_line_items SET id_uuid = gen_random_uuid() WHERE id_uuid IS NULL;
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM dead_letter_line_items WHERE id_uuid IS NULL) > 0 THEN
        RAISE EXCEPTION 'dead_letter_line_items has NULL id_uuid rows';
    END IF;
END $$;
ALTER TABLE dead_letter_line_items DROP CONSTRAINT dead_letter_line_items_pkey CASCADE;
ALTER TABLE dead_letter_line_items DROP COLUMN id;
ALTER TABLE dead_letter_line_items RENAME COLUMN id_uuid TO id;
ALTER TABLE dead_letter_line_items ADD PRIMARY KEY (id);
ALTER TABLE dead_letter_line_items ALTER COLUMN id SET DEFAULT gen_random_uuid();

COMMIT;
```

---

## Phase 3: Model Standardization

**Priority:** 🟡 P2 — Required after Phase 2 completes  
**Downtime:** None (code-only changes)  
**Risk:** Low — models updated to match already-migrated DB schema

### Models Requiring Updates After Phase 2

After Phase 2 completes, the following models must be updated to use UUID primary keys:

| Model | File | Change Required |
|---|---|---|
| `BrandAPICredential` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `Order` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `OrderLine` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)`, FK `order_id` → UUID |
| `OrganizationBrandBinding` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `TenantCredential` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `SyncRun` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `SyncRequest` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `SyncHealth` | `models/sync_health.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `DeadLetterLineItem` | `models/sync_health.py` | `Integer` → `PG_UUID(as_uuid=True)`, FK `order_id` → UUID |
| `SyncDeadLetter` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |
| `SyncMetricsSnapshot` | `models/__init__.py` | `Integer` → `PG_UUID(as_uuid=True)` |

### Standard UUID Column Pattern

All models should use this pattern after migration:

```python
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy import text

id: Mapped[UUID] = mapped_column(
    PG_UUID(as_uuid=True),
    primary_key=True,
    server_default=text("gen_random_uuid()"),
)
```

### Standard UUID FK Pattern

```python
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

referenced_id: Mapped[UUID | None] = mapped_column(
    PG_UUID(as_uuid=True),
    ForeignKey("referenced_table.id"),
    nullable=True,
)
```

### Models Already Correct (No Changes Needed)

- `Driver` (`models/driver.py`) — already uses `PG_UUID(as_uuid=True)`
- `Route` (`models/route.py`) — already uses `PG_UUID(as_uuid=True)`
- `RouteStop` (`models/route_stop.py`) — already uses `PG_UUID(as_uuid=True)`
- `RouteEvent` (`models/route_event.py`) — already uses `PG_UUID(as_uuid=True)`
- `DriverLocation` (`models/driver_location.py`) — already uses `PG_UUID(as_uuid=True)`
- `DriverRouteHistory` (`models/driver_route_history.py`) — already uses `PG_UUID(as_uuid=True)`
- `AssistantSession` (`models/assistant_models.py`) — will be correct after Phase 1
- `AssistantMessage` (`models/assistant_models.py`) — will be correct after Phase 1
- `AssistantPendingAction` (`models/assistant_models.py`) — will be correct after Phase 1
- `AssistantAuditLog` (`models/assistant_models.py`) — will be correct after Phase 1

### Auth Models (PR #389 Scope)

The auth models (`models/auth_models.py`) already use `UUID(as_uuid=True)`. The database uses `VARCHAR(255)`. This is addressed in PR #389 and is out of scope for this migration strategy.

---

## Phase 4: Verification and Cleanup

**Priority:** 🟢 P3 — Final validation after all phases complete  
**Downtime:** None  
**Risk:** None (read-only verification)

### Schema Validation Queries

```sql
-- 1. Verify all tables use UUID PKs
SELECT
    t.table_name,
    c.column_name,
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
ORDER BY t.table_name;
-- Expected: all rows should show data_type = 'uuid'

-- 2. Verify no NULL PKs
SELECT 'orders' AS t, COUNT(*) FROM orders WHERE id IS NULL
UNION ALL SELECT 'order_lines', COUNT(*) FROM order_lines WHERE id IS NULL
UNION ALL SELECT 'sync_runs', COUNT(*) FROM sync_runs WHERE id IS NULL
UNION ALL SELECT 'sync_requests', COUNT(*) FROM sync_requests WHERE id IS NULL
UNION ALL SELECT 'brand_api_credentials', COUNT(*) FROM brand_api_credentials WHERE id IS NULL
UNION ALL SELECT 'tenant_credentials', COUNT(*) FROM tenant_credentials WHERE id IS NULL
UNION ALL SELECT 'drivers', COUNT(*) FROM drivers WHERE id IS NULL
UNION ALL SELECT 'routes', COUNT(*) FROM routes WHERE id IS NULL
UNION ALL SELECT 'route_stops', COUNT(*) FROM route_stops WHERE id IS NULL
UNION ALL SELECT 'assistant_sessions', COUNT(*) FROM assistant_sessions WHERE id IS NULL
UNION ALL SELECT 'assistant_messages', COUNT(*) FROM assistant_messages WHERE id IS NULL
UNION ALL SELECT 'assistant_pending_actions', COUNT(*) FROM assistant_pending_actions WHERE id IS NULL
UNION ALL SELECT 'assistant_audit_logs', COUNT(*) FROM assistant_audit_logs WHERE id IS NULL;
-- Expected: all counts = 0

-- 3. Verify FK integrity
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table,
    ccu.column_name AS foreign_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.referential_constraints rc
    ON tc.constraint_name = rc.constraint_name
JOIN information_schema.key_column_usage ccu
    ON rc.unique_constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'public'
ORDER BY tc.table_name;

-- 4. Verify no orphaned FK rows
SELECT COUNT(*) FROM order_lines ol
LEFT JOIN orders o ON o.id = ol.order_id
WHERE ol.order_id IS NOT NULL AND o.id IS NULL;
-- Expected: 0

SELECT COUNT(*) FROM assistant_messages m
LEFT JOIN assistant_sessions s ON s.id = m.session_id
WHERE m.session_id IS NOT NULL AND s.id IS NULL;
-- Expected: 0

-- 5. Verify UUID default is set on all PK columns
SELECT table_name, column_name, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND column_name = 'id'
  AND column_default NOT LIKE '%gen_random_uuid%'
  AND column_default IS NOT NULL;
-- Expected: 0 rows (all PKs should default to gen_random_uuid())
```

### ORM Query Tests

After Phase 3, run these tests to verify ORM compatibility:

```python
# Test 1: Insert and retrieve each model
from models import Order, OrderLine, SyncRun, BrandAPICredential
import uuid

# Each insert should auto-generate a UUID PK
async with db.begin():
    run = SyncRun(brand_id="test", integration_name="leaflink", status="queued", mode="incremental")
    db.add(run)
    await db.flush()
    assert isinstance(run.id, uuid.UUID), f"Expected UUID, got {type(run.id)}"

# Test 2: FK joins work
result = await db.execute(
    select(Order).where(Order.sync_run_id == run.id)
)

# Test 3: Assistant session creation
from models.assistant_models import AssistantSession, AssistantMessage
session = AssistantSession(org_id="test-org")
db.add(session)
await db.flush()
assert isinstance(session.id, str) and len(session.id) == 36

msg = AssistantMessage(session_id=session.id, role="user", content="hello")
db.add(msg)
await db.flush()
```

### Cleanup Tasks

After all phases complete:

1. **Remove SERIAL sequences** — After UUID migration, the old `{table}_id_seq` sequences are orphaned. Drop them:
   ```sql
   DROP SEQUENCE IF EXISTS orders_id_seq CASCADE;
   DROP SEQUENCE IF EXISTS order_lines_id_seq CASCADE;
   DROP SEQUENCE IF EXISTS sync_runs_id_seq CASCADE;
   -- etc.
   ```

2. **Remove `pulled_qty` column** — The `order_lines` table has a `pulled_qty` column in the DB migration but not in the ORM model. Verify it is unused before dropping.

3. **Consolidate duplicate migrations** — Two files share the date prefix `2026_05_13_01_*`. Rename one to `2026_05_13_02_*` to avoid confusion.

4. **Update `database.py` schema inspection** — The `inspect_schema_at_startup()` function only inspects `orders` and `order_lines`. Extend it to inspect all tables after migration.

---

## Deployment Checklist

### Pre-Migration (All Phases)

- [ ] Take a full RDS snapshot before running any migration
- [ ] Verify snapshot completed successfully
- [ ] Run migration on staging environment first
- [ ] Verify all application tests pass on staging
- [ ] Confirm row counts match between staging and production
- [ ] Schedule maintenance window (Phase 2 only — brief table locks)
- [ ] Notify team of planned migration

### Phase 1 Deployment (Assistant Tables)

- [ ] Backup: `pg_dump -t assistant_sessions -t assistant_messages -t assistant_pending_actions -t assistant_audit_logs`
- [ ] Run `2026_05_28_01_fix_assistant_tables.sql` in a transaction
- [ ] Verify: `SELECT COUNT(*) FROM assistant_sessions WHERE id IS NULL;` → 0
- [ ] Verify: `SELECT id, LENGTH(id) FROM assistant_sessions LIMIT 5;` → 36-char UUIDs
- [ ] Deploy updated application code
- [ ] Monitor error logs for 15 minutes
- [ ] Rollback trigger: any `operator does not exist` or `UndefinedColumnError` in logs

### Phase 2 Deployment (INTEGER → UUID)

- [ ] Full RDS snapshot
- [ ] Run migrations in order: 02 → 03 → 04
- [ ] Each migration runs in its own transaction
- [ ] Verify row counts unchanged after each migration
- [ ] Verify FK integrity after each migration
- [ ] Deploy Phase 3 model updates immediately after Phase 2 completes
- [ ] Monitor sync worker for errors
- [ ] Rollback trigger: any `operator does not exist: uuid = integer` in logs

### Phase 3 Deployment (Model Updates)

- [ ] Deploy as a single PR immediately after Phase 2
- [ ] No DB changes — code only
- [ ] Run full test suite before deploying
- [ ] Monitor for 30 minutes after deploy

### Phase 4 Deployment (Verification)

- [ ] Run all verification queries
- [ ] Confirm all counts are 0 (no NULLs, no orphans)
- [ ] Drop orphaned sequences
- [ ] Update documentation

---

## Downtime Estimates

| Phase | Operation | Estimated Lock Duration | Notes |
|---|---|---|---|
| Phase 1 | `ALTER TABLE assistant_*` | < 1 second per table | Tables are small |
| Phase 1 | `DROP COLUMN id` + `RENAME` | < 1 second per table | Tables are small |
| Phase 2 | `ALTER TABLE orders ADD COLUMN` | < 1 second | Non-blocking |
| Phase 2 | `UPDATE orders SET id_uuid` | 0 (no lock) | ~16k rows, fast |
| Phase 2 | `CREATE UNIQUE INDEX CONCURRENTLY` | 0 (no lock) | Concurrent build |
| Phase 2 | `DROP CONSTRAINT + DROP COLUMN + RENAME` | 1–5 seconds | Brief exclusive lock |
| Phase 3 | Code deploy | 0 | Rolling deploy |

Total estimated downtime: **< 30 seconds** across all phases, concentrated in Phase 2 table lock operations.

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Migration fails mid-transaction | Low | High | All migrations wrapped in `BEGIN/COMMIT`; automatic rollback on failure |
| Row count mismatch after migration | Very Low | High | Integrity checks in migration script; verify before committing |
| Application writes during migration | Low | Medium | Phase 2 lock is brief (< 5s); acceptable for production |
| ORM cache holds stale column metadata | Low | Medium | `dispose_and_recreate_engine()` called at startup after migrations |
| Asyncpg prepared statement cache confusion | Low | Medium | `execution_options={"compiled_cache": None}` already set in `database.py` |
| Rollback needed after Phase 2 | Very Low | Very High | Full RDS snapshot before Phase 2; rollback restores snapshot |
| Auth table VARCHAR/UUID conflict (PR #389) | Medium | High | Handled separately in PR #389; do not modify auth tables in this migration |

---

## Do NOT Modify

Per the original requirements, the following are out of scope and must not be touched:

- `main.py` bootstrap lifespan flow
- `opsyn-sync-worker/main.py` sync worker lifecycle
- Scheduler loop
- `database.py` DB initialization order
- PR #389 UUID/VARCHAR fixes for auth tables
