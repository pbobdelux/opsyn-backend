-- Migration: 2026_04_23_02_add_tenant_credentials
-- STATUS: SKIPPED_LEGACY_MIGRATION
-- REASON: Uses SERIAL PK; superseded by 2026_05_04_02_create_full_schema_aws_rds.sql
--         which creates tenant_credentials with the correct schema (also SERIAL for now,
--         but managed as part of the canonical full-schema migration).
--         This file is quarantined to prevent duplicate-table conflicts on fresh databases
--         where 2026_05_04_02 has already run.
--
-- DO NOT REMOVE this file — it is tracked in SKIPPED_LEGACY_MIGRATIONS in migration_runner.py
-- and must remain on disk so the runner can log its skip reason deterministically.

CREATE TABLE IF NOT EXISTS tenant_credentials (
    id          SERIAL PRIMARY KEY,
    org_id      VARCHAR(120) NOT NULL UNIQUE,
    api_secret  VARCHAR(255) NOT NULL,   -- SHA-256 hex digest of the raw secret
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenant_credentials_org_id ON tenant_credentials(org_id);

-- Seed initial tenant for org_onboarding.
-- Replace the placeholder with the SHA-256 hex digest of your actual secret:
--   python3 -c "import hashlib; print(hashlib.sha256(b'YOUR_SECRET').hexdigest())"
-- Or set via environment variable and insert at deploy time.
--
-- INSERT INTO tenant_credentials (org_id, api_secret, is_active)
-- VALUES ('org_onboarding', '<sha256_hex_of_OPSYN_TENANT_SECRET_ORG_ONBOARDING>', TRUE);
