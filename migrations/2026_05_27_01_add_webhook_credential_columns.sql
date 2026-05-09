-- Migration: 2026_05_27_01_add_webhook_credential_columns
--
-- Replaces the plaintext webhook_key column on brand_api_credentials with
-- secure per-brand webhook configuration columns:
--
--   webhook_key_secret_ref     — AWS Secrets Manager secret name/ARN
--   webhook_key_last4          — last 4 chars of key for display only
--   webhook_enabled            — feature flag per brand
--   webhook_signature_required — per-brand signature enforcement
--   leaflink_company_id        — LeafLink company ID for webhook tenant resolution
--
-- The old webhook_key column (plaintext) is dropped.
-- All migrations are idempotent (IF EXISTS / IF NOT EXISTS guards).

-- ---------------------------------------------------------------------------
-- 1. Drop the plaintext webhook_key column (if it exists)
-- ---------------------------------------------------------------------------
ALTER TABLE brand_api_credentials
    DROP COLUMN IF EXISTS webhook_key;

-- ---------------------------------------------------------------------------
-- 2. Add new webhook credential columns
-- ---------------------------------------------------------------------------
ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS webhook_key_secret_ref VARCHAR(255),
    ADD COLUMN IF NOT EXISTS webhook_key_last4 VARCHAR(4),
    ADD COLUMN IF NOT EXISTS webhook_enabled BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS webhook_signature_required BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS leaflink_company_id VARCHAR(120);

-- ---------------------------------------------------------------------------
-- 3. Indexes for efficient webhook tenant resolution and status queries
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_webhook_enabled
    ON brand_api_credentials(webhook_enabled)
    WHERE webhook_enabled = true;

CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_leaflink_company_id
    ON brand_api_credentials(leaflink_company_id)
    WHERE leaflink_company_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 4. Create leaflink_webhook_events table for inbound webhook audit log
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS leaflink_webhook_events (
    id                  SERIAL PRIMARY KEY,
    event_type          VARCHAR(80),
    raw_payload         JSONB,

    -- Tenant resolution outcome
    resolution_status   VARCHAR(20) NOT NULL DEFAULT 'unresolved',
    resolution_error    TEXT,
    resolved_brand_id   VARCHAR(120),
    resolved_org_id     VARCHAR(120),
    brand_id            VARCHAR(120),

    -- Company/seller IDs extracted from payload
    payload_company_id  VARCHAR(120),
    payload_seller_id   VARCHAR(120),

    -- Signature verification result
    -- NULL = skipped, TRUE = valid, FALSE = invalid/missing
    signature_valid     BOOLEAN,
    signature_error     TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 5. Indexes on leaflink_webhook_events
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_webhook_events_brand_resolution
    ON leaflink_webhook_events(brand_id, resolution_status);

CREATE INDEX IF NOT EXISTS ix_webhook_events_resolution_created
    ON leaflink_webhook_events(resolution_status, created_at);

CREATE INDEX IF NOT EXISTS ix_webhook_events_created_at
    ON leaflink_webhook_events(created_at);

CREATE INDEX IF NOT EXISTS ix_webhook_events_brand_id
    ON leaflink_webhook_events(brand_id)
    WHERE brand_id IS NOT NULL;
