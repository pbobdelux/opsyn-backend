-- Migration: 2026_05_27_01_add_webhook_credential_fields
-- Adds per-brand webhook credential fields to brand_api_credentials.
--
-- New columns:
--   webhook_key_secret_ref   — AWS Secrets Manager ARN/reference for the raw webhook key
--   webhook_key_last4        — Last 4 chars of webhook key for display/audit (never the full key)
--   webhook_enabled          — Whether webhook processing is enabled for this brand
--   webhook_signature_required — Whether LL-Signature verification is required (default true)
--   leaflink_company_id      — LeafLink company ID for fast tenant resolution from webhook payloads
--
-- Deprecation:
--   webhook_key column is set to nullable and marked deprecated.
--   New code must use webhook_key_secret_ref + AWS Secrets Manager instead.
--
-- Security rationale:
--   Storing raw webhook keys in plaintext violates multi-tenant security requirements.
--   All new webhook keys must be stored in AWS Secrets Manager; only the ARN reference
--   and last-4 display hint are stored in the database.

-- Add new secure webhook credential fields
ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS webhook_key_secret_ref VARCHAR(255),
    ADD COLUMN IF NOT EXISTS webhook_key_last4 VARCHAR(4),
    ADD COLUMN IF NOT EXISTS webhook_enabled BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS webhook_signature_required BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS leaflink_company_id VARCHAR(120);

-- Deprecate plaintext webhook_key: make nullable (already is), add comment
COMMENT ON COLUMN brand_api_credentials.webhook_key
    IS 'DEPRECATED: use webhook_key_secret_ref instead. Raw webhook key stored in plaintext — do not populate for new credentials.';

-- Index on leaflink_company_id for fast tenant resolution from webhook payloads
CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_leaflink_company_id
    ON brand_api_credentials (leaflink_company_id);

-- Index on webhook_enabled for efficient filtering of webhook-active brands
CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_webhook_enabled
    ON brand_api_credentials (webhook_enabled)
    WHERE webhook_enabled = true;
