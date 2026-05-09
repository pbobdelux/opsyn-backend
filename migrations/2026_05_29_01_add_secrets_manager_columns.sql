-- Migration: 2026_05_29_01_add_secrets_manager_columns
-- Adds AWS Secrets Manager reference columns to brand_api_credentials.
--
-- New columns:
--   api_key_secret_ref   — AWS Secrets Manager ARN/reference for the raw API key
--   api_key_last4        — Last 4 chars of API key for display/audit (never the full key)
--
-- Note: webhook_key_secret_ref, webhook_key_last4, webhook_enabled,
--       webhook_signature_required, and leaflink_company_id were added in
--       migration 2026_05_27_01_add_webhook_credential_fields.sql.
--
-- Backward compatibility:
--   Existing api_key and webhook_key columns are preserved.
--   New code should use api_key_secret_ref + AWS Secrets Manager for new credentials.
--   Set ALLOW_PLAINTEXT_SECRET_FALLBACK=true during migration to allow legacy fallback.
--
-- Security rationale:
--   Storing raw API keys in plaintext violates multi-tenant security requirements.
--   All new API keys must be stored in AWS Secrets Manager; only the ARN reference
--   and last-4 display hint are stored in the database.

-- Add API key secret reference columns
ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS api_key_secret_ref VARCHAR(255),
    ADD COLUMN IF NOT EXISTS api_key_last4 VARCHAR(4);

-- Add comment to deprecated api_key column
COMMENT ON COLUMN brand_api_credentials.api_key
    IS 'DEPRECATED: use api_key_secret_ref instead. Raw API key stored in plaintext — do not populate for new credentials.';

-- Index on api_key_secret_ref for fast lookup
CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_api_key_secret_ref
    ON brand_api_credentials (api_key_secret_ref)
    WHERE api_key_secret_ref IS NOT NULL;
