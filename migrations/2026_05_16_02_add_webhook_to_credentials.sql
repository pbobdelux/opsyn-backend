-- Migration: 2026_05_16_02_add_webhook_to_credentials
-- Adds webhook_key and org_id columns to brand_api_credentials.
-- webhook_key: LeafLink webhook secret for HMAC-SHA256 signature verification.
-- org_id: Organization UUID for fast tenant resolution from webhook payloads.

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS webhook_key VARCHAR(255),
    ADD COLUMN IF NOT EXISTS org_id UUID;

CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_webhook_key
    ON brand_api_credentials(webhook_key);

CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_org_id
    ON brand_api_credentials(org_id);
