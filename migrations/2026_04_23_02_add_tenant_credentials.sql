-- Migration: 2026_04_23_02_add_tenant_credentials
-- Adds the tenant_credentials table used by the multi-tenant auth layer.
-- Secrets stored in api_secret must be SHA-256 hex digests of the raw secret.

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
