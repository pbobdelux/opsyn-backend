-- Migration: 2026_04_28_01_add_credential_last_checked_at
-- Adds last_checked_at column to brand_api_credentials for per-brand
-- credential validation tracking (multi-tenant LeafLink auth).

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMP WITH TIME ZONE;
