-- Migration: 2026_05_12_02_add_auth_scheme_to_credentials
-- Adds the auth_scheme column to brand_api_credentials for storing
-- auto-detected authentication scheme (Bearer, Token, or Raw).

ALTER TABLE brand_api_credentials
    ADD COLUMN IF NOT EXISTS auth_scheme VARCHAR(20);
