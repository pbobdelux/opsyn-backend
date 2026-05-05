-- Migration: 2026_05_17_01_fix_leaflink_noble_nectar_credentials
-- Fixes Noble Nectar LeafLink credentials to use the correct base URL
-- (marketplace.leaflink.com) and auth scheme (Api-Key).
-- The old base URL (www.leaflink.com) and Token/Bearer auth schemes
-- return "API access may not be enabled for this key" / "Invalid token".

UPDATE brand_api_credentials
SET base_url    = 'https://marketplace.leaflink.com/api/v2',
    auth_scheme = 'Api-Key'
WHERE brand_id         = '380e963d-36fc-4928-a4f4-e569cd535f9e'
  AND integration_name = 'leaflink';
