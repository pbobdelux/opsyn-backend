-- Migration: 2026_05_19_01_fix_leaflink_stale_credentials
-- Fixes all LeafLink credentials that still carry the stale marketplace domain
-- or the deprecated Api-Key auth scheme.
--
-- The correct values are:
--   base_url    = 'https://www.leaflink.com/api/v2'
--   auth_scheme = 'Token'
--
-- This migration is idempotent: rows that already have the correct values are
-- not touched (the WHERE clause filters them out).

DO $$
DECLARE
    _fixed_count INTEGER;
BEGIN
    UPDATE brand_api_credentials
    SET
        base_url    = 'https://www.leaflink.com/api/v2',
        auth_scheme = 'Token'
    WHERE integration_name = 'leaflink'
      AND (
          base_url    ILIKE '%marketplace.leaflink.com%'
          OR auth_scheme = 'Api-Key'
      );

    GET DIAGNOSTICS _fixed_count = ROW_COUNT;

    RAISE NOTICE '[LEAFLINK_MIGRATION] fixed_credentials count=%', _fixed_count;
END;
$$;
