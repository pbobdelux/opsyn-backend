-- Migration: 2026_05_01_00_fix_leaflink_stale_credentials
-- Repairs all LeafLink credentials that carry a stale marketplace domain
-- or the deprecated Api-Key auth scheme.
--
-- Correct values:
--   base_url    = 'https://www.leaflink.com/api/v2'
--   auth_scheme = 'Token'
--
-- This migration is idempotent: rows already carrying the correct values are
-- not touched (the WHERE clause filters them out).
--
-- Uses IF EXISTS guards so it is safe to run even before brand_api_credentials
-- exists (e.g. on a brand-new database where schema migrations have not yet run).
--
-- Filename prefix 2026_05_01_00 ensures this sorts BEFORE all other May-2026
-- migrations and is further promoted to run first by the migration runner's
-- fix_leaflink ordering rule.

DO $$
DECLARE
    _fixed_count INTEGER := 0;
    _table_exists BOOLEAN;
BEGIN
    RAISE NOTICE '[LEAFLINK_REPAIR_EXECUTED] status=started';

    -- Check whether the target table exists before attempting any DML.
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name   = 'brand_api_credentials'
    ) INTO _table_exists;

    IF NOT _table_exists THEN
        RAISE NOTICE '[LEAFLINK_REPAIR_RESULT] fixed_count=0 status=skipped reason=table_not_found';
        RETURN;
    END IF;

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

    RAISE NOTICE '[LEAFLINK_REPAIR_RESULT] fixed_count=% status=success', _fixed_count;
END;
$$;
