-- Migration: 2026_05_23_01_add_dead_letter_detail_fields
-- Adds detailed diagnostic columns to sync_dead_letters for structured failure analysis.
--
-- New columns:
--   failure_category       — specific failure type (replaces generic 'malformed')
--   exception_type         — Python exception class name (e.g. KeyError, ValueError)
--   exception_message      — full exception message (up to 1000 chars)
--   traceback_summary      — first 500 chars of the traceback
--   payload_keys           — JSONB array of top-level keys in the raw payload
--   problematic_field      — which field caused the failure
--   problematic_value_preview — first 100 chars of the problematic value
--   customer_name          — customer name from the payload (if available)
--   failure_stage          — structured stage name (header_extract, header_transform,
--                            header_insert, line_item_extract, line_item_transform,
--                            line_item_insert)
--
-- [DEAD_LETTER_DETAIL_FIELDS_ADDED]

ALTER TABLE sync_dead_letters
    ADD COLUMN IF NOT EXISTS failure_category       VARCHAR(80),
    ADD COLUMN IF NOT EXISTS exception_type         VARCHAR(120),
    ADD COLUMN IF NOT EXISTS exception_message      TEXT,
    ADD COLUMN IF NOT EXISTS traceback_summary      TEXT,
    ADD COLUMN IF NOT EXISTS payload_keys           JSONB,
    ADD COLUMN IF NOT EXISTS problematic_field      VARCHAR(120),
    ADD COLUMN IF NOT EXISTS problematic_value_preview TEXT,
    ADD COLUMN IF NOT EXISTS customer_name          VARCHAR(255),
    ADD COLUMN IF NOT EXISTS failure_stage          VARCHAR(80);

-- Index for querying by failure category (most common analysis pattern)
CREATE INDEX IF NOT EXISTS ix_sync_dead_letters_failure_category
    ON sync_dead_letters (brand_id, failure_category)
    WHERE resolved_at IS NULL;

-- Index for querying by failure stage
CREATE INDEX IF NOT EXISTS ix_sync_dead_letters_failure_stage
    ON sync_dead_letters (brand_id, failure_stage)
    WHERE resolved_at IS NULL;

COMMENT ON COLUMN sync_dead_letters.failure_category IS
    'Specific failure category: missing_customer | missing_order_number | missing_external_order_id | '
    'invalid_money | invalid_timestamp | invalid_status | malformed_line_items | orphan_line_items | '
    'duplicate_external_id | invalid_json_payload | serializer_error | db_type_error | unknown_transform_error';

COMMENT ON COLUMN sync_dead_letters.exception_type IS 'Python exception class name (e.g. KeyError, ValueError, TypeError)';
COMMENT ON COLUMN sync_dead_letters.exception_message IS 'Full exception message (up to 1000 chars)';
COMMENT ON COLUMN sync_dead_letters.traceback_summary IS 'First 500 chars of the Python traceback';
COMMENT ON COLUMN sync_dead_letters.payload_keys IS 'JSONB array of top-level keys present in the raw payload';
COMMENT ON COLUMN sync_dead_letters.problematic_field IS 'The specific field that caused the failure';
COMMENT ON COLUMN sync_dead_letters.problematic_value_preview IS 'First 100 chars of the problematic field value';
COMMENT ON COLUMN sync_dead_letters.customer_name IS 'Customer name extracted from the payload (if available)';
COMMENT ON COLUMN sync_dead_letters.failure_stage IS
    'Structured stage: header_extract | header_transform | header_insert | '
    'line_item_extract | line_item_transform | line_item_insert';
