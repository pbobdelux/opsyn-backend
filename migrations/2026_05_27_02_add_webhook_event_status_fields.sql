-- Migration: 2026_05_27_02_add_webhook_event_status_fields
-- Creates the leaflink_webhook_events table with full tenant resolution
-- and signature verification tracking.
--
-- This table stores every inbound LeafLink webhook event for audit,
-- idempotency, and debugging of unresolved/ambiguous tenant events.
--
-- Key design decisions:
--   - tenant_resolution_status tracks whether we could identify the brand
--   - signature_verification_status tracks HMAC-SHA256 verification outcome
--   - duplicate_of_event_id enables deduplication tracking
--   - Composite unique index on (brand_id, idempotency_key) for tenant-scoped idempotency
--   - brand_id is nullable to allow storing events before tenant is resolved

CREATE TABLE IF NOT EXISTS leaflink_webhook_events (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Tenant identification (nullable until resolved)
    brand_id                    VARCHAR(120),
    org_id                      VARCHAR(120),
    -- Event metadata
    event_type                  VARCHAR(80),
    idempotency_key             VARCHAR(255),
    raw_payload                 JSONB,
    -- Tenant resolution tracking
    tenant_resolution_status    VARCHAR(20) NOT NULL DEFAULT 'unresolved',
    -- verified | skipped | invalid | missing
    signature_verification_status VARCHAR(20),
    signature_verification_error  TEXT,
    -- Deduplication: if this event is a duplicate, reference the original
    duplicate_of_event_id       UUID REFERENCES leaflink_webhook_events(id) ON DELETE SET NULL,
    -- Processing outcome
    enqueued_job_id             INTEGER,
    processing_error            TEXT,
    -- Timestamps
    received_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at                TIMESTAMP WITH TIME ZONE,
    created_at                  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index on tenant_resolution_status for querying unresolved/ambiguous events
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_resolution_status
    ON leaflink_webhook_events (tenant_resolution_status);

-- Index on brand_id for per-brand event queries
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_brand_id
    ON leaflink_webhook_events (brand_id);

-- Index on received_at for time-range queries
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_received_at
    ON leaflink_webhook_events (received_at DESC);

-- Composite unique index on (brand_id, idempotency_key) for tenant-scoped idempotency.
-- Partial: only enforced when both brand_id and idempotency_key are non-null.
-- This prevents duplicate processing of the same event for the same brand.
CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_event_brand_idempotency
    ON leaflink_webhook_events (brand_id, idempotency_key)
    WHERE brand_id IS NOT NULL AND idempotency_key IS NOT NULL;

-- Index on signature_verification_status for health monitoring queries
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_sig_status
    ON leaflink_webhook_events (signature_verification_status);
