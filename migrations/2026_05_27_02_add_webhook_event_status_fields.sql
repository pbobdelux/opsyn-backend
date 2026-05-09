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
--   - raw_payload is capped at 1MB (enforced in application layer)

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
    -- unresolved | resolved | ambiguous
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

-- Index on tenant_resolution_status for fast filtering of unresolved events
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_tenant_status
    ON leaflink_webhook_events (tenant_resolution_status);

-- Index on brand_id for per-brand queries
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_brand_id
    ON leaflink_webhook_events (brand_id);

-- Index on received_at for time-range queries
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_received_at
    ON leaflink_webhook_events (received_at DESC);

-- Partial unique index on (brand_id, idempotency_key) for tenant-scoped idempotency
-- Only applies when brand_id is not null (unresolved events are excluded)
CREATE UNIQUE INDEX IF NOT EXISTS ix_leaflink_webhook_events_brand_idempotency
    ON leaflink_webhook_events (brand_id, idempotency_key)
    WHERE brand_id IS NOT NULL;
