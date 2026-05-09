-- Migration: 2026_05_26_01_add_leaflink_webhook_events
-- Creates the leaflink_webhook_events table for idempotency tracking.
--
-- Each inbound LeafLink webhook POST is recorded here before any processing.
-- The idempotency_key (UNIQUE) prevents duplicate processing of the same event.
--
-- Columns:
--   id               UUID PK
--   brand_id         UUID FK to brands (stored as VARCHAR to match existing pattern)
--   org_id           UUID nullable
--   event_type       VARCHAR: order_created, order_updated, product_created, product_updated
--   object_type      VARCHAR: order, product
--   object_id        VARCHAR: external order/product ID from LeafLink
--   payload_json     JSONB: full webhook payload
--   idempotency_key  VARCHAR: UNIQUE — prevents duplicate processing
--   status           VARCHAR: pending, processed, failed
--   error_message    TEXT nullable
--   created_at       TIMESTAMP UTC
--   processed_at     TIMESTAMP UTC nullable

CREATE TABLE IF NOT EXISTS leaflink_webhook_events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id         VARCHAR(120) NOT NULL,
    org_id           VARCHAR(120),
    event_type       VARCHAR(80),
    object_type      VARCHAR(50),
    object_id        VARCHAR(255),
    payload_json     JSONB,
    idempotency_key  VARCHAR(512) NOT NULL,
    status           VARCHAR(20) NOT NULL DEFAULT 'pending',
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ
);

-- Unique index on idempotency_key — core deduplication guard
CREATE UNIQUE INDEX IF NOT EXISTS uix_leaflink_webhook_events_idempotency_key
    ON leaflink_webhook_events (idempotency_key);

-- Composite index for brand-scoped time-ordered queries
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_brand_created
    ON leaflink_webhook_events (brand_id, created_at DESC);

-- Composite index for brand-scoped status queries
CREATE INDEX IF NOT EXISTS ix_leaflink_webhook_events_brand_status
    ON leaflink_webhook_events (brand_id, status);
