-- Migration: 2026_05_26_02_add_sync_requests_queue
-- Adds webhook-first queue columns to the existing sync_requests table.
--
-- The existing sync_requests table uses SERIAL PK and VARCHAR brand_id.
-- This migration adds the columns needed for the webhook-first architecture
-- without breaking the existing full-resync worker.
--
-- New columns:
--   type         VARCHAR: webhook_order, webhook_product, incremental_recent_orders, full_resync
--   object_id    VARCHAR nullable: specific order/product ID for targeted fetches
--   retry_count  INT default 0
--   max_retries  INT default 3
--   error_message TEXT nullable (separate from existing 'error' column)
--   started_at   TIMESTAMPTZ nullable (already exists — skip if present)
--
-- New indexes:
--   (brand_id, type, status, created_at)
--   (status, created_at)

-- Add type column (defaults to 'full_resync' so existing rows keep working)
ALTER TABLE sync_requests
    ADD COLUMN IF NOT EXISTS type VARCHAR(50) NOT NULL DEFAULT 'full_resync';

-- Add object_id for targeted single-order/product fetches
ALTER TABLE sync_requests
    ADD COLUMN IF NOT EXISTS object_id VARCHAR(255);

-- Add retry tracking columns
ALTER TABLE sync_requests
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE sync_requests
    ADD COLUMN IF NOT EXISTS max_retries INTEGER NOT NULL DEFAULT 3;

-- Add error_message (distinct from legacy 'error' column)
ALTER TABLE sync_requests
    ADD COLUMN IF NOT EXISTS error_message TEXT;

-- Composite index for priority-ordered queue polling
CREATE INDEX IF NOT EXISTS ix_sync_requests_brand_type_status_created
    ON sync_requests (brand_id, type, status, created_at);

-- Global status+created index for worker polling across all brands
CREATE INDEX IF NOT EXISTS ix_sync_requests_status_created
    ON sync_requests (status, created_at);
