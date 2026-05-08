-- Add org_id column to sync_requests table.
--
-- This column stores the Organization UUID at the time a sync job is enqueued
-- (from the X-OPSYN-ORG request header).  The background worker reads it back
-- and propagates it to every order INSERT so that orders.org_id is never NULL.
--
-- Without this column the worker has no way to know which org the sync belongs
-- to, causing all orders to be inserted with org_id = NULL.  Those orders are
-- invisible to GET /orders which filters by org_id for multi-tenant isolation.

ALTER TABLE sync_requests
    ADD COLUMN IF NOT EXISTS org_id VARCHAR(120);

-- Index for efficient org-scoped queries on the queue
CREATE INDEX IF NOT EXISTS ix_sync_requests_org_id ON sync_requests(org_id);
