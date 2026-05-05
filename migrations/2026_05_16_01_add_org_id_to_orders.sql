-- Add org_id column to orders table for multi-tenant isolation
-- This allows filtering orders by org_id AND brand_id, preventing cross-tenant data leakage.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS org_id VARCHAR(120);

-- Index for efficient org-scoped queries
CREATE INDEX IF NOT EXISTS ix_orders_org_id ON orders(org_id);

-- Composite index for the primary multi-tenant query pattern: WHERE org_id = ? AND brand_id = ?
CREATE INDEX IF NOT EXISTS ix_orders_org_brand ON orders(org_id, brand_id);
