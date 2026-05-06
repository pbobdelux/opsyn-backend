-- Migration: Add LeafLink order freshness + reconciliation fields

ALTER TABLE orders
ADD COLUMN IF NOT EXISTS leaflink_last_synced_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS leaflink_raw JSONB,
ADD COLUMN IF NOT EXISTS last_status_change_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS last_payment_sync_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS sync_source TEXT DEFAULT 'system',
ADD COLUMN IF NOT EXISTS needs_reconciliation BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS stale BOOLEAN DEFAULT FALSE;

-- Optional indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_last_synced
ON orders (leaflink_last_synced_at);

CREATE INDEX IF NOT EXISTS idx_orders_needs_reconciliation
ON orders (needs_reconciliation);

CREATE INDEX IF NOT EXISTS idx_orders_stale
ON orders (stale);