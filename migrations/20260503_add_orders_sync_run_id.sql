-- Add missing columns to orders table for sync tracking
-- These columns are required by the current sync_leaflink_background_continuous() logic

ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_run_id UUID;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS review_status VARCHAR(50);

-- Create index on sync_run_id for efficient filtering
CREATE INDEX IF NOT EXISTS idx_orders_sync_run_id ON orders(sync_run_id);
