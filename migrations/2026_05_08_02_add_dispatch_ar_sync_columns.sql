-- Migration: 2026_05_08_02_add_dispatch_ar_sync_columns
-- Adds all missing dispatch, AR, and sync_health columns to the orders table.
-- This is a catch-all migration that ensures every column the Order model
-- defines actually exists in the database, using IF NOT EXISTS on every
-- ALTER so it is safe to run against any schema state.
--
-- Root cause: UndefinedColumnError: column orders.assigned_driver_id does not exist
-- Impact: Every order upsert was failing, blocking all ~16k orders from syncing.
--
-- Columns added (all nullable or with safe defaults):
--   Dispatch: assigned_driver_id, assigned_driver_name, delivery_status,
--             delivery_date, route_number, route_id, driver_note,
--             delivery_instructions
--   AR:       payment_status, amount_paid, balance_due, due_date,
--             days_overdue, invoice_number, ar_note
--   Sync:     sync_health (JSONB), sync_health_status, sync_health_missing_fields,
--             sync_health_last_error

-- ============================================================================
-- Dispatch fields
-- ============================================================================

ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_driver_id UUID REFERENCES drivers(id) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_driver_name VARCHAR(255) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_status VARCHAR(30) DEFAULT 'pending';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_date DATE DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS route_number VARCHAR(50) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS route_id UUID REFERENCES routes(id) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS driver_note TEXT DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_instructions TEXT DEFAULT NULL;

-- Add check constraint for delivery_status (skip if already exists)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_orders_delivery_status'
          AND conrelid = 'orders'::regclass
    ) THEN
        ALTER TABLE orders ADD CONSTRAINT ck_orders_delivery_status
            CHECK (delivery_status IN ('pending','assigned','out_for_delivery','delivered','failed','needs_reschedule','cancelled'));
    END IF;
END$$;

-- ============================================================================
-- Accounts-receivable (AR) fields
-- ============================================================================

ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_status VARCHAR(30) DEFAULT 'unpaid';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_paid NUMERIC(12,2) DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS balance_due NUMERIC(12,2) DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS due_date DATE DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS days_overdue INTEGER DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS invoice_number VARCHAR(100) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS ar_note TEXT DEFAULT NULL;

-- Add check constraint for payment_status (skip if already exists)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_orders_payment_status'
          AND conrelid = 'orders'::regclass
    ) THEN
        ALTER TABLE orders ADD CONSTRAINT ck_orders_payment_status
            CHECK (payment_status IN ('unpaid','partial','paid','overdue','collection_issue','write_off'));
    END IF;
END$$;

-- ============================================================================
-- Sync health fields (per-order)
-- ============================================================================

ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health JSONB DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health_status VARCHAR(20) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health_missing_fields JSONB DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health_last_error TEXT DEFAULT NULL;

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_orders_assigned_driver ON orders(assigned_driver_id);
CREATE INDEX IF NOT EXISTS ix_orders_delivery_status ON orders(delivery_status);
CREATE INDEX IF NOT EXISTS ix_orders_payment_status ON orders(payment_status);
CREATE INDEX IF NOT EXISTS ix_orders_route_id ON orders(route_id);

CREATE INDEX IF NOT EXISTS ix_orders_sync_health_status
    ON orders (brand_id, sync_health_status)
    WHERE sync_health_status IS NOT NULL;

-- Expression index on sync_health JSONB status field
CREATE INDEX IF NOT EXISTS ix_orders_sync_health_json_status
    ON orders ((sync_health->>'status'))
    WHERE sync_health IS NOT NULL;

COMMENT ON COLUMN orders.assigned_driver_id IS 'UUID of the driver assigned to deliver this order';
COMMENT ON COLUMN orders.delivery_status IS 'Delivery lifecycle: pending|assigned|out_for_delivery|delivered|failed|needs_reschedule|cancelled';
COMMENT ON COLUMN orders.payment_status IS 'AR status: unpaid|partial|paid|overdue|collection_issue|write_off';
COMMENT ON COLUMN orders.sync_health IS 'JSONB blob with per-order sync health: {status, missing_fields, last_error, last_synced_at}';
COMMENT ON COLUMN orders.sync_health_status IS 'Per-order sync health: ok | partial | failed';
COMMENT ON COLUMN orders.sync_health_missing_fields IS 'List of missing required fields for this order (JSONB array)';
COMMENT ON COLUMN orders.sync_health_last_error IS 'Last sync error message for this order';
