-- Migration: 004_add_dispatch_ar_fields_to_orders
-- Adds dispatch and accounts-receivable (AR) fields to the orders table.
-- Connects orders to the driver/route system built in PRs #332-340.
--
-- All new columns are nullable or have safe defaults — zero risk to existing data.
-- LeafLink sync logic and existing columns are unchanged.
--
-- New capabilities:
--   - Track which driver/route an order is assigned to
--   - Delivery status tracking (pending → assigned → out_for_delivery → delivered)
--   - Payment status tracking (unpaid → partial → paid, with overdue/collection_issue/write_off)
--   - AR aging (days_overdue, due_date)
--   - Delivery instructions and driver notes

-- ============================================================================
-- Dispatch fields
-- ============================================================================

ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_driver_id UUID REFERENCES drivers(id) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_driver_name VARCHAR(255) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_status VARCHAR(30) DEFAULT 'pending'
    CHECK (delivery_status IN ('pending','assigned','out_for_delivery','delivered','failed','needs_reschedule','cancelled'));
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_date DATE DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS route_number VARCHAR(50) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS route_id UUID REFERENCES routes(id) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS driver_note TEXT DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_instructions TEXT DEFAULT NULL;

-- ============================================================================
-- Accounts-receivable (AR) fields
-- ============================================================================

ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_status VARCHAR(30) DEFAULT 'unpaid'
    CHECK (payment_status IN ('unpaid','partial','paid','overdue','collection_issue','write_off'));
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_paid NUMERIC(12,2) DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS balance_due NUMERIC(12,2) DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS due_date DATE DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS days_overdue INTEGER DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS invoice_number VARCHAR(100) DEFAULT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS ar_note TEXT DEFAULT NULL;

-- ============================================================================
-- Indexes for efficient dispatch/AR queries
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_orders_assigned_driver ON orders(assigned_driver_id);
CREATE INDEX IF NOT EXISTS ix_orders_delivery_status ON orders(delivery_status);
CREATE INDEX IF NOT EXISTS ix_orders_payment_status ON orders(payment_status);
CREATE INDEX IF NOT EXISTS ix_orders_route_id ON orders(route_id);
