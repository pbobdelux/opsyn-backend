-- Migration: 2026_05_08_03_add_missing_dispatch_ar_columns
-- Adds all missing dispatch, AR, and sync_health columns to the orders table.
--
-- Root cause: asyncpg.exceptions.UndefinedColumnError: column orders.assigned_driver_id
-- does not exist — the Order SQLAlchemy model defines 16+ columns that were never
-- successfully added to the production database. Previous migrations (004 and
-- 2026_05_08_02) failed with DatatypeMismatchError due to FK references to
-- drivers/routes tables and DATE vs TIMESTAMPTZ type conflicts.
--
-- This migration is fully idempotent: every statement uses IF NOT EXISTS so it
-- is safe to run against any schema state, including databases where some or all
-- of these columns already exist.
--
-- Columns added:
--   Dispatch: assigned_driver_id, assigned_driver_name, delivery_status,
--             delivery_date, route_number, route_id, driver_note,
--             delivery_instructions
--   AR:       payment_status, amount_paid, balance_due, due_date,
--             days_overdue, invoice_number, ar_note
--   Sync:     sync_health_status, sync_health_missing_fields,
--             sync_health_last_error, sync_health

-- ============================================================================
-- Dispatch fields
-- ============================================================================

-- UUID stored as TEXT to avoid FK dependency on drivers table
ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_driver_id TEXT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_driver_name TEXT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_status TEXT NULL DEFAULT 'pending';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_date TIMESTAMPTZ NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS route_number TEXT NULL;
-- UUID stored as TEXT to avoid FK dependency on routes table
ALTER TABLE orders ADD COLUMN IF NOT EXISTS route_id TEXT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS driver_note TEXT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_instructions TEXT NULL;

-- ============================================================================
-- Accounts-receivable (AR) fields
-- ============================================================================

ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_status TEXT NULL DEFAULT 'unpaid';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_paid NUMERIC NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS balance_due NUMERIC NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS due_date TIMESTAMPTZ NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS days_overdue INTEGER NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS invoice_number TEXT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS ar_note TEXT NULL;

-- ============================================================================
-- Sync health fields (per-order)
-- ============================================================================

ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health_status TEXT NULL DEFAULT 'ok';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health_missing_fields JSONB NULL DEFAULT '[]'::jsonb;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health_last_error TEXT NULL;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sync_health JSONB NULL DEFAULT '{}'::jsonb;

-- ============================================================================
-- Indexes (all use IF NOT EXISTS — safe to re-run)
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_orders_assigned_driver ON orders (assigned_driver_id);
CREATE INDEX IF NOT EXISTS ix_orders_delivery_status ON orders (delivery_status);
CREATE INDEX IF NOT EXISTS ix_orders_payment_status ON orders (payment_status);
CREATE INDEX IF NOT EXISTS ix_orders_route_id ON orders (route_id);
CREATE INDEX IF NOT EXISTS ix_orders_sync_health_status ON orders (brand_id, sync_health_status) WHERE sync_health_status IS NOT NULL;

-- ============================================================================
-- Column comments
-- ============================================================================

COMMENT ON COLUMN orders.assigned_driver_id IS 'UUID (as text) of the driver assigned to deliver this order';
COMMENT ON COLUMN orders.delivery_status IS 'Delivery lifecycle: pending | assigned | out_for_delivery | delivered | failed | needs_reschedule | cancelled';
COMMENT ON COLUMN orders.payment_status IS 'AR status: unpaid | partial | paid | overdue | collection_issue | write_off';
COMMENT ON COLUMN orders.sync_health_status IS 'Per-order sync health: ok | partial | failed';
COMMENT ON COLUMN orders.sync_health_missing_fields IS 'JSONB array of missing required field names for this order';
COMMENT ON COLUMN orders.sync_health_last_error IS 'Last sync error message recorded for this order';
COMMENT ON COLUMN orders.sync_health IS 'JSONB blob: {status, missing_fields, last_error, last_synced_at}';
