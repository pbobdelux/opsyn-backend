-- Migration: 2026_05_13_01_add_sync_run_id_to_orders
-- Adds the sync_run_id column to orders table to track which sync run
-- created or updated each order.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS sync_run_id INTEGER;
