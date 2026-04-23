-- Migration: Add PIN field to drivers table

ALTER TABLE drivers
ADD COLUMN IF NOT EXISTS pin VARCHAR(4) NULL;
