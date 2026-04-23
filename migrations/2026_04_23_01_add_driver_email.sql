-- Migration: Add email field to drivers table

ALTER TABLE drivers
ADD COLUMN IF NOT EXISTS email VARCHAR(255) NULL;
