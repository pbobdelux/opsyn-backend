-- Add org_code column to organizations table for human-friendly lookup
-- org_code is a short, lowercase identifier (e.g., "noble" for Noble Nectar)
-- Nullable to preserve backward compatibility with existing rows

ALTER TABLE organizations
    ADD COLUMN IF NOT EXISTS org_code VARCHAR(100) UNIQUE;

-- Index for fast case-insensitive org_code lookups
CREATE INDEX IF NOT EXISTS idx_organizations_org_code ON organizations(org_code);
