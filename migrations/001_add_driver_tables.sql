-- =============================================================================
-- Migration: 001_add_driver_tables.sql
-- Description: Add organizations, drivers, and driver_auth tables for
--              multitenant driver authentication.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- organizations
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS organizations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    company_code    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_organizations_company_code UNIQUE (company_code)
);

CREATE INDEX IF NOT EXISTS idx_organizations_company_code ON organizations (company_code);
CREATE INDEX IF NOT EXISTS idx_organizations_status        ON organizations (status);

-- ---------------------------------------------------------------------------
-- drivers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS drivers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    brand_id        UUID,
    full_name       TEXT NOT NULL,
    phone           TEXT,
    email           TEXT,
    status          TEXT NOT NULL DEFAULT 'active',
    availability    TEXT NOT NULL DEFAULT 'available',
    employee_code   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_drivers_org_id       ON drivers (org_id);
CREATE INDEX IF NOT EXISTS idx_drivers_status       ON drivers (status);
CREATE INDEX IF NOT EXISTS idx_drivers_org_status   ON drivers (org_id, status);

-- ---------------------------------------------------------------------------
-- driver_auth
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS driver_auth (
    driver_id           UUID PRIMARY KEY REFERENCES drivers (id) ON DELETE CASCADE,
    pin_hash            TEXT NOT NULL,
    last_login_at       TIMESTAMPTZ,
    pin_last_rotated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Trigger: keep updated_at current on organizations
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_organizations_updated_at'
    ) THEN
        CREATE TRIGGER trg_organizations_updated_at
        BEFORE UPDATE ON organizations
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_drivers_updated_at'
    ) THEN
        CREATE TRIGGER trg_drivers_updated_at
        BEFORE UPDATE ON drivers
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;
END;
$$;
