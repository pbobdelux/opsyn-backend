-- Migration: 2026_05_21_02_add_drivers_routes_stops
-- Creates the drivers, routes, and route_stops tables for delivery route management.
-- Uses UUID primary keys, TIMESTAMP WITH TIME ZONE for all datetime columns,
-- and org_id scoping for multi-tenant isolation.
--
-- Execution order matters: drivers → routes → route_stops (FK dependencies).

-- ============================================================================
-- STEP 1: Drop legacy dispatch tables that are superseded by this migration.
-- These tables used integer PKs and simpler schemas.
-- ============================================================================

-- route_stops references dispatch_routes, so drop it first.
DROP TABLE IF EXISTS route_stops CASCADE;
DROP TABLE IF EXISTS dispatch_routes CASCADE;

-- ============================================================================
-- STEP 2: Recreate drivers table with UUID PKs and enriched schema.
-- The legacy drivers table used SERIAL PKs; this replaces it entirely.
-- ============================================================================

DROP TABLE IF EXISTS drivers CASCADE;

CREATE TABLE drivers (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id            UUID NOT NULL,
    name              VARCHAR(255) NOT NULL,
    email             VARCHAR(255),
    phone             VARCHAR(50),
    status            VARCHAR(20) NOT NULL DEFAULT 'active',
    passcode_hash     VARCHAR(255),
    invite_code       VARCHAR(50),
    license_plate     VARCHAR(50),
    vehicle_type      VARCHAR(50),
    notes             TEXT,
    preferences       JSON NOT NULL DEFAULT '{}',
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deactivated_at    TIMESTAMP WITH TIME ZONE,

    CONSTRAINT ck_drivers_status
        CHECK (status IN ('active', 'inactive')),

    CONSTRAINT uq_drivers_org_email
        UNIQUE (org_id, email)
        -- Partial uniqueness (only when email is not null) is enforced via the
        -- index below; the table-level UNIQUE above is a fallback for non-null rows.
);

-- Indexes for drivers
CREATE INDEX IF NOT EXISTS ix_drivers_org_id
    ON drivers (org_id);

CREATE INDEX IF NOT EXISTS ix_drivers_org_status
    ON drivers (org_id, status);

-- Partial unique index: org_id + email uniqueness only when email IS NOT NULL
CREATE UNIQUE INDEX IF NOT EXISTS uq_drivers_org_email_partial
    ON drivers (org_id, email)
    WHERE email IS NOT NULL;

-- Drop the non-partial unique constraint now that the partial index covers it
ALTER TABLE drivers DROP CONSTRAINT IF EXISTS uq_drivers_org_email;

-- ============================================================================
-- STEP 3: Create routes table (new table, replaces dispatch_routes).
-- ============================================================================

CREATE TABLE IF NOT EXISTS routes (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id              UUID NOT NULL,
    route_number        VARCHAR(50),
    status              VARCHAR(30) NOT NULL DEFAULT 'draft',
    assigned_driver_id  UUID REFERENCES drivers(id),
    route_date          DATE NOT NULL,
    total_stops         INTEGER NOT NULL DEFAULT 0,
    total_value         NUMERIC(12, 2) NOT NULL DEFAULT 0,
    total_units         INTEGER NOT NULL DEFAULT 0,
    notes               TEXT,
    created_by          UUID,
    version             INTEGER NOT NULL DEFAULT 1,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at        TIMESTAMP WITH TIME ZONE,

    CONSTRAINT ck_routes_status
        CHECK (status IN ('draft', 'assigned', 'out_for_delivery', 'completed', 'cancelled'))
);

-- Indexes for routes
CREATE INDEX IF NOT EXISTS ix_routes_org_id
    ON routes (org_id);

CREATE INDEX IF NOT EXISTS ix_routes_org_date
    ON routes (org_id, route_date);

CREATE INDEX IF NOT EXISTS ix_routes_driver
    ON routes (assigned_driver_id);

-- ============================================================================
-- STEP 4: Create route_stops table.
-- ============================================================================

CREATE TABLE IF NOT EXISTS route_stops (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id          UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id            UUID NOT NULL,
    stop_order        INTEGER NOT NULL,
    stop_type         VARCHAR(30) NOT NULL DEFAULT 'leaflink_order',
    source_order_id   UUID,
    customer_name     VARCHAR(255),
    stop_name         VARCHAR(255),
    address           TEXT,
    contact_name      VARCHAR(255),
    contact_phone     VARCHAR(50),
    notes             TEXT,
    time_window       VARCHAR(100),
    priority          INTEGER NOT NULL DEFAULT 0,
    status            VARCHAR(20) NOT NULL DEFAULT 'pending',
    ar_status         VARCHAR(30) NOT NULL DEFAULT 'not_applicable',
    amount_due        NUMERIC(12, 2) NOT NULL DEFAULT 0,
    amount_collected  NUMERIC(12, 2) NOT NULL DEFAULT 0,
    completed_at      TIMESTAMP WITH TIME ZONE,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_route_stops_type
        CHECK (stop_type IN ('leaflink_order', 'manual_stop', 'bank', 'processor_pickup', 'sample_dropoff', 'supply_pickup', 'other')),

    CONSTRAINT ck_route_stops_status
        CHECK (status IN ('pending', 'arrived', 'completed', 'failed', 'skipped')),

    CONSTRAINT ck_route_stops_ar_status
        CHECK (ar_status IN ('unpaid', 'partial', 'paid', 'collection_issue', 'not_applicable')),

    CONSTRAINT uq_route_stops_order
        UNIQUE (route_id, stop_order)
);

-- Indexes for route_stops
CREATE INDEX IF NOT EXISTS ix_route_stops_route_id
    ON route_stops (route_id);

CREATE INDEX IF NOT EXISTS ix_route_stops_org_id
    ON route_stops (org_id);

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================
-- Tables created: drivers (UUID PKs), routes, route_stops
-- Legacy tables dropped: dispatch_routes, route_stops (old integer-PK version)
-- All datetime columns use TIMESTAMP WITH TIME ZONE.
-- All UUID columns use PostgreSQL UUID type.
-- org_id scoping applied to all three tables for multi-tenant isolation.
