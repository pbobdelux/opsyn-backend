-- Migration: Add routes and route_stops tables for V1 atomic route creation

-- ============================================================
-- routes table
-- ============================================================
CREATE TABLE IF NOT EXISTS routes (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'draft',
    driver_id   UUID,
    route_date  DATE NOT NULL,
    version     INT  NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_routes_brand_id   ON routes (brand_id);
CREATE INDEX IF NOT EXISTS idx_routes_route_date ON routes (route_date);
CREATE INDEX IF NOT EXISTS idx_routes_driver_id  ON routes (driver_id);

-- ============================================================
-- route_stops table
-- ============================================================
CREATE TABLE IF NOT EXISTS route_stops (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id   UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    order_id   INT  NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sequence   INT  NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_route_stop_sequence UNIQUE (route_id, sequence)
);

-- Indexes for joins
CREATE INDEX IF NOT EXISTS idx_route_stops_route_id  ON route_stops (route_id);
CREATE INDEX IF NOT EXISTS idx_route_stops_order_id  ON route_stops (order_id);

-- ============================================================
-- Auto-update updated_at triggers
-- ============================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_routes_updated_at ON routes;
CREATE TRIGGER trg_routes_updated_at
    BEFORE UPDATE ON routes
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_route_stops_updated_at ON route_stops;
CREATE TRIGGER trg_route_stops_updated_at
    BEFORE UPDATE ON route_stops
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
