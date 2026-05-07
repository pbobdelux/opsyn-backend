CREATE TABLE IF NOT EXISTS driver_locations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    org_id UUID NOT NULL,
    route_id UUID REFERENCES routes(id) ON DELETE SET NULL,
    latitude NUMERIC(10,7) NOT NULL,
    longitude NUMERIC(10,7) NOT NULL,
    accuracy_meters NUMERIC(8,2),
    speed_mph NUMERIC(6,2),
    heading NUMERIC(5,2),
    altitude_meters NUMERIC(8,2),
    battery_percent INTEGER,
    is_moving BOOLEAN DEFAULT false,
    source VARCHAR(20) DEFAULT 'gps' CHECK (source IN ('gps','network','manual')),
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX ix_driver_locations_driver_recorded ON driver_locations(driver_id, recorded_at DESC);
CREATE INDEX ix_driver_locations_route ON driver_locations(route_id);
CREATE INDEX ix_driver_locations_org_time ON driver_locations(org_id, recorded_at DESC);

-- NOTE: In production, partition this table by recorded_at monthly.
-- Data grows fast (10-30 pings/second per driver). Consider archiving
-- locations older than 90 days to a separate table or S3.
