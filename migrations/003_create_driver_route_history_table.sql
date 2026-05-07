CREATE TABLE IF NOT EXISTS driver_route_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    route_id UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id UUID NOT NULL,
    event_type VARCHAR(30) NOT NULL CHECK (event_type IN (
        'route_started','stop_arrived','stop_departed','stop_completed',
        'stop_failed','stop_skipped','route_completed','route_paused',
        'route_resumed','break_started','break_ended','deviation_detected'
    )),
    stop_id UUID REFERENCES route_stops(id) ON DELETE SET NULL,
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    address_snapshot TEXT,
    event_metadata JSONB DEFAULT '{}',
    notes TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX ix_driver_route_history_route ON driver_route_history(route_id, recorded_at ASC);
CREATE INDEX ix_driver_route_history_driver ON driver_route_history(driver_id, recorded_at DESC);
