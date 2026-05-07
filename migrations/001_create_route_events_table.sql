-- Migration: Create route_events table for audit logging
-- Phase 5: Route event audit trail

CREATE TABLE IF NOT EXISTS route_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    actor_type VARCHAR(20) NOT NULL,
    actor_id UUID NOT NULL,
    event_metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT ck_event_type CHECK (event_type IN ('published', 'driver_assigned', 'stop_status_changed', 'collection_recorded', 'stops_reordered', 'route_completed')),
    CONSTRAINT ck_actor_type CHECK (actor_type IN ('admin', 'driver'))
);

CREATE INDEX IF NOT EXISTS ix_route_events_route_id ON route_events(route_id);
CREATE INDEX IF NOT EXISTS ix_route_events_org_id ON route_events(org_id);
CREATE INDEX IF NOT EXISTS ix_route_events_created_at ON route_events(created_at);
