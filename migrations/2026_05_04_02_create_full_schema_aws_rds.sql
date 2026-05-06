-- ============================================================================
-- COMPREHENSIVE SCHEMA MIGRATION FOR AWS RDS OPSYN DATABASE
-- ============================================================================
-- This migration creates all required application tables.
-- It preserves existing auth tables (employees, passcodes, etc.)
-- Run this ONCE on AWS RDS to bring it to parity with Railway Postgres.
-- ============================================================================

-- ============================================================================
-- BRAND API CREDENTIALS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS brand_api_credentials (
    id SERIAL PRIMARY KEY,
    brand_id VARCHAR(120) NOT NULL,
    integration_name VARCHAR(50) NOT NULL DEFAULT 'leaflink',
    base_url VARCHAR(255),
    api_key TEXT,
    vendor_key TEXT,
    company_id VARCHAR(120),
    is_active BOOLEAN NOT NULL DEFAULT true,
    sync_status VARCHAR(50) NOT NULL DEFAULT 'idle',
    last_sync_at TIMESTAMPTZ,
    last_checked_at TIMESTAMPTZ,
    last_error TEXT,
    last_synced_page INTEGER DEFAULT 0,
    total_pages_available INTEGER,
    total_orders_available INTEGER,
    auth_scheme VARCHAR(20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(brand_id, integration_name)
);

CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_brand_id ON brand_api_credentials(brand_id);
CREATE INDEX IF NOT EXISTS ix_brand_api_credentials_integration ON brand_api_credentials(integration_name);

-- ============================================================================
-- ORDERS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    brand_id VARCHAR(120) NOT NULL,
    external_order_id VARCHAR(120) NOT NULL,
    order_number VARCHAR(120),
    customer_name VARCHAR(255),
    status VARCHAR(80),
    total_cents INTEGER,
    amount NUMERIC(12, 2),
    item_count INTEGER,
    unit_count INTEGER,
    line_items_json JSONB,
    source VARCHAR(50) NOT NULL DEFAULT 'leaflink',
    review_status VARCHAR(50),
    sync_status VARCHAR(50) NOT NULL DEFAULT 'ok',
    raw_payload JSONB,
    external_created_at TIMESTAMPTZ,
    external_updated_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sync_run_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(brand_id, external_order_id)
);

CREATE INDEX IF NOT EXISTS ix_orders_brand_id ON orders(brand_id);
CREATE INDEX IF NOT EXISTS ix_orders_order_number ON orders(order_number);
CREATE INDEX IF NOT EXISTS ix_orders_external_order_id ON orders(external_order_id);
CREATE INDEX IF NOT EXISTS ix_orders_sync_run_id ON orders(sync_run_id);
CREATE INDEX IF NOT EXISTS ix_orders_customer_name ON orders(customer_name);

-- ============================================================================
-- ORDER LINES TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_lines (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku VARCHAR(255),
    product_name VARCHAR(255),
    quantity INTEGER,
    pulled_qty INTEGER NOT NULL DEFAULT 0,
    packed_qty INTEGER NOT NULL DEFAULT 0,
    unit_price_cents INTEGER,
    total_price_cents INTEGER,
    unit_price NUMERIC(12, 2),
    total_price NUMERIC(12, 2),
    mapped_product_id VARCHAR(120),
    mapping_status VARCHAR(50) DEFAULT 'unknown',
    mapping_issue VARCHAR(255),
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_order_lines_order_id ON order_lines(order_id);
CREATE INDEX IF NOT EXISTS ix_order_lines_sku ON order_lines(sku);

-- ============================================================================
-- ORGANIZATION BRAND BINDINGS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS organization_brand_bindings (
    id SERIAL PRIMARY KEY,
    org_id VARCHAR(120) NOT NULL,
    brand_id VARCHAR(120) NOT NULL,
    brand_name VARCHAR(255),
    source VARCHAR(50) DEFAULT 'manual',
    is_default BOOLEAN NOT NULL DEFAULT true,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, brand_id)
);

CREATE INDEX IF NOT EXISTS ix_org_brand_bindings_org_id ON organization_brand_bindings(org_id);
CREATE INDEX IF NOT EXISTS ix_org_brand_bindings_brand_id ON organization_brand_bindings(brand_id);

-- ============================================================================
-- DRIVERS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    org_id VARCHAR(120) NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    license_plate VARCHAR(50),
    status VARCHAR(50) NOT NULL DEFAULT 'available',
    pin VARCHAR(4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_drivers_org_id ON drivers(org_id);
CREATE INDEX IF NOT EXISTS ix_drivers_email ON drivers(email);

-- ============================================================================
-- DISPATCH ROUTES TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS dispatch_routes (
    id SERIAL PRIMARY KEY,
    org_id VARCHAR(120) NOT NULL,
    driver_id INTEGER REFERENCES drivers(id) ON DELETE SET NULL,
    name VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    scheduled_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_dispatch_routes_org_id ON dispatch_routes(org_id);
CREATE INDEX IF NOT EXISTS ix_dispatch_routes_driver_id ON dispatch_routes(driver_id);

-- ============================================================================
-- ROUTE STOPS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS route_stops (
    id SERIAL PRIMARY KEY,
    route_id INTEGER NOT NULL REFERENCES dispatch_routes(id) ON DELETE CASCADE,
    order_id INTEGER REFERENCES orders(id) ON DELETE SET NULL,
    stop_order INTEGER NOT NULL DEFAULT 0,
    address VARCHAR(500),
    customer_name VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_route_stops_route_id ON route_stops(route_id);
CREATE INDEX IF NOT EXISTS ix_route_stops_order_id ON route_stops(order_id);

-- ============================================================================
-- TENANT CREDENTIALS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS tenant_credentials (
    id SERIAL PRIMARY KEY,
    org_id VARCHAR(120) NOT NULL UNIQUE,
    api_secret VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_tenant_credentials_org_id ON tenant_credentials(org_id);

-- ============================================================================
-- SYNC RUNS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS sync_runs (
    id SERIAL PRIMARY KEY,
    brand_id VARCHAR(255) NOT NULL,
    integration_name VARCHAR(50) NOT NULL DEFAULT 'leaflink',
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    mode VARCHAR(20) NOT NULL DEFAULT 'incremental',
    pages_synced INTEGER NOT NULL DEFAULT 0,
    total_pages INTEGER,
    orders_loaded_this_run INTEGER NOT NULL DEFAULT 0,
    total_orders_available INTEGER,
    current_cursor TEXT,
    current_page INTEGER DEFAULT 1,
    last_successful_cursor TEXT,
    last_successful_page INTEGER,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_progress_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    error_count INTEGER NOT NULL DEFAULT 0,
    stalled_reason VARCHAR(255),
    worker_id VARCHAR(100),
    last_heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_id ON sync_runs(brand_id);
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_status ON sync_runs(brand_id, status);
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_started ON sync_runs(brand_id, started_at);

-- ============================================================================
-- SYNC REQUESTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS sync_requests (
    id SERIAL PRIMARY KEY,
    brand_id VARCHAR(120) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    start_page INTEGER DEFAULT 1,
    total_pages INTEGER,
    total_orders_available INTEGER,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_sync_requests_brand_id ON sync_requests(brand_id);
CREATE INDEX IF NOT EXISTS ix_sync_requests_status ON sync_requests(status);

-- ============================================================================
-- ASSISTANT TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS assistant_sessions (
    id SERIAL PRIMARY KEY,
    org_id VARCHAR(120) NOT NULL,
    user_id VARCHAR(120),
    session_key VARCHAR(255),
    context JSONB,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ensure session_key column exists even if the table was created by an earlier
-- schema version (e.g. via SQLAlchemy create_all) that did not include it.
ALTER TABLE assistant_sessions
    ADD COLUMN IF NOT EXISTS session_key VARCHAR(255);

-- Add unique constraint only if it does not already exist.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'assistant_sessions_session_key_key'
          AND conrelid = 'assistant_sessions'::regclass
    ) THEN
        ALTER TABLE assistant_sessions
            ADD CONSTRAINT assistant_sessions_session_key_key UNIQUE (session_key);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_assistant_sessions_org_id ON assistant_sessions(org_id);
CREATE INDEX IF NOT EXISTS ix_assistant_sessions_session_key ON assistant_sessions(session_key);

CREATE TABLE IF NOT EXISTS assistant_messages (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_assistant_messages_session_id ON assistant_messages(session_id);

CREATE TABLE IF NOT EXISTS assistant_pending_actions (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    action_type VARCHAR(100) NOT NULL,
    action_data JSONB,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_assistant_pending_actions_session_id ON assistant_pending_actions(session_id);

CREATE TABLE IF NOT EXISTS assistant_audit_logs (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES assistant_sessions(id) ON DELETE SET NULL,
    action VARCHAR(255) NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_assistant_audit_logs_session_id ON assistant_audit_logs(session_id);

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================
-- All tables created successfully.
-- Existing auth tables (employees, passcodes, etc.) are preserved.
-- The database is now ready for the application to connect.
