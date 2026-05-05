-- Create orders table for LeafLink order storage
-- Stores orders synced from LeafLink API
-- Scoped by org_id and brand_id for multi-tenant isolation

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
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
    external_created_at TIMESTAMP WITH TIME ZONE,
    external_updated_at TIMESTAMP WITH TIME ZONE,
    synced_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_synced_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    sync_run_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_brand_external_order UNIQUE (brand_id, external_order_id)
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS ix_orders_brand_id ON orders(brand_id);
CREATE INDEX IF NOT EXISTS ix_orders_external_order_id ON orders(external_order_id);
CREATE INDEX IF NOT EXISTS ix_orders_order_number ON orders(order_number);
CREATE INDEX IF NOT EXISTS ix_orders_synced_at ON orders(synced_at DESC);
CREATE INDEX IF NOT EXISTS ix_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS ix_orders_review_status ON orders(review_status);

-- Create order_lines table for line items
CREATE TABLE IF NOT EXISTS order_lines (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
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
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes for order_lines
CREATE INDEX IF NOT EXISTS ix_order_lines_order_id ON order_lines(order_id);
CREATE INDEX IF NOT EXISTS ix_order_lines_sku ON order_lines(sku);
CREATE INDEX IF NOT EXISTS ix_order_lines_mapping_status ON order_lines(mapping_status);

-- Create sync_runs table for tracking sync operations
CREATE TABLE IF NOT EXISTS sync_runs (
    id BIGSERIAL PRIMARY KEY,
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
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_progress_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    error_count INTEGER NOT NULL DEFAULT 0,
    stalled_reason VARCHAR(255),
    worker_id VARCHAR(100),
    last_heartbeat_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create indexes for sync_runs
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_id ON sync_runs(brand_id);
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_status ON sync_runs(brand_id, status);
CREATE INDEX IF NOT EXISTS ix_sync_runs_brand_started ON sync_runs(brand_id, started_at);

-- Create sync_requests table for sync queue
CREATE TABLE IF NOT EXISTS sync_requests (
    id BIGSERIAL PRIMARY KEY,
    brand_id VARCHAR(120) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    start_page INTEGER DEFAULT 1,
    total_pages INTEGER,
    total_orders_available INTEGER,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Create indexes for sync_requests
CREATE INDEX IF NOT EXISTS ix_sync_requests_brand_id ON sync_requests(brand_id);
CREATE INDEX IF NOT EXISTS ix_sync_requests_status ON sync_requests(status);
