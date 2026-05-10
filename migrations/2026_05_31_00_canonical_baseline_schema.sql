CREATE TABLE IF NOT EXISTS organizations (
    id         VARCHAR(255) PRIMARY KEY,
    slug       VARCHAR(255) UNIQUE NOT NULL,
    name       VARCHAR(255) NOT NULL,
    is_active  BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS brands (
    id         VARCHAR(255) PRIMARY KEY,
    org_id     VARCHAR(255) NOT NULL REFERENCES organizations(id),
    slug       VARCHAR(255) NOT NULL,
    name       VARCHAR(255) NOT NULL,
    is_active  BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS employees (
    id         VARCHAR(255) PRIMARY KEY,
    org_id     VARCHAR(255) NOT NULL REFERENCES organizations(id),
    first_name VARCHAR(255) NOT NULL,
    last_name  VARCHAR(255) NOT NULL,
    email      VARCHAR(255) UNIQUE NOT NULL,
    is_active  BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS employee_passcodes (
    id            VARCHAR(255) PRIMARY KEY,
    employee_id   VARCHAR(255) NOT NULL REFERENCES employees(id),
    passcode_hash VARCHAR(255) NOT NULL,
    is_active     BOOLEAN DEFAULT true,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS employee_brand_access (
    id          VARCHAR(255) PRIMARY KEY,
    employee_id VARCHAR(255) NOT NULL REFERENCES employees(id),
    brand_id    VARCHAR(255) NOT NULL REFERENCES brands(id),
    role        VARCHAR(50) DEFAULT 'viewer',
    is_active   BOOLEAN DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS employee_app_access (
    id          VARCHAR(255) PRIMARY KEY,
    employee_id VARCHAR(255) NOT NULL REFERENCES employees(id),
    app_id      VARCHAR(255) NOT NULL,
    role        VARCHAR(50) DEFAULT 'viewer',
    is_active   BOOLEAN DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'employees') THEN
        RAISE NOTICE '[MIGRATION] employees table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'employees' AND column_name = 'email') THEN
        RAISE NOTICE '[MIGRATION] email column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_employees_email') THEN
        CREATE INDEX idx_employees_email ON employees(email);
        RAISE NOTICE '[MIGRATION] Created index idx_employees_email';
    ELSE
        RAISE NOTICE '[MIGRATION] Index idx_employees_email already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'employees') THEN
        RAISE NOTICE '[MIGRATION] employees table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'employees' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_employees_org_id') THEN
        CREATE INDEX idx_employees_org_id ON employees(org_id);
        RAISE NOTICE '[MIGRATION] Created index idx_employees_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index idx_employees_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'employee_passcodes') THEN
        RAISE NOTICE '[MIGRATION] employee_passcodes table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'employee_passcodes' AND column_name = 'employee_id') THEN
        RAISE NOTICE '[MIGRATION] employee_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_employee_passcodes_employee_id') THEN
        CREATE INDEX idx_employee_passcodes_employee_id ON employee_passcodes(employee_id);
        RAISE NOTICE '[MIGRATION] Created index idx_employee_passcodes_employee_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index idx_employee_passcodes_employee_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'employee_brand_access') THEN
        RAISE NOTICE '[MIGRATION] employee_brand_access table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'employee_brand_access' AND column_name = 'employee_id') THEN
        RAISE NOTICE '[MIGRATION] employee_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_employee_brand_access_employee_id') THEN
        CREATE INDEX idx_employee_brand_access_employee_id ON employee_brand_access(employee_id);
        RAISE NOTICE '[MIGRATION] Created index idx_employee_brand_access_employee_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index idx_employee_brand_access_employee_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'employee_app_access') THEN
        RAISE NOTICE '[MIGRATION] employee_app_access table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'employee_app_access' AND column_name = 'employee_id') THEN
        RAISE NOTICE '[MIGRATION] employee_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_employee_app_access_employee_id') THEN
        CREATE INDEX idx_employee_app_access_employee_id ON employee_app_access(employee_id);
        RAISE NOTICE '[MIGRATION] Created index idx_employee_app_access_employee_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index idx_employee_app_access_employee_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS brand_api_credentials (
    id                         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id                   VARCHAR(120) NOT NULL,
    integration_name           VARCHAR(50) NOT NULL DEFAULT 'leaflink',
    base_url                   VARCHAR(255),
    api_key                    TEXT,
    api_key_secret_ref         VARCHAR(255),
    api_key_last4              VARCHAR(4),
    vendor_key                 TEXT,
    company_id                 VARCHAR(120),
    is_active                  BOOLEAN NOT NULL DEFAULT true,
    sync_status                VARCHAR(50) NOT NULL DEFAULT 'idle',
    last_sync_at               TIMESTAMPTZ,
    last_checked_at            TIMESTAMPTZ,
    last_error                 TEXT,
    last_synced_page           INTEGER DEFAULT 0,
    total_pages_available      INTEGER,
    total_orders_available     INTEGER,
    auth_scheme                VARCHAR(20),
    webhook_key                TEXT,
    webhook_key_secret_ref     VARCHAR(255),
    webhook_key_last4          VARCHAR(4),
    webhook_enabled            BOOLEAN NOT NULL DEFAULT false,
    webhook_signature_required BOOLEAN NOT NULL DEFAULT true,
    leaflink_company_id        VARCHAR(120),
    org_id                     VARCHAR(120),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(brand_id, integration_name)
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'brand_api_credentials') THEN
        RAISE NOTICE '[MIGRATION] brand_api_credentials table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'brand_api_credentials' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_brand_api_credentials_brand_id') THEN
        CREATE INDEX ix_brand_api_credentials_brand_id ON brand_api_credentials(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_brand_api_credentials_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_brand_api_credentials_brand_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'brand_api_credentials') THEN
        RAISE NOTICE '[MIGRATION] brand_api_credentials table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'brand_api_credentials' AND column_name = 'integration_name') THEN
        RAISE NOTICE '[MIGRATION] integration_name column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_brand_api_credentials_integration') THEN
        CREATE INDEX ix_brand_api_credentials_integration ON brand_api_credentials(integration_name);
        RAISE NOTICE '[MIGRATION] Created index ix_brand_api_credentials_integration';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_brand_api_credentials_integration already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'brand_api_credentials') THEN
        RAISE NOTICE '[MIGRATION] brand_api_credentials table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'brand_api_credentials' AND column_name = 'sync_status') THEN
        RAISE NOTICE '[MIGRATION] sync_status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_brand_api_credentials_sync_status') THEN
        CREATE INDEX ix_brand_api_credentials_sync_status ON brand_api_credentials(sync_status) WHERE sync_status = 'syncing';
        RAISE NOTICE '[MIGRATION] Created index ix_brand_api_credentials_sync_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_brand_api_credentials_sync_status already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'brand_api_credentials') THEN
        RAISE NOTICE '[MIGRATION] brand_api_credentials table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'brand_api_credentials' AND column_name = 'leaflink_company_id') THEN
        RAISE NOTICE '[MIGRATION] leaflink_company_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_brand_api_credentials_leaflink_company_id') THEN
        CREATE INDEX ix_brand_api_credentials_leaflink_company_id ON brand_api_credentials(leaflink_company_id);
        RAISE NOTICE '[MIGRATION] Created index ix_brand_api_credentials_leaflink_company_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_brand_api_credentials_leaflink_company_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'brand_api_credentials') THEN
        RAISE NOTICE '[MIGRATION] brand_api_credentials table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'brand_api_credentials' AND column_name = 'webhook_enabled') THEN
        RAISE NOTICE '[MIGRATION] webhook_enabled column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_brand_api_credentials_webhook_enabled') THEN
        CREATE INDEX ix_brand_api_credentials_webhook_enabled ON brand_api_credentials(webhook_enabled) WHERE webhook_enabled = true;
        RAISE NOTICE '[MIGRATION] Created index ix_brand_api_credentials_webhook_enabled';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_brand_api_credentials_webhook_enabled already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'brand_api_credentials') THEN
        RAISE NOTICE '[MIGRATION] brand_api_credentials table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'brand_api_credentials' AND column_name = 'api_key_secret_ref') THEN
        RAISE NOTICE '[MIGRATION] api_key_secret_ref column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_brand_api_credentials_api_key_secret_ref') THEN
        CREATE INDEX ix_brand_api_credentials_api_key_secret_ref ON brand_api_credentials(api_key_secret_ref) WHERE api_key_secret_ref IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_brand_api_credentials_api_key_secret_ref';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_brand_api_credentials_api_key_secret_ref already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS sync_runs (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id               VARCHAR(255) NOT NULL,
    integration_name       VARCHAR(50) NOT NULL DEFAULT 'leaflink',
    status                 VARCHAR(20) NOT NULL DEFAULT 'queued',
    mode                   VARCHAR(20) NOT NULL DEFAULT 'incremental',
    pages_synced           INTEGER NOT NULL DEFAULT 0,
    total_pages            INTEGER,
    orders_loaded_this_run INTEGER NOT NULL DEFAULT 0,
    total_orders_available INTEGER,
    current_cursor         TEXT,
    current_page           INTEGER DEFAULT 1,
    last_successful_cursor TEXT,
    last_successful_page   INTEGER,
    last_next_url          TEXT,
    started_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_progress_at       TIMESTAMPTZ,
    completed_at           TIMESTAMPTZ,
    last_error             TEXT,
    error_count            INTEGER NOT NULL DEFAULT 0,
    stalled_reason         VARCHAR(255),
    worker_id              VARCHAR(100),
    last_heartbeat_at      TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_runs') THEN
        RAISE NOTICE '[MIGRATION] sync_runs table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_runs' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_runs_brand_id') THEN
        CREATE INDEX ix_sync_runs_brand_id ON sync_runs(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_runs_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_runs_brand_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_runs') THEN
        RAISE NOTICE '[MIGRATION] sync_runs table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_runs' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_runs' AND column_name = 'status') THEN
        RAISE NOTICE '[MIGRATION] status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_runs_brand_status') THEN
        CREATE INDEX ix_sync_runs_brand_status ON sync_runs(brand_id, status);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_runs_brand_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_runs_brand_status already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_runs') THEN
        RAISE NOTICE '[MIGRATION] sync_runs table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_runs' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_runs' AND column_name = 'started_at') THEN
        RAISE NOTICE '[MIGRATION] started_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_runs_brand_started') THEN
        CREATE INDEX ix_sync_runs_brand_started ON sync_runs(brand_id, started_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_runs_brand_started';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_runs_brand_started already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS sync_requests (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id               VARCHAR(120) NOT NULL,
    org_id                 VARCHAR(120),
    status                 VARCHAR(50) DEFAULT 'pending',
    start_page             INTEGER DEFAULT 1,
    total_pages            INTEGER,
    total_orders_available INTEGER,
    error                  TEXT,
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    started_at             TIMESTAMPTZ,
    completed_at           TIMESTAMPTZ
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_requests') THEN
        RAISE NOTICE '[MIGRATION] sync_requests table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_requests' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_requests_brand_id') THEN
        CREATE INDEX ix_sync_requests_brand_id ON sync_requests(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_requests_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_requests_brand_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_requests') THEN
        RAISE NOTICE '[MIGRATION] sync_requests table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_requests' AND column_name = 'status') THEN
        RAISE NOTICE '[MIGRATION] status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_requests_status') THEN
        CREATE INDEX ix_sync_requests_status ON sync_requests(status) WHERE status = 'pending';
        RAISE NOTICE '[MIGRATION] Created index ix_sync_requests_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_requests_status already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_requests') THEN
        RAISE NOTICE '[MIGRATION] sync_requests table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_requests' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_requests_org_id') THEN
        CREATE INDEX ix_sync_requests_org_id ON sync_requests(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_requests_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_requests_org_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS drivers (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id         UUID NOT NULL,
    name           VARCHAR(255) NOT NULL,
    email          VARCHAR(255),
    phone          VARCHAR(50),
    status         VARCHAR(20) NOT NULL DEFAULT 'active',
    passcode_hash  VARCHAR(255),
    invite_code    VARCHAR(50),
    license_plate  VARCHAR(50),
    vehicle_type   VARCHAR(50),
    notes          TEXT,
    preferences    JSON NOT NULL DEFAULT '{}',
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deactivated_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT ck_drivers_status CHECK (status IN ('active', 'inactive'))
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'drivers') THEN
        RAISE NOTICE '[MIGRATION] drivers table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'drivers' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_drivers_org_id') THEN
        CREATE INDEX ix_drivers_org_id ON drivers(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_drivers_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_drivers_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'drivers') THEN
        RAISE NOTICE '[MIGRATION] drivers table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'drivers' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'drivers' AND column_name = 'status') THEN
        RAISE NOTICE '[MIGRATION] status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_drivers_org_status') THEN
        CREATE INDEX ix_drivers_org_status ON drivers(org_id, status);
        RAISE NOTICE '[MIGRATION] Created index ix_drivers_org_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_drivers_org_status already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'drivers') THEN
        RAISE NOTICE '[MIGRATION] drivers table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'drivers' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'drivers' AND column_name = 'email') THEN
        RAISE NOTICE '[MIGRATION] email column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'uq_drivers_org_email_partial') THEN
        CREATE UNIQUE INDEX uq_drivers_org_email_partial ON drivers(org_id, email) WHERE email IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index uq_drivers_org_email_partial';
    ELSE
        RAISE NOTICE '[MIGRATION] Index uq_drivers_org_email_partial already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS routes (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id             UUID NOT NULL,
    route_number       VARCHAR(50),
    status             VARCHAR(30) NOT NULL DEFAULT 'draft',
    assigned_driver_id UUID REFERENCES drivers(id),
    route_date         DATE NOT NULL,
    total_stops        INTEGER NOT NULL DEFAULT 0,
    total_value        NUMERIC(12, 2) NOT NULL DEFAULT 0,
    total_units        INTEGER NOT NULL DEFAULT 0,
    notes              TEXT,
    created_by         UUID,
    version            INTEGER NOT NULL DEFAULT 1,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at       TIMESTAMP WITH TIME ZONE,
    CONSTRAINT ck_routes_status CHECK (status IN ('draft', 'assigned', 'out_for_delivery', 'completed', 'cancelled'))
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'routes') THEN
        RAISE NOTICE '[MIGRATION] routes table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'routes' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_routes_org_id') THEN
        CREATE INDEX ix_routes_org_id ON routes(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_routes_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_routes_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'routes') THEN
        RAISE NOTICE '[MIGRATION] routes table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'routes' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'routes' AND column_name = 'route_date') THEN
        RAISE NOTICE '[MIGRATION] route_date column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_routes_org_date') THEN
        CREATE INDEX ix_routes_org_date ON routes(org_id, route_date);
        RAISE NOTICE '[MIGRATION] Created index ix_routes_org_date';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_routes_org_date already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'routes') THEN
        RAISE NOTICE '[MIGRATION] routes table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'routes' AND column_name = 'assigned_driver_id') THEN
        RAISE NOTICE '[MIGRATION] assigned_driver_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_routes_driver') THEN
        CREATE INDEX ix_routes_driver ON routes(assigned_driver_id);
        RAISE NOTICE '[MIGRATION] Created index ix_routes_driver';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_routes_driver already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS route_stops (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id         UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id           UUID NOT NULL,
    stop_order       INTEGER NOT NULL,
    stop_type        VARCHAR(30) NOT NULL DEFAULT 'leaflink_order',
    source_order_id  UUID,
    customer_name    VARCHAR(255),
    stop_name        VARCHAR(255),
    address          TEXT,
    contact_name     VARCHAR(255),
    contact_phone    VARCHAR(50),
    notes            TEXT,
    time_window      VARCHAR(100),
    priority         INTEGER NOT NULL DEFAULT 0,
    status           VARCHAR(20) NOT NULL DEFAULT 'pending',
    ar_status        VARCHAR(30) NOT NULL DEFAULT 'not_applicable',
    amount_due       NUMERIC(12, 2) NOT NULL DEFAULT 0,
    amount_collected NUMERIC(12, 2) NOT NULL DEFAULT 0,
    completed_at     TIMESTAMP WITH TIME ZONE,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_route_stops_type CHECK (stop_type IN ('leaflink_order', 'manual_stop', 'bank', 'processor_pickup', 'sample_dropoff', 'supply_pickup', 'other')),
    CONSTRAINT ck_route_stops_status CHECK (status IN ('pending', 'arrived', 'completed', 'failed', 'skipped')),
    CONSTRAINT ck_route_stops_ar_status CHECK (ar_status IN ('unpaid', 'partial', 'paid', 'collection_issue', 'not_applicable')),
    CONSTRAINT uq_route_stops_order UNIQUE (route_id, stop_order)
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'route_stops') THEN
        RAISE NOTICE '[MIGRATION] route_stops table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'route_stops' AND column_name = 'route_id') THEN
        RAISE NOTICE '[MIGRATION] route_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_route_stops_route_id') THEN
        CREATE INDEX ix_route_stops_route_id ON route_stops(route_id);
        RAISE NOTICE '[MIGRATION] Created index ix_route_stops_route_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_route_stops_route_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'route_stops') THEN
        RAISE NOTICE '[MIGRATION] route_stops table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'route_stops' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_route_stops_org_id') THEN
        CREATE INDEX ix_route_stops_org_id ON route_stops(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_route_stops_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_route_stops_org_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS orders (
    id                         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id                   VARCHAR(120) NOT NULL,
    org_id                     VARCHAR(120),
    external_order_id          VARCHAR(120) NOT NULL,
    order_number               VARCHAR(120),
    customer_name              VARCHAR(255),
    status                     VARCHAR(80),
    total_cents                INTEGER,
    amount                     NUMERIC(12, 2),
    item_count                 INTEGER,
    unit_count                 INTEGER,
    line_items_json            JSONB,
    source                     VARCHAR(50) NOT NULL DEFAULT 'leaflink',
    review_status              VARCHAR(50),
    sync_status                VARCHAR(50) NOT NULL DEFAULT 'ok',
    raw_payload                JSONB,
    external_created_at        TIMESTAMPTZ,
    external_updated_at        TIMESTAMPTZ,
    synced_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_synced_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sync_run_id                UUID REFERENCES sync_runs(id),
    assigned_driver_id         UUID REFERENCES drivers(id),
    assigned_driver_name       VARCHAR(255),
    delivery_status            VARCHAR(30) DEFAULT 'pending',
    delivery_date              DATE,
    route_number               VARCHAR(50),
    route_id                   UUID REFERENCES routes(id),
    driver_note                TEXT,
    delivery_instructions      TEXT,
    payment_status             VARCHAR(30) DEFAULT 'unpaid',
    amount_paid                NUMERIC(12, 2) DEFAULT 0,
    balance_due                NUMERIC(12, 2) DEFAULT 0,
    due_date                   DATE,
    days_overdue               INTEGER DEFAULT 0,
    invoice_number             VARCHAR(100),
    ar_note                    TEXT,
    sync_health                JSONB,
    sync_health_status         VARCHAR(20),
    sync_health_missing_fields JSONB,
    sync_health_last_error     TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(brand_id, external_order_id),
    CONSTRAINT ck_orders_delivery_status CHECK (delivery_status IN ('pending', 'assigned', 'out_for_delivery', 'delivered', 'failed', 'needs_reschedule', 'cancelled')),
    CONSTRAINT ck_orders_payment_status CHECK (payment_status IN ('unpaid', 'partial', 'paid', 'overdue', 'collection_issue', 'write_off'))
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_brand_id') THEN
        CREATE INDEX ix_orders_brand_id ON orders(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_brand_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'order_number') THEN
        RAISE NOTICE '[MIGRATION] order_number column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_order_number') THEN
        CREATE INDEX ix_orders_order_number ON orders(order_number);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_order_number';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_order_number already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'external_order_id') THEN
        RAISE NOTICE '[MIGRATION] external_order_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_external_order_id') THEN
        CREATE INDEX ix_orders_external_order_id ON orders(external_order_id);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_external_order_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_external_order_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'sync_run_id') THEN
        RAISE NOTICE '[MIGRATION] sync_run_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_sync_run_id') THEN
        CREATE INDEX ix_orders_sync_run_id ON orders(sync_run_id) WHERE sync_run_id IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_orders_sync_run_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_sync_run_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'customer_name') THEN
        RAISE NOTICE '[MIGRATION] customer_name column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_customer_name') THEN
        CREATE INDEX ix_orders_customer_name ON orders(customer_name);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_customer_name';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_customer_name already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_org_id') THEN
        CREATE INDEX ix_orders_org_id ON orders(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_org_brand') THEN
        CREATE INDEX ix_orders_org_brand ON orders(org_id, brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_org_brand';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_org_brand already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'assigned_driver_id') THEN
        RAISE NOTICE '[MIGRATION] assigned_driver_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_assigned_driver') THEN
        CREATE INDEX ix_orders_assigned_driver ON orders(assigned_driver_id);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_assigned_driver';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_assigned_driver already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'delivery_status') THEN
        RAISE NOTICE '[MIGRATION] delivery_status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_delivery_status') THEN
        CREATE INDEX ix_orders_delivery_status ON orders(delivery_status);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_delivery_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_delivery_status already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'payment_status') THEN
        RAISE NOTICE '[MIGRATION] payment_status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_payment_status') THEN
        CREATE INDEX ix_orders_payment_status ON orders(payment_status);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_payment_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_payment_status already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'route_id') THEN
        RAISE NOTICE '[MIGRATION] route_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_route_id') THEN
        CREATE INDEX ix_orders_route_id ON orders(route_id);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_route_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_route_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'updated_at') THEN
        RAISE NOTICE '[MIGRATION] updated_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_brand_updated_at') THEN
        CREATE INDEX ix_orders_brand_updated_at ON orders(brand_id, updated_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_brand_updated_at';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_brand_updated_at already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'created_at') THEN
        RAISE NOTICE '[MIGRATION] created_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_brand_created_at') THEN
        CREATE INDEX ix_orders_brand_created_at ON orders(brand_id, created_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_orders_brand_created_at';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_brand_created_at already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
        RAISE NOTICE '[MIGRATION] orders table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'sync_health_status') THEN
        RAISE NOTICE '[MIGRATION] sync_health_status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_orders_sync_health_status') THEN
        CREATE INDEX ix_orders_sync_health_status ON orders(brand_id, sync_health_status) WHERE sync_health_status IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_orders_sync_health_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_orders_sync_health_status already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS order_lines (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id          UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku               VARCHAR(255),
    product_name      VARCHAR(255),
    quantity          INTEGER,
    pulled_qty        INTEGER NOT NULL DEFAULT 0,
    packed_qty        INTEGER NOT NULL DEFAULT 0,
    unit_price_cents  INTEGER,
    total_price_cents INTEGER,
    unit_price        NUMERIC(12, 2),
    total_price       NUMERIC(12, 2),
    mapped_product_id VARCHAR(120),
    mapping_status    VARCHAR(50) DEFAULT 'unknown',
    mapping_issue     VARCHAR(255),
    raw_payload       JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'order_lines') THEN
        RAISE NOTICE '[MIGRATION] order_lines table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'order_lines' AND column_name = 'order_id') THEN
        RAISE NOTICE '[MIGRATION] order_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_order_lines_order_id') THEN
        CREATE INDEX ix_order_lines_order_id ON order_lines(order_id);
        RAISE NOTICE '[MIGRATION] Created index ix_order_lines_order_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_order_lines_order_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'order_lines') THEN
        RAISE NOTICE '[MIGRATION] order_lines table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'order_lines' AND column_name = 'sku') THEN
        RAISE NOTICE '[MIGRATION] sku column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_order_lines_sku') THEN
        CREATE INDEX ix_order_lines_sku ON order_lines(sku);
        RAISE NOTICE '[MIGRATION] Created index ix_order_lines_sku';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_order_lines_sku already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'order_lines') THEN
        RAISE NOTICE '[MIGRATION] order_lines table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'order_lines' AND column_name = 'order_id') THEN
        RAISE NOTICE '[MIGRATION] order_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'order_lines' AND column_name = 'sku') THEN
        RAISE NOTICE '[MIGRATION] sku column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'order_lines' AND column_name = 'product_name') THEN
        RAISE NOTICE '[MIGRATION] product_name column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'uq_order_line_identity') THEN
        CREATE UNIQUE INDEX uq_order_line_identity ON order_lines(order_id, sku, product_name) WHERE sku IS NOT NULL AND product_name IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index uq_order_line_identity';
    ELSE
        RAISE NOTICE '[MIGRATION] Index uq_order_line_identity already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS sync_health (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id                VARCHAR(120) NOT NULL,
    last_successful_sync_at TIMESTAMP WITH TIME ZONE,
    last_attempted_sync_at  TIMESTAMP WITH TIME ZONE,
    last_error              TEXT,
    last_error_at           TIMESTAMP WITH TIME ZONE,
    consecutive_failures    INTEGER NOT NULL DEFAULT 0,
    last_page_synced        INTEGER,
    total_orders_synced     INTEGER NOT NULL DEFAULT 0,
    total_line_items_synced INTEGER NOT NULL DEFAULT 0,
    orders_fetched_last_run INTEGER NOT NULL DEFAULT 0,
    orders_written_last_run INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_sync_health_brand_id UNIQUE (brand_id)
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_health') THEN
        RAISE NOTICE '[MIGRATION] sync_health table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_health' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_health_brand_id') THEN
        CREATE INDEX ix_sync_health_brand_id ON sync_health(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_health_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_health_brand_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS sync_dead_letters (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source                    VARCHAR(120) NOT NULL DEFAULT 'leaflink',
    brand_id                  UUID NOT NULL,
    org_id                    UUID,
    external_id               VARCHAR(255),
    order_number              VARCHAR(255),
    raw_payload               JSONB NOT NULL,
    error_stage               VARCHAR(120) NOT NULL,
    error_message             TEXT NOT NULL,
    retry_count               INTEGER NOT NULL DEFAULT 0,
    last_retry_at             TIMESTAMP WITH TIME ZONE,
    resolved_at               TIMESTAMP WITH TIME ZONE,
    failure_category          VARCHAR(80),
    exception_type            VARCHAR(120),
    exception_message         TEXT,
    traceback_summary         TEXT,
    payload_keys              JSONB,
    problematic_field         VARCHAR(120),
    problematic_value_preview TEXT,
    customer_name             VARCHAR(255),
    failure_stage             VARCHAR(80),
    created_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_dead_letters') THEN
        RAISE NOTICE '[MIGRATION] sync_dead_letters table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_dead_letters_brand_id') THEN
        CREATE INDEX ix_sync_dead_letters_brand_id ON sync_dead_letters(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_dead_letters_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_dead_letters_brand_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_dead_letters') THEN
        RAISE NOTICE '[MIGRATION] sync_dead_letters table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'source') THEN
        RAISE NOTICE '[MIGRATION] source column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_dead_letters_source_brand') THEN
        CREATE INDEX ix_sync_dead_letters_source_brand ON sync_dead_letters(source, brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_dead_letters_source_brand';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_dead_letters_source_brand already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_dead_letters') THEN
        RAISE NOTICE '[MIGRATION] sync_dead_letters table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'external_id') THEN
        RAISE NOTICE '[MIGRATION] external_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_dead_letters_external_id') THEN
        CREATE INDEX ix_sync_dead_letters_external_id ON sync_dead_letters(external_id) WHERE external_id IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_sync_dead_letters_external_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_dead_letters_external_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_dead_letters') THEN
        RAISE NOTICE '[MIGRATION] sync_dead_letters table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'created_at') THEN
        RAISE NOTICE '[MIGRATION] created_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_dead_letters_unresolved') THEN
        CREATE INDEX ix_sync_dead_letters_unresolved ON sync_dead_letters(brand_id, created_at DESC) WHERE resolved_at IS NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_sync_dead_letters_unresolved';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_dead_letters_unresolved already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_dead_letters') THEN
        RAISE NOTICE '[MIGRATION] sync_dead_letters table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'last_retry_at') THEN
        RAISE NOTICE '[MIGRATION] last_retry_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_dead_letters_last_retry') THEN
        CREATE INDEX ix_sync_dead_letters_last_retry ON sync_dead_letters(brand_id, last_retry_at DESC) WHERE resolved_at IS NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_sync_dead_letters_last_retry';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_dead_letters_last_retry already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_dead_letters') THEN
        RAISE NOTICE '[MIGRATION] sync_dead_letters table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'failure_category') THEN
        RAISE NOTICE '[MIGRATION] failure_category column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_dead_letters_failure_category') THEN
        CREATE INDEX ix_sync_dead_letters_failure_category ON sync_dead_letters(brand_id, failure_category) WHERE resolved_at IS NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_sync_dead_letters_failure_category';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_dead_letters_failure_category already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_dead_letters') THEN
        RAISE NOTICE '[MIGRATION] sync_dead_letters table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_dead_letters' AND column_name = 'failure_stage') THEN
        RAISE NOTICE '[MIGRATION] failure_stage column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_dead_letters_failure_stage') THEN
        CREATE INDEX ix_sync_dead_letters_failure_stage ON sync_dead_letters(brand_id, failure_stage) WHERE resolved_at IS NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_sync_dead_letters_failure_stage';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_dead_letters_failure_stage already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS sync_metrics_snapshots (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id                  VARCHAR(120) NOT NULL,
    sync_run_id               VARCHAR(120),
    total_local_orders        INTEGER,
    total_ok                  INTEGER,
    total_partial             INTEGER,
    total_failed              INTEGER,
    dead_letter_count         INTEGER,
    count_by_failure_category JSONB,
    pages_processed           INTEGER,
    records_processed         INTEGER,
    sync_rate                 FLOAT,
    estimated_completion      TIMESTAMP WITH TIME ZONE,
    last_successful_sync_at   TIMESTAMP WITH TIME ZONE,
    updated_at                TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(brand_id, sync_run_id)
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_metrics_snapshots') THEN
        RAISE NOTICE '[MIGRATION] sync_metrics_snapshots table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_metrics_snapshots' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_metrics_snapshots_brand_id') THEN
        CREATE INDEX ix_sync_metrics_snapshots_brand_id ON sync_metrics_snapshots(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_metrics_snapshots_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_metrics_snapshots_brand_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_metrics_snapshots') THEN
        RAISE NOTICE '[MIGRATION] sync_metrics_snapshots table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_metrics_snapshots' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sync_metrics_snapshots' AND column_name = 'updated_at') THEN
        RAISE NOTICE '[MIGRATION] updated_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_sync_metrics_snapshots_brand_updated') THEN
        CREATE INDEX ix_sync_metrics_snapshots_brand_updated ON sync_metrics_snapshots(brand_id, updated_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_sync_metrics_snapshots_brand_updated';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_sync_metrics_snapshots_brand_updated already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS dead_letter_line_items (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id          VARCHAR(120) NOT NULL,
    external_order_id VARCHAR(120) NOT NULL,
    order_id          UUID REFERENCES orders(id) ON DELETE SET NULL,
    sku               VARCHAR(255),
    product_name      VARCHAR(255),
    raw_payload       JSONB,
    failure_reason    TEXT,
    failure_count     INTEGER NOT NULL DEFAULT 1,
    last_failed_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dead_letter_brand_order_sku UNIQUE (brand_id, external_order_id, sku)
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'dead_letter_line_items') THEN
        RAISE NOTICE '[MIGRATION] dead_letter_line_items table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'dead_letter_line_items' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'dead_letter_line_items' AND column_name = 'external_order_id') THEN
        RAISE NOTICE '[MIGRATION] external_order_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_dead_letter_brand_external_order') THEN
        CREATE INDEX ix_dead_letter_brand_external_order ON dead_letter_line_items(brand_id, external_order_id);
        RAISE NOTICE '[MIGRATION] Created index ix_dead_letter_brand_external_order';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_dead_letter_brand_external_order already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS organization_brand_bindings (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id     VARCHAR(120) NOT NULL,
    brand_id   VARCHAR(120) NOT NULL,
    brand_name VARCHAR(255),
    source     VARCHAR(50) DEFAULT 'manual',
    is_default BOOLEAN NOT NULL DEFAULT true,
    is_active  BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, brand_id)
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'organization_brand_bindings') THEN
        RAISE NOTICE '[MIGRATION] organization_brand_bindings table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'organization_brand_bindings' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_org_brand_bindings_org_id') THEN
        CREATE INDEX ix_org_brand_bindings_org_id ON organization_brand_bindings(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_org_brand_bindings_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_org_brand_bindings_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'organization_brand_bindings') THEN
        RAISE NOTICE '[MIGRATION] organization_brand_bindings table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'organization_brand_bindings' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_org_brand_bindings_brand_id') THEN
        CREATE INDEX ix_org_brand_bindings_brand_id ON organization_brand_bindings(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_org_brand_bindings_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_org_brand_bindings_brand_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS tenant_credentials (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id     VARCHAR(120) NOT NULL UNIQUE,
    api_secret VARCHAR(255) NOT NULL,
    is_active  BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tenant_credentials') THEN
        RAISE NOTICE '[MIGRATION] tenant_credentials table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tenant_credentials' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_tenant_credentials_org_id') THEN
        CREATE INDEX ix_tenant_credentials_org_id ON tenant_credentials(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_tenant_credentials_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_tenant_credentials_org_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS route_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id       UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id         UUID NOT NULL,
    event_type     VARCHAR(50) NOT NULL,
    actor_type     VARCHAR(20) NOT NULL,
    actor_id       UUID NOT NULL,
    event_metadata JSONB DEFAULT '{}',
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT ck_route_events_event_type CHECK (event_type IN ('published', 'driver_assigned', 'stop_status_changed', 'collection_recorded', 'stops_reordered', 'route_completed')),
    CONSTRAINT ck_route_events_actor_type CHECK (actor_type IN ('admin', 'driver'))
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'route_events') THEN
        RAISE NOTICE '[MIGRATION] route_events table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'route_events' AND column_name = 'route_id') THEN
        RAISE NOTICE '[MIGRATION] route_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_route_events_route_id') THEN
        CREATE INDEX ix_route_events_route_id ON route_events(route_id);
        RAISE NOTICE '[MIGRATION] Created index ix_route_events_route_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_route_events_route_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'route_events') THEN
        RAISE NOTICE '[MIGRATION] route_events table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'route_events' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_route_events_org_id') THEN
        CREATE INDEX ix_route_events_org_id ON route_events(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_route_events_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_route_events_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'route_events') THEN
        RAISE NOTICE '[MIGRATION] route_events table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'route_events' AND column_name = 'created_at') THEN
        RAISE NOTICE '[MIGRATION] created_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_route_events_created_at') THEN
        CREATE INDEX ix_route_events_created_at ON route_events(created_at);
        RAISE NOTICE '[MIGRATION] Created index ix_route_events_created_at';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_route_events_created_at already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS driver_locations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id       UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    org_id          UUID NOT NULL,
    route_id        UUID REFERENCES routes(id) ON DELETE SET NULL,
    latitude        NUMERIC(10, 7) NOT NULL,
    longitude       NUMERIC(10, 7) NOT NULL,
    accuracy_meters NUMERIC(8, 2),
    speed_mph       NUMERIC(6, 2),
    heading         NUMERIC(5, 2),
    altitude_meters NUMERIC(8, 2),
    battery_percent INTEGER,
    is_moving       BOOLEAN DEFAULT false,
    source          VARCHAR(20) DEFAULT 'gps' CHECK (source IN ('gps', 'network', 'manual')),
    recorded_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'driver_locations') THEN
        RAISE NOTICE '[MIGRATION] driver_locations table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_locations' AND column_name = 'driver_id') THEN
        RAISE NOTICE '[MIGRATION] driver_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_locations' AND column_name = 'recorded_at') THEN
        RAISE NOTICE '[MIGRATION] recorded_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_driver_locations_driver_recorded') THEN
        CREATE INDEX ix_driver_locations_driver_recorded ON driver_locations(driver_id, recorded_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_driver_locations_driver_recorded';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_driver_locations_driver_recorded already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'driver_locations') THEN
        RAISE NOTICE '[MIGRATION] driver_locations table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_locations' AND column_name = 'route_id') THEN
        RAISE NOTICE '[MIGRATION] route_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_driver_locations_route') THEN
        CREATE INDEX ix_driver_locations_route ON driver_locations(route_id);
        RAISE NOTICE '[MIGRATION] Created index ix_driver_locations_route';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_driver_locations_route already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'driver_locations') THEN
        RAISE NOTICE '[MIGRATION] driver_locations table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_locations' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_locations' AND column_name = 'recorded_at') THEN
        RAISE NOTICE '[MIGRATION] recorded_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_driver_locations_org_time') THEN
        CREATE INDEX ix_driver_locations_org_time ON driver_locations(org_id, recorded_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_driver_locations_org_time';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_driver_locations_org_time already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS driver_route_history (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id        UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    route_id         UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    org_id           UUID NOT NULL,
    event_type       VARCHAR(30) NOT NULL CHECK (event_type IN (
        'route_started', 'stop_arrived', 'stop_departed', 'stop_completed',
        'stop_failed', 'stop_skipped', 'route_completed', 'route_paused',
        'route_resumed', 'break_started', 'break_ended', 'deviation_detected'
    )),
    stop_id          UUID REFERENCES route_stops(id) ON DELETE SET NULL,
    latitude         NUMERIC(10, 7),
    longitude        NUMERIC(10, 7),
    address_snapshot TEXT,
    event_metadata   JSONB DEFAULT '{}',
    notes            TEXT,
    recorded_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'driver_route_history') THEN
        RAISE NOTICE '[MIGRATION] driver_route_history table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_route_history' AND column_name = 'route_id') THEN
        RAISE NOTICE '[MIGRATION] route_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_route_history' AND column_name = 'recorded_at') THEN
        RAISE NOTICE '[MIGRATION] recorded_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_driver_route_history_route') THEN
        CREATE INDEX ix_driver_route_history_route ON driver_route_history(route_id, recorded_at ASC);
        RAISE NOTICE '[MIGRATION] Created index ix_driver_route_history_route';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_driver_route_history_route already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'driver_route_history') THEN
        RAISE NOTICE '[MIGRATION] driver_route_history table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_route_history' AND column_name = 'driver_id') THEN
        RAISE NOTICE '[MIGRATION] driver_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'driver_route_history' AND column_name = 'recorded_at') THEN
        RAISE NOTICE '[MIGRATION] recorded_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_driver_route_history_driver') THEN
        CREATE INDEX ix_driver_route_history_driver ON driver_route_history(driver_id, recorded_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_driver_route_history_driver';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_driver_route_history_driver already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS assistant_sessions (
    id            VARCHAR(36) PRIMARY KEY,
    org_id        VARCHAR(120) NOT NULL,
    user_id       VARCHAR(120),
    session_key   VARCHAR(255),
    app_context   VARCHAR(80),
    device_id     VARCHAR(120),
    context       JSONB,
    metadata_json JSONB,
    is_active     BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    -- Verify table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'assistant_sessions'
    ) THEN
        RAISE NOTICE '[MIGRATION] assistant_sessions table does not exist — skipping constraint';
        RETURN;
    END IF;

    -- Verify column exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'assistant_sessions'
          AND column_name = 'session_key'
    ) THEN
        RAISE NOTICE '[MIGRATION] session_key column does not exist — skipping constraint';
        RETURN;
    END IF;

    -- Verify constraint doesn't already exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = 'public'
          AND table_name = 'assistant_sessions'
          AND constraint_name = 'assistant_sessions_session_key_key'
    ) THEN
        ALTER TABLE assistant_sessions
            ADD CONSTRAINT assistant_sessions_session_key_key UNIQUE (session_key);
        RAISE NOTICE '[MIGRATION] Created constraint assistant_sessions_session_key_key';
    ELSE
        RAISE NOTICE '[MIGRATION] Constraint assistant_sessions_session_key_key already exists';
    END IF;
END;
$$;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_sessions') THEN
        RAISE NOTICE '[MIGRATION] assistant_sessions table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_sessions' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_sessions_org_id') THEN
        CREATE INDEX ix_assistant_sessions_org_id ON assistant_sessions(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_sessions_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_sessions_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_sessions') THEN
        RAISE NOTICE '[MIGRATION] assistant_sessions table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_sessions' AND column_name = 'session_key') THEN
        RAISE NOTICE '[MIGRATION] session_key column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_sessions_session_key') THEN
        CREATE INDEX ix_assistant_sessions_session_key ON assistant_sessions(session_key);
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_sessions_session_key';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_sessions_session_key already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS assistant_messages (
    id         VARCHAR(36) PRIMARY KEY,
    session_id VARCHAR(36) NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    role       VARCHAR(50) NOT NULL,
    content    TEXT NOT NULL,
    input_type VARCHAR(20) NOT NULL DEFAULT 'text',
    metadata   JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_messages') THEN
        RAISE NOTICE '[MIGRATION] assistant_messages table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_messages' AND column_name = 'session_id') THEN
        RAISE NOTICE '[MIGRATION] session_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_messages_session_id') THEN
        CREATE INDEX ix_assistant_messages_session_id ON assistant_messages(session_id);
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_messages_session_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_messages_session_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS assistant_pending_actions (
    id              VARCHAR(36) PRIMARY KEY,
    confirmation_id VARCHAR(36),
    session_id      VARCHAR(36) NOT NULL REFERENCES assistant_sessions(id) ON DELETE CASCADE,
    org_id          VARCHAR(120),
    user_id         VARCHAR(120),
    action_type     VARCHAR(100) NOT NULL,
    action_name     VARCHAR(120),
    action_data     JSONB,
    payload_json    JSONB NOT NULL DEFAULT '{}',
    risk_level      VARCHAR(20) NOT NULL DEFAULT 'medium',
    status          VARCHAR(50) DEFAULT 'pending',
    expires_at      TIMESTAMPTZ,
    executed_at     TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    -- Verify table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'assistant_pending_actions'
    ) THEN
        RAISE NOTICE '[MIGRATION] assistant_pending_actions table does not exist — skipping constraint';
        RETURN;
    END IF;

    -- Verify column exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'assistant_pending_actions'
          AND column_name = 'confirmation_id'
    ) THEN
        RAISE NOTICE '[MIGRATION] confirmation_id column does not exist — skipping constraint';
        RETURN;
    END IF;

    -- Verify constraint doesn't already exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = 'public'
          AND table_name = 'assistant_pending_actions'
          AND constraint_name = 'assistant_pending_actions_confirmation_id_key'
    ) THEN
        ALTER TABLE assistant_pending_actions
            ADD CONSTRAINT assistant_pending_actions_confirmation_id_key
            UNIQUE (confirmation_id);
        RAISE NOTICE '[MIGRATION] Created constraint assistant_pending_actions_confirmation_id_key';
    ELSE
        RAISE NOTICE '[MIGRATION] Constraint assistant_pending_actions_confirmation_id_key already exists';
    END IF;
END;
$$;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_pending_actions') THEN
        RAISE NOTICE '[MIGRATION] assistant_pending_actions table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_pending_actions' AND column_name = 'session_id') THEN
        RAISE NOTICE '[MIGRATION] session_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_pending_actions_session_id') THEN
        CREATE INDEX ix_assistant_pending_actions_session_id ON assistant_pending_actions(session_id);
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_pending_actions_session_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_pending_actions_session_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_pending_actions') THEN
        RAISE NOTICE '[MIGRATION] assistant_pending_actions table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_pending_actions' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_pending_actions_org_id') THEN
        CREATE INDEX ix_assistant_pending_actions_org_id ON assistant_pending_actions(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_pending_actions_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_pending_actions_org_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_pending_actions') THEN
        RAISE NOTICE '[MIGRATION] assistant_pending_actions table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_pending_actions' AND column_name = 'confirmation_id') THEN
        RAISE NOTICE '[MIGRATION] confirmation_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_pending_actions_confirmation_id') THEN
        CREATE UNIQUE INDEX ix_assistant_pending_actions_confirmation_id ON assistant_pending_actions(confirmation_id) WHERE confirmation_id IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_pending_actions_confirmation_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_pending_actions_confirmation_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS assistant_audit_logs (
    id            VARCHAR(36) PRIMARY KEY,
    session_id    VARCHAR(36) REFERENCES assistant_sessions(id) ON DELETE SET NULL,
    org_id        VARCHAR(120),
    user_id       VARCHAR(120),
    action        VARCHAR(255) NOT NULL,
    action_name   VARCHAR(120),
    risk_level    VARCHAR(20) NOT NULL DEFAULT 'medium',
    request_json  JSONB NOT NULL DEFAULT '{}',
    result_json   JSONB,
    details       JSONB,
    status        VARCHAR(20),
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_audit_logs') THEN
        RAISE NOTICE '[MIGRATION] assistant_audit_logs table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_audit_logs' AND column_name = 'session_id') THEN
        RAISE NOTICE '[MIGRATION] session_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_audit_logs_session_id') THEN
        CREATE INDEX ix_assistant_audit_logs_session_id ON assistant_audit_logs(session_id);
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_audit_logs_session_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_audit_logs_session_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'assistant_audit_logs') THEN
        RAISE NOTICE '[MIGRATION] assistant_audit_logs table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'assistant_audit_logs' AND column_name = 'org_id') THEN
        RAISE NOTICE '[MIGRATION] org_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_assistant_audit_logs_org_id') THEN
        CREATE INDEX ix_assistant_audit_logs_org_id ON assistant_audit_logs(org_id);
        RAISE NOTICE '[MIGRATION] Created index ix_assistant_audit_logs_org_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_assistant_audit_logs_org_id already exists';
    END IF;
END $;

CREATE TABLE IF NOT EXISTS leaflink_webhook_events (
    id                            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brand_id                      VARCHAR(120),
    org_id                        VARCHAR(120),
    event_type                    VARCHAR(80),
    idempotency_key               VARCHAR(255),
    raw_payload                   JSONB,
    tenant_resolution_status      VARCHAR(20) NOT NULL DEFAULT 'unresolved',
    signature_verification_status VARCHAR(20),
    signature_verification_error  TEXT,
    duplicate_of_event_id         UUID REFERENCES leaflink_webhook_events(id) ON DELETE SET NULL,
    enqueued_job_id               INTEGER,
    processing_error              TEXT,
    received_at                   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at                  TIMESTAMP WITH TIME ZONE,
    created_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events') THEN
        RAISE NOTICE '[MIGRATION] leaflink_webhook_events table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events' AND column_name = 'tenant_resolution_status') THEN
        RAISE NOTICE '[MIGRATION] tenant_resolution_status column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_leaflink_webhook_events_tenant_status') THEN
        CREATE INDEX ix_leaflink_webhook_events_tenant_status ON leaflink_webhook_events(tenant_resolution_status);
        RAISE NOTICE '[MIGRATION] Created index ix_leaflink_webhook_events_tenant_status';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_leaflink_webhook_events_tenant_status already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events') THEN
        RAISE NOTICE '[MIGRATION] leaflink_webhook_events table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_leaflink_webhook_events_brand_id') THEN
        CREATE INDEX ix_leaflink_webhook_events_brand_id ON leaflink_webhook_events(brand_id);
        RAISE NOTICE '[MIGRATION] Created index ix_leaflink_webhook_events_brand_id';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_leaflink_webhook_events_brand_id already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events') THEN
        RAISE NOTICE '[MIGRATION] leaflink_webhook_events table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events' AND column_name = 'received_at') THEN
        RAISE NOTICE '[MIGRATION] received_at column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_leaflink_webhook_events_received_at') THEN
        CREATE INDEX ix_leaflink_webhook_events_received_at ON leaflink_webhook_events(received_at DESC);
        RAISE NOTICE '[MIGRATION] Created index ix_leaflink_webhook_events_received_at';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_leaflink_webhook_events_received_at already exists';
    END IF;
END $;

DO $
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events') THEN
        RAISE NOTICE '[MIGRATION] leaflink_webhook_events table does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events' AND column_name = 'brand_id') THEN
        RAISE NOTICE '[MIGRATION] brand_id column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'leaflink_webhook_events' AND column_name = 'idempotency_key') THEN
        RAISE NOTICE '[MIGRATION] idempotency_key column does not exist — skipping index';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ix_leaflink_webhook_events_brand_idempotency') THEN
        CREATE UNIQUE INDEX ix_leaflink_webhook_events_brand_idempotency ON leaflink_webhook_events(brand_id, idempotency_key) WHERE brand_id IS NOT NULL;
        RAISE NOTICE '[MIGRATION] Created index ix_leaflink_webhook_events_brand_idempotency';
    ELSE
        RAISE NOTICE '[MIGRATION] Index ix_leaflink_webhook_events_brand_idempotency already exists';
    END IF;
END $;
