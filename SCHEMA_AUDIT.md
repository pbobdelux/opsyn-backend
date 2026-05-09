# Schema Audit Report — Opsyn Backend

**Date:** 2026-05-28  
**Service:** opsyn-backend (FastAPI + SQLAlchemy + asyncpg)  
**Database:** AWS RDS PostgreSQL  
**Auditor:** Automated schema audit against migration history and ORM models

---

## Executive Summary

The database schema has accumulated significant inconsistencies across its migration history. The core issue is a **primary key type split**: tables created in the initial bulk migration (`2026_05_04_02_create_full_schema_aws_rds.sql`) use `SERIAL` (INTEGER) primary keys, while newer tables and models introduced in May 2026 use `UUID` primary keys with `gen_random_uuid()`. A subsequent migration (`2026_05_21_02_add_drivers_routes_stops.sql`) partially corrected this by dropping and recreating the `drivers`, `dispatch_routes`, and `route_stops` tables with UUID PKs — but the ORM models for `orders`, `order_lines`, `brand_api_credentials`, `tenant_credentials`, `sync_runs`, `sync_requests`, `sync_health`, `dead_letter_line_items`, `assistant_sessions`, `assistant_messages`, `assistant_pending_actions`, and `assistant_audit_logs` still define INTEGER primary keys, matching the database.

The assistant models are a separate concern: the database has `SERIAL` (INTEGER) PKs for all four assistant tables, but the ORM models use `String(36)` (UUID-as-string) PKs — a direct type mismatch that will cause query failures.

---

## 1. Schema Inventory

### 1.1 Auth Tables (created by `2026_05_04_01_setup_auth_tables.sql`)

These tables use `VARCHAR(255)` primary keys. The `auth_models.py` ORM now uses `UUID(as_uuid=True)` — this mismatch was addressed in PR #389.

| Table | DB PK Type | Model PK Type | Status |
|---|---|---|---|
| `organizations` | `VARCHAR(255)` | `UUID(as_uuid=True)` | ⚠️ Fixed in PR #389 |
| `brands` | `VARCHAR(255)` | `UUID(as_uuid=True)` | ⚠️ Fixed in PR #389 |
| `employees` | `VARCHAR(255)` | `UUID(as_uuid=True)` | ⚠️ Fixed in PR #389 |
| `employee_passcodes` | `VARCHAR(255)` | `UUID(as_uuid=True)` | ⚠️ Fixed in PR #389 |
| `employee_brand_access` | `VARCHAR(255)` | `UUID(as_uuid=True)` | ⚠️ Fixed in PR #389 |
| `employee_app_access` | `VARCHAR(255)` | `UUID(as_uuid=True)` | ⚠️ Fixed in PR #389 |

**Note:** The `2026_05_15_01_add_crm_app_access.sql` migration inserts a row using `gen_random_uuid()::text`, confirming these IDs are UUID strings stored as VARCHAR.

---

### 1.2 Operational Tables (created by `2026_05_04_02_create_full_schema_aws_rds.sql`)

These tables were created with `SERIAL PRIMARY KEY` (INTEGER). Their ORM models in `models/__init__.py` also use `Integer` primary keys — so the model/DB match is correct, but the type is inconsistent with the UUID-based tables.

| Table | DB PK Type | Model PK Type | Model File | Match? |
|---|---|---|---|---|
| `brand_api_credentials` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `BrandAPICredential` | ✅ Match |
| `orders` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `Order` | ✅ Match |
| `order_lines` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `OrderLine` | ✅ Match |
| `organization_brand_bindings` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `OrganizationBrandBinding` | ✅ Match |
| `tenant_credentials` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `TenantCredential` | ✅ Match |
| `sync_runs` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `SyncRun` | ✅ Match |
| `sync_requests` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `SyncRequest` | ✅ Match |

**Note:** `2026_05_15_02_create_orders_table.sql` uses `BIGSERIAL` for `orders`, `order_lines`, `sync_runs`, and `sync_requests`. The effective type in production depends on which migration ran first and whether `IF NOT EXISTS` skipped the later one. The ORM models use `Integer` in all cases, which is compatible with both `SERIAL` and `BIGSERIAL`.

---

### 1.3 Assistant Tables (created by `2026_05_04_02_create_full_schema_aws_rds.sql`)

These tables were created with `SERIAL PRIMARY KEY` (INTEGER) in the database. However, the ORM models in `models/assistant_models.py` define `String(36)` primary keys (UUID-as-string). **This is an active type mismatch.**

| Table | DB PK Type | Model PK Type | Model File | Match? |
|---|---|---|---|---|
| `assistant_sessions` | `SERIAL` (INTEGER) | `String(36)` | `models/assistant_models.py` → `AssistantSession` | ❌ MISMATCH |
| `assistant_messages` | `SERIAL` (INTEGER) | `String(36)` | `models/assistant_models.py` → `AssistantMessage` | ❌ MISMATCH |
| `assistant_pending_actions` | `SERIAL` (INTEGER) | `String(36)` | `models/assistant_models.py` → `AssistantPendingAction` | ❌ MISMATCH |
| `assistant_audit_logs` | `SERIAL` (INTEGER) | `String(36)` | `models/assistant_models.py` → `AssistantAuditLog` | ❌ MISMATCH |

**FK chain:** `assistant_messages.session_id` → `assistant_sessions.id`, `assistant_pending_actions.session_id` → `assistant_sessions.id`, `assistant_audit_logs.session_id` → `assistant_sessions.id`. All FK columns in the DB are `INTEGER`, but the ORM models declare them as `String` (via `ForeignKey("assistant_sessions.id")`). Any ORM query that filters or joins on these columns will fail with `operator does not exist: integer = character varying`.

---

### 1.4 Dispatch / Route Tables (created by `2026_05_21_02_add_drivers_routes_stops.sql`)

Migration `2026_05_21_02` explicitly dropped the old INTEGER-PK `drivers`, `dispatch_routes`, and `route_stops` tables and recreated them with UUID PKs. It also created a new `routes` table (replacing `dispatch_routes`). These tables are consistent with their ORM models.

| Table | DB PK Type | Model PK Type | Model File | Match? |
|---|---|---|---|---|
| `drivers` | `UUID` | `PG_UUID(as_uuid=True)` | `models/driver.py` → `Driver` | ✅ Match |
| `routes` | `UUID` | `PG_UUID(as_uuid=True)` | `models/route.py` → `Route` | ✅ Match |
| `route_stops` | `UUID` | `PG_UUID(as_uuid=True)` | `models/route_stop.py` → `RouteStop` | ✅ Match |

**Note:** The old `dispatch_routes` table no longer exists. The `routes` table is the canonical replacement.

---

### 1.5 Route Support Tables (created by migrations 001–003)

| Table | DB PK Type | Model PK Type | Model File | Match? |
|---|---|---|---|---|
| `route_events` | `UUID` | `PG_UUID(as_uuid=True)` | `models/route_event.py` → `RouteEvent` | ✅ Match |
| `driver_locations` | `UUID` | `PG_UUID(as_uuid=True)` | `models/driver_location.py` → `DriverLocation` | ✅ Match |
| `driver_route_history` | `UUID` | `PG_UUID(as_uuid=True)` | `models/driver_route_history.py` → `DriverRouteHistory` | ✅ Match |

---

### 1.6 Sync Health / Dead Letter Tables (created by `2026_05_18_01_add_sync_health_tables.sql`)

| Table | DB PK Type | Model PK Type | Model File | Match? |
|---|---|---|---|---|
| `sync_health` | `SERIAL` (INTEGER) | `Integer` | `models/sync_health.py` → `SyncHealth` | ✅ Match |
| `dead_letter_line_items` | `SERIAL` (INTEGER) | `Integer` | `models/sync_health.py` → `DeadLetterLineItem` | ✅ Match |

---

### 1.7 Sync Dead Letters Table (created by `2026_05_20_01_add_sync_dead_letters.sql`)

| Table | DB PK Type | Model PK Type | Model File | Match? |
|---|---|---|---|---|
| `sync_dead_letters` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `SyncDeadLetter` | ✅ Match |

---

### 1.8 Metrics / Webhook Tables (created by later migrations)

| Table | DB PK Type | Model PK Type | Model File | Match? |
|---|---|---|---|---|
| `sync_metrics_snapshots` | `SERIAL` (INTEGER) | `Integer` | `models/__init__.py` → `SyncMetricsSnapshot` | ✅ Match |
| `leaflink_webhook_events` | `UUID` | `PG_UUID(as_uuid=False)` (string) | `models/__init__.py` → `LeafLinkWebhookEvent` | ✅ Match |

---

## 2. Foreign Key Relationship Inventory

### 2.1 Active FK Relationships and Type Compatibility

| Referencing Table | FK Column | Referenced Table | Referenced PK | DB Type Match | ORM Type Match |
|---|---|---|---|---|---|
| `order_lines.order_id` | INTEGER | `orders.id` | INTEGER | ✅ | ✅ |
| `orders.sync_run_id` | INTEGER | `sync_runs.id` | INTEGER | ✅ | ✅ |
| `orders.assigned_driver_id` | UUID (TEXT in migration 03) | `drivers.id` | UUID | ⚠️ See note | ⚠️ See note |
| `orders.route_id` | UUID (TEXT in migration 03) | `routes.id` | UUID | ⚠️ See note | ⚠️ See note |
| `dead_letter_line_items.order_id` | INTEGER | `orders.id` | INTEGER | ✅ | ✅ |
| `routes.assigned_driver_id` | UUID | `drivers.id` | UUID | ✅ | ✅ |
| `route_stops.route_id` | UUID | `routes.id` | UUID | ✅ | ✅ |
| `route_events.route_id` | UUID | `routes.id` | UUID | ✅ | ✅ |
| `driver_locations.driver_id` | UUID | `drivers.id` | UUID | ✅ | ✅ |
| `driver_locations.route_id` | UUID | `routes.id` | UUID | ✅ | ✅ |
| `driver_route_history.driver_id` | UUID | `drivers.id` | UUID | ✅ | ✅ |
| `driver_route_history.route_id` | UUID | `routes.id` | UUID | ✅ | ✅ |
| `driver_route_history.stop_id` | UUID | `route_stops.id` | UUID | ✅ | ✅ |
| `assistant_messages.session_id` | INTEGER | `assistant_sessions.id` | INTEGER | ✅ DB | ❌ ORM (String) |
| `assistant_pending_actions.session_id` | INTEGER | `assistant_sessions.id` | INTEGER | ✅ DB | ❌ ORM (String) |
| `assistant_audit_logs.session_id` | INTEGER | `assistant_sessions.id` | INTEGER | ✅ DB | ❌ ORM (String) |
| `brands.org_id` | VARCHAR(255) | `organizations.id` | VARCHAR(255) | ✅ DB | ⚠️ PR #389 |
| `employees.org_id` | VARCHAR(255) | `organizations.id` | VARCHAR(255) | ✅ DB | ⚠️ PR #389 |
| `employee_passcodes.employee_id` | VARCHAR(255) | `employees.id` | VARCHAR(255) | ✅ DB | ⚠️ PR #389 |
| `employee_brand_access.employee_id` | VARCHAR(255) | `employees.id` | VARCHAR(255) | ✅ DB | ⚠️ PR #389 |
| `employee_brand_access.brand_id` | VARCHAR(255) | `brands.id` | VARCHAR(255) | ✅ DB | ⚠️ PR #389 |
| `employee_app_access.employee_id` | VARCHAR(255) | `employees.id` | VARCHAR(255) | ✅ DB | ⚠️ PR #389 |
| `leaflink_webhook_events.duplicate_of_event_id` | UUID | `leaflink_webhook_events.id` | UUID | ✅ | ✅ |

**Note on `orders.assigned_driver_id` and `orders.route_id`:** Migration `2026_05_08_03` added these as `TEXT` columns (no FK constraint) to avoid type dependency issues. Migration `2026_05_08_02` added them as `UUID` with FK constraints. The ORM model (`models/__init__.py`) declares them as `PG_UUID(as_uuid=False)` with `ForeignKey("drivers.id")` and `ForeignKey("routes.id")`. The actual column type in production depends on which migration ran and whether the FK constraint was successfully applied. This is a known ambiguity requiring verification.

---

## 3. Column-Level Mismatches

### 3.1 Critical Mismatches (will cause runtime errors)

| Table | Column | DB Type | ORM Type | Error Pattern |
|---|---|---|---|---|
| `assistant_sessions` | `id` | `INTEGER` (SERIAL) | `String(36)` | `operator does not exist: integer = character varying` |
| `assistant_messages` | `id` | `INTEGER` (SERIAL) | `String(36)` | Same |
| `assistant_messages` | `session_id` | `INTEGER` | `String` (FK) | Same |
| `assistant_pending_actions` | `id` | `INTEGER` (SERIAL) | `String(36)` | Same |
| `assistant_pending_actions` | `session_id` | `INTEGER` | `String` (FK) | Same |
| `assistant_audit_logs` | `id` | `INTEGER` (SERIAL) | `String(36)` | Same |
| `assistant_audit_logs` | `session_id` | `INTEGER` | `String` (FK) | Same |

### 3.2 Columns Present in ORM but Absent from Initial Migration

These columns were added by subsequent `ALTER TABLE` migrations. They should exist in production if all migrations have been applied.

| Table | Column | Added By Migration |
|---|---|---|
| `orders` | `sync_run_id` | `2026_05_13_01_add_sync_run_id_to_orders.sql` |
| `orders` | `org_id` | `2026_05_16_01_add_org_id_to_orders.sql` |
| `orders` | `assigned_driver_id` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `assigned_driver_name` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `delivery_status` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `delivery_date` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `route_number` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `route_id` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `driver_note` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `delivery_instructions` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `payment_status` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `amount_paid` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `balance_due` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `due_date` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `days_overdue` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `invoice_number` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `ar_note` | `2026_05_08_02` or `2026_05_08_03` |
| `orders` | `sync_health_status` | `2026_05_22_01_add_order_sync_health_fields.sql` |
| `orders` | `sync_health_missing_fields` | `2026_05_22_01_add_order_sync_health_fields.sql` |
| `orders` | `sync_health_last_error` | `2026_05_22_01_add_order_sync_health_fields.sql` |
| `orders` | `sync_health` | `2026_05_08_02` or `2026_05_08_03` |
| `sync_runs` | `last_next_url` | `2026_05_13_01_add_sync_runs_last_next_url.sql` |
| `sync_requests` | `org_id` | `2026_05_25_01_add_org_id_to_sync_requests.sql` |
| `brand_api_credentials` | `webhook_key` | `2026_05_16_02_add_webhook_to_credentials.sql` |
| `brand_api_credentials` | `org_id` | `2026_05_16_02_add_webhook_to_credentials.sql` |
| `brand_api_credentials` | `webhook_key_secret_ref` | `2026_05_27_01_add_webhook_credential_fields.sql` |
| `brand_api_credentials` | `webhook_key_last4` | `2026_05_27_01_add_webhook_credential_fields.sql` |
| `brand_api_credentials` | `webhook_enabled` | `2026_05_27_01_add_webhook_credential_fields.sql` |
| `brand_api_credentials` | `webhook_signature_required` | `2026_05_27_01_add_webhook_credential_fields.sql` |
| `brand_api_credentials` | `leaflink_company_id` | `2026_05_27_01_add_webhook_credential_fields.sql` |
| `sync_health` | `last_error_at` | `2026_05_21_01_add_dead_letter_retry_fields.sql` |
| `sync_health` | `orders_fetched_last_run` | `2026_05_21_01_add_dead_letter_retry_fields.sql` |
| `sync_health` | `orders_written_last_run` | `2026_05_21_01_add_dead_letter_retry_fields.sql` |
| `sync_dead_letters` | `last_retry_at` | `2026_05_21_01_add_dead_letter_retry_fields.sql` |
| `sync_dead_letters` | `failure_category` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `exception_type` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `exception_message` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `traceback_summary` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `payload_keys` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `problematic_field` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `problematic_value_preview` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `customer_name` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `sync_dead_letters` | `failure_stage` | `2026_05_23_01_add_dead_letter_detail_fields.sql` |
| `organizations` | `org_code` | `2026_05_14_01_add_org_code_to_organizations.sql` |

### 3.3 Columns in ORM but Not in Any Migration

These columns exist in ORM models but have no corresponding migration. They would only exist if `Base.metadata.create_all()` was run, or if they were added manually.

| Table | Column | ORM Model | Notes |
|---|---|---|---|
| `drivers` | `passcode_hash` | `Driver` | In `2026_05_21_02` migration ✅ |
| `drivers` | `invite_code` | `Driver` | In `2026_05_21_02` migration ✅ |
| `drivers` | `vehicle_type` | `Driver` | In `2026_05_21_02` migration ✅ |
| `drivers` | `notes` | `Driver` | In `2026_05_21_02` migration ✅ |
| `drivers` | `preferences` | `Driver` | In `2026_05_21_02` migration ✅ |
| `drivers` | `deactivated_at` | `Driver` | In `2026_05_21_02` migration ✅ |
| `assistant_sessions` | `app_context` | `AssistantSession` | ❌ No migration — only in ORM |
| `assistant_sessions` | `device_id` | `AssistantSession` | ❌ No migration — only in ORM |
| `assistant_sessions` | `metadata_json` | `AssistantSession` | ❌ No migration — only in ORM |
| `assistant_messages` | `input_type` | `AssistantMessage` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `confirmation_id` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `org_id` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `user_id` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `action_name` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `payload_json` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `risk_level` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `expires_at` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `executed_at` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_pending_actions` | `error_message` | `AssistantPendingAction` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `org_id` | `AssistantAuditLog` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `user_id` | `AssistantAuditLog` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `action_name` | `AssistantAuditLog` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `risk_level` | `AssistantAuditLog` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `request_json` | `AssistantAuditLog` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `result_json` | `AssistantAuditLog` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `status` | `AssistantAuditLog` | ❌ No migration — only in ORM |
| `assistant_audit_logs` | `error_message` | `AssistantAuditLog` | ❌ No migration — only in ORM |

---

## 4. SQLAlchemy Model Inventory

### 4.1 Models Using UUID Primary Keys (correct, target state)

| Model | File | PK Column | PK Type | Notes |
|---|---|---|---|---|
| `Driver` | `models/driver.py` | `id` | `PG_UUID(as_uuid=True)` | ✅ Correct |
| `Route` | `models/route.py` | `id` | `PG_UUID(as_uuid=True)` | ✅ Correct |
| `RouteStop` | `models/route_stop.py` | `id` | `PG_UUID(as_uuid=True)` | ✅ Correct |
| `RouteEvent` | `models/route_event.py` | `id` | `PG_UUID(as_uuid=True)` | ✅ Correct |
| `DriverLocation` | `models/driver_location.py` | `id` | `PG_UUID(as_uuid=True)` | ✅ Correct |
| `DriverRouteHistory` | `models/driver_route_history.py` | `id` | `PG_UUID(as_uuid=True)` | ✅ Correct |
| `LeafLinkWebhookEvent` | `models/__init__.py` | `id` | `PG_UUID(as_uuid=False)` | ✅ Correct (string UUID) |

### 4.2 Models Using Integer Primary Keys (legacy, consistent with DB)

| Model | File | PK Column | PK Type | Notes |
|---|---|---|---|---|
| `BrandAPICredential` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `Order` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `OrderLine` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `OrganizationBrandBinding` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `TenantCredential` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `SyncRun` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `SyncRequest` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `SyncHealth` | `models/sync_health.py` | `id` | `Integer` | Consistent with DB |
| `DeadLetterLineItem` | `models/sync_health.py` | `id` | `Integer` | Consistent with DB |
| `SyncDeadLetter` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |
| `SyncMetricsSnapshot` | `models/__init__.py` | `id` | `Integer` | Consistent with DB |

### 4.3 Models Using String(36) Primary Keys (MISMATCH with DB)

| Model | File | PK Column | PK Type | DB Type | Status |
|---|---|---|---|---|---|
| `AssistantSession` | `models/assistant_models.py` | `id` | `String(36)` | `INTEGER` | ❌ MISMATCH |
| `AssistantMessage` | `models/assistant_models.py` | `id` | `String(36)` | `INTEGER` | ❌ MISMATCH |
| `AssistantPendingAction` | `models/assistant_models.py` | `id` | `String(36)` | `INTEGER` | ❌ MISMATCH |
| `AssistantAuditLog` | `models/assistant_models.py` | `id` | `String(36)` | `INTEGER` | ❌ MISMATCH |

### 4.4 Auth Models (VARCHAR(255) in DB, UUID in ORM — PR #389)

| Model | File | PK Column | PK Type | DB Type | Status |
|---|---|---|---|---|---|
| `Organization` | `models/auth_models.py` | `id` | `UUID(as_uuid=True)` | `VARCHAR(255)` | ⚠️ PR #389 |
| `Brand` | `models/auth_models.py` | `id` | `UUID(as_uuid=True)` | `VARCHAR(255)` | ⚠️ PR #389 |
| `Employee` | `models/auth_models.py` | `id` | `UUID(as_uuid=True)` | `VARCHAR(255)` | ⚠️ PR #389 |
| `EmployeePasscode` | `models/auth_models.py` | `id` | `UUID(as_uuid=True)` | `VARCHAR(255)` | ⚠️ PR #389 |
| `EmployeeBrandAccess` | `models/auth_models.py` | `id` | `UUID(as_uuid=True)` | `VARCHAR(255)` | ⚠️ PR #389 |
| `EmployeeAppAccess` | `models/auth_models.py` | `id` | `UUID(as_uuid=True)` | `VARCHAR(255)` | ⚠️ PR #389 |

---

## 5. Risk Assessment

### 5.1 Critical (Active Failures)

| Issue | Tables Affected | Error | Priority |
|---|---|---|---|
| Assistant model PK type mismatch | `assistant_sessions`, `assistant_messages`, `assistant_pending_actions`, `assistant_audit_logs` | `operator does not exist: integer = character varying` on any ORM query | 🔴 P0 |
| Assistant ORM columns missing from DB | All four assistant tables | `UndefinedColumnError` on insert/select for `app_context`, `device_id`, `confirmation_id`, `action_name`, etc. | 🔴 P0 |

### 5.2 High (Potential Failures Under Specific Conditions)

| Issue | Tables Affected | Error | Priority |
|---|---|---|---|
| `orders.assigned_driver_id` type ambiguity | `orders` | FK constraint failure or type cast error if column is TEXT but model expects UUID | 🟠 P1 |
| `orders.route_id` type ambiguity | `orders` | Same as above | 🟠 P1 |
| Auth table VARCHAR vs UUID | All auth tables | Type mismatch on UUID comparison if asyncpg strict mode | 🟠 P1 (PR #389) |

### 5.3 Medium (Technical Debt, No Immediate Failure)

| Issue | Tables Affected | Impact | Priority |
|---|---|---|---|
| INTEGER PKs on operational tables | `orders`, `order_lines`, `brand_api_credentials`, `sync_runs`, `sync_requests`, `tenant_credentials` | Cannot use UUID-based distributed ID generation; SERIAL exhaustion at ~2.1B rows | 🟡 P2 |
| INTEGER PKs on sync health tables | `sync_health`, `dead_letter_line_items`, `sync_dead_letters`, `sync_metrics_snapshots` | Same as above | 🟡 P2 |
| Mixed FK types on `orders` | `orders.sync_run_id` (INTEGER FK to `sync_runs.id`) | Will break if `sync_runs` migrates to UUID | 🟡 P2 |

### 5.4 Low (Documentation / Cleanup)

| Issue | Impact | Priority |
|---|---|---|
| `dispatch_routes` table referenced in old migrations but dropped | Confusion in migration history | 🟢 P3 |
| `2026_05_15_02_create_orders_table.sql` creates tables that already exist (IF NOT EXISTS) | No runtime impact | 🟢 P3 |
| Duplicate migration files for same date (`2026_05_13_01_*`) | Naming confusion | 🟢 P3 |

---

## 6. Verification Queries

Run these queries against the production database to confirm the current state:

```sql
-- 1. Check primary key types for all tables
SELECT
    t.table_name,
    c.column_name,
    c.data_type,
    c.udt_name,
    c.column_default
FROM information_schema.tables t
JOIN information_schema.columns c
    ON t.table_name = c.table_name
    AND t.table_schema = c.table_schema
JOIN information_schema.table_constraints tc
    ON tc.table_name = t.table_name
    AND tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = t.table_schema
JOIN information_schema.key_column_usage kcu
    ON kcu.constraint_name = tc.constraint_name
    AND kcu.table_name = t.table_name
    AND kcu.column_name = c.column_name
WHERE t.table_schema = 'public'
ORDER BY t.table_name;

-- 2. Check all foreign key constraints and their column types
SELECT
    tc.table_name AS referencing_table,
    kcu.column_name AS referencing_column,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column,
    rc.column_name AS ref_col_type,
    lc.data_type AS local_type,
    fc.data_type AS foreign_type
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.referential_constraints rc_ref
    ON tc.constraint_name = rc_ref.constraint_name
JOIN information_schema.key_column_usage ccu
    ON rc_ref.unique_constraint_name = ccu.constraint_name
JOIN information_schema.columns lc
    ON lc.table_name = tc.table_name
    AND lc.column_name = kcu.column_name
    AND lc.table_schema = 'public'
JOIN information_schema.columns fc
    ON fc.table_name = ccu.table_name
    AND fc.column_name = ccu.column_name
    AND fc.table_schema = 'public'
JOIN information_schema.referential_constraints rc
    ON rc.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'public'
ORDER BY tc.table_name;

-- 3. Verify assistant table PK types specifically
SELECT table_name, column_name, data_type, udt_name, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'assistant_sessions', 'assistant_messages',
      'assistant_pending_actions', 'assistant_audit_logs'
  )
  AND column_name = 'id'
ORDER BY table_name;

-- 4. Check which assistant columns exist vs what ORM expects
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'assistant_sessions', 'assistant_messages',
      'assistant_pending_actions', 'assistant_audit_logs'
  )
ORDER BY table_name, ordinal_position;

-- 5. Verify orders.assigned_driver_id and orders.route_id types
SELECT column_name, data_type, udt_name, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'orders'
  AND column_name IN ('assigned_driver_id', 'route_id', 'sync_run_id')
ORDER BY column_name;

-- 6. Row counts for impact assessment
SELECT
    'orders' AS table_name, COUNT(*) AS row_count FROM orders
UNION ALL SELECT 'order_lines', COUNT(*) FROM order_lines
UNION ALL SELECT 'brand_api_credentials', COUNT(*) FROM brand_api_credentials
UNION ALL SELECT 'drivers', COUNT(*) FROM drivers
UNION ALL SELECT 'routes', COUNT(*) FROM routes
UNION ALL SELECT 'route_stops', COUNT(*) FROM route_stops
UNION ALL SELECT 'assistant_sessions', COUNT(*) FROM assistant_sessions
UNION ALL SELECT 'assistant_messages', COUNT(*) FROM assistant_messages
UNION ALL SELECT 'assistant_pending_actions', COUNT(*) FROM assistant_pending_actions
UNION ALL SELECT 'assistant_audit_logs', COUNT(*) FROM assistant_audit_logs
UNION ALL SELECT 'sync_runs', COUNT(*) FROM sync_runs
UNION ALL SELECT 'sync_requests', COUNT(*) FROM sync_requests
UNION ALL SELECT 'tenant_credentials', COUNT(*) FROM tenant_credentials
ORDER BY table_name;
```

---

## 7. Summary of Findings

| Category | Count | Severity |
|---|---|---|
| Critical model/DB type mismatches (assistant tables) | 4 tables, 7 columns | 🔴 P0 |
| Missing ORM columns in DB (assistant tables) | 4 tables, ~20 columns | 🔴 P0 |
| Ambiguous FK type (orders → drivers/routes) | 2 columns | 🟠 P1 |
| Auth table VARCHAR vs UUID (PR #389) | 6 tables | 🟠 P1 |
| INTEGER PKs on legacy operational tables | 11 tables | 🟡 P2 |
| Tables consistent between DB and ORM | 18 tables | ✅ |

The most urgent issue is the assistant table schema: the database has `SERIAL` (INTEGER) PKs and a minimal column set from the 2026-05-04 migration, while the ORM models define `String(36)` PKs and a significantly richer column schema. Any route that touches assistant functionality will fail at the ORM layer. The fix requires either a migration to align the DB to the ORM (preferred) or a model rollback to match the DB.
