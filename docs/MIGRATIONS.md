# Migration System

This document describes how the opsyn-backend migration system works, how to
add new migrations, how to quarantine obsolete ones, and how to troubleshoot
problems.

---

## Overview

Migrations are plain `.sql` files stored in the `migrations/` directory.  The
runner (`services/migration_runner.py`) is called during FastAPI startup and
applies any pending files in dependency order.  Each applied migration is
recorded in the `schema_migrations` table so it is never applied twice.

---

## Migration Ordering Rules

Migrations are executed in the following priority order:

1. **LeafLink repair migrations** — filenames matching `fix_leaflink` run first
   (e.g. `2026_05_01_00_fix_leaflink_stale_credentials.sql`).
2. **Dependency-ordered migrations** — files listed in `MIGRATION_DEPENDENCIES`
   in `migration_runner.py` are topologically sorted so that base tables are
   always created before the tables that reference them.
3. **All remaining migrations** — sorted alphabetically (timestamp order).

### Why dependency ordering matters

The `001_create_route_events_table.sql`, `002_create_driver_locations_table.sql`,
and `003_create_driver_route_history_table.sql` files reference `routes`,
`drivers`, and `route_stops` tables.  Those tables are created by
`2026_05_21_02_add_drivers_routes_stops.sql`.  Alphabetically `001` sorts before
`2026_05_21`, so without explicit dependencies the runner would fail with
`relation does not exist`.  The `MIGRATION_DEPENDENCIES` dict fixes this.

---

## Critical vs Noncritical Migrations

Migrations are classified as **critical** or **noncritical**:

| Classification | Behaviour on failure |
|---|---|
| **Critical** | Raises `RuntimeError`, aborts startup, logs `[MIGRATION_FAILED_CRITICAL]` and `[STARTUP_FAILED]` |
| **Noncritical** | Logs `[MIGRATION_FAILED_NONCRITICAL]`, continues with remaining migrations |

Critical migrations are listed in `CRITICAL_MIGRATIONS` in `migration_runner.py`:

```python
CRITICAL_MIGRATIONS: frozenset[str] = frozenset({
    "2026_05_04_01_setup_auth_tables.sql",
    "2026_05_04_02_create_full_schema_aws_rds.sql",
    "2026_05_30_01_reconcile_uuid_integer_mismatches.sql",
})
```

A migration should be marked critical if the application **cannot function at
all** without it (e.g. the auth tables or the full base schema).  Most
incremental `ALTER TABLE` migrations should be noncritical.

---

## How to Quarantine Obsolete Migrations

When a migration is superseded by a later one and must never be applied again,
add it to `SKIPPED_LEGACY_MIGRATIONS` in `migration_runner.py`:

```python
SKIPPED_LEGACY_MIGRATIONS: dict[str, str] = {
    "2026_04_23_02_add_tenant_credentials.sql": (
        "Uses SERIAL PK; superseded by 2026_05_04_02_create_full_schema_aws_rds.sql"
    ),
}
```

**Rules:**
- **Never delete the file from disk.** The runner needs it present to log the
  skip reason deterministically.  Deleting it would cause the runner to silently
  lose the quarantine record.
- **Always provide a reason.** The reason is logged with `[MIGRATION_SKIPPED_LEGACY]`
  so the skip is always auditable in Railway deploy logs.
- **Add a header comment to the SQL file** explaining the quarantine status
  (see `2026_04_23_02_add_tenant_credentials.sql` for an example).

---

## Migration Checksums

Each applied migration records a SHA-256 checksum of its file content in the
`schema_migrations` table (`checksum` column).

On subsequent startups, if a previously-applied migration file has changed on
disk, the runner logs:

```
[MIGRATION_CHECKSUM_CHANGED] migration=<file> stored_checksum=<old> current_checksum=<new> action=skipping_already_applied
```

The migration is **not re-applied** — it is skipped as usual.  The warning
exists to detect accidental edits to already-applied migrations.

**If you need to change an already-applied migration:**
1. Create a new migration file with the corrective SQL.
2. Do **not** edit the original file (it will trigger the checksum warning on
   every deploy).

---

## How to Run Smoke Tests Locally

The smoke test script validates the migration runner against a real database:

```bash
# Using DATABASE_URL from environment
python scripts/migration_smoke_test.py

# Using a specific database URL
python scripts/migration_smoke_test.py --db-url "postgresql://user:pass@host/db"

# Verbose output
python scripts/migration_smoke_test.py --verbose

# CI usage (exit code 0 = pass, 1 = fail)
python scripts/migration_smoke_test.py --db-url "$TEST_DATABASE_URL" && echo "PASSED"
```

The script runs four tests:

| Test | Description |
|---|---|
| `empty_database` | Drops `schema_migrations` and runs all migrations from scratch |
| `partial_database` | Runs migrations twice; verifies idempotency (second run applies nothing) |
| `dirty_database` | Checks for UUID/INTEGER FK mismatches and missing parent tables |
| `validator` | Runs `migration_validator.py` and verifies no fatal issues |

Output is a JSON report on stdout.  Example:

```json
{
  "db_url_host": "mydb.rds.amazonaws.com",
  "tests": [
    {
      "test": "empty_database",
      "passed": true,
      "elapsed_ms": 1234,
      "checks": ["migrations_applied=47", "no_critical_failures=true"],
      "warnings": [],
      "failures": []
    }
  ],
  "summary": { "total": 4, "passed": 4, "failed": 0, "warnings": 0 },
  "overall_passed": true
}
```

---

## Log Markers Reference

All migration log markers are prefixed with `[` for easy grepping in Railway
deploy logs.

| Marker | Level | Meaning |
|---|---|---|
| `[MIGRATION_DISCOVERED]` | INFO | One line per `.sql` file found on disk |
| `[MIGRATION_SKIPPED_LEGACY]` | INFO | Quarantined migration skipped; includes reason |
| `[MIGRATION_EXECUTION_PLAN]` | INFO | Ordered list of migrations to attempt (post-quarantine) |
| `[MIGRATION_ORDER]` | INFO | Per-migration execution start |
| `[MIGRATION_EXECUTED]` | INFO | Per-migration success; includes duration and checksum prefix |
| `[MIGRATION_FAILED_CRITICAL]` | ERROR | Critical migration failed; startup will abort |
| `[MIGRATION_FAILED_NONCRITICAL]` | WARNING | Noncritical migration failed; startup continues |
| `[MIGRATION_CHECKSUM_CHANGED]` | WARNING | Previously-applied migration file was modified on disk |
| `[MIGRATION_SUMMARY]` | INFO | Final counts: discovered / skipped / applied / failed |
| `[STARTUP_READY]` | INFO | All startup phases complete; app is ready to serve traffic |
| `[STARTUP_FAILED]` | ERROR | Critical failure during startup; process will exit |

### Grepping for health in Railway logs

```bash
# Confirm clean boot
grep STARTUP_READY deploy.log

# Check for any migration failures
grep MIGRATION_FAILED deploy.log

# See the full migration summary
grep MIGRATION_SUMMARY deploy.log

# See quarantined migrations
grep MIGRATION_SKIPPED_LEGACY deploy.log

# Detect modified migration files
grep MIGRATION_CHECKSUM_CHANGED deploy.log
```

---

## Troubleshooting

### `MIGRATION_FAILED_NONCRITICAL` in logs

A noncritical migration failed.  Common causes:

1. **Column already exists** — the migration uses `ADD COLUMN` without
   `IF NOT EXISTS`.  Fix: add `IF NOT EXISTS` to the `ALTER TABLE` statement.
2. **Table does not exist** — a dependency is missing.  Fix: add the dependency
   to `MIGRATION_DEPENDENCIES` in `migration_runner.py`.
3. **Type mismatch** — a FK column type doesn't match the PK it references.
   Fix: run the reconciliation migration or create a new corrective migration.

### `MIGRATION_CHECKSUM_CHANGED` in logs

A migration file was edited after it was applied.  The migration will not be
re-applied.  If the change was intentional (e.g. a comment fix), this warning
is harmless.  If the change was a schema fix, create a new migration file
instead.

### `STARTUP_FAILED reason=migration_critical_failure`

A critical migration failed.  The app will not start.  Check the
`[MIGRATION_FAILED_CRITICAL]` log entry above it for the specific error.
Common causes:

1. **Database unreachable** — check `DATABASE_URL` and network connectivity.
2. **Permission denied** — the database user lacks `CREATE TABLE` privileges.
3. **Syntax error in SQL** — check the migration file for invalid SQL.

### Missing parent table errors (`relation does not exist`)

The `001`/`002`/`003` migrations reference `routes`, `drivers`, and
`route_stops`.  If those tables don't exist yet, add the dependency to
`MIGRATION_DEPENDENCIES`:

```python
"001_create_route_events_table.sql": [
    "2026_05_21_02_add_drivers_routes_stops.sql",
],
```

### UUID/INTEGER FK mismatch

Run the reconciliation migration:
`2026_05_30_01_reconcile_uuid_integer_mismatches.sql`

If it has already been applied, check the `[MIGRATION_VALIDATION_FAIL]` log
entry for the specific table and column, then create a new corrective migration.

---

## Adding a New Migration

1. Create a new `.sql` file in `migrations/` with a timestamp-based name:
   ```
   migrations/YYYY_MM_DD_NN_description.sql
   ```
2. Use `IF NOT EXISTS` / `IF EXISTS` guards wherever possible to make the
   migration idempotent.
3. If the migration depends on another migration that sorts later alphabetically,
   add it to `MIGRATION_DEPENDENCIES`.
4. If the migration is essential for the app to function, add it to
   `CRITICAL_MIGRATIONS`.
5. Test locally with the smoke test script before deploying.
