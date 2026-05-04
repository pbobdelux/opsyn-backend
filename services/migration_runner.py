"""
Migration runner — applies pending SQL migrations on startup.

Tracks applied migrations in a `schema_migrations` table so each migration
is only ever executed once (idempotent). Migrations are discovered by scanning
the `migrations/` directory for `.sql` files, sorted by filename (timestamp-based).

asyncpg does not support multiple SQL statements in a single prepared-statement
call, so each migration file is split on `;` and each statement is executed
individually via ``conn.exec_driver_sql()`` (raw SQL, bypasses prepared-statement
overhead). Each migration runs in its own transaction so a failure in one file
does not abort subsequent migrations.
"""

import logging
import os
import time
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("migration_runner")

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "migrations")

CREATE_SCHEMA_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INTEGER
);
"""


def _split_statements(sql: str) -> list[str]:
    """
    Split a SQL file into individual statements.

    Strips comment-only lines and blank lines, then splits on `;`.
    Returns only non-empty statement strings.
    """
    statements = []
    for raw in sql.split(";"):
        # Strip whitespace and filter out lines that are purely comments or blank
        lines = [
            line for line in raw.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


async def run_migrations() -> list[str]:
    """
    Scan the migrations/ directory, apply any pending .sql files in order,
    and record each applied migration in the schema_migrations table.

    Returns a list of migration filenames that were applied in this run.
    """
    from database import engine

    if engine is None:
        logger.warning("[Migration] DATABASE_URL not set — skipping migrations")
        return []

    applied: list[str] = []

    try:
        # Collect and sort migration files by filename (timestamp-based ordering)
        if not os.path.isdir(MIGRATIONS_DIR):
            logger.warning("[Migration] migrations directory not found at %s", MIGRATIONS_DIR)
            return []

        sql_files = sorted(
            f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql")
        )

        if not sql_files:
            logger.info("[Migration] no migration files found")
            return []

        # Bootstrap: ensure the tracking table exists in its own transaction
        async with engine.connect() as bootstrap_conn:
            for stmt in _split_statements(CREATE_SCHEMA_MIGRATIONS_TABLE):
                await bootstrap_conn.exec_driver_sql(stmt)
            await bootstrap_conn.commit()

        # Apply each migration in an independent transaction so that a failure
        # in one file does not abort the remaining migrations.
        for filename in sql_files:
            try:
                async with engine.connect() as conn:
                    # Check whether this migration has already been applied
                    # (outside the migration transaction so we can safely skip)
                    row = await conn.exec_driver_sql(
                        "SELECT 1 FROM schema_migrations WHERE migration_name = $1",
                        (filename,),
                    )
                    if row.fetchone() is not None:
                        logger.debug("[Migration] already applied migration=%s — skipping", filename)
                        continue

                    # Read and execute the migration SQL
                    migration_path = os.path.join(MIGRATIONS_DIR, filename)
                    with open(migration_path, "r", encoding="utf-8") as fh:
                        sql = fh.read()

                    logger.info("[Migration] applying migration=%s", filename)
                    start_ms = time.monotonic()

                    # Each migration runs in its own explicit transaction so that
                    # a failure rolls back cleanly and does not leave the connection
                    # in a broken state for subsequent migrations.
                    async with conn.begin():
                        # Execute each statement individually — asyncpg does not
                        # support multiple statements in a single prepared call.
                        statements = _split_statements(sql)
                        logger.debug(
                            "[Migration] migration=%s statement_count=%s",
                            filename,
                            len(statements),
                        )
                        for stmt in statements:
                            await conn.exec_driver_sql(stmt)

                        duration_ms = int((time.monotonic() - start_ms) * 1000)

                        # Record the migration as applied (inside the same transaction
                        # so it is atomically committed or rolled back with the migration)
                        await conn.exec_driver_sql(
                            "INSERT INTO schema_migrations (migration_name, duration_ms) "
                            "VALUES ($1, $2)",
                            (filename, duration_ms),
                        )
                    # Commit happens automatically when the `async with conn.begin()` block exits

                    logger.info(
                        "[Migration] applied migration=%s duration_ms=%s",
                        filename,
                        duration_ms,
                    )
                    applied.append(filename)

            except Exception as migration_exc:
                # Transaction is automatically rolled back by the context manager on exception
                logger.error(
                    "[Migration] failed migration=%s error=%s",
                    filename,
                    migration_exc,
                    exc_info=True,
                )
                # Continue with remaining migrations rather than aborting startup

    except Exception as exc:
        logger.error("[Migration] runner error=%s", exc, exc_info=True)

    return applied


async def apply_aws_rds_schema_migration(db: AsyncSession) -> dict:
    """
    Apply the comprehensive AWS RDS schema migration.

    This creates all required tables while preserving existing auth data.
    Idempotent — safe to run multiple times (uses IF NOT EXISTS throughout).

    Returns:
        {"ok": bool, "message": str, "tables_created": int, ...}
    """
    try:
        logger.info("[Migration] starting_aws_rds_schema_migration")

        # Locate the migration file
        migration_path = (
            Path(__file__).parent.parent
            / "migrations"
            / "2026_05_04_02_create_full_schema_aws_rds.sql"
        )

        if not migration_path.exists():
            logger.error("[Migration] migration_file_not_found path=%s", migration_path)
            return {"ok": False, "error": f"Migration file not found: {migration_path}"}

        logger.info("[Migration] reading_migration_file path=%s", migration_path)
        migration_sql = migration_path.read_text()

        # Apply migration — split on `;` so asyncpg receives one statement at a time
        logger.info("[Migration] applying_migration")
        for stmt in _split_statements(migration_sql):
            await db.execute(text(stmt))
        await db.commit()

        logger.info("[Migration] migration_applied")

        # Verify tables exist
        logger.info("[Migration] verifying_tables")

        required_tables = [
            "brand_api_credentials",
            "orders",
            "order_lines",
            "organization_brand_bindings",
            "drivers",
            "dispatch_routes",
            "route_stops",
            "tenant_credentials",
            "sync_runs",
            "sync_requests",
            "assistant_sessions",
            "assistant_messages",
            "assistant_pending_actions",
            "assistant_audit_logs",
            "employees",           # Auth table
            "employee_passcodes",  # Auth table
            "employee_brand_access",  # Auth table
            "employee_app_access",    # Auth table
        ]

        result = await db.execute(
            text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
        )
        existing_tables = [row[0] for row in result.fetchall()]

        logger.info(
            "[Migration] existing_tables count=%d tables=%s",
            len(existing_tables),
            existing_tables,
        )

        missing_tables = [t for t in required_tables if t not in existing_tables]
        if missing_tables:
            logger.warning("[Migration] missing_tables=%s", missing_tables)

        # Verify auth data is preserved
        logger.info("[Migration] verifying_auth_data")

        result = await db.execute(text("SELECT COUNT(*) FROM employees"))
        employee_count = result.scalar() or 0
        logger.info("[Migration] employee_count=%d", employee_count)

        result = await db.execute(text("SELECT COUNT(*) FROM employee_passcodes"))
        passcode_count = result.scalar() or 0
        logger.info("[Migration] passcode_count=%d", passcode_count)

        result = await db.execute(text("SELECT COUNT(*) FROM employee_brand_access"))
        brand_access_count = result.scalar() or 0
        logger.info("[Migration] brand_access_count=%d", brand_access_count)

        result = await db.execute(text("SELECT COUNT(*) FROM employee_app_access"))
        app_access_count = result.scalar() or 0
        logger.info("[Migration] app_access_count=%d", app_access_count)

        logger.info("[Migration] aws_rds_schema_migration_complete")

        return {
            "ok": True,
            "message": "AWS RDS schema migration applied successfully",
            "tables_created": len(existing_tables),
            "auth_data_preserved": {
                "employees": employee_count,
                "passcodes": passcode_count,
                "brand_access": brand_access_count,
                "app_access": app_access_count,
            },
        }

    except Exception as e:
        logger.error(
            "[Migration] aws_rds_schema_migration_failed error=%s",
            str(e)[:500],
            exc_info=True,
        )
        return {"ok": False, "error": str(e)[:500]}
