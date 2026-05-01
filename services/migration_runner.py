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
                # Use a short-lived connection just to check whether this migration
                # has already been applied, before opening the write transaction.
                async with engine.connect() as check_conn:
                    row = await check_conn.exec_driver_sql(
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

                # engine.begin() opens a connection and starts a transaction in one
                # step, so there is no prior autobegin to conflict with.  Calling
                # conn.begin() on top of an engine.connect() connection would raise
                # "can't call begin() here" because SQLAlchemy already issued an
                # implicit BEGIN via autobegin.
                async with engine.begin() as conn:
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
                # Commit happens automatically when the `async with engine.begin()` block exits

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
