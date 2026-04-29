"""
Migration runner — applies pending SQL migrations on startup.

Tracks applied migrations in a `schema_migrations` table so each migration
is only ever executed once (idempotent). Migrations are discovered by scanning
the `migrations/` directory for `.sql` files, sorted by filename (timestamp-based).
"""

import logging
import os
import time

from sqlalchemy import text

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

        async with engine.begin() as conn:
            # Ensure the tracking table exists before anything else
            await conn.execute(text(CREATE_SCHEMA_MIGRATIONS_TABLE))

            for filename in sql_files:
                try:
                    # Check whether this migration has already been applied
                    row = await conn.execute(
                        text(
                            "SELECT 1 FROM schema_migrations WHERE migration_name = :name"
                        ),
                        {"name": filename},
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

                    await conn.execute(text(sql))

                    duration_ms = int((time.monotonic() - start_ms) * 1000)

                    # Record the migration as applied
                    await conn.execute(
                        text(
                            "INSERT INTO schema_migrations (migration_name, duration_ms) "
                            "VALUES (:name, :duration_ms)"
                        ),
                        {"name": filename, "duration_ms": duration_ms},
                    )

                    logger.info(
                        "[Migration] applied migration=%s duration_ms=%s",
                        filename,
                        duration_ms,
                    )
                    applied.append(filename)

                except Exception as migration_exc:
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
