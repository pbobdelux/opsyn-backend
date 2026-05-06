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

Ordering guarantee: any migration whose filename contains "fix_leaflink" is
promoted to run first, before all other migrations (alphabetical within that
group, then alphabetical for the rest).
"""

import logging
import os
import re
import time

logger = logging.getLogger("migration_runner")

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "migrations")

# Migrations whose filenames match this pattern are promoted to run first.
_LEAFLINK_REPAIR_PATTERN = re.compile(r"fix_leaflink", re.IGNORECASE)

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
    Split a SQL file into individual statements, correctly handling
    dollar-quoted blocks (DO $ ... $) so that semicolons inside
    PL/pgSQL bodies are not treated as statement terminators.

    Algorithm:
      - Scan character-by-character tracking whether we are inside a
        dollar-quote (e.g. ``$`` or ``$tag`).
      - Only split on ``;`` when outside a dollar-quote.
      - Strip comment-only lines and blank lines from each resulting chunk.
    """
    statements: list[str] = []
    current: list[str] = []
    dollar_tag: str | None = None  # None = not inside a dollar-quote
    i = 0

    while i < len(sql):
        ch = sql[i]

        if dollar_tag is None:
            # Look for the start of a dollar-quote: $tag$ or $
            if ch == "$":
                # Find the closing $ of the opening tag
                j = sql.find("$", i + 1)
                if j != -1:
                    tag = sql[i : j + 1]  # e.g. "$" or "$body$"
                    dollar_tag = tag
                    current.append(tag)
                    i = j + 1
                    continue
            if ch == ";":
                chunk = "".join(current)
                # Strip comment-only lines and blank lines
                lines = [
                    line for line in chunk.splitlines()
                    if line.strip() and not line.strip().startswith("--")
                ]
                stmt = "\n".join(lines).strip()
                if stmt:
                    statements.append(stmt)
                current = []
                i += 1
                continue
        else:
            # Inside a dollar-quote — look for the matching closing tag
            if sql[i : i + len(dollar_tag)] == dollar_tag:
                current.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
                continue

        current.append(ch)
        i += 1

    # Handle any trailing content after the last semicolon
    if current:
        chunk = "".join(current)
        lines = [
            line for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)

    return statements


def _order_migrations(sql_files: list[str]) -> list[str]:
    """
    Return migration filenames in execution order:
      1. LeafLink repair migrations (filenames matching ``fix_leaflink``), sorted.
      2. All other migrations, sorted alphabetically.
    """
    repair = sorted(f for f in sql_files if _LEAFLINK_REPAIR_PATTERN.search(f))
    others = sorted(f for f in sql_files if not _LEAFLINK_REPAIR_PATTERN.search(f))
    return repair + others


async def run_migrations() -> list[str]:
    """
    Scan the migrations/ directory, apply any pending .sql files in order,
    and record each applied migration in the schema_migrations table.

    - LeafLink repair migrations run first (see ``_order_migrations``).
    - Non-critical migration failures are logged and skipped; the runner
      continues with the remaining migrations.
    - A summary is logged after all migrations have been attempted.

    Returns a list of migration filenames that were applied in this run.
    """
    from database import engine

    if engine is None:
        logger.warning("[Migration] DATABASE_URL not set — skipping migrations")
        return []

    applied: list[str] = []
    failed: list[str] = []

    try:
        # Collect and sort migration files by filename (timestamp-based ordering)
        if not os.path.isdir(MIGRATIONS_DIR):
            logger.warning("[Migration] migrations directory not found at %s", MIGRATIONS_DIR)
            return []

        raw_files = [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql")]
        sql_files = _order_migrations(raw_files)

        if not sql_files:
            logger.info("[Migration] no migration files found")
            return []

        # Bootstrap: ensure the tracking table exists in its own transaction.
        # This is a CRITICAL step — if it fails, we cannot track migrations at all.
        try:
            async with engine.connect() as bootstrap_conn:
                for stmt in _split_statements(CREATE_SCHEMA_MIGRATIONS_TABLE):
                    await bootstrap_conn.exec_driver_sql(stmt)
                await bootstrap_conn.commit()
        except Exception as bootstrap_exc:
            logger.error(
                "[Migration] CRITICAL bootstrap failed — cannot create schema_migrations table: %s",
                bootstrap_exc,
                exc_info=True,
            )
            raise RuntimeError(
                f"[Migration] Critical failure: could not bootstrap schema_migrations table: {bootstrap_exc}"
            )

        # Apply each migration in an independent top-level transaction so that a
        # failure in one file does not abort the remaining migrations.
        for order_idx, filename in enumerate(sql_files, start=1):
            logger.info(
                "[MIGRATION_ORDER] executing_migration=%s order=%d",
                filename,
                order_idx,
            )
            try:
                # Use a plain connection (no implicit transaction) to check whether
                # this migration has already been applied.
                async with engine.connect() as check_conn:
                    row = await check_conn.exec_driver_sql(
                        "SELECT 1 FROM schema_migrations WHERE migration_name = $1",
                        (filename,),
                    )
                    if row.fetchone() is not None:
                        logger.debug("[Migration] already applied migration=%s — skipping", filename)
                        continue

                # Read the migration SQL before opening the transaction connection.
                migration_path = os.path.join(MIGRATIONS_DIR, filename)
                with open(migration_path, "r", encoding="utf-8") as fh:
                    sql = fh.read()

                logger.info("[Migration] applying migration=%s", filename)
                start_ms = time.monotonic()

                # Use engine.begin() so the connection starts a fresh top-level
                # transaction — no nested begin() calls that can trigger
                # "cannot execute DDL in a read-only transaction" errors.
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

                    # Record the migration as applied inside the same transaction
                    # so it is atomically committed or rolled back with the migration.
                    await conn.exec_driver_sql(
                        "INSERT INTO schema_migrations (migration_name, duration_ms) "
                        "VALUES ($1, $2)",
                        (filename, duration_ms),
                    )
                # Transaction is committed automatically when engine.begin() block exits.

                logger.info(
                    "[Migration] applied migration=%s duration_ms=%s",
                    filename,
                    duration_ms,
                )
                applied.append(filename)

            except Exception as migration_exc:
                # Transaction is automatically rolled back by the context manager on exception.
                # Log as non-critical and continue — one broken migration must not abort startup.
                logger.warning(
                    "[MIGRATION_FAILED_NONCRITICAL] migration=%s error=%s",
                    filename,
                    migration_exc,
                )
                logger.debug(
                    "[Migration] failed migration=%s full traceback",
                    filename,
                    exc_info=True,
                )
                failed.append(filename)
                # Continue with remaining migrations rather than aborting startup

    except RuntimeError:
        # Re-raise critical failures (e.g. bootstrap failure) immediately.
        raise
    except Exception as exc:
        logger.error("[Migration] runner error=%s", exc, exc_info=True)

    total = len(applied) + len(failed)
    logger.info(
        "[MIGRATION_SUMMARY] total=%d passed=%d failed=%d",
        total,
        len(applied),
        len(failed),
    )
    if failed:
        logger.warning(
            "[MIGRATION_SUMMARY] failed_migrations=%s",
            ", ".join(failed),
        )

    return applied
