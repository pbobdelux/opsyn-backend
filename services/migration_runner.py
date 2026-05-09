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

Ordering guarantees (applied in priority order):
  1. LeafLink repair migrations (filenames matching ``fix_leaflink``) run first.
  2. Migrations listed in MIGRATION_DEPENDENCIES are topologically sorted so
     that base tables are always created before the tables that reference them.
     This fixes the ordering problem where 001/002/003 (which reference routes,
     drivers, route_stops) were executing before
     2026_05_21_02_add_drivers_routes_stops.sql created those tables.
  3. All remaining migrations run in alphabetical (timestamp) order.
"""

import logging
import os
import re
import time
from collections import defaultdict, deque

logger = logging.getLogger("migration_runner")

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "migrations")

# Migrations whose filenames match this pattern are promoted to run first.
_LEAFLINK_REPAIR_PATTERN = re.compile(r"fix_leaflink", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Explicit dependency graph for migrations that cannot be ordered purely by
# filename.  Keys are migration filenames; values are lists of filenames that
# MUST have been applied before the key migration runs.
#
# Background: the 001/002/003 files were created before the drivers/routes
# tables existed (2026_05_21_02).  Alphabetically "001" < "2026_05_21_02",
# so without this graph the runner would attempt to create route_events,
# driver_locations, and driver_route_history before the tables they reference
# exist — causing "relation does not exist" errors.
# ---------------------------------------------------------------------------
MIGRATION_DEPENDENCIES: dict[str, list[str]] = {
    # Base schema — no dependencies
    "2026_05_04_01_setup_auth_tables.sql": [],
    "2026_05_04_02_create_full_schema_aws_rds.sql": [],

    # drivers / routes / route_stops must exist before the tables below
    "001_create_route_events_table.sql": [
        "2026_05_21_02_add_drivers_routes_stops.sql",
    ],
    "002_create_driver_locations_table.sql": [
        "2026_05_21_02_add_drivers_routes_stops.sql",
    ],
    "003_create_driver_route_history_table.sql": [
        "2026_05_21_02_add_drivers_routes_stops.sql",
    ],

    # Reconciliation must run after all base tables and the 001/002/003 group
    "2026_05_30_01_reconcile_uuid_integer_mismatches.sql": [
        "2026_05_04_02_create_full_schema_aws_rds.sql",
        "2026_05_21_02_add_drivers_routes_stops.sql",
        "001_create_route_events_table.sql",
        "002_create_driver_locations_table.sql",
        "003_create_driver_route_history_table.sql",
        "2026_05_28_02_migrate_sync_runs_to_uuid.sql",
        "2026_05_28_03_migrate_orders_to_uuid.sql",
        "2026_05_28_04_migrate_remaining_integer_tables.sql",
    ],
}

# Migrations that are CRITICAL: a failure in one of these raises immediately
# and aborts the startup sequence.  All other migrations are NONCRITICAL —
# failures are logged and the runner continues with the remaining files.
CRITICAL_MIGRATIONS: frozenset[str] = frozenset({
    "2026_05_04_01_setup_auth_tables.sql",
    "2026_05_04_02_create_full_schema_aws_rds.sql",
    "2026_05_30_01_reconcile_uuid_integer_mismatches.sql",
})

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


def _topological_sort(files: list[str], deps: dict[str, list[str]]) -> list[str]:
    """
    Return *files* in topological order respecting *deps*.

    Files not mentioned in *deps* are treated as having no dependencies and
    are interleaved in alphabetical order relative to their position in the
    resolved graph.

    Algorithm: Kahn's BFS (stable — preserves alphabetical order among nodes
    with equal in-degree so the output is deterministic).

    Raises ``ValueError`` on a dependency cycle.
    """
    # Only consider files that are actually present on disk.
    file_set = set(files)

    # Build adjacency list and in-degree map restricted to present files.
    # Edge: dep → f  (dep must run before f)
    in_degree: dict[str, int] = defaultdict(int)
    dependents: dict[str, list[str]] = defaultdict(list)  # dep → [files that need dep]

    for f in files:
        if f not in in_degree:
            in_degree[f] = 0  # ensure every file appears in the map

    for f, prerequisites in deps.items():
        if f not in file_set:
            continue
        for prereq in prerequisites:
            if prereq not in file_set:
                # Prerequisite not present on disk — skip (may already be applied
                # or intentionally absent in this environment).
                continue
            dependents[prereq].append(f)
            in_degree[f] += 1

    # Seed the queue with all nodes that have no unresolved prerequisites,
    # sorted alphabetically for a stable, deterministic output.
    queue: deque[str] = deque(sorted(f for f in files if in_degree[f] == 0))
    result: list[str] = []

    while queue:
        node = queue.popleft()
        result.append(node)
        # Reduce in-degree for every file that depends on this node.
        newly_free = []
        for dependent in dependents.get(node, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                newly_free.append(dependent)
        # Insert newly-freed nodes in alphabetical order to keep output stable.
        for f in sorted(newly_free):
            queue.append(f)

    if len(result) != len(files):
        cycle_nodes = [f for f in files if f not in set(result)]
        raise ValueError(
            f"[Migration] Dependency cycle detected among: {cycle_nodes}"
        )

    return result


def _order_migrations(sql_files: list[str]) -> list[str]:
    """
    Return migration filenames in execution order:
      1. LeafLink repair migrations (filenames matching ``fix_leaflink``), sorted.
      2. All other migrations, topologically sorted by MIGRATION_DEPENDENCIES
         (base tables before dependent tables), then alphabetically within
         each tier.

    This ensures that e.g. 2026_05_21_02_add_drivers_routes_stops.sql always
    runs before 001_create_route_events_table.sql even though "001" sorts
    before "2026" alphabetically.
    """
    repair = sorted(f for f in sql_files if _LEAFLINK_REPAIR_PATTERN.search(f))
    others = [f for f in sql_files if not _LEAFLINK_REPAIR_PATTERN.search(f)]

    try:
        ordered_others = _topological_sort(others, MIGRATION_DEPENDENCIES)
    except ValueError as exc:
        logger.error("%s — falling back to alphabetical order", exc)
        ordered_others = sorted(others)

    return repair + ordered_others


async def run_migrations() -> list[str]:
    """
    Scan the migrations/ directory, apply any pending .sql files in dependency
    order, and record each applied migration in the schema_migrations table.

    - LeafLink repair migrations run first (see ``_order_migrations``).
    - Migrations in MIGRATION_DEPENDENCIES are topologically sorted so base
      tables are always created before the tables that reference them.
    - Migrations in CRITICAL_MIGRATIONS raise immediately on failure, aborting
      startup.  All other migrations are NONCRITICAL — failures are logged and
      the runner continues with the remaining files.
    - A [MIGRATION_SUMMARY] line is logged after all migrations have been
      attempted, reporting applied/failed_critical/failed_noncritical counts.

    Returns a list of migration filenames that were applied in this run.
    """
    from database import engine

    if engine is None:
        logger.warning("[Migration] DATABASE_URL not set — skipping migrations")
        return []

    applied: list[str] = []
    failed_critical: list[str] = []
    failed_noncritical: list[str] = []

    try:
        # Collect and sort migration files by dependency order.
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
        # failure in one file does not abort the remaining migrations (unless the
        # migration is classified as CRITICAL).
        for order_idx, filename in enumerate(sql_files, start=1):
            is_critical = filename in CRITICAL_MIGRATIONS
            logger.info(
                "[MIGRATION_ORDER] executing_migration=%s order=%d critical=%s",
                filename,
                order_idx,
                is_critical,
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
                    "[MIGRATION_EXECUTED] migration=%s status=success duration_ms=%s",
                    filename,
                    duration_ms,
                )
                applied.append(filename)

            except Exception as migration_exc:
                # Transaction is automatically rolled back by the context manager.
                if is_critical:
                    logger.error(
                        "[MIGRATION_FAILED_CRITICAL] migration=%s error=%s",
                        filename,
                        migration_exc,
                        exc_info=True,
                    )
                    failed_critical.append(filename)
                    raise RuntimeError(
                        f"[Migration] Critical migration failed: {filename}: {migration_exc}"
                    ) from migration_exc
                else:
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
                    failed_noncritical.append(filename)
                    # Continue with remaining migrations rather than aborting startup

    except RuntimeError:
        # Re-raise critical failures (bootstrap failure or critical migration
        # failure) immediately so the caller can hard-fail startup.
        raise
    except Exception as exc:
        logger.error("[Migration] runner error=%s", exc, exc_info=True)

    total = len(applied) + len(failed_critical) + len(failed_noncritical)
    logger.info(
        "[MIGRATION_SUMMARY] total=%d applied=%d failed_critical=%d failed_noncritical=%d",
        total,
        len(applied),
        len(failed_critical),
        len(failed_noncritical),
    )
    if failed_noncritical:
        logger.warning(
            "[MIGRATION_SUMMARY] failed_noncritical_migrations=%s",
            ", ".join(failed_noncritical),
        )

    return applied
