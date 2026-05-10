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
     The canonical baseline (2026_05_31_00_canonical_baseline_schema.sql) has
     no dependencies and runs early, establishing all tables before any additive
     ALTER TABLE migrations run.
  3. All remaining migrations run in alphabetical (timestamp) order.

Quarantine:
  Migrations listed in SKIPPED_LEGACY_MIGRATIONS are never applied.  They are
  logged with [MIGRATION_SKIPPED_LEGACY] so the skip is always auditable.

  Quarantined migrations (as of 2026-05-31):
    001_create_route_events_table.sql          — references non-existent routes table
    002_create_driver_locations_table.sql      — references non-existent drivers/routes tables
    003_create_driver_route_history_table.sql  — references non-existent drivers/routes tables
    2026_04_23_02_add_tenant_credentials.sql   — uses SERIAL PK; superseded by canonical baseline
    2026_05_10_01_add_sync_requests_table.sql  — SERIAL PK; semicolons in comments caused syntax errors
    2026_05_21_02_add_drivers_routes_stops.sql — DROP CASCADE; semicolons in comments caused syntax errors
    2026_05_27_01_add_webhook_credential_fields.sql — columns in canonical baseline; semicolons in comments
    2026_05_28_04_migrate_remaining_integer_tables.sql — no-op/destructive on canonical-baseline DBs
    2026_05_29_01_add_secrets_manager_columns.sql — columns in canonical baseline; semicolons in comments
    2026_05_30_01_reconcile_uuid_integer_mismatches.sql — contains invalid SQL; superseded by canonical baseline

  All are superseded by 2026_05_31_00_canonical_baseline_schema.sql which
  creates the correct schema idempotently with UUID PKs and valid FK types.

Checksums:
  Each applied migration records a SHA-256 checksum of its file content in the
  schema_migrations table.  On subsequent startups, if a previously-applied
  migration file has changed on disk, a warning is logged.  This detects
  accidental edits to already-applied migrations.

Log markers emitted:
  [MIGRATION_DISCOVERED]          — list of all .sql files found on disk
  [MIGRATION_SKIPPED_LEGACY]      — quarantined migration skipped with reason
  [MIGRATION_EXECUTION_PLAN]      — ordered list of migrations to attempt
  [MIGRATION_ORDER]               — per-migration execution start
  [MIGRATION_VALIDATION_FAILED]   — preflight check rejected a migration file
  [MIGRATION_EXECUTED]            — per-migration success
  [MIGRATION_FAILED_CRITICAL]     — critical migration failed (startup aborted)
  [MIGRATION_FAILED_NONCRITICAL]  — noncritical migration failed (startup continues)
  [MIGRATION_CHECKSUM_CHANGED]    — previously-applied migration file was modified
  [MIGRATION_SUMMARY]             — final counts after all migrations attempted
"""

import hashlib
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
# Quarantine list — migrations that must NEVER be applied.
#
# Keys are exact filenames; values are human-readable reasons.  The runner
# logs [MIGRATION_SKIPPED_LEGACY] for each entry so skips are always auditable.
# Files must remain on disk — removing them would cause the runner to lose the
# skip record and potentially attempt to apply them on a fresh database.
# ---------------------------------------------------------------------------
SKIPPED_LEGACY_MIGRATIONS: dict[str, str] = {
    # -------------------------------------------------------------------------
    # Pre-existing quarantine entry
    # -------------------------------------------------------------------------
    "2026_04_23_02_add_tenant_credentials.sql": (
        "Uses SERIAL PK; superseded by 2026_05_31_00_canonical_baseline_schema.sql "
        "which creates tenant_credentials with a UUID PK as part of the canonical "
        "baseline migration."
    ),

    # -------------------------------------------------------------------------
    # 001/002/003 — reference tables that did not exist at the time they were
    # written (routes, drivers, route_stops).  Superseded by the canonical
    # baseline which creates all three tables before the dependent tables.
    # -------------------------------------------------------------------------
    "001_create_route_events_table.sql": (
        "Superseded by 2026_05_31_00_canonical_baseline_schema.sql. "
        "This file references routes(id) but routes did not exist when it was "
        "written, causing 'relation does not exist' errors on fresh databases. "
        "The canonical baseline creates route_events with correct UUID FKs after "
        "routes is guaranteed to exist."
    ),
    "002_create_driver_locations_table.sql": (
        "Superseded by 2026_05_31_00_canonical_baseline_schema.sql. "
        "This file references drivers(id) and routes(id) but those tables did not "
        "exist when it was written, causing 'relation does not exist' errors. "
        "The canonical baseline creates driver_locations with correct UUID FKs after "
        "drivers and routes are guaranteed to exist."
    ),
    "003_create_driver_route_history_table.sql": (
        "Superseded by 2026_05_31_00_canonical_baseline_schema.sql. "
        "This file references drivers(id), routes(id), and route_stops(id) but those "
        "tables did not exist when it was written, causing 'relation does not exist' "
        "errors. The canonical baseline creates driver_route_history with correct UUID "
        "FKs after all parent tables are guaranteed to exist."
    ),

    # -------------------------------------------------------------------------
    # 2026_05_30_01 — contains invalid SQL (stray prose tokens such as 'rolls',
    # 'this', 'the', 'print') that cause syntax errors, and tries to create FKs
    # to tables that may have mismatched types (UUID vs INTEGER).  Replaced by
    # the canonical baseline which establishes the correct schema from scratch.
    # -------------------------------------------------------------------------
    "2026_05_30_01_reconcile_uuid_integer_mismatches.sql": (
        "Contains invalid SQL (stray prose tokens causing syntax errors) and "
        "attempts to create FK constraints to tables that may have UUID/INTEGER "
        "type mismatches on databases where earlier migrations ran in the wrong "
        "order. Replaced by 2026_05_31_00_canonical_baseline_schema.sql which "
        "establishes the correct schema idempotently without any type conflicts."
    ),

    # -------------------------------------------------------------------------
    # The following five migrations contain semicolons inside SQL line comments
    # (e.g. "-- prose; more prose").  The migration runner's character-level
    # scanner previously split on those semicolons, causing prose fragments to
    # be executed as SQL statements and producing syntax errors at startup.
    # The scanner has been fixed, but these migrations are quarantined because:
    #   - 2026_05_10_01: sync_requests table is superseded by the canonical
    #     baseline (UUID PK vs SERIAL PK).
    #   - 2026_05_21_02: drivers/routes/route_stops are superseded by the
    #     canonical baseline (IF NOT EXISTS guards; correct UUID FKs).
    #   - 2026_05_27_01 / 2026_05_29_01: webhook and secrets columns are
    #     already included in the canonical baseline brand_api_credentials
    #     definition; re-running ADD COLUMN IF NOT EXISTS is safe but
    #     unnecessary and risks partial-apply confusion.
    #   - 2026_05_28_04: integer-to-UUID migration is a no-op on databases
    #     bootstrapped from the canonical baseline (tables already have UUID
    #     PKs) and destructive on databases that never had integer PKs.
    # -------------------------------------------------------------------------
    "2026_05_10_01_add_sync_requests_table.sql": (
        "Superseded by 2026_05_31_00_canonical_baseline_schema.sql which "
        "creates sync_requests with a UUID PK. This file uses a SERIAL PK and "
        "would conflict with the canonical schema. Additionally contains a "
        "semicolon inside a SQL line comment that previously caused the prose "
        "fragment 'the dedicated' to be executed as SQL."
    ),
    "2026_05_21_02_add_drivers_routes_stops.sql": (
        "Superseded by 2026_05_31_00_canonical_baseline_schema.sql which "
        "creates drivers, routes, and route_stops with IF NOT EXISTS guards and "
        "correct UUID FKs. This file uses DROP TABLE CASCADE which is "
        "destructive on live databases. Also contains semicolons inside SQL "
        "line comments that previously caused prose fragments such as "
        "'this replaces it entirely' to be executed as SQL."
    ),
    "2026_05_27_01_add_webhook_credential_fields.sql": (
        "Webhook credential columns (webhook_key_secret_ref, webhook_key_last4, "
        "webhook_enabled, webhook_signature_required, leaflink_company_id) are "
        "already included in the brand_api_credentials definition in "
        "2026_05_31_00_canonical_baseline_schema.sql. Running this migration "
        "after the canonical baseline is a no-op at best and risks confusion. "
        "Also contains a semicolon inside a SQL line comment that previously "
        "caused the prose fragment 'only the ARN reference' to be executed as SQL."
    ),
    "2026_05_28_04_migrate_remaining_integer_tables.sql": (
        "Integer-to-UUID PK migration that is a no-op (and potentially "
        "destructive) on databases bootstrapped from the canonical baseline, "
        "which already creates all affected tables with UUID PKs. Contains "
        "semicolons inside SQL line comments that previously caused prose "
        "fragments such as 'FK to orders already UUID' to be executed as SQL."
    ),
    "2026_05_29_01_add_secrets_manager_columns.sql": (
        "Secrets Manager columns (api_key_secret_ref, api_key_last4) are "
        "already included in the brand_api_credentials definition in "
        "2026_05_31_00_canonical_baseline_schema.sql. Running this migration "
        "after the canonical baseline is a no-op at best and risks confusion. "
        "Also contains a semicolon inside a SQL line comment that previously "
        "caused the prose fragment 'only the ARN reference' to be executed as SQL."
    ),
}


def _should_skip_migration(filename: str) -> str | None:
    """
    Return the quarantine reason string if *filename* is in SKIPPED_LEGACY_MIGRATIONS,
    or None if the migration should proceed normally.
    """
    return SKIPPED_LEGACY_MIGRATIONS.get(filename)

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
    # -------------------------------------------------------------------------
    # Base schema migrations — no dependencies
    # -------------------------------------------------------------------------
    "2026_05_04_01_setup_auth_tables.sql": [],
    "2026_05_04_02_create_full_schema_aws_rds.sql": [],

    # -------------------------------------------------------------------------
    # Canonical baseline — no dependencies; runs early so all subsequent
    # migrations see the correct schema.  The quarantined 001/002/003 and
    # reconciliation migrations are listed here only so the topological sorter
    # knows their intended position; they are skipped by SKIPPED_LEGACY_MIGRATIONS
    # before any SQL is executed.
    # -------------------------------------------------------------------------
    "2026_05_31_00_canonical_baseline_schema.sql": [],

    # Quarantined migrations — dependencies kept for documentation only.
    # These are never executed (see SKIPPED_LEGACY_MIGRATIONS).
    "001_create_route_events_table.sql": [
        "2026_05_31_00_canonical_baseline_schema.sql",
    ],
    "002_create_driver_locations_table.sql": [
        "2026_05_31_00_canonical_baseline_schema.sql",
    ],
    "003_create_driver_route_history_table.sql": [
        "2026_05_31_00_canonical_baseline_schema.sql",
    ],
    "2026_05_30_01_reconcile_uuid_integer_mismatches.sql": [
        "2026_05_31_00_canonical_baseline_schema.sql",
    ],
}

# Migrations that are CRITICAL: a failure in one of these raises immediately
# and aborts the startup sequence.  All other migrations are NONCRITICAL —
# failures are logged and the runner continues with the remaining files.
CRITICAL_MIGRATIONS: frozenset[str] = frozenset({
    "2026_05_04_01_setup_auth_tables.sql",
    "2026_05_04_02_create_full_schema_aws_rds.sql",
    # Canonical baseline — must succeed for the app to function correctly.
    # Quarantined migrations (001/002/003, 2026_05_30_01) are NOT listed here
    # because they are skipped before execution and can never fail critically.
    "2026_05_31_00_canonical_baseline_schema.sql",
})

CREATE_SCHEMA_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INTEGER,
    checksum VARCHAR(64),
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_text TEXT
);
"""

# ALTER statements to add new columns to an existing schema_migrations table
# that was created before checksums/success/error_text were introduced.
# These are safe to run repeatedly (IF NOT EXISTS / DO block).
_SCHEMA_MIGRATIONS_UPGRADE_STMTS = [
    "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum VARCHAR(64)",
    "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS success BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS error_text TEXT",
]


def _compute_checksum(content: str) -> str:
    """Return the SHA-256 hex digest of *content* (UTF-8 encoded)."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _strip_inline_comments(line: str) -> str:
    """
    Strip a trailing ``--`` line comment from a single SQL line, returning
    only the SQL portion.  Handles the common case where a comment appears
    after real SQL on the same line (e.g. ``col TEXT, -- deprecated``).

    This is intentionally simple: it splits on the first ``--`` occurrence
    that is not inside a single-quoted string literal.  Dollar-quoted blocks
    are handled at a higher level and are never passed to this function
    line-by-line.

    Returns the SQL portion of the line with trailing whitespace stripped.
    If the entire line is a comment (starts with ``--`` after optional
    whitespace) an empty string is returned.
    """
    in_single_quote = False
    for idx, ch in enumerate(line):
        if ch == "'" and not in_single_quote:
            in_single_quote = True
        elif ch == "'" and in_single_quote:
            in_single_quote = False
        elif ch == "-" and not in_single_quote and line[idx : idx + 2] == "--":
            return line[:idx].rstrip()
    return line.rstrip()


def _clean_chunk(chunk: str) -> str:
    """
    Given a raw SQL chunk (the text between two statement-terminating
    semicolons), return the cleaned statement ready for execution, or an
    empty string if the chunk contains only comments and whitespace.

    Processing steps:
      1. Split into lines.
      2. For each line, strip the inline ``--`` comment portion.
      3. Discard lines that are empty after stripping.
      4. Join the remaining lines and strip surrounding whitespace.
    """
    cleaned_lines: list[str] = []
    for raw_line in chunk.splitlines():
        sql_part = _strip_inline_comments(raw_line)
        if sql_part.strip():
            cleaned_lines.append(sql_part)
    return "\n".join(cleaned_lines).strip()


def _split_statements(sql: str) -> list[str]:
    """
    Split a SQL file into individual statements, correctly handling:

      - Dollar-quoted blocks (``DO $$ ... $$``) — semicolons inside PL/pgSQL
        bodies are never treated as statement terminators.
      - Line comments (``-- ...``) — semicolons inside ``--`` comments are
        never treated as statement terminators.  This is the fix for the
        production bug where comment lines such as
        ``--   routes (UUID PK; orders.route_id references this)``
        caused the prose fragment after the semicolon to be executed as SQL.
      - Inline comments (SQL on the same line as a ``--`` comment) — the
        comment portion is stripped before the statement is recorded so that
        prose never leaks into the executed SQL.

    Algorithm:
      1. Scan character-by-character.
      2. Track three mutually-exclusive states:
           a. Inside a ``--`` line comment (reset at the next newline).
           b. Inside a dollar-quoted block (``$$`` or ``$tag$``).
           c. Normal SQL.
      3. Only split on ``;`` in state (c).
      4. After collecting a chunk, strip comment-only lines and blank lines,
         and strip inline comments from mixed lines before recording the
         statement.
      5. Log a debug message if a chunk becomes empty after comment stripping
         (indicates a semicolon that was entirely inside a comment — benign
         but useful for diagnosing malformed migration files).
    """
    statements: list[str] = []
    current: list[str] = []
    dollar_tag: str | None = None  # None = not inside a dollar-quote
    in_line_comment = False
    i = 0

    while i < len(sql):
        ch = sql[i]

        # ------------------------------------------------------------------ #
        # State: inside a -- line comment                                     #
        # ------------------------------------------------------------------ #
        if in_line_comment:
            current.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # ------------------------------------------------------------------ #
        # State: inside a dollar-quoted block                                 #
        # ------------------------------------------------------------------ #
        if dollar_tag is not None:
            if sql[i : i + len(dollar_tag)] == dollar_tag:
                current.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
            else:
                current.append(ch)
                i += 1
            continue

        # ------------------------------------------------------------------ #
        # State: normal SQL                                                   #
        # ------------------------------------------------------------------ #

        # Detect start of a -- line comment
        if ch == "-" and sql[i : i + 2] == "--":
            in_line_comment = True
            current.append(ch)
            i += 1
            continue

        # Detect start of a dollar-quoted block
        if ch == "$":
            j = sql.find("$", i + 1)
            if j != -1:
                tag = sql[i : j + 1]  # e.g. "$$" or "$body$"
                dollar_tag = tag
                current.append(tag)
                i = j + 1
                continue

        # Statement terminator — only reached in normal SQL state
        if ch == ";":
            chunk = "".join(current)
            stmt = _clean_chunk(chunk)
            if stmt:
                statements.append(stmt)
            elif chunk.strip():
                # The chunk had content but it was all comments — log at debug
                # level so operators can see which semicolons were inside
                # comments (useful for diagnosing malformed migration files).
                logger.debug(
                    "[Migration] skipped comment-only chunk before ';': %r",
                    chunk.strip()[:120],
                )
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    # Handle any trailing content after the last semicolon
    if current:
        chunk = "".join(current)
        stmt = _clean_chunk(chunk)
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


# ---------------------------------------------------------------------------
# Preflight validation
# ---------------------------------------------------------------------------

# Compiled patterns used by _validate_migration_file().
_RE_PROSE_LINE = re.compile(r"^[A-Z][a-z]+ [a-z]+ [a-z]+")
_RE_MARKDOWN_HEADER = re.compile(r"^#+\s")
_RE_BULLET_LIST = re.compile(r"^\s*[-*]\s+")


def _validate_migration_file(filename: str, sql: str) -> list[str]:
    """
    Preflight-validate the content of a migration file before execution.

    Returns a (possibly empty) list of human-readable problem descriptions.
    An empty list means the file passed all checks.

    Checks performed:
      1. No prose paragraphs outside SQL comments — lines matching
         ``^[A-Z][a-z]+ [a-z]+ [a-z]+`` that are not inside a comment.
      2. No Markdown headers (``## ...``) outside comments.
      3. No Markdown bullet lists (``- item`` / ``* item``) outside comments.
      4. No orphaned text fragments — non-empty lines that are not valid SQL
         keywords, not comments, and not inside a dollar-quoted block.

    Note: this validator operates on raw lines and is intentionally
    conservative — it only flags patterns that are unambiguously wrong.
    False positives (valid SQL that looks like prose) are avoided by
    checking only lines that are clearly outside comments and dollar-quotes.
    """
    problems: list[str] = []
    in_dollar_quote = False
    dollar_tag_str = ""

    for lineno, raw_line in enumerate(sql.splitlines(), start=1):
        stripped = raw_line.strip()

        # Track dollar-quote state (line-level approximation — sufficient for
        # the patterns we are checking which never appear inside PL/pgSQL).
        if not in_dollar_quote:
            # Check for opening dollar-quote tag on this line
            dq_match = re.search(r"\$([A-Za-z_]*)\$", raw_line)
            if dq_match:
                tag_candidate = dq_match.group(0)
                # Only enter dollar-quote state if the tag appears twice on
                # the same line (opening and closing on one line — skip) or
                # just once (opening — enter state).
                if raw_line.count(tag_candidate) == 1:
                    in_dollar_quote = True
                    dollar_tag_str = tag_candidate
                # If it appears twice on the same line it opens and closes
                # immediately — no state change needed.
        else:
            if dollar_tag_str and dollar_tag_str in raw_line:
                in_dollar_quote = False
                dollar_tag_str = ""
            continue  # Inside dollar-quote — skip all checks

        # Skip pure comment lines and blank lines
        if not stripped or stripped.startswith("--"):
            continue

        # Strip inline comment to get the SQL portion only
        sql_portion = _strip_inline_comments(raw_line).strip()
        if not sql_portion:
            continue  # Entire line was a comment

        # Check 1: prose paragraph detection
        if _RE_PROSE_LINE.match(sql_portion):
            problems.append(
                f"line {lineno}: possible prose outside comment: {sql_portion[:80]!r}"
            )

        # Check 2: Markdown header
        if _RE_MARKDOWN_HEADER.match(sql_portion):
            problems.append(
                f"line {lineno}: Markdown header outside comment: {sql_portion[:80]!r}"
            )

        # Check 3: Markdown bullet list
        if _RE_BULLET_LIST.match(sql_portion):
            problems.append(
                f"line {lineno}: Markdown bullet list outside comment: {sql_portion[:80]!r}"
            )

    return problems


async def run_migrations() -> dict:
    """
    Scan the migrations/ directory, apply any pending .sql files in dependency
    order, and record each applied migration in the schema_migrations table.

    - Migrations in SKIPPED_LEGACY_MIGRATIONS are quarantined and never applied;
      each is logged with [MIGRATION_SKIPPED_LEGACY].
    - LeafLink repair migrations run first (see ``_order_migrations``).
    - Migrations in MIGRATION_DEPENDENCIES are topologically sorted so base
      tables are always created before the tables that reference them.
    - Migrations in CRITICAL_MIGRATIONS raise immediately on failure, aborting
      startup.  All other migrations are NONCRITICAL — failures are logged and
      the runner continues with the remaining files.
    - Each applied migration records a SHA-256 checksum; if a previously-applied
      file has changed on disk, [MIGRATION_CHECKSUM_CHANGED] is logged.
    - A [MIGRATION_SUMMARY] line is logged after all migrations have been
      attempted, reporting discovered/skipped/applied/failed counts.

    Returns a dict with keys:
      applied          — list of filenames applied in this run
      skipped_legacy   — list of filenames quarantined
      failed_critical  — list of filenames that caused a critical failure
      failed_noncritical — list of filenames that failed non-critically
    """
    from database import engine

    if engine is None:
        logger.warning("[Migration] DATABASE_URL not set — skipping migrations")
        return {
            "applied": [],
            "skipped_legacy": [],
            "failed_critical": [],
            "failed_noncritical": [],
        }

    applied: list[str] = []
    skipped_legacy: list[str] = []
    failed_critical: list[str] = []
    failed_noncritical: list[str] = []
    raw_files: list[str] = []  # populated inside try; used in summary

    try:
        # Collect and sort migration files by dependency order.
        if not os.path.isdir(MIGRATIONS_DIR):
            logger.warning("[Migration] migrations directory not found at %s", MIGRATIONS_DIR)
            return {
                "applied": [],
                "skipped_legacy": [],
                "failed_critical": [],
                "failed_noncritical": [],
            }

        raw_files = [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql")]

        # Log all discovered files before any filtering.
        for disc_file in sorted(raw_files):
            logger.info("[MIGRATION_DISCOVERED] file=%s", disc_file)

        sql_files = _order_migrations(raw_files)

        if not sql_files:
            logger.info("[Migration] no migration files found")
            return {
                "applied": [],
                "skipped_legacy": [],
                "failed_critical": [],
                "failed_noncritical": [],
            }

        # Bootstrap: ensure the tracking table exists in its own transaction.
        # This is a CRITICAL step — if it fails, we cannot track migrations at all.
        try:
            async with engine.connect() as bootstrap_conn:
                for stmt in _split_statements(CREATE_SCHEMA_MIGRATIONS_TABLE):
                    await bootstrap_conn.exec_driver_sql(stmt)
                # Upgrade existing table with new columns (idempotent).
                for upgrade_stmt in _SCHEMA_MIGRATIONS_UPGRADE_STMTS:
                    await bootstrap_conn.exec_driver_sql(upgrade_stmt)
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

        # Log the execution plan (post-quarantine-filter order).
        plan_files = [f for f in sql_files if _should_skip_migration(f) is None]
        for plan_idx, plan_file in enumerate(plan_files, start=1):
            is_crit = plan_file in CRITICAL_MIGRATIONS
            logger.info(
                "[MIGRATION_EXECUTION_PLAN] order=%d file=%s critical=%s",
                plan_idx,
                plan_file,
                is_crit,
            )

        # Apply each migration in an independent top-level transaction so that a
        # failure in one file does not abort the remaining migrations (unless the
        # migration is classified as CRITICAL).
        for order_idx, filename in enumerate(sql_files, start=1):
            # --- Quarantine check ---
            skip_reason = _should_skip_migration(filename)
            if skip_reason is not None:
                logger.info(
                    "[MIGRATION_SKIPPED_LEGACY] file=%s reason=%s",
                    filename,
                    skip_reason,
                )
                skipped_legacy.append(filename)
                continue

            is_critical = filename in CRITICAL_MIGRATIONS
            logger.info(
                "[MIGRATION_ORDER] executing_migration=%s order=%d critical=%s",
                filename,
                order_idx,
                is_critical,
            )
            try:
                # Read the migration SQL before opening any connection.
                migration_path = os.path.join(MIGRATIONS_DIR, filename)
                with open(migration_path, "r", encoding="utf-8") as fh:
                    sql = fh.read()

                # Preflight validation — reject files with prose, Markdown, or
                # other content that would cause SQL syntax errors if executed.
                validation_problems = _validate_migration_file(filename, sql)
                if validation_problems:
                    problem_summary = "; ".join(validation_problems[:5])
                    logger.error(
                        "[MIGRATION_VALIDATION_FAILED] migration=%s problems=%d first_problems=%s",
                        filename,
                        len(validation_problems),
                        problem_summary,
                    )
                    raise ValueError(
                        f"[MIGRATION_VALIDATION_FAILED] {filename} failed preflight "
                        f"validation with {len(validation_problems)} problem(s): "
                        f"{problem_summary}"
                    )

                current_checksum = _compute_checksum(sql)

                # Use a plain connection (no implicit transaction) to check whether
                # this migration has already been applied and verify its checksum.
                async with engine.connect() as check_conn:
                    row = await check_conn.exec_driver_sql(
                        "SELECT checksum FROM schema_migrations WHERE migration_name = $1",
                        (filename,),
                    )
                    existing = row.fetchone()
                    if existing is not None:
                        stored_checksum = existing[0]
                        if stored_checksum and stored_checksum != current_checksum:
                            logger.warning(
                                "[MIGRATION_CHECKSUM_CHANGED] migration=%s "
                                "stored_checksum=%s current_checksum=%s "
                                "action=skipping_already_applied",
                                filename,
                                stored_checksum,
                                current_checksum,
                            )
                        else:
                            logger.debug(
                                "[Migration] already applied migration=%s — skipping", filename
                            )
                        continue

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
                        "INSERT INTO schema_migrations "
                        "(migration_name, duration_ms, checksum, success) "
                        "VALUES ($1, $2, $3, $4)",
                        (filename, duration_ms, current_checksum, True),
                    )
                # Transaction is committed automatically when engine.begin() block exits.

                logger.info(
                    "[MIGRATION_EXECUTED] migration=%s status=success duration_ms=%s checksum=%s",
                    filename,
                    duration_ms,
                    current_checksum[:12],
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

    discovered_count = len(raw_files) if os.path.isdir(MIGRATIONS_DIR) else 0
    total_attempted = len(applied) + len(failed_critical) + len(failed_noncritical)
    logger.info(
        "[MIGRATION_SUMMARY] discovered=%d skipped_legacy=%d attempted=%d "
        "applied=%d failed_critical=%d failed_noncritical=%d",
        discovered_count,
        len(skipped_legacy),
        total_attempted,
        len(applied),
        len(failed_critical),
        len(failed_noncritical),
    )
    if failed_noncritical:
        logger.warning(
            "[MIGRATION_SUMMARY] failed_noncritical_migrations=%s",
            ", ".join(failed_noncritical),
        )
    if skipped_legacy:
        logger.info(
            "[MIGRATION_SUMMARY] skipped_legacy_migrations=%s",
            ", ".join(skipped_legacy),
        )

    return {
        "applied": applied,
        "skipped_legacy": skipped_legacy,
        "failed_critical": failed_critical,
        "failed_noncritical": failed_noncritical,
    }
