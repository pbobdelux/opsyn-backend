import asyncio
import logging
import os
from typing import AsyncGenerator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger("opsyn-backend")


def normalize_database_url(url: str) -> str:
    if not url:
        return url

    url = url.strip()

    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    parsed = urlparse(url)
    query_params = parse_qsl(parsed.query, keep_blank_values=True)

    # For asyncpg (postgresql+asyncpg driver):
    # - Remove 'sslmode' (asyncpg doesn't support it in URL)
    # - Keep 'ssl' (asyncpg does support it in URL)
    # - SSL will be forced via connect_args={"ssl": "require"} anyway
    filtered_params = [
        (k, v) for (k, v) in query_params
        if k.lower() != "sslmode"  # Remove sslmode - asyncpg doesn't support it
    ]

    rebuilt = parsed._replace(query=urlencode(filtered_params))
    return urlunparse(rebuilt)


# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
# Production uses AWS RDS Postgres (opsyn_prod database)
# Development can use Railway Postgres or local Postgres
#
# DATABASE_URL format:
#   AWS RDS: postgresql+asyncpg://user:pass@host:5432/database
#   Railway: postgresql+asyncpg://user:pass@postgres.railway.internal:5432/railway
#   Local:   postgresql+asyncpg://user:pass@localhost:5432/opsyn_dev
#
# Engine creation is DEFERRED — importing this module does NOT create the engine.
# Call initialize_database_after_bootstrap() after bootstrap completes.
# ============================================================================

# ---------------------------------------------------------------------------
# Lazy engine state — populated by initialize_database_after_bootstrap()
# ---------------------------------------------------------------------------
_engine = None
_AsyncSessionLocal = None
_bootstrap_complete = False

# ---------------------------------------------------------------------------
# Singleton engine tracking — prevents double initialization.
# ---------------------------------------------------------------------------
_engine_id: str | None = None
_engine_creation_lock: asyncio.Lock | None = None


def _get_engine_creation_lock() -> asyncio.Lock:
    global _engine_creation_lock
    if _engine_creation_lock is None:
        _engine_creation_lock = asyncio.Lock()
    return _engine_creation_lock

# DATABASE_URL is computed lazily so importing this module never raises.
DATABASE_URL: str = ""


class Base(DeclarativeBase):
    pass


def is_bootstrap_complete() -> bool:
    """Return True if initialize_database_after_bootstrap() has been called."""
    return _bootstrap_complete


def get_engine():
    """Return the async engine, raising if bootstrap has not run yet."""
    if _engine is None:
        raise RuntimeError(
            "Database not initialized — bootstrap must run first. "
            "Call initialize_database_after_bootstrap() before using the engine."
        )
    return _engine


def get_async_session_local():
    """Get AsyncSessionLocal, raising if database not initialized yet."""
    global _AsyncSessionLocal
    if _AsyncSessionLocal is None:
        raise RuntimeError(
            "[DB_NOT_INITIALIZED] AsyncSessionLocal not available — "
            "database must be initialized via initialize_database_after_bootstrap() first"
        )
    logger.info("[DB_SESSION_FACTORY_REQUESTED] returning AsyncSessionLocal")
    return _AsyncSessionLocal


def get_engine_id() -> str | None:
    """Return the hex engine_id of the canonical engine, or None if not yet initialized."""
    return _engine_id


async def validate_single_engine() -> None:
    """Verify that only one engine exists and it matches the canonical engine_id.

    Raises RuntimeError if:
    - No engine has been initialized yet.
    - The current engine's id does not match the recorded _engine_id.
    - The pool settings are not pool_size=20, max_overflow=40.

    Logs [DB_ENGINE_SINGLETON_VERIFIED] on success.
    """
    if _engine is None:
        raise RuntimeError(
            "[DB_ENGINE_SINGLETON_VIOLATION] validate_single_engine() called but "
            "_engine is None — initialize_database_after_bootstrap() has not run."
        )
    if _engine_id is None:
        raise RuntimeError(
            "[DB_ENGINE_SINGLETON_VIOLATION] validate_single_engine() called but "
            "_engine_id is None — engine was created outside initialize_database_after_bootstrap()."
        )
    current_id = hex(id(_engine))
    if current_id != _engine_id:
        raise RuntimeError(
            f"[DB_ENGINE_SINGLETON_VIOLATION] engine_id mismatch: "
            f"current={current_id} canonical={_engine_id} — "
            "a second engine was created outside initialize_database_after_bootstrap()."
        )
    _pool = _engine.pool
    _ps = _pool.size()
    _mo = _pool._max_overflow
    if _ps != 20:
        raise RuntimeError(
            f"[DB_ENGINE_SINGLETON_VIOLATION] pool_size={_ps} expected=20 — "
            "engine has wrong pool settings."
        )
    if _mo != 40:
        raise RuntimeError(
            f"[DB_ENGINE_SINGLETON_VIOLATION] max_overflow={_mo} expected=40 — "
            "engine has wrong pool settings."
        )
    logger.info(
        "[DB_ENGINE_SINGLETON_VERIFIED] engine_id=%s pool_size=%d max_overflow=%d — "
        "single canonical engine confirmed",
        _engine_id,
        _ps,
        _mo,
    )


def _build_database_url() -> str:
    """Normalize and validate DATABASE_URL from the environment."""
    raw = os.getenv("DATABASE_URL", "")

    if not raw:
        logger.error("[BOOT_DB] DATABASE_URL is empty or not set")
        raise ValueError("DATABASE_URL environment variable is not set")

    logger.info("[BOOT_DB] validating_database_url")

    # Check for common malformations
    if raw.startswith("DATABASE_URL"):
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=starts_with_DATABASE_URL"
        )
        raise ValueError(
            "DATABASE_URL is malformed: value starts with 'DATABASE_URL'. "
            "Remove the 'DATABASE_URL=' prefix from the value."
        )

    if "\n" in raw or "\r" in raw:
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=contains_newlines"
        )
        raise ValueError(
            "DATABASE_URL is malformed: contains newlines. "
            "Ensure the value is a single line with no line breaks."
        )

    if raw.startswith('"') or raw.startswith("'"):
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=starts_with_quote"
        )
        raise ValueError(
            "DATABASE_URL is malformed: value starts with a quote. "
            "Remove quotes from the variable value."
        )

    # Try to parse as SQLAlchemy URL
    try:
        from sqlalchemy.engine.url import make_url
        _parsed_url = make_url(raw)
        logger.info(
            "[BOOT_DB] database_url_valid drivername=%s host=%s database=%s",
            _parsed_url.drivername,
            _parsed_url.host or "unknown",
            _parsed_url.database or "unknown",
        )
    except Exception as _parse_exc:
        logger.error(
            "[BOOT_DB] invalid_database_url raw_prefix=%s error=%s",
            raw[:25] if raw else "EMPTY",
            str(_parse_exc),
        )
        raise ValueError(
            f"DATABASE_URL is not a valid SQLAlchemy URL: {str(_parse_exc)}. "
            f"Expected format: postgresql+asyncpg://user:pass@host:5432/database"
        )

    # Ensure sslmode=require is in the URL before normalization
    if "sslmode=require" not in raw and "ssl=require" not in raw:
        logger.warning("[BOOT_DB] sslmode_not_in_url adding_sslmode=require")
        if "?" in raw:
            raw = raw + "&sslmode=require"
        else:
            raw = raw + "?sslmode=require"
        logger.info("[BOOT_DB] sslmode_added")

    url = normalize_database_url(raw)

    if url:
        _parsed = urlparse(url)
        _safe_url = f"{_parsed.scheme}://{_parsed.hostname}:{_parsed.port}/{_parsed.path.lstrip('/')}"
        logger.info("[Database] DATABASE_URL=%s", _safe_url)

        _host = _parsed.hostname or "unknown"
        _port = _parsed.port or 5432
        _db = _parsed.path.lstrip("/") or "unknown"

        logger.info("[DB] AWS_RDS_CONNECTION host=%s port=%s database=%s", _host, _port, _db)

        if "rds.amazonaws.com" in _host:
            logger.info("[DB] CONNECTED_TO_AWS_RDS")
        else:
            logger.warning("[DB] NOT_CONNECTED_TO_AWS_RDS host=%s", _host)

    return url


async def initialize_database_after_bootstrap() -> None:
    """
    Create the async engine and session factory AFTER bootstrap has completed.

    This MUST be called by main.py after bootstrap_schema_recovery() succeeds
    and BEFORE any ORM models or routers are imported.

    Populates the module-level _engine, _AsyncSessionLocal, DATABASE_URL, and
    _bootstrap_complete flag so all downstream code can use them.

    Raises RuntimeError if called more than once (singleton guard).
    """
    global _engine, _AsyncSessionLocal, DATABASE_URL, _bootstrap_complete, _engine_id

    logger.info("[DB_INIT_START] initializing database module")

    async with _get_engine_creation_lock():
        if _engine is not None:
            raise RuntimeError(
                f"[DB_ENGINE_SINGLETON_VIOLATION] initialize_database_after_bootstrap() called "
                f"twice — engine already exists with engine_id={hex(id(_engine))}. "
                "Only one canonical engine is allowed."
            )

        logger.info("[BOOTSTRAP_DB_INIT] building DATABASE_URL")
        DATABASE_URL = _build_database_url()

        logger.info(
            "[BOOTSTRAP_DB_INIT] creating engine database_url=%s",
            DATABASE_URL[:50] + "..." if DATABASE_URL else "NOT_SET",
        )

        _parsed_url = urlparse(DATABASE_URL)
        _drivername = _parsed_url.scheme
        _host = _parsed_url.hostname or "unknown"
        _has_ssl_in_url = "ssl" in _parsed_url.query.lower() or "sslmode" in _parsed_url.query.lower()

        logger.info(
            "[BOOTSTRAP_DB_INIT] drivername=%s host=%s ssl_in_url=%s",
            _drivername,
            _host,
            _has_ssl_in_url,
        )

        _engine = create_async_engine(
            DATABASE_URL,
            echo=False,
            pool_pre_ping=True,
            pool_size=20,
            max_overflow=40,
            pool_timeout=60,
            pool_recycle=1800,
            connect_args={"ssl": "require"},
            execution_options={"compiled_cache": None},
        )

        # Record the canonical engine_id immediately after creation.
        _engine_id = hex(id(_engine))

        logger.info("[DB_INIT_ENGINE_CREATED] async engine created")
        logger.info("[BOOTSTRAP_DB_INIT] engine_created connect_args_ssl=require compiled_cache=disabled pool_size=20 max_overflow=40 pool_timeout=60 pool_recycle=1800")

        # ---------------------------------------------------------------------------
        # [DB_ENGINE_CANONICAL] — log that this is the ONE canonical engine.
        # [DB_ENGINE_ID]        — log hex(id(engine)) so all startup paths can verify
        #                         they share the same engine object.
        # [DB_POOL_CONFIG]      — log pool settings for production observability.
        # ---------------------------------------------------------------------------
        logger.info(
            "[DB_ENGINE_CANONICAL] canonical async engine created in database.py "
            "engine_id=%s pool_size=20 max_overflow=40 pool_timeout=60 "
            "pool_recycle=1800 pool_pre_ping=true",
            _engine_id,
        )
        logger.info(
            "[DB_ENGINE_ID] engine_id=%s source=database.initialize_database_after_bootstrap",
            _engine_id,
        )
        logger.info(
            "[DB_POOL_CONFIG] pool_size=20 max_overflow=40 pool_timeout=60 "
            "pool_recycle=1800 pool_pre_ping=true engine_id=%s",
            _engine_id,
        )

        # ---------------------------------------------------------------------------
        # Pool config assertions — fail startup immediately if the engine was somehow
        # created with wrong settings (e.g. SQLAlchemy defaults: pool_size=5, max_overflow=10).
        # ---------------------------------------------------------------------------
        _init_pool = _engine.pool
        _init_pool_size = _init_pool.size()
        _init_max_overflow = _init_pool._max_overflow
        if _init_pool_size != 20:
            logger.error(
                "[POOL_CONFIG_MISMATCH] pool_size=%d expected=20 engine_id=%s",
                _init_pool_size,
                _engine_id,
            )
            raise RuntimeError(
                f"[POOL_CONFIG_MISMATCH] pool_size={_init_pool_size} expected=20 — "
                "engine was created with wrong pool settings"
            )
        if _init_max_overflow != 40:
            logger.error(
                "[POOL_CONFIG_MISMATCH] max_overflow=%d expected=40 engine_id=%s",
                _init_max_overflow,
                _engine_id,
            )
            raise RuntimeError(
                f"[POOL_CONFIG_MISMATCH] max_overflow={_init_max_overflow} expected=40 — "
                "engine was created with wrong pool settings"
            )
        logger.info(
            "[DB_POOL_CONFIG_VALIDATED] pool_size=%d max_overflow=%d engine_id=%s — "
            "pool configuration matches expected values",
            _init_pool_size,
            _init_max_overflow,
            _engine_id,
        )
        logger.info(
            "[DB_ENGINE_VALIDATION] pool_size=%d max_overflow=%d engine_id=%s — "
            "engine settings confirmed correct",
            _init_pool_size,
            _init_max_overflow,
            _engine_id,
        )

        _AsyncSessionLocal = async_sessionmaker(
            bind=_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        logger.info("[DB_INIT_SESSION_FACTORY_CREATED] session factory created")
        logger.info("[BOOTSTRAP_DB_INIT] session_factory_created")
        logger.info(
            "[DB_SESSION_FACTORY] AsyncSessionLocal created engine_id=%s "
            "source=database.initialize_database_after_bootstrap",
            _engine_id,
        )

        # Verify connection works
        logger.info("[DB_INIT_CONNECTION_VERIFIED] testing database connection")
        async with _engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

        _bootstrap_complete = True
        logger.info("[DB_INIT_COMPLETE] database initialization complete")

        # Update module-level aliases so `from database import engine` and
        # `from database import AsyncSessionLocal` work after bootstrap.
        # Note: code that already did `from database import engine` before bootstrap
        # will still have None — they must re-import or use `import database; database.engine`.
        import sys as _sys
        _mod = _sys.modules[__name__]
        _mod.engine = _engine
        _mod.AsyncSessionLocal = _AsyncSessionLocal

        logger.info(
            "[DB_ENGINE_SINGLETON_VERIFIED] engine_id=%s pool_size=%d max_overflow=%d — "
            "single canonical engine initialized",
            _engine_id,
            _init_pool_size,
            _init_max_overflow,
        )
        logger.info("[BOOTSTRAP_DB_INIT_COMPLETE] database module initialized bootstrap_complete=true")


# ---------------------------------------------------------------------------
# _initialize_engine: canonical alias used by the sync worker bootstrap.
# Identical to initialize_database_after_bootstrap() — both names are valid.
# ---------------------------------------------------------------------------
_initialize_engine = initialize_database_after_bootstrap

# ---------------------------------------------------------------------------
# Backward-compatible module-level aliases.
# These are None until initialize_database_after_bootstrap() is called.
# initialize_database_after_bootstrap() updates these via sys.modules so that
# code doing `from database import engine` AFTER bootstrap gets the real engine.
# ---------------------------------------------------------------------------
engine = None
AsyncSessionLocal = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    if _AsyncSessionLocal is None:
        raise RuntimeError(
            "DATABASE_URL is not configured — bootstrap must run first"
        )

    async with _AsyncSessionLocal() as session:
        yield session


async def refresh_connection_pool() -> None:
    """
    Dispose of the current connection pool and create a new one.
    Use this after schema changes to ensure new connections see updated schema.
    """
    global _engine, _AsyncSessionLocal, engine, AsyncSessionLocal

    if _engine is None:
        return

    logger.info("[Database] disposing_connection_pool")
    await _engine.dispose()
    logger.info("[Database] connection_pool_disposed")

    # Recreate engine and session factory with SSL forced
    _engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=40,
        pool_timeout=60,
        pool_recycle=1800,
        connect_args={"ssl": "require"},
        execution_options={"compiled_cache": None},
    )

    _AsyncSessionLocal = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    # Keep module-level aliases in sync
    engine = _engine
    AsyncSessionLocal = _AsyncSessionLocal

    logger.info("[Database] connection_pool_recreated with connect_args_ssl=require compiled_cache=disabled pool_size=20 max_overflow=40 pool_timeout=60 pool_recycle=1800")
    logger.info(
        "[DB_ENGINE_ID] engine_id=%s source=database.refresh_connection_pool",
        hex(id(_engine)),
    )
    logger.info(
        "[DB_POOL_CONFIG] pool_size=20 max_overflow=40 pool_timeout=60 "
        "pool_recycle=1800 pool_pre_ping=true engine_id=%s source=refresh_connection_pool",
        hex(id(_engine)),
    )


# ---------------------------------------------------------------------------
# Module-level dict populated by inspect_schema_at_startup().
# Keys: table name (str), Values: dict mapping column_name → data_type.
# ---------------------------------------------------------------------------
_schema_column_types: dict[str, dict[str, str]] = {}


def get_schema_column_types() -> dict[str, dict[str, str]]:
    """Return the schema column type map populated at startup."""
    return _schema_column_types


def has_column(table_name: str, column_name: str) -> bool:
    """Check whether a column exists in the cached schema for the given table.

    Returns True if the column was found in the ``public`` schema during
    startup inspection, False otherwise.  Logs a one-time diagnostic line
    for ``packed_qty`` so the result is always visible in the startup logs.
    """
    exists = column_name in _schema_column_types.get(table_name, {})
    if column_name == "packed_qty":
        logger.info(
            "[SCHEMA_HAS_COLUMN] table=%s column=%s exists=%s",
            table_name,
            column_name,
            str(exists).lower(),
        )
    return exists


async def dispose_and_recreate_engine() -> None:
    """Dispose the engine to flush asyncpg prepared-statement cache, then recreate it.

    Call this once at startup (after migrations) so that any stale prepared
    statement metadata from a previous deploy is discarded before the first
    sync operation runs.
    """
    global _engine, _AsyncSessionLocal, engine, AsyncSessionLocal

    if _engine is None:
        return

    logger.info("[DB_STARTUP] disposing_engine_for_fresh_cache")
    await _engine.dispose()

    _engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=40,
        pool_timeout=60,
        pool_recycle=1800,
        connect_args={"ssl": "require"},
        execution_options={"compiled_cache": None},  # Disable statement cache to prevent UUID/VARCHAR type confusion
    )

    _AsyncSessionLocal = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    # Keep module-level aliases in sync
    engine = _engine
    AsyncSessionLocal = _AsyncSessionLocal

    logger.info("[DB_STARTUP] engine_recreated_after_dispose compiled_cache=disabled")
    logger.info(
        "[DB_ENGINE_ID] engine_id=%s source=database.dispose_and_recreate_engine",
        hex(id(_engine)),
    )
    logger.info(
        "[DB_POOL_CONFIG] pool_size=20 max_overflow=40 pool_timeout=60 "
        "pool_recycle=1800 pool_pre_ping=true engine_id=%s source=dispose_and_recreate_engine",
        hex(id(_engine)),
    )


async def confirm_database_identity() -> None:
    """Query and log the database identity at startup.

    Confirms:
    - Database name (current_database())
    - Connected user (current_user)
    - Server host (inet_server_addr())
    - Server port (inet_server_port())

    Logs as [DB_IDENTITY_CONFIRM] for easy grepping.
    """
    if _engine is None:
        logger.warning("[DB_IDENTITY_CONFIRM] engine not available — skipping identity check")
        return

    try:
        async with _engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT
                        current_database() as database,
                        current_user as user,
                        inet_server_addr() as host,
                        inet_server_port() as port
                """)
            )
            row = result.fetchone()

            if row:
                database, user, host, port = row
                logger.info(
                    "[DB_IDENTITY_CONFIRM] database=%s user=%s host=%s port=%s",
                    database,
                    user,
                    host or "localhost",
                    port or "5432",
                )
            else:
                logger.warning("[DB_IDENTITY_CONFIRM] query returned no results")
    except Exception as exc:
        logger.error(
            "[DB_IDENTITY_CONFIRM] failed to query database identity: %s",
            exc,
            exc_info=True,
        )


async def inspect_schema_at_startup() -> None:
    """Query information_schema to log and cache actual PostgreSQL column types.

    Inspects the ``orders`` and ``order_lines`` tables and stores the results
    in the module-level ``_schema_column_types`` dict so that sync code can
    check column existence at runtime without hitting the DB again.

    Logs:
        [SCHEMA_COLUMNS] table=orders columns={...}
        [SCHEMA_COLUMNS] table=order_lines columns={...}
        [SCHEMA_VALIDATION] packed_qty_exists=true|false
    """
    global _schema_column_types

    if _engine is None:
        logger.warning("[SCHEMA_INSPECT] engine not available — skipping schema inspection")
        return

    try:
        async with _engine.connect() as conn:
            for table_name in ("orders", "order_lines"):
                result = await conn.execute(
                    text(
                        """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = :table_name
                        ORDER BY ordinal_position
                        """
                    ),
                    {"table_name": table_name},
                )
                rows = result.fetchall()
                col_map = {row[0]: row[1] for row in rows}
                _schema_column_types[table_name] = col_map
                logger.info(
                    "[SCHEMA_COLUMNS] table=%s columns=%s",
                    table_name,
                    col_map,
                )

        packed_qty_exists = "packed_qty" in _schema_column_types.get("order_lines", {})
        logger.info(
            "[SCHEMA_VALIDATION] packed_qty_exists=%s",
            str(packed_qty_exists).lower(),
        )

        # Log order_lines schema for production visibility
        import json as _json
        logger.info(
            "[ORDER_LINES_SCHEMA] columns=%s",
            _json.dumps(_schema_column_types.get("order_lines", {})),
        )

    except Exception as exc:
        logger.error(
            "[SCHEMA_INSPECT] failed to inspect schema: %s",
            exc,
            exc_info=True,
        )


async def audit_id_column_types() -> None:
    """Log actual PostgreSQL column types for all ID columns at startup.

    Queries information_schema.columns for the tables that participate in
    LeafLink sync and logs each ID column's actual data_type.  This makes
    type mismatches (e.g. VARCHAR vs UUID) immediately visible in startup
    logs so future regressions are caught before they cause runtime errors.

    Key invariants verified:
      - orders.brand_id        → character varying  (NOT uuid)
      - orders.org_id          → character varying  (NOT uuid)
      - order_lines.order_id   → uuid               (IS uuid)
      - sync_health.brand_id   → character varying  (NOT uuid)
      - sync_runs.brand_id     → character varying  (NOT uuid)
      - dead_letter_line_items.brand_id → character varying (NOT uuid)
      - sync_dead_letters.brand_id      → character varying (NOT uuid)

    Logs [DB_ID_TYPE_AUDIT] for each column and [DB_ID_TYPE_MISMATCH] if
    any column does not match the expected type.
    """
    if _engine is None:
        logger.warning("[DB_ID_TYPE_AUDIT] engine not available — skipping audit")
        return

    # Expected types: (table, column) → expected data_type substring
    _expected: list[tuple[str, str, str, str]] = [
        ("orders",               "brand_id",  "character varying", "VARCHAR"),
        ("orders",               "org_id",    "character varying", "VARCHAR"),
        ("order_lines",          "order_id",  "uuid",              "UUID"),
        ("sync_health",          "brand_id",  "character varying", "VARCHAR"),
        ("sync_runs",            "brand_id",  "character varying", "VARCHAR"),
        ("dead_letter_line_items", "brand_id", "character varying", "VARCHAR"),
        ("sync_dead_letters",    "brand_id",  "character varying", "VARCHAR"),
    ]

    try:
        async with _engine.connect() as conn:
            for table_name, col_name, expected_type, label in _expected:
                try:
                    result = await conn.execute(
                        text(
                            """
                            SELECT data_type, udt_name, character_maximum_length
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name   = :table_name
                              AND column_name  = :col_name
                            """
                        ),
                        {"table_name": table_name, "col_name": col_name},
                    )
                    row = result.fetchone()
                    if row is None:
                        logger.warning(
                            "[DB_ID_TYPE_AUDIT] table=%s column=%s status=NOT_FOUND"
                            " — table may not exist yet",
                            table_name, col_name,
                        )
                        continue

                    actual_type, udt_name, max_len = row
                    matches = expected_type in actual_type.lower()
                    logger.info(
                        "[DB_ID_TYPE_AUDIT] table=%s column=%s actual_type=%s"
                        " udt=%s max_len=%s expected=%s match=%s",
                        table_name, col_name, actual_type, udt_name,
                        max_len, label, str(matches).lower(),
                    )
                    if not matches:
                        logger.error(
                            "[DB_ID_TYPE_MISMATCH] table=%s column=%s"
                            " actual=%s expected=%s — SQL casts must match actual type."
                            " VARCHAR columns must NOT use CAST(:x AS UUID).",
                            table_name, col_name, actual_type, label,
                        )
                except Exception as col_exc:
                    logger.warning(
                        "[DB_ID_TYPE_AUDIT] table=%s column=%s audit_error=%s",
                        table_name, col_name, str(col_exc)[:200],
                    )
    except Exception as exc:
        logger.error(
            "[DB_ID_TYPE_AUDIT] failed to run audit: %s",
            exc,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Pool health telemetry — importable background task
# ---------------------------------------------------------------------------

_POOL_SIZE_CONFIGURED = 20
_POOL_MAX_OVERFLOW_CONFIGURED = 40
_POOL_TOTAL_CAPACITY_CONFIGURED = _POOL_SIZE_CONFIGURED + _POOL_MAX_OVERFLOW_CONFIGURED


async def log_pool_health() -> None:
    """
    Background task: log DB connection-pool health metrics every 60 seconds.

    Emits [DB_POOL_HEALTH] with:
      checked_out         — connections currently in use
      overflow            — connections beyond pool_size
      pool_size           — configured pool_size (20)
      utilization_percent — checked_out / total_capacity * 100
      available           — remaining capacity before pool is exhausted

    Handles asyncio.CancelledError gracefully so it can be used as a
    long-running asyncio.Task that is cancelled on shutdown.

    Usage::

        task = asyncio.create_task(log_pool_health())
        # ... later ...
        task.cancel()
        await task
    """
    import asyncio as _asyncio

    while True:
        try:
            await _asyncio.sleep(60)
            if _engine is None:
                continue
            pool = _engine.pool
            checked_out = pool.checkedout()
            overflow = max(0, checked_out - _POOL_SIZE_CONFIGURED)
            utilization_percent = round(
                checked_out / _POOL_TOTAL_CAPACITY_CONFIGURED * 100, 1
            )
            available = max(0, _POOL_TOTAL_CAPACITY_CONFIGURED - checked_out)
            logger.info(
                "[DB_POOL_HEALTH] checked_out=%d overflow=%d pool_size=%d "
                "utilization_percent=%.1f available=%d",
                checked_out,
                overflow,
                _POOL_SIZE_CONFIGURED,
                utilization_percent,
                available,
            )
        except _asyncio.CancelledError:
            logger.info("[DB_POOL_HEALTH] log_pool_health task cancelled, exiting")
            return
        except Exception as exc:
            logger.warning(
                "[DB_POOL_HEALTH] failed to read pool stats: %s", str(exc)[:200]
            )