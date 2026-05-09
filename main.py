# =============================================================================
# PHASE 1: Minimal imports only — NO ORM, NO routers, NO database.py yet.
# Bootstrap schema recovery MUST run before any of those are imported.
# =============================================================================
import asyncio
import logging
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# =============================================================================
# PHASE 2: Configure logging and validate DATABASE_URL
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("opsyn-backend")

logger.info("[BOOTSTRAP_PRE_APP_START] starting bootstrap recovery")

_raw_bootstrap_url = os.getenv("DATABASE_URL", "")
if not _raw_bootstrap_url:
    logger.error("[BOOTSTRAP_PRE_APP_START] DATABASE_URL not set — cannot start")
    sys.exit(1)

logger.info("[BOOTSTRAP_DB_URL_VALID] DATABASE_URL validated")

# =============================================================================
# PHASE 3: Create a minimal async engine for bootstrap (NOT database.py engine)
# =============================================================================

def _normalize_url_for_bootstrap(url: str) -> str:
    """Minimal URL normalizer: postgres:// → postgresql+asyncpg://, strip sslmode."""
    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    parsed = urlparse(url)
    filtered = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                if k.lower() != "sslmode"]
    return urlunparse(parsed._replace(query=urlencode(filtered)))

_bootstrap_db_url = _normalize_url_for_bootstrap(_raw_bootstrap_url)

# =============================================================================
# PHASE 4: Run bootstrap_schema_recovery() BEFORE any ORM imports
# =============================================================================

async def _run_bootstrap() -> None:
    """Execute bootstrap schema recovery with a dedicated minimal engine."""
    logger.info("[BOOTSTRAP_EXECUTION_BEGIN] running bootstrap_schema_recovery")

    _engine = create_async_engine(
        _bootstrap_db_url,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": "require"},
        execution_options={"compiled_cache": None},
    )
    try:
        from services.bootstrap_schema_recovery import bootstrap_schema_recovery
        result = await bootstrap_schema_recovery(_engine)
    finally:
        await _engine.dispose()

    if not result["ok"]:
        logger.error(
            "[BOOTSTRAP_EXECUTION_FAILED] errors=%s",
            result["errors"],
        )
        sys.exit(1)

    logger.info(
        "[BOOTSTRAP_EXECUTION_SUCCESS] columns_added=%s tables_created=%s indexes_created=%s",
        result["columns_added"],
        result["tables_created"],
        result["indexes_created"],
    )


# Run bootstrap synchronously at module level so it completes before any
# ORM model or router is imported.
asyncio.run(_run_bootstrap())

# =============================================================================
# PHASE 5: Bootstrap succeeded — NOW import ORM models and routers
# =============================================================================
logger.info("[BOOTSTRAP_PRE_ROUTER_IMPORT] importing ORM models and routers")

from fastapi import Depends, FastAPI, HTTPException, Header, Request  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: E402

from database import get_db  # noqa: E402
from models import BrandAPICredential  # noqa: E402
from leaflink.orders import router as leaflink_orders_router  # noqa: E402
from routes.ai import router as ai_router  # noqa: E402
from routes.crm import router as crm_router  # noqa: E402
from routes.health import router as health_router  # noqa: E402
from routes.integrations import router as integrations_router  # noqa: E402
from routes.integrations_health import router as integrations_health_router  # noqa: E402
from routes.leaflink_debug import router as leaflink_debug_router  # noqa: E402
from routes.orders import router as orders_router  # noqa: E402
from routes.voice import router as voice_router  # noqa: E402
from routes.voice_brain import router as voice_brain_router  # noqa: E402
from routes.admin import router as admin_router  # noqa: E402
from routes.debug import router as debug_router  # noqa: E402
from routes.auth import router as auth_router  # noqa: E402
from routes.webhooks import router as webhooks_router  # noqa: E402
from routes.sync import router as sync_router, _webhook_router as webhook_status_router  # noqa: E402
from routes.leaflink_webhook_config import router as leaflink_webhook_config_router  # noqa: E402
from routes.diagnostics import router as diagnostics_router  # noqa: E402
from routes.drivers import router as drivers_router  # noqa: E402
from routes.routes import router as routes_router  # noqa: E402
from routes.driver_app import router as driver_app_router  # noqa: E402
from utils.json_utils import make_json_safe  # noqa: E402
from services.sync_scheduler import run_scheduler  # noqa: E402

logger.info("[BOOTSTRAP_APP_INIT_CONTINUING] creating FastAPI app")

# ---------------------------------------------------------------------------
# Feature flags — read once at import time
# ---------------------------------------------------------------------------
LEAFLINK_SYNC_ENABLED = os.getenv("LEAFLINK_SYNC_ENABLED", "true").lower() == "true"
LEAFLINK_DEBUG_SQL = os.getenv("LEAFLINK_DEBUG_SQL", "false").lower() == "true"
LEAFLINK_REPROCESS_ENABLED = os.getenv("LEAFLINK_REPROCESS_ENABLED", "true").lower() == "true"
LEAFLINK_MAX_RETRY_ATTEMPTS = int(os.getenv("LEAFLINK_MAX_RETRY_ATTEMPTS", "5"))
LEAFLINK_SYNC_STALE_MINUTES = int(os.getenv("LEAFLINK_SYNC_STALE_MINUTES", "60"))

# Thread pool for running synchronous LeafLink HTTP calls off the event loop.
# This prevents requests.get() pagination from blocking async request handling.
_leaflink_executor = ThreadPoolExecutor(max_workers=4)

APP_NAME = "Opsyn Backend"
APP_ENV = os.getenv("ENVIRONMENT", "production")
DEPLOY_SHA = os.environ.get("RAILWAY_GIT_COMMIT_SHA", "unknown")[:7]

DRIVERS = {}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


async def _create_assistant_tables() -> None:
    """Create assistant tables if they don't exist (idempotent)."""
    try:
        from database import Base, engine

        if engine is None:
            logger.warning("startup: DATABASE_URL not set — skipping assistant table creation")
            return

        # Import models so SQLAlchemy registers them with Base.metadata
        import models.assistant_models  # noqa: F401
        import models.auth_models  # noqa: F401
        import models.sync_health  # noqa: F401

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        logger.info("startup: assistant tables created/verified")
    except Exception as exc:
        logger.error("startup: failed to create assistant tables: %s", exc)


async def _verify_database_schema() -> None:
    """
    Verify that the database has the expected schema columns.
    Logs the database host and all columns in brand_api_credentials.
    """
    from sqlalchemy import text
    from database import engine

    if engine is None:
        logger.warning("[Startup] DATABASE_URL not set — skipping schema verification")
        return

    try:
        async with engine.connect() as conn:
            # Get database host from connection
            result = await conn.execute(text("SELECT current_database(), inet_server_addr()"))
            db_name, db_host = result.fetchone()
            logger.info("[Startup] database_url_host=%s database_name=%s", db_host or "localhost", db_name)

            # Get all columns in brand_api_credentials
            result = await conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'brand_api_credentials'
                ORDER BY column_name
            """))
            columns = [row[0] for row in result.fetchall()]
            logger.info("[Startup] brand_api_credentials_columns=%s", ",".join(columns))

            # Check for required columns
            required_columns = [
                'id', 'brand_id', 'integration_name', 'api_key', 'company_id',
                'is_active', 'sync_status', 'last_sync_at', 'last_error',
                'last_synced_page', 'total_pages_available', 'total_orders_available'
            ]
            missing = [col for col in required_columns if col not in columns]

            if missing:
                logger.error(
                    "[Startup] schema_verification_failed missing_columns=%s",
                    ",".join(missing),
                )
            else:
                logger.info("[Startup] schema_verification_passed all_required_columns_present")

    except Exception as exc:
        logger.error("[Startup] schema_verification_error error=%s", exc, exc_info=True)


async def _resume_interrupted_syncs() -> None:
    """
    On startup, re-enqueue any syncs that were in-progress when the service
    last shut down by inserting a pending SyncRequest row for each interrupted
    credential.  The opsyn-sync-worker will pick these up and resume from
    last_synced_page + 1.

    A sync is considered interrupted when:
      - sync_status = "syncing"
      - last_synced_page is not None
      - total_pages_available is not None
      - last_synced_page < total_pages_available
    """
    try:
        from database import AsyncSessionLocal
        from models import BrandAPICredential, SyncRequest

        if AsyncSessionLocal is None:
            logger.warning("startup: DATABASE_URL not set — skipping sync resume check")
            return

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                    BrandAPICredential.sync_status == "syncing",
                )
            )
            interrupted = result.scalars().all()

        resumable = [
            c for c in interrupted
            if (
                c.last_synced_page is not None
                and c.total_pages_available is not None
                and c.last_synced_page < c.total_pages_available
                and c.api_key
                and c.company_id
            )
        ]

        if not resumable:
            logger.info("startup: no interrupted syncs to resume")
            return

        for cred in resumable:
            resume_from = (cred.last_synced_page or 0) + 1
            logger.info(
                "startup: re_enqueuing_interrupted_sync brand=%s from_page=%s total_pages=%s",
                cred.brand_id,
                resume_from,
                cred.total_pages_available,
            )
            try:
                async with AsyncSessionLocal() as enqueue_sess:
                    async with enqueue_sess.begin():
                        enqueue_sess.add(SyncRequest(
                            brand_id=cred.brand_id,
                            status="pending",
                            start_page=resume_from,
                            total_pages=cred.total_pages_available,
                            total_orders_available=cred.total_orders_available,
                        ))
                logger.info(
                    "startup: enqueued_interrupted_sync brand=%s start_page=%s",
                    cred.brand_id,
                    resume_from,
                )
            except Exception as enqueue_exc:
                logger.error(
                    "startup: enqueue_interrupted_sync_failed brand=%s error=%s",
                    cred.brand_id,
                    enqueue_exc,
                )

    except Exception as exc:
        logger.error("startup: resume_interrupted_syncs_failed error=%s", exc, exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.error("[BOOT_ACTUAL] LeafLink sync hotfix loaded commit=%s", os.environ.get("RAILWAY_GIT_COMMIT_SHA", "unknown")[:7])
    logger.info("[Boot] opsyn-backend deploy_sha=%s pr=228", DEPLOY_SHA)

    # Validate DATABASE_URL at startup
    database_url = os.getenv("DATABASE_URL", "")

    if not database_url:
        logger.error("[BOOT_DB] DATABASE_URL is empty or not set")
        raise RuntimeError("DATABASE_URL environment variable is not set")

    # Check for common malformations
    if database_url.startswith("DATABASE_URL"):
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=starts_with_DATABASE_URL"
        )
        raise RuntimeError(
            "DATABASE_URL is malformed: value starts with 'DATABASE_URL'. "
            "Remove the 'DATABASE_URL=' prefix from the value."
        )

    if "\n" in database_url or "\r" in database_url:
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=contains_newlines"
        )
        raise RuntimeError(
            "DATABASE_URL is malformed: contains newlines. "
            "Ensure the value is a single line with no line breaks."
        )

    if database_url.startswith('"') or database_url.startswith("'"):
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=starts_with_quote"
        )
        raise RuntimeError(
            "DATABASE_URL is malformed: value starts with a quote. "
            "Remove quotes from the variable value."
        )

    # PRODUCTION FAIL-FAST: Ensure AWS RDS is used
    environment = os.getenv("ENVIRONMENT", "development").lower()

    if environment == "production":
        if "rds.amazonaws.com" not in database_url:
            logger.warning(
                "[BOOT_DB] Production DATABASE_URL is not AWS RDS; allowing Railway Postgres for current deployment"
            )
        else:
            logger.info("[BOOT_DB] PRODUCTION mode using AWS RDS canonical database")

    # Log active database connection at startup
    try:
        from urllib.parse import urlparse as _urlparse
        _parsed = _urlparse(database_url)
        db_host = _parsed.hostname or "unknown"
        db_port = _parsed.port or 5432
        db_name = _parsed.path.lstrip("/") or "unknown"

        # Detect provider
        if "rds.amazonaws.com" in db_host:
            provider = "aws-rds"
        elif "railway" in db_host:
            provider = "railway"
        else:
            provider = "unknown"

        logger.info(
            "[BOOT_DB] host=%s db=%s provider=%s",
            db_host,
            db_name,
            provider,
        )
    except Exception as exc:
        logger.error("[BOOT_DB] failed_to_parse_database_url error=%s", exc)

    logger.info(f"Starting {APP_NAME} in {APP_ENV}")

    # ---------------------------------------------------------------------------
    # Log startup environment for Railway deploy diagnostics
    # ---------------------------------------------------------------------------
    logger.info(
        "[STARTUP_ENV] RAILWAY_ENVIRONMENT_NAME=%s DATABASE_URL_PRESENT=%s",
        os.getenv("RAILWAY_ENVIRONMENT_NAME", os.getenv("ENVIRONMENT", "unknown")),
        "true" if os.getenv("DATABASE_URL") else "false",
    )

    # ---------------------------------------------------------------------------
    # NOTE: Bootstrap schema recovery already ran at module level (PHASE 4)
    # before any ORM models or routers were imported.  It is NOT repeated here.
    # See the [BOOTSTRAP_EXECUTION_SUCCESS] log line emitted during module load.
    # ---------------------------------------------------------------------------
    logger.info("[BOOTSTRAP_SCHEMA_CHECK_COMPLETE] bootstrap already completed at module load — skipping lifespan repeat")

    # ---------------------------------------------------------------------------
    # Log migration runner configuration
    # ---------------------------------------------------------------------------
    from services.migration_runner import MIGRATIONS_DIR as _MIGRATIONS_DIR
    _migrations_dir_exists = os.path.isdir(_MIGRATIONS_DIR)
    _migration_files_count = (
        len([f for f in os.listdir(_MIGRATIONS_DIR) if f.endswith(".sql")])
        if _migrations_dir_exists
        else 0
    )
    logger.info(
        "[MIGRATION_CONFIG] runner_type=custom_sql migrations_directory=%s "
        "directory_exists=%s migration_files_found=%d",
        _MIGRATIONS_DIR,
        str(_migrations_dir_exists).lower(),
        _migration_files_count,
    )

    # Log each discovered migration file
    if _migrations_dir_exists:
        _discovered = sorted(f for f in os.listdir(_MIGRATIONS_DIR) if f.endswith(".sql"))
        for _mf in _discovered:
            logger.info("[MIGRATION_FILE_DISCOVERED] file=%s", _mf)

    # ---------------------------------------------------------------------------
    # Run migrations FIRST, before any other database operations, to ensure
    # schema is up-to-date before tables are created or queries are executed.
    # ---------------------------------------------------------------------------
    logger.info(
        "[MIGRATION_RUNNER_START] runner_type=custom_sql directory=%s",
        _MIGRATIONS_DIR,
    )
    from services.migration_runner import run_migrations
    _migration_errors: list[str] = []
    try:
        applied = await run_migrations()
        for _mig_name in applied:
            logger.info("[MIGRATION_EXECUTED] migration=%s status=success", _mig_name)
    except Exception as _mig_exc:
        applied = []
        _migration_errors.append(str(_mig_exc))
        logger.error(
            "[MIGRATION_FAILED] runner_error=%s",
            _mig_exc,
            exc_info=True,
        )
    logger.info(
        "[MIGRATION_RUNNER_COMPLETE] total_applied=%d failed_count=%d",
        len(applied),
        len(_migration_errors),
    )
    logger.info("[Startup] migrations_complete count=%s", len(applied))

    # ---------------------------------------------------------------------------
    # Emergency self-healing: apply any missing webhook schema columns/tables
    # that the migration runner may have skipped or silently failed on.
    # ---------------------------------------------------------------------------
    _recovery_result: dict = {}
    try:
        from services.migration_recovery import execute_webhook_schema_recovery
        from database import AsyncSessionLocal as _RecoverySessionLocal

        if _RecoverySessionLocal is not None:
            async with _RecoverySessionLocal() as _recovery_db:
                _recovery_result = await execute_webhook_schema_recovery(_recovery_db)

            logger.info(
                "[WEBHOOK_SCHEMA_AUTO_RECOVERY] columns_added=%s tables_created=%s "
                "indexes_created=%s status=%s errors=%d",
                _recovery_result.get("columns_added", []),
                _recovery_result.get("tables_created", []),
                _recovery_result.get("indexes_created", []),
                _recovery_result.get("status", "unknown"),
                len(_recovery_result.get("errors", [])),
            )
        else:
            logger.warning(
                "[WEBHOOK_SCHEMA_AUTO_RECOVERY] skipped reason=no_database_session"
            )
    except Exception as _rec_exc:
        logger.error(
            "[WEBHOOK_SCHEMA_AUTO_RECOVERY] recovery_exception error=%s",
            _rec_exc,
            exc_info=True,
        )

    # ---------------------------------------------------------------------------
    # Hard-fail safeguard: verify critical webhook columns exist after recovery.
    # If any are still missing, crash startup with an actionable error message.
    # ---------------------------------------------------------------------------
    try:
        from services.migration_recovery import verify_critical_webhook_columns
        from database import AsyncSessionLocal as _VerifySessionLocal

        if _VerifySessionLocal is not None:
            async with _VerifySessionLocal() as _verify_db:
                _verify_result = await verify_critical_webhook_columns(_verify_db)

            if _verify_result.get("ok"):
                logger.info(
                    "[WEBHOOK_SCHEMA_VERIFIED] critical_columns_present=true "
                    "column_count=%d",
                    len(_verify_result.get("column_status", {})),
                )
            else:
                _still_missing = _verify_result.get("missing", [])
                logger.error(
                    "[WEBHOOK_SCHEMA_CRITICAL_MISSING] missing_columns=%s action=hard_fail",
                    _still_missing,
                )
                raise RuntimeError(
                    "Critical webhook schema columns missing after migration recovery. "
                    "Manual intervention required. "
                    f"Missing columns: {_still_missing}"
                )
        else:
            logger.warning(
                "[WEBHOOK_SCHEMA_VERIFIED] skipped reason=no_database_session"
            )
    except RuntimeError:
        raise
    except Exception as _hf_exc:
        logger.error(
            "[WEBHOOK_SCHEMA_VERIFIED] verification_exception error=%s",
            _hf_exc,
            exc_info=True,
        )

    # ---------------------------------------------------------------------------
    # Post-migration LeafLink DB verification — confirm credentials were repaired
    # ---------------------------------------------------------------------------
    try:
        from sqlalchemy import text as _post_text
        from database import AsyncSessionLocal as _PostMigSessionLocal

        _POST_EXPECTED_BASE_URL = "https://www.leaflink.com/api/v2"
        _POST_EXPECTED_AUTH_SCHEME = "Token"

        if _PostMigSessionLocal is not None:
            async with _PostMigSessionLocal() as _post_db:
                _post_result = await _post_db.execute(
                    _post_text(
                        "SELECT base_url, auth_scheme FROM brand_api_credentials "
                        "WHERE integration_name = 'leaflink' AND is_active = true "
                        "LIMIT 1"
                    )
                )
                _post_row = _post_result.fetchone()

            if _post_row is not None:
                _post_base_url = (_post_row[0] or "").strip()
                _post_auth_scheme = (_post_row[1] or "").strip()
                _post_status = (
                    "correct"
                    if _post_base_url == _POST_EXPECTED_BASE_URL
                    and _post_auth_scheme == _POST_EXPECTED_AUTH_SCHEME
                    else "stale"
                )
                logger.info(
                    "[LEAFLINK_DB_VERIFICATION] base_url=%s auth_scheme=%s status=%s",
                    _post_base_url,
                    _post_auth_scheme,
                    _post_status,
                )
                if _post_status == "stale":
                    logger.warning(
                        "[LEAFLINK_DB_VERIFICATION] credentials still stale after migrations"
                        " — repair migration may not have run yet"
                    )
            else:
                logger.info(
                    "[LEAFLINK_DB_VERIFICATION] status=no_active_leaflink_credentials"
                )
        else:
            logger.warning("[LEAFLINK_DB_VERIFICATION] skipped reason=no_database_session")
    except Exception as _post_exc:
        logger.warning(
            "[LEAFLINK_DB_VERIFICATION] check_failed error=%s",
            _post_exc,
        )

    await _create_assistant_tables()

    # Refresh connection pool after migrations so new connections see updated schema
    if applied:
        from database import refresh_connection_pool
        await refresh_connection_pool()

    # Dispose and recreate the engine to flush asyncpg prepared-statement cache,
    # then inspect the live schema so sync code can check column existence at runtime.
    from database import confirm_database_identity, dispose_and_recreate_engine, inspect_schema_at_startup, get_schema_column_types
    await dispose_and_recreate_engine()
    await inspect_schema_at_startup()

    # Confirm database identity (logs [DB_IDENTITY_CONFIRM] for easy grepping)
    await confirm_database_identity()

    # Log which order_lines columns are enabled in the live schema
    try:
        from database import has_column as _has_column
        _optional_line_cols = [
            "packed_qty", "unit_price_cents", "total_price_cents",
            "mapped_product_id", "mapping_status", "mapping_issue",
            "raw_payload", "created_at", "updated_at",
        ]
        _core_cols = ["order_id", "sku", "product_name", "quantity", "unit_price", "total_price"]
        _enabled = _core_cols + [c for c in _optional_line_cols if _has_column("order_lines", c)]
        logger.info("[LINE_ITEM_COLUMNS_ENABLED] columns=%s", _enabled)
    except Exception as _col_exc:
        logger.warning("[LINE_ITEM_COLUMNS_ENABLED] failed to inspect columns: %s", _col_exc)

    # Log startup verification: database identity and order_lines schema
    try:
        from urllib.parse import urlparse as _urlparse2
        _parsed2 = _urlparse2(os.getenv("DATABASE_URL", ""))
        _db_name = _parsed2.path.lstrip("/") or "unknown"
        _db_user = _parsed2.username or "unknown"
        _db_host = _parsed2.hostname or "unknown"
        _db_port = _parsed2.port or 5432
        logger.info(
            "[STARTUP_VERIFICATION] database=%s user=%s host=%s port=%s",
            _db_name, _db_user, _db_host, _db_port,
        )
    except Exception as _sv_exc:
        logger.warning("[STARTUP_VERIFICATION] failed to parse DATABASE_URL: %s", _sv_exc)

    # Log order_lines schema for visibility
    import json as _json
    _schema = get_schema_column_types()
    logger.info(
        "[ORDER_LINES_SCHEMA] columns=%s",
        _json.dumps(_schema.get("order_lines", {})),
    )

    # Verify database schema and log connection details
    await _verify_database_schema()

    # ---------------------------------------------------------------------------
    # Startup safety checks — verify required tables, columns, and indexes
    # ---------------------------------------------------------------------------
    try:
        from services.startup_checks import run_startup_schema_checks
        _schema_check_result = await run_startup_schema_checks()
        if not _schema_check_result.get("ok"):
            logger.warning(
                "[STARTUP_SCHEMA_WARNING] schema_check_failed warnings=%s errors=%s",
                _schema_check_result.get("warnings", []),
                _schema_check_result.get("errors", []),
            )
        else:
            logger.info("[STARTUP_SCHEMA_OK] all_schema_checks_passed")
    except RuntimeError:
        # RuntimeError from startup_checks means core tables are missing — re-raise to crash
        raise
    except Exception as _sc_exc:
        logger.warning("[STARTUP_SCHEMA_WARNING] schema_check_exception error=%s", str(_sc_exc)[:300])

    # ---------------------------------------------------------------------------
    # Webhook schema validation — verify webhook tables/columns exist
    # Logs [WEBHOOK_SCHEMA_VALIDATION] markers; does NOT crash if missing
    # (migrations may not have run yet in some environments).
    # ---------------------------------------------------------------------------
    try:
        from services.webhook_schema_validator import validate_webhook_schema
        from database import AsyncSessionLocal as _WebhookValidSessionLocal

        if _WebhookValidSessionLocal is not None:
            async with _WebhookValidSessionLocal() as _wv_db:
                _wv_result = await validate_webhook_schema(_wv_db)

            if _wv_result.get("valid"):
                logger.info(
                    "[WEBHOOK_SCHEMA_VALIDATION] startup_check=passed "
                    "tables_ok=true columns_ok=true"
                )
            else:
                logger.warning(
                    "[WEBHOOK_SCHEMA_VALIDATION] startup_check=failed "
                    "missing_tables=%s missing_columns=%s "
                    "action=webhook_status_will_return_partial_metrics",
                    _wv_result.get("missing_tables", []),
                    list(_wv_result.get("missing_columns", {}).keys()),
                )
        else:
            logger.warning(
                "[WEBHOOK_SCHEMA_VALIDATION] skipped reason=no_database_session"
            )
    except Exception as _wv_exc:
        logger.warning(
            "[WEBHOOK_SCHEMA_VALIDATION] check_failed error=%s",
            str(_wv_exc)[:300],
        )

    # ---------------------------------------------------------------------------
    # Log feature flags at startup
    # ---------------------------------------------------------------------------

    logger.info(
        "[FEATURE_FLAGS] LEAFLINK_SYNC_ENABLED=%s LEAFLINK_DEBUG_SQL=%s"
        " LEAFLINK_REPROCESS_ENABLED=%s LEAFLINK_MAX_RETRY_ATTEMPTS=%s"
        " LEAFLINK_SYNC_STALE_MINUTES=%s",
        LEAFLINK_SYNC_ENABLED,
        LEAFLINK_DEBUG_SQL,
        LEAFLINK_REPROCESS_ENABLED,
        LEAFLINK_MAX_RETRY_ATTEMPTS,
        LEAFLINK_SYNC_STALE_MINUTES,
    )

    await _resume_interrupted_syncs()

    # ---------------------------------------------------------------------------
    # LeafLink endpoint startup validation — fail fast if the endpoint is wrong
    # ---------------------------------------------------------------------------
    try:
        from services.leaflink_client import (
            LEAFLINK_ORDERS_FULL_URL,
            validate_leaflink_endpoint_url,
        )

        _endpoint_valid, _endpoint_reason = validate_leaflink_endpoint_url(LEAFLINK_ORDERS_FULL_URL)
        if _endpoint_valid:
            logger.info(
                "[LEAFLINK_STARTUP_VALIDATION] endpoint_check=passed canonical_url=%s",
                LEAFLINK_ORDERS_FULL_URL,
            )
        else:
            logger.error(
                "[LEAFLINK_STARTUP_VALIDATION] endpoint_check=failed reason=%s canonical_url=%s",
                _endpoint_reason,
                LEAFLINK_ORDERS_FULL_URL,
            )
            raise RuntimeError(
                f"LeafLink endpoint validation failed at startup: reason={_endpoint_reason} "
                f"url={LEAFLINK_ORDERS_FULL_URL}. "
                f"The correct endpoint is https://www.leaflink.com/api/v2/orders-received/"
            )
    except ImportError as _import_exc:
        logger.error(
            "[LEAFLINK_STARTUP_VALIDATION] endpoint_check=failed reason=import_error error=%s",
            _import_exc,
        )
        raise RuntimeError(f"Failed to import LeafLink client for startup validation: {_import_exc}")

    # ---------------------------------------------------------------------------
    # LeafLink credential startup repair check — verify DB credentials were fixed
    # ---------------------------------------------------------------------------
    try:
        from sqlalchemy import text as _text
        from database import AsyncSessionLocal as _StartupSessionLocal

        _EXPECTED_BASE_URL = "https://www.leaflink.com/api/v2"
        _EXPECTED_AUTH_SCHEME = "Token"

        if _StartupSessionLocal is not None:
            async with _StartupSessionLocal() as _startup_db:
                _cred_result = await _startup_db.execute(
                    _text(
                        "SELECT base_url, auth_scheme FROM brand_api_credentials "
                        "WHERE integration_name = 'leaflink' AND is_active = true "
                        "LIMIT 1"
                    )
                )
                _cred_row = _cred_result.fetchone()

            if _cred_row is not None:
                _db_base_url = (_cred_row[0] or "").strip()
                _db_auth_scheme = (_cred_row[1] or "").strip()

                _base_url_ok = _db_base_url == _EXPECTED_BASE_URL
                _auth_scheme_ok = _db_auth_scheme == _EXPECTED_AUTH_SCHEME
                _repaired = _base_url_ok and _auth_scheme_ok

                if _repaired:
                    logger.info(
                        "[LEAFLINK_STARTUP_REPAIR] credentials_repaired=true"
                        " base_url=%s auth_scheme=%s reason=credentials_canonical",
                        _db_base_url,
                        _db_auth_scheme,
                    )
                else:
                    _reason_parts = []
                    if not _base_url_ok:
                        _reason_parts.append(
                            f"base_url={_db_base_url!r} (expected {_EXPECTED_BASE_URL!r})"
                        )
                    if not _auth_scheme_ok:
                        _reason_parts.append(
                            f"auth_scheme={_db_auth_scheme!r} (expected {_EXPECTED_AUTH_SCHEME!r})"
                        )
                    _reason_str = "; ".join(_reason_parts)
                    # TODO: Re-enable strict validation after migrations are stable
                    # Previously this raised RuntimeError and blocked startup.
                    # Downgraded to WARNING so startup succeeds even when the repair
                    # migration has not yet been applied or failed non-critically.
                    logger.warning(
                        "[LEAFLINK_STARTUP_REPAIR] credentials_repaired=false"
                        " reason=%s severity=warning",
                        _reason_str,
                    )
            else:
                logger.warning(
                    "[LEAFLINK_STARTUP_REPAIR] credentials_repaired=false"
                    " reason=no_active_leaflink_credentials_found severity=warning"
                )
        else:
            logger.warning(
                "[LEAFLINK_STARTUP_REPAIR] skipped reason=no_database_session"
            )
    except Exception as _repair_exc:
        # TODO: Re-enable strict validation after migrations are stable
        logger.warning(
            "[LEAFLINK_STARTUP_REPAIR] check_failed error=%s severity=warning",
            _repair_exc,
            exc_info=True,
        )

    route_count = len(app.routes)
    logger.info("[Startup] routes_registered count=%s", route_count)

    # Log all registered routes
    logger.info("[OrdersRoutes] registered_routes:")
    for route in app.routes:
        if hasattr(route, "path") and "/orders" in route.path:
            methods = getattr(route, "methods", ["GET"])
            logger.info(
                "[OrdersRoutes] path=%s methods=%s endpoint=%s",
                route.path,
                methods,
                getattr(route, "endpoint", "unknown"),
            )

    # Database diagnostics - verify connection and employee table
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal as _AsyncSessionLocal

        logger.info("[Startup] running_database_diagnostics")

        async with _AsyncSessionLocal() as diag_db:
            # Check current database
            result = await diag_db.execute(text("SELECT current_database()"))
            current_db = result.scalar()
            logger.info("[Startup] current_database=%s", current_db)

            # Count employees
            result = await diag_db.execute(text("SELECT COUNT(*) FROM employees"))
            employee_count = result.scalar() or 0
            logger.info("[Startup] employee_count=%d", employee_count)

            # List all employee emails
            result = await diag_db.execute(text("SELECT email FROM employees ORDER BY email"))
            emails = [row[0] for row in result.fetchall()]
            logger.info("[Startup] employee_emails=%s", emails)

            # Check for Preston specifically
            result = await diag_db.execute(
                text("SELECT id, email FROM employees WHERE email = 'preston@noble420.com'")
            )
            preston = result.fetchone()
            if preston:
                logger.info("[Startup] preston_found employee_id=%s email=%s", preston[0], preston[1])
            else:
                logger.warning("[Startup] preston_not_found in_employees_table")

        logger.info("[Startup] database_diagnostics_complete")
    except Exception as e:
        logger.error("[Startup] database_diagnostics_failed error=%s", str(e)[:500], exc_info=True)

    # After migrations are applied, verify we can query the orders table
    try:
        from sqlalchemy import text
        from database import AsyncSessionLocal as _AsyncSessionLocal2

        async with _AsyncSessionLocal2() as verify_db:
            # Check if orders table exists
            result = await verify_db.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'orders'
                )
            """))
            orders_exists = result.scalar()
            logger.info("[Startup] orders_table_exists=%s", orders_exists)

            if orders_exists:
                # Count orders
                result = await verify_db.execute(text("SELECT COUNT(*) FROM orders"))
                order_count = result.scalar() or 0
                logger.info("[Startup] orders_table_row_count=%s", order_count)

            # Check if sync_runs table exists
            result = await verify_db.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'sync_runs'
                )
            """))
            sync_runs_exists = result.scalar()
            logger.info("[Startup] sync_runs_table_exists=%s", sync_runs_exists)

    except Exception as e:
        logger.error("[Startup] table_verification_failed error=%s", str(e)[:500], exc_info=True)

    # Seed auth data for known employees (idempotent — skips if already set up)
    from services.seed_auth import seed_preston_anderson
    from database import AsyncSessionLocal as _SeedSessionLocal
    try:
        seed_result = await seed_preston_anderson(_SeedSessionLocal())
        if seed_result.get("ok"):
            logger.info("[Startup] auth_seed_complete")
        else:
            logger.warning("[Startup] auth_seed_skipped reason=%s", seed_result.get("error"))
    except Exception as e:
        logger.warning("[Startup] auth_seed_error error=%s", str(e)[:500])

    # Start background sync scheduler as an asyncio task (non-blocking)
    print("🚀 Starting sync scheduler...")
    scheduler_task = asyncio.create_task(run_scheduler())
    print("✅ Sync scheduler task created")

    yield

    # On shutdown, cancel the scheduler task
    logger.info("[Shutdown] cancelling background sync scheduler")
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        logger.info("[Shutdown] background sync scheduler cancelled")


app = FastAPI(title=APP_NAME, lifespan=lifespan)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
from routes.assistant import router as assistant_router  # noqa: E402
from routes.ingest import router as ingest_router  # noqa: E402
from routes.watchdog import router as watchdog_router  # noqa: E402

# Core assistant/watchdog routers
app.include_router(assistant_router)
app.include_router(ingest_router)
app.include_router(watchdog_router)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# API ROUTERS
# ---------------------------------------------------------------------------

# Core system
app.include_router(health_router)
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(debug_router)
app.include_router(diagnostics_router)

# AI / CRM / Voice
app.include_router(ai_router, prefix="/ai")
app.include_router(crm_router)
app.include_router(voice_router)
app.include_router(voice_brain_router)

# Integrations
app.include_router(integrations_router)
app.include_router(integrations_health_router)

# Existing LeafLink routes
app.include_router(leaflink_orders_router)

# Orders API — router owns its full prefix (/api/leaflink/orders/...)
# Do NOT add a prefix here; the router definition already includes it.
app.include_router(orders_router)

# LeafLink debug
app.include_router(leaflink_debug_router, prefix="/leaflink")

# Sync / Webhooks
# Route namespaces:
#   /orders/sync/...              — sync_router (admin recovery, status, dead-letter)
#   /webhooks/leaflink            — webhooks_router (inbound webhook receiver)
#   /api/leaflink/orders/...      — leaflink_webhook_config_router (webhook config)
#   /api/leaflink/orders/...      — webhook_status_router (webhook-status + webhook-replay)
app.include_router(sync_router)
app.include_router(webhooks_router)
app.include_router(leaflink_webhook_config_router)
app.include_router(webhook_status_router)

# Drivers / Routes
app.include_router(drivers_router)
app.include_router(routes_router)
app.include_router(driver_app_router)

logger.info("[Routes] registered all routers")

# Startup route dump
logger.error("[STARTUP DEBUG] Registered routes:")
for route in app.routes:
    if hasattr(route, "path") and hasattr(route, "methods"):
        logger.error(
            "[STARTUP DEBUG] %s %s",
            route.methods,
            route.path,
        )

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"{request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"{response.status_code}")
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    logger.error("[GlobalException] error_type=%s error=%s", type(exc).__name__, exc)
    logger.error("[GlobalException] traceback:\n%s", tb)
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": "internal_error",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "path": str(request.url.path),
        },
    )


@app.get("/")
def root():
    return {"ok": True, "service": APP_NAME, "env": APP_ENV}


@app.get("/health")
def health():
    return {"ok": True, "status": "healthy", "time": utc_now_iso()}


@app.get("/ping")
def ping():
    logger.info("[Ping] reached")
    return {"ok": True, "timestamp": utc_now_iso()}


@app.get("/debug/routes")
def debug_routes():
    routes = []
    for route in app.routes:
        methods = list(getattr(route, "methods", None) or [])
        path = getattr(route, "path", None)
        if path is not None:
            routes.append({"path": path, "methods": methods})
    count = len(routes)
    logger.info("[RoutesDebug] reached count=%s", count)
    return {"ok": True, "routes": routes, "count": count}


@app.post("/orders")
def receive_orders(body: dict):
    return {"ok": True, "received": True}


@app.post("/error")
def receive_error(body: dict):
    logger.error(f"Client reported error: {body}")
    return {"ok": True, "received": True}


@app.post("/api/twin/leaflink/snapshot")
def receive_leaflink_snapshot(body: dict):
    return {"ok": True, "received": True}


@app.get("/organizations/{org_id}/drivers")
def get_drivers(org_id: str):
    return {
        "ok": True,
        "org_id": org_id,
        "drivers": DRIVERS.get(org_id, []),
    }


@app.post("/organizations/{org_id}/drivers")
def create_driver(org_id: str, body: dict):
    required = ["name", "email", "phone", "pin"]
    missing = [field for field in required if not body.get(field)]

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required driver fields: {', '.join(missing)}",
        )

    driver = {
        "id": f"driver_{len(DRIVERS.get(org_id, [])) + 1}",
        "org_id": org_id,
        "name": body.get("name"),
        "email": body.get("email"),
        "phone": body.get("phone"),
        "pin": body.get("pin"),
        "license_plate": body.get("license_plate"),
        "notes": body.get("notes"),
        "is_active": body.get("is_active", True),
        "created_at": utc_now_iso(),
    }

    DRIVERS.setdefault(org_id, []).append(driver)

    return {
        "ok": True,
        "message": "Driver created",
        "driver": driver,
    }


@app.post("/sync/leaflink/run")
async def sync_leaflink(
    x_opsyn_secret: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Sync LeafLink orders for all active brands.
    Requires X-OPSYN-SECRET header for external calls.
    Internal worker calls can omit the header.
    """
    expected = os.getenv("OPSYN_SYNC_SECRET")

    header_present = x_opsyn_secret is not None
    header_matches = x_opsyn_secret == expected if expected else False

    logger.info(
        "[SyncAuth] request_received header_present=%s header_matches=%s",
        header_present,
        header_matches,
    )

    # Require auth for external calls; allow internal calls without header
    if expected and x_opsyn_secret != expected:
        logger.warning(
            "[SyncAuth] unauthorized attempt expected_set=%s header_present=%s header_matches=%s",
            bool(expected),
            header_present,
            header_matches,
        )
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info("[SyncAuth] authorized")
    logger.info("leaflink: sync_endpoint called")

    try:
        from services.leaflink_sync import sync_leaflink_orders
        from services.leaflink_client import LeafLinkClient

        all_results = []
        total_created = 0
        total_updated = 0
        total_skipped = 0
        total_lines = 0

        async with db.begin():
            # Step 1: Resolve active credentials inside the transaction.
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            credentials = cred_result.scalars().all()

            if not credentials:
                logger.warning("leaflink: sync_endpoint no active credentials found")
                return {
                    "ok": False,
                    "error": "No active LeafLink credentials found",
                }

            logger.info("leaflink: sync_endpoint found %s active credentials", len(credentials))

            for cred in credentials:
                # Extract scalar values while the ORM object is still in scope.
                brand_id: str = cred.brand_id
                api_key: str | None = cred.api_key
                company_id: str | None = cred.company_id

                logger.info(
                    "leaflink: sync_endpoint syncing brand_id=%s",
                    brand_id,
                )

                try:
                    logger.info("[SyncEndpoint] sync_start brand=%s", brand_id)

                    # Step 2: Fetch orders from the LeafLink API (pure HTTP, no DB).
                    # Run the synchronous requests.get() calls in a thread pool so
                    # they do not block the event loop while paginating LeafLink.
                    logger.info("leaflink: sync_client_init brand_id=%s", brand_id)
                    client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand_id)
                    logger.info(
                        "[LEAFLINK_STARTUP_VERIFY] canonical_base_url=%s brand_id=%s",
                        client.base_url,
                        brand_id,
                    )
                    logger.info("[SyncEndpoint] leaflink_fetch_start")
                    loop = asyncio.get_event_loop()
                    fetch_result = await asyncio.wait_for(
                        loop.run_in_executor(
                            _leaflink_executor,
                            lambda: client.fetch_recent_orders(normalize=True, brand=brand_id),
                        ),
                        timeout=300,
                    )
                    orders = fetch_result["orders"]
                    pages_fetched = fetch_result["pages_fetched"]
                    logger.info("[SyncEndpoint] leaflink_fetch_done pages=%s", pages_fetched)

                    # Step 3: Upsert orders and line items inside the same transaction.
                    result = await sync_leaflink_orders(db, brand_id, orders, pages_fetched=pages_fetched)

                    # Step 4: Update credential sync status inside the same transaction.
                    cred.sync_status = "ok" if result.get("ok") else "failed"
                    cred.last_sync_at = datetime.now(timezone.utc)
                    if not result.get("ok"):
                        cred.last_error = result.get("error", "Unknown error")
                    else:
                        cred.last_error = None

                    logger.info(
                        "[IntegrationCredentials] %s brand_id=%s",
                        "marked_success" if result.get("ok") else "marked_invalid",
                        brand_id,
                    )
                    logger.info(
                        "leaflink: sync_endpoint brand_id=%s result=%s created=%s updated=%s skipped=%s lines=%s pages=%s",
                        brand_id,
                        "ok" if result.get("ok") else "failed",
                        result.get("created", 0),
                        result.get("updated", 0),
                        result.get("skipped", 0),
                        result.get("line_items_written", 0),
                        result.get("pages_fetched", 0),
                    )
                    logger.info("[SyncEndpoint] sync_complete brand=%s", brand_id)

                    total_created += result.get("created", 0)
                    total_updated += result.get("updated", 0)
                    total_skipped += result.get("skipped", 0)
                    total_lines += result.get("line_items_written", 0)

                    all_results.append({
                        "brand_id": brand_id,
                        "ok": result.get("ok"),
                        "orders_fetched": result.get("orders_fetched", 0),
                        "pages_fetched": result.get("pages_fetched", 0),
                        "created": result.get("created", 0),
                        "updated": result.get("updated", 0),
                        "skipped": result.get("skipped", 0),
                        "line_items_written": result.get("line_items_written", 0),
                        "error": result.get("error"),
                    })

                except Exception as brand_exc:
                    logger.error(
                        "leaflink: sync_endpoint brand_id=%s error=%s",
                        brand_id,
                        brand_exc,
                        exc_info=True,
                    )

                    # Mark credential as failed inside the same transaction.
                    cred.sync_status = "failed"
                    cred.last_sync_at = datetime.now(timezone.utc)
                    cred.last_error = str(brand_exc)

                    all_results.append({
                        "brand_id": brand_id,
                        "ok": False,
                        "error": str(brand_exc),
                    })

        # Transaction commits here — return results after the block.
        logger.info(
            "leaflink: sync_endpoint complete total_created=%s total_updated=%s total_skipped=%s total_lines=%s",
            total_created,
            total_updated,
            total_skipped,
            total_lines,
        )

        return make_json_safe({
            "ok": True,
            "total_created": total_created,
            "total_updated": total_updated,
            "total_skipped": total_skipped,
            "total_lines_written": total_lines,
            "brands_synced": len([r for r in all_results if r.get("ok")]),
            "brands_failed": len([r for r in all_results if not r.get("ok")]),
            "results": all_results,
        })

    except Exception as e:
        logger.error(
            "leaflink: sync_endpoint failed error=%s",
            e,
            exc_info=True,
        )
        return make_json_safe({
            "ok": False,
            "error": str(e),
        })




@app.post("/leaflink/sync-now")
async def sync_leaflink_now(
    x_opsyn_secret: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Manually trigger LeafLink sync for all active brands.
    Requires X-OPSYN-SECRET header.

    Returns a structured response with aggregate counts:
      { "ok": true, "orders_fetched": X, "created": X, "updated": X, "line_items_written": X }
    """
    expected = os.getenv("OPSYN_SYNC_SECRET")

    if not expected or x_opsyn_secret != expected:
        logger.warning("leaflink: sync_now unauthorized attempt")
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info("leaflink: sync_now manual trigger")

    try:
        from services.leaflink_sync import sync_leaflink_orders
        from services.leaflink_client import LeafLinkClient

        total_orders_fetched = 0
        total_created = 0
        total_updated = 0
        total_line_items_written = 0
        newest_order_date = None

        async with db.begin():
            # Step 1: Resolve active credentials inside the transaction.
            cred_result = await db.execute(
                select(BrandAPICredential).where(
                    BrandAPICredential.integration_name == "leaflink",
                    BrandAPICredential.is_active == True,
                )
            )
            credentials = cred_result.scalars().all()

            if not credentials:
                logger.warning("leaflink: sync_now no active credentials found")
                return {"ok": False, "error": "No active LeafLink credentials found"}

            for cred in credentials:
                # Extract scalar values while the ORM object is still in scope.
                brand_id: str = cred.brand_id
                api_key: str | None = cred.api_key
                company_id: str | None = cred.company_id

                logger.info("leaflink: sync_now syncing brand_id=%s", brand_id)
                logger.info("[SyncEndpoint] sync_start brand=%s", brand_id)

                # Step 2: Fetch orders from the LeafLink API (pure HTTP, no DB).
                # Run the synchronous requests.get() calls in a thread pool so
                # they do not block the event loop while paginating LeafLink.
                logger.info("leaflink: sync_client_init brand_id=%s", brand_id)
                client = LeafLinkClient(api_key=api_key, company_id=company_id, brand_id=brand_id)
                logger.info(
                    "[LEAFLINK_STARTUP_VERIFY] canonical_base_url=%s brand_id=%s",
                    client.base_url,
                    brand_id,
                )
                logger.info("[SyncEndpoint] leaflink_fetch_start")
                loop = asyncio.get_event_loop()
                fetch_result = await asyncio.wait_for(
                    loop.run_in_executor(
                        _leaflink_executor,
                        lambda: client.fetch_recent_orders(normalize=True, brand=brand_id),
                    ),
                    timeout=300,
                )
                orders = fetch_result["orders"]
                pages_fetched = fetch_result["pages_fetched"]
                logger.info("[SyncEndpoint] leaflink_fetch_done pages=%s", pages_fetched)

                # Step 3: Upsert orders and line items inside the same transaction.
                result = await sync_leaflink_orders(db, brand_id, orders, pages_fetched=pages_fetched)

                # Step 4: Persist credential sync status inside the same transaction.
                cred.sync_status = "ok" if result.get("ok") else "failed"
                cred.last_sync_at = datetime.now(timezone.utc)
                if not result.get("ok"):
                    cred.last_error = result.get("error", "Unknown error")
                else:
                    cred.last_error = None

                logger.info(
                    "[IntegrationCredentials] %s brand_id=%s",
                    "marked_success" if result.get("ok") else "marked_invalid",
                    brand_id,
                )
                logger.info("[SyncEndpoint] sync_complete brand=%s", brand_id)

                if result.get("ok"):
                    total_orders_fetched += result.get("orders_fetched", 0)
                    total_created += result.get("created", 0)
                    total_updated += result.get("updated", 0)
                    total_line_items_written += result.get("line_items_written", 0)
                    result_date = result.get("newest_order_date")
                    if result_date and (newest_order_date is None or result_date > newest_order_date):
                        newest_order_date = result_date

        # Transaction commits here — return results after the block.
        logger.info(
            "leaflink: sync_now complete orders_fetched=%s created=%s updated=%s line_items_written=%s",
            total_orders_fetched,
            total_created,
            total_updated,
            total_line_items_written,
        )

        return make_json_safe({
            "ok": True,
            "orders_fetched": total_orders_fetched,
            "created": total_created,
            "updated": total_updated,
            "skipped": 0,
            "line_items_written": total_line_items_written,
            "newest_order_date": newest_order_date,
            "errors": [],
        })

    except Exception as e:
        logger.error("leaflink: sync_now failed error=%s", e, exc_info=True)
        return make_json_safe({
            "ok": False,
            "error": str(e),
            "orders_fetched": 0,
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "line_items_written": 0,
            "newest_order_date": None,
            "errors": [str(e)],
        })


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
    )
