import logging
import os
from typing import AsyncGenerator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

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

    # IMPORTANT: Preserve SSL parameters (sslmode, ssl)
    # Do NOT filter out sslmode or ssl parameters
    filtered_params = query_params  # Keep all parameters including SSL

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
# Production startup will FAIL if DATABASE_URL does not contain 'rds.amazonaws.com'
# ============================================================================

_raw_database_url = os.getenv("DATABASE_URL", "")

# Validate DATABASE_URL format before normalization
if _raw_database_url:
    logger.info("[BOOT_DB] validating_database_url")

    # Check for common malformations
    if _raw_database_url.startswith("DATABASE_URL"):
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=starts_with_DATABASE_URL"
        )
        raise ValueError(
            "DATABASE_URL is malformed: value starts with 'DATABASE_URL'. "
            "Remove the 'DATABASE_URL=' prefix from the value."
        )

    if "\n" in _raw_database_url or "\r" in _raw_database_url:
        logger.error(
            "[BOOT_DB] invalid_database_url malformed=true reason=contains_newlines"
        )
        raise ValueError(
            "DATABASE_URL is malformed: contains newlines. "
            "Ensure the value is a single line with no line breaks."
        )

    if _raw_database_url.startswith('"') or _raw_database_url.startswith("'"):
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
        _parsed_url = make_url(_raw_database_url)

        logger.info(
            "[BOOT_DB] database_url_valid drivername=%s host=%s database=%s",
            _parsed_url.drivername,
            _parsed_url.host or "unknown",
            _parsed_url.database or "unknown",
        )
    except Exception as _parse_exc:
        logger.error(
            "[BOOT_DB] invalid_database_url raw_prefix=%s error=%s",
            _raw_database_url[:25] if _raw_database_url else "EMPTY",
            str(_parse_exc),
        )
        raise ValueError(
            f"DATABASE_URL is not a valid SQLAlchemy URL: {str(_parse_exc)}. "
            f"Expected format: postgresql+asyncpg://user:pass@host:5432/database"
        )
else:
    logger.error("[BOOT_DB] DATABASE_URL is empty or not set")
    raise ValueError("DATABASE_URL environment variable is not set")

DATABASE_URL = normalize_database_url(_raw_database_url)

if DATABASE_URL:
    _parsed = urlparse(DATABASE_URL)
    _safe_url = f"{_parsed.scheme}://{_parsed.hostname}:{_parsed.port}/{_parsed.path.lstrip('/')}"
    logger.info("[Database] DATABASE_URL=%s", _safe_url)
else:
    logger.warning("[Database] DATABASE_URL not set")


class Base(DeclarativeBase):
    pass


if DATABASE_URL:
    logger.info(
        "[DB] initializing_engine database_url=%s",
        DATABASE_URL[:50] + "..." if DATABASE_URL else "NOT_SET",
    )

    # Check if SSL is in the URL
    _parsed_url = urlparse(DATABASE_URL)
    _has_ssl_in_url = "ssl" in _parsed_url.query.lower() or "sslmode" in _parsed_url.query.lower()
    logger.info("[DB] ssl_in_url=%s", _has_ssl_in_url)

    # Force SSL in connect_args for asyncpg
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": "require"},  # Force SSL for asyncpg
    )

    logger.info("[DB] engine_created with ssl=require")

    AsyncSessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    logger.info("[DB] session_factory_created")
else:
    engine = None
    AsyncSessionLocal = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    if AsyncSessionLocal is None:
        raise RuntimeError("DATABASE_URL is not configured")

    async with AsyncSessionLocal() as session:
        yield session


async def refresh_connection_pool() -> None:
    """
    Dispose of the current connection pool and create a new one.
    Use this after schema changes to ensure new connections see updated schema.
    """
    global engine, AsyncSessionLocal

    if engine is None:
        return

    logger.info("[Database] disposing_connection_pool")
    await engine.dispose()
    logger.info("[Database] connection_pool_disposed")

    # Recreate engine and session factory with SSL forced
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": "require"},  # Force SSL for asyncpg
    )

    AsyncSessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    logger.info("[Database] connection_pool_recreated with ssl=require")