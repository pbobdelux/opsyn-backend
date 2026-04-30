# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
# Production uses AWS RDS Postgres (opsyn_prod database)
# Development can use Railway Postgres or local Postgres
#
# DATABASE_URL format:
#   AWS RDS: postgresql+asyncpg://user:pass@opsyn-prod.xxxxx.rds.amazonaws.com:5432/opsyn_prod
#   Railway: postgresql+asyncpg://user:pass@postgres.railway.internal:5432/railway
#   Local:   postgresql+asyncpg://user:pass@localhost:5432/opsyn_dev
#
# Production startup will FAIL if DATABASE_URL does not contain 'rds.amazonaws.com'
# ============================================================================

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

    # asyncpg does NOT accept sslmode in the DSN the way psycopg2 does
    filtered_params = [(k, v) for (k, v) in query_params if k.lower() != "sslmode"]

    rebuilt = parsed._replace(query=urlencode(filtered_params))
    return urlunparse(rebuilt)


DATABASE_URL = normalize_database_url(os.getenv("DATABASE_URL", ""))

if DATABASE_URL:
    _parsed = urlparse(DATABASE_URL)
    _safe_url = f"{_parsed.scheme}://{_parsed.hostname}:{_parsed.port}/{_parsed.path.lstrip('/')}"
    logger.info("[Database] DATABASE_URL=%s", _safe_url)
else:
    logger.warning("[Database] DATABASE_URL not set")


class Base(DeclarativeBase):
    pass


if DATABASE_URL:
    logger.info("[DB] DATABASE_URL is set")
    logger.info(
        "[DB] initializing_engine database_url=%s",
        DATABASE_URL[:50] + "..." if DATABASE_URL else "NOT_SET",
    )

    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )

    logger.info("[DB] engine_created successfully")

    AsyncSessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    logger.info("[DB] session_factory_created")

    # Parse and log connection details with provider detection
    try:
        _parsed_db = urlparse(DATABASE_URL)
        _db_host = _parsed_db.hostname or "unknown"
        _db_port = _parsed_db.port or 5432
        _db_name = _parsed_db.path.lstrip("/") or "unknown"

        # Detect provider
        if "rds.amazonaws.com" in _db_host:
            _provider = "aws-rds"
        elif "railway" in _db_host:
            _provider = "railway"
        else:
            _provider = "unknown"

        logger.info(
            "[DB] connection_details host=%s port=%s database=%s provider=%s",
            _db_host,
            _db_port,
            _db_name,
            _provider,
        )
    except Exception as _exc:
        logger.error("[DB] failed_to_parse_url error=%s", _exc)
else:
    logger.error("[DB] DATABASE_URL is NOT set - database connection will fail")
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

    # Recreate engine and session factory
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )

    AsyncSessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    logger.info("[Database] connection_pool_recreated")