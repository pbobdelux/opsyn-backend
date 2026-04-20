import os
from typing import AsyncGenerator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


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


class Base(DeclarativeBase):
    pass


if DATABASE_URL:
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
else:
    engine = None
    AsyncSessionLocal = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    if AsyncSessionLocal is None:
        raise RuntimeError("DATABASE_URL is not configured")

    async with AsyncSessionLocal() as session:
        yield session