# backend/db/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from backend.config import get_settings

settings = get_settings()

# connect_args is only needed for SQLite (check_same_thread).
# Postgres (asyncpg) needs no special connect args.
_connect_args = {}
if settings.DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}

_pool_kwargs = {"pool_pre_ping": True, "pool_recycle": 300, "pool_size": 10, "max_overflow": 20}
if settings.DATABASE_URL.startswith("sqlite"):
    _pool_kwargs = {}

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    connect_args=_connect_args,
    **_pool_kwargs,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    from backend.db import models  # noqa: F401 — registers all models

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ================================================================
# Engine lifecycle — call close_db() on graceful shutdown to
# prevent Windows ProactorEventLoop crashes from lingering
# asyncpg connection-pool cleanup.
# ================================================================

async def close_db():
    """Gracefully close the async engine connection pool."""
    if engine is not None:
        await engine.dispose()


# ================================================================
# Sync engine — used ONLY by push_notifications.send_push_sync(),
# called from worker.py's sync command-handling thread. A tiny
# read/delete query doesn't justify routing through the async
# session + run_coroutine_threadsafe machinery used elsewhere.
# ================================================================

_sync_engine = None
_sync_session = None


def _sync_engine_for_push():
    global _sync_engine
    if _sync_engine is None:
        # asyncpg / aiosqlite URLs -> sync driver equivalents
        sync_url = settings.DATABASE_URL.replace(
            "postgresql+asyncpg", "postgresql+psycopg2"
        ).replace("sqlite+aiosqlite", "sqlite")
        connect_args = (
            {"check_same_thread": False} if sync_url.startswith("sqlite") else {}
        )
        _sync_engine = create_engine(
            sync_url, pool_pre_ping=True, connect_args=connect_args
        )
    return _sync_engine


def get_sync_session():
    global _sync_session
    if _sync_session is None:
        _sync_session = sessionmaker(
            bind=_sync_engine_for_push(), expire_on_commit=False
        )
    return _sync_session()
