"""
Central database access for snowforecast.

One place to configure the datastore, replacing the ~67 scattered
``sqlite3.connect(...)`` call sites. The backend is chosen by ``DATABASE_URL``:

    DATABASE_URL=postgresql+psycopg2://user:pass@10.0.0.249:5432/snowforecast   # prod (NAS)
    # unset -> SQLite fallback at $DB_PATH, for local dev / tests / CI

Usage:
    from snowforecast.storage.db import get_engine, session_scope

    with session_scope() as session:
        ...                       # ORM work, auto commit/rollback/close

    df = pd.read_sql(text("SELECT ... WHERE date = :d"), get_engine(),
                     params={"d": day})    # pandas reads against the engine
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

# SQLite connect timeout (seconds) — the NAS filesystem needs a generous
# busy-timeout to avoid "database is locked"; mirrors the old timeout=30.
_SQLITE_TIMEOUT = 30

_engine: Engine | None = None
_SessionFactory: sessionmaker | None = None


def database_url() -> str:
    """Resolve the active database URL.

    Prefers ``DATABASE_URL`` (Postgres in prod). Falls back to a SQLite URL
    built from ``DB_PATH`` so local dev, tests and CI run with no Postgres.
    """
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    db_path = os.environ.get("DB_PATH", "global_snowfall.db")
    return f"sqlite:///{db_path}"


def get_engine() -> Engine:
    """Return the process-wide SQLAlchemy Engine (created once, cached)."""
    global _engine, _SessionFactory
    if _engine is None:
        url = database_url()
        kwargs: dict = {"future": True}
        if url.startswith("sqlite"):
            # Match the old per-connection timeout=30 behaviour.
            kwargs["connect_args"] = {"timeout": _SQLITE_TIMEOUT}
        else:
            # Recycle stale LAN connections and verify liveness on checkout.
            kwargs["pool_pre_ping"] = True
            kwargs["pool_recycle"] = 1800
        _engine = create_engine(url, **kwargs)
        _SessionFactory = sessionmaker(bind=_engine, future=True, expire_on_commit=False)
    return _engine


def get_session() -> Session:
    """Return a new ORM Session. Caller is responsible for closing it.

    Prefer :func:`session_scope` for automatic commit/rollback/close.
    """
    if _SessionFactory is None:
        get_engine()
    assert _SessionFactory is not None
    return _SessionFactory()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Transactional ORM session: commits on success, rolls back on error."""
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def reset_engine() -> None:
    """Dispose and clear the cached engine (used by tests that switch URLs)."""
    global _engine, _SessionFactory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionFactory = None
