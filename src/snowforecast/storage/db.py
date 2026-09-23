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

import argparse
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


def get_engine(db_path: str | None = None) -> Engine:
    """Return the process-wide SQLAlchemy Engine (created once, cached).

    Pass db_path to force a SQLite engine at that file, bypassing
    DATABASE_URL/DB_PATH -- e.g. so a diagnostic or backtest script can be
    pointed at a copy of the database instead of the live NAS target.
    """
    global _engine, _SessionFactory
    if _engine is None:
        url = f"sqlite:///{db_path}" if db_path else database_url()
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


def add_db_path_arg(parser: argparse.ArgumentParser) -> None:
    """Add --db-path to a script's parser: point at a copy of the database
    instead of the live DATABASE_URL/DB_PATH target (e.g. the NAS Postgres
    instance). Use with get_engine(args.db_path)."""
    parser.add_argument(
        "--db-path",
        help="SQLite file to use instead of the configured DATABASE_URL/DB_PATH "
        "-- point this at a copy, not the live database, when testing",
    )


def describe_engine(engine: Engine) -> str:
    """Human-readable, credential-safe description of what an engine targets."""
    return engine.url.render_as_string(hide_password=True)


def reset_engine() -> None:
    """Dispose and clear the cached engine (used by tests that switch URLs)."""
    global _engine, _SessionFactory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionFactory = None
