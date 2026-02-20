"""Database engine and session management.

Provides functions to create SQLAlchemy engine, sessions, and initialize tables.
"""

import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .models import Base

# Default PostgreSQL connection string
DEFAULT_DATABASE_URL = "postgresql://lookthrough:lookthrough@localhost:5432/lookthrough"

# Module-level engine cache
_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    """Get or create the SQLAlchemy engine from DATABASE_URL environment variable.

    Uses DATABASE_URL env var, defaulting to local PostgreSQL if not set.

    Returns:
        SQLAlchemy Engine instance
    """
    global _engine

    if _engine is None:
        database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
        _engine = create_engine(database_url, echo=False)

    return _engine


def get_session() -> Session:
    """Create and return a new database session.

    Returns:
        SQLAlchemy Session instance
    """
    global _SessionLocal

    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(bind=engine)

    return _SessionLocal()


@contextmanager
def get_session_context() -> Generator[Session, None, None]:
    """Context manager for database sessions with automatic cleanup.

    Yields:
        SQLAlchemy Session instance

    Example:
        with get_session_context() as session:
            session.query(DimFund).all()
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """Create all tables defined in the ORM models.

    Uses Base.metadata.create_all() to create any tables that don't exist.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    print(f"Database tables created/verified at: {engine.url}")


def reset_engine() -> None:
    """Reset the cached engine and session factory.

    Useful for testing or when changing database connections.
    """
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionLocal = None
