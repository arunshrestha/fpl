from typing import Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config.db_config import get_database_url

_engine: Engine | None = None


def get_engine(engine_kwargs: Dict[str, Any] | None = None) -> Engine:
    """
    Return a singleton SQLAlchemy Engine.

    Uses DATABASE_URL from the environment.
    """
    global _engine

    if _engine is not None:
        return _engine

    url = get_database_url()

    kwargs = engine_kwargs or {}
    kwargs.setdefault("pool_pre_ping", True)
    kwargs.setdefault("future", True)

    _engine = create_engine(url, **kwargs)
    return _engine


def get_connection():
    """
    Context-managed database connection.

    Usage:
        with get_connection() as conn:
            conn.execute(...)
    """
    return get_engine().begin()


def dispose_engine() -> None:
    """Dispose the engine (useful for tests)."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
