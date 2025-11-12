import os
from typing import Optional, Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from config.db_config import get_db_url

# cache engines per environment key
_engines: Dict[str, Engine] = {}


def _env_key(env: Optional[str]) -> str:
    """Normalized cache key for environment (lowercase)."""
    return (env or os.getenv("ENV", "local")).lower()


def get_engine(env: Optional[str] = None, engine_kwargs: Optional[Dict[str, Any]] = None) -> Engine:
    """
    Return a SQLAlchemy Engine for the requested environment.

    - env: "local"/"dev"/"staging"/"prod" or None (uses ENV environment variable, default "local")
    - engine_kwargs: optional dict forwarded to sqlalchemy.create_engine (e.g., {"connect_args": {"sslmode":"require"}})

    Caches one Engine instance per env key.
    Raises RuntimeError when no DB URL can be resolved for the environment.
    """
    key = _env_key(env)
    if key in _engines:
        return _engines[key]

    url = get_db_url(env)
    if not url:
        raise RuntimeError(
            f"No database URL found for environment '{key}'.\n"
            f"Set DATABASE_URL_{key.upper()} or POSTGRES_USER_{key.upper()}/POSTGRES_PASSWORD_{key.upper()}/"
            "POSTGRES_HOST_{key.upper()}/POSTGRES_DB_{key.upper()} (or DB_URL_{env}) as appropriate."
        )

    kwargs = dict(engine_kwargs or {})
    # default to SQLAlchemy future mode
    kwargs.setdefault("future", True)

    engine = create_engine(url, **kwargs)
    _engines[key] = engine
    return engine


def get_connection(env: Optional[str] = None):
    """
    Return a Connection context manager for the requested environment.

    Usage:
        with get_connection('staging') as conn:
            conn.execute(text("SELECT 1"))
    """
    engine = get_engine(env)
    return engine.connect()


def dispose_engines() -> None:
    """Dispose and clear all cached engines (useful in tests/cleanup)."""
    global _engines
    for e in _engines.values():
        try:
            e.dispose()
        except Exception:
            pass
    _engines = {}