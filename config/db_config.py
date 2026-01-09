import os
from typing import Final

from config.settings import APP_ENV  # ensures env is loaded first

SQLALCHEMY_SCHEME: Final = "postgresql+psycopg2://"


def get_database_url() -> str:
    """
    Return a SQLAlchemy-compatible DATABASE_URL.

    Required:
      DATABASE_URL=postgresql://user:pass@host:5432/db?sslmode=require
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Set it in .env.local (local) or GitHub Secrets (staging/prod)."
        )

    # Normalize for SQLAlchemy
    if url.startswith("postgres://"):
        return url.replace("postgres://", SQLALCHEMY_SCHEME, 1)

    if url.startswith("postgresql://"):
        return url.replace("postgresql://", SQLALCHEMY_SCHEME, 1)

    return url
