import os
from typing import Optional
from urllib.parse import quote_plus
from config import settings

def _env_up(env: Optional[str]) -> str:
    return (env or settings.ENV).upper()

def _try_env(key_base: str, env: Optional[str]) -> Optional[str]:
    up = _env_up(env)
    v = os.getenv(f"{key_base}_{up}")
    if v:
        return v
    return os.getenv(key_base)

def get_db_url(env: Optional[str] = None) -> Optional[str]:
    """
    Return SQLAlchemy-ready DB URL (postgresql+psycopg2://user:pass@host:port/db).
    Precedence:
      1) DATABASE_URL_{ENV} / DB_URL_{ENV} or DATABASE_URL / DB_URL
      2) Build from POSTGRES_USER_{ENV}, POSTGRES_PASSWORD_{ENV}, POSTGRES_HOST_{ENV}, POSTGRES_PORT_{ENV}, POSTGRES_DB_{ENV}
    """
    up = _env_up(env)

    # full URL candidates
    for candidate in (os.getenv(f"DATABASE_URL_{up}"), os.getenv("DATABASE_URL"), os.getenv(f"DB_URL_{up}"), os.getenv("DB_URL")):
        if candidate:
            # normalize to SQLAlchemy psycopg2 driver prefix
            if candidate.startswith("postgres://"):
                candidate = candidate.replace("postgres://", "postgresql+psycopg2://", 1)
            if candidate.startswith("postgresql://"):
                candidate = candidate.replace("postgresql://", "postgresql+psycopg2://", 1)
            return candidate

    # build from parts
    user = _try_env("POSTGRES_USER", env) or _try_env("DB_USER", env)
    password = _try_env("POSTGRES_PASSWORD", env) or _try_env("DB_PASS", env) or ""
    host = _try_env("POSTGRES_HOST", env) or _try_env("DB_HOST", env)
    port = _try_env("POSTGRES_PORT", env) or "5432"
    db = _try_env("POSTGRES_DB", env) or _try_env("DB_NAME", env)

    if not (user and host and db):
        return None

    user_ = quote_plus(user)
    password_ = quote_plus(password) if password else ""
    auth = f"{user_}:{password_}@" if password_ else f"{user_}@"
    url = f"postgresql+psycopg2://{auth}{host}:{port}/{db}"

    sslmode = _try_env("DB_SSLMODE", env) or os.getenv("SSLMODE")
    if sslmode:
        delimiter = "&" if "?" in url else "?"
        url = f"{url}{delimiter}sslmode={sslmode}"
    return url

def get_jdbc_url(env: Optional[str] = None) -> Optional[str]:
    """
    Return JDBC URL for Flyway (jdbc:postgresql://host:port/db[?sslmode=require]).
    Prefers explicit host/db env vars (POSTGRES_HOST_{ENV} etc).
    """
    host = _try_env("POSTGRES_HOST", env) or _try_env("DB_HOST", env)
    port = _try_env("POSTGRES_PORT", env) or "5432"
    db = _try_env("POSTGRES_DB", env) or _try_env("DB_NAME", env)

    if not (host and db):
        return None

    jdbc = f"jdbc:postgresql://{host}:{port}/{db}"
    sslmode = _try_env("DB_SSLMODE", env) or os.getenv("SSLMODE")
    if sslmode:
        jdbc = f"{jdbc}?sslmode={sslmode}"
    return jdbc

def get_schema(logical_name: str = "target", env: Optional[str] = None) -> str:
    """
    Map logical schema names to actual schema names via env variables:
      - get_schema('staging_tmp') -> STAGING_TMP_SCHEMA_{ENV} or STAGING_TMP_SCHEMA or RAW_STAGING_SCHEMA
      - get_schema('target') -> TARGET_SCHEMA_{ENV} or TARGET_SCHEMA or RAW_SCHEMA
    """
    name = logical_name.lower()
    up = _env_up(env)
    candidates_map = {
        "staging_tmp": ["STAGING_TMP_SCHEMA", "RAW_STAGING_SCHEMA"],
        "staging": ["STAGING_SCHEMA", "RAW_STAGING_SCHEMA"],
        "target": ["TARGET_SCHEMA", "RAW_SCHEMA"],
        "raw": ["RAW_SCHEMA"],
    }
    candidates = candidates_map.get(name, [name.upper() + "_SCHEMA"])

    # env-specific candidate
    for key in candidates:
        v = os.getenv(f"{key}_{up}")
        if v:
            return v
    # plain var
    for key in candidates:
        v = os.getenv(key)
        if v:
            return v

    defaults = {
        "staging_tmp": "raw_staging_tmp",
        "staging": "raw_staging",
        "target": "raw",
        "raw": "raw",
    }
    return defaults.get(name, "raw")