import os
from pathlib import Path

# Canonical environment switch
APP_ENV = os.getenv("APP_ENV", "local").lower()

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def load_local_env() -> None:
    """
    Load .env.local for local development only.
    Safe no-op in staging/prod.
    """
    if APP_ENV != "local":
        return

    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        return

    env_file = PROJECT_ROOT / ".env.local"
    if env_file.exists():
        load_dotenv(env_file)


# Load immediately on import
load_local_env()
