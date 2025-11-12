import os
from pathlib import Path
from typing import Optional

# Optional: if you want local .env loading, install python-dotenv in your venv.
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

# Canonical environment variable: use ENV (local|staging|prod)
ENV = os.getenv("ENV", "local").lower()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load .env.<ENV> automatically for local dev if python-dotenv is installed
if ENV in ("local", "dev") and load_dotenv:
    env_file = PROJECT_ROOT / f".env.{ENV}"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file)