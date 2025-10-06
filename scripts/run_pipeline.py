"""
Run the full ETL pipeline manually.
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Set environment before importing pipeline
env = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] in ("dev", "prod") else "dev"
os.environ["FPL_ENV"] = env

# Load the correct .env file before any other imports
project_root = Path(os.getenv("FPL_PROJECT_ROOT", Path(__file__).resolve().parent.parent))
env_file = project_root / f".env.{env}"
if not env_file.exists():
    raise FileNotFoundError(f"Environment file {env_file} not found.")
load_dotenv(env_file)

from etl.pipeline import run_etl

if __name__ == "__main__":
    run_etl()