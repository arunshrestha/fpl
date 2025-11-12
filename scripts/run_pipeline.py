"""
Run ETL pipeline for local, staging, or prod.
This entrypoint ensures `ENV` is set before any config is imported.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import argparse

def load_environment(env: str):
    """
    Set ENV and load local .env.<env> when present.
    For CI (staging/prod), environment variables should be supplied by the workflow.
    """
    os.environ["ENV"] = env
    project_root = Path(os.getenv("FPL_PROJECT_ROOT", Path(__file__).resolve().parent.parent))
    env_file = project_root / f".env.{env}"

    if env == "local" and env_file.exists():
        # optional: requires python-dotenv in the dev venv to actually load
        load_dotenv(dotenv_path=env_file)

def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline.")
    parser.add_argument("--env", default="local", choices=["local", "staging", "prod"], help="Environment to run")
    args = parser.parse_args()

    env = args.env

    # 1) set ENV and load .env if local
    load_environment(env)

    # 2) import ETL pipeline after ENV is set so config modules read correct env
    from etl.pipeline import run_etl

    # 3) run ETL
    run_etl(env=env)

if __name__ == "__main__":
    main()