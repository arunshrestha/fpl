import os
import sys
import argparse
from sqlalchemy import text

def main():
    parser = argparse.ArgumentParser(description="Quick DB connection test.")
    parser.add_argument("--env", help="Environment to use (local|staging|prod). If omitted reads ENV env var.", default=None)
    args = parser.parse_args()

    # Determine environment and ensure it's available to config modules
    env = args.env or os.getenv("ENV", "local")
    os.environ["ENV"] = env

    # import after ENV is set so config modules read the correct environment
    from db.postgres_client import get_engine

    try:
        engine = get_engine()
    except Exception as exc:
        print(f"[ERROR] Failed to create DB engine for env='{env}': {exc}", file=sys.stderr)
        sys.exit(2)

    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT now()"))
            print(f"ENV={env} DB now(): {r.scalar()}")
    except Exception as exc:
        print(f"[ERROR] Query failed for env='{env}': {exc}", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()