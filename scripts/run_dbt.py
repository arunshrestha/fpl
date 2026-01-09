"""
Run dbt models locally.

- Uses DATABASE_URL from environment
- Automatically loads .env.local
- Injects canonical schema env vars from config/schemas.py
- Intended for LOCAL USE ONLY
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import shlex
import argparse
from config import schemas  # <-- import your canonical schemas


def build_dbt_cmd(command: str, args) -> list[str]:
    cmd = ["dbt", command]

    if args.select:
        cmd += ["--select", args.select]
    elif args.models:
        cmd += ["--select", args.models]

    if args.threads:
        cmd += ["--threads", str(args.threads)]

    if args.full_refresh:
        cmd += ["--full-refresh"]

    if args.vars:
        cmd += ["--vars", args.vars]

    # pass through anything after --
    if args.dbt_args:
        extra = list(args.dbt_args)
        if extra and extra[0] == "--":
            extra = extra[1:]
        cmd += extra

    return cmd


def run_dbt(command: str, dbt_cmd: list[str]):
    project_root = Path(__file__).resolve().parent.parent

    # Always local
    os.environ.setdefault("APP_ENV", "local")

    # Load .env.local for DATABASE_URL
    dotenv_path = project_root / ".env.local"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
    else:
        raise RuntimeError(".env.local not found (required for local dbt runs)")

    if "DATABASE_URL" not in os.environ:
        raise RuntimeError("DATABASE_URL must be set for dbt")

    # Inject canonical schemas so dbt parsing works
    os.environ.setdefault("RAW_SCHEMA", schemas.RAW)
    os.environ.setdefault("STAGING_SCHEMA", schemas.STAGING)
    os.environ.setdefault("INTERMEDIATE_SCHEMA", schemas.INTERMEDIATE)
    os.environ.setdefault("MART_SCHEMA", schemas.MART)

    dbt_dir = project_root / "dbt"
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(dbt_dir)

    print("[dbt] running:", " ".join(shlex.quote(p) for p in dbt_cmd))

    result = subprocess.run(
        dbt_cmd,
        cwd=dbt_dir,
        env=env,
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise SystemExit(f"[ERROR] dbt {command} failed")


def main(argv=None):
    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser(description="Run dbt locally")
    parser.add_argument("command", nargs="?", default="run")
    parser.add_argument("--select", "-s")
    parser.add_argument("--models", "-m")
    parser.add_argument("--threads", "-t", type=int)
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--vars", "-v")
    parser.add_argument("dbt_args", nargs=argparse.REMAINDER)

    args = parser.parse_args(argv)

    cmd = build_dbt_cmd(args.command, args)
    run_dbt(args.command, cmd)


if __name__ == "__main__":
    main()
