import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import argparse
import shlex

def build_dbt_cmd(command: str, args) -> list:
    cmd = ["dbt", command]

    if args.select:
        cmd += ["--select", args.select]
    elif args.models:
        cmd += ["--select", args.models]

    if args.threads:
        cmd += ["--threads", str(args.threads)]

    if args.data:
        cmd += ["--data"]
    if args.schema:
        cmd += ["--schema"]

    if args.full_refresh:
        cmd += ["--full-refresh"]

    if args.vars:
        cmd += ["--vars", args.vars]

    # append extra dbt args passed after --, but strip any script-level flags
    if args.dbt_args:
        extra = list(args.dbt_args)
        if extra and extra[0] == "--":
            extra = extra[1:]

        internal_flags_with_value = {"--env", "-e"}
        cleaned = []
        i = 0
        while i < len(extra):
            tok = extra[i]
            if tok in internal_flags_with_value:
                # skip this flag and the next token if it's a value
                if i + 1 < len(extra) and not extra[i + 1].startswith("-"):
                    i += 2
                else:
                    i += 1
                continue
            cleaned.append(tok)
            i += 1

        cmd += cleaned

    return cmd

def run_dbt(command: str, env: str, dbt_cmd: list | None = None):
    project_root = Path(os.getenv("FPL_PROJECT_ROOT", Path(__file__).resolve().parent.parent))

    env_path = project_root / f".env.{env}"
    # load .env.<env> if present; allow missing file (so env vars can be provided another way)
    if env_path.exists():
        load_dotenv(env_path)
    else:
        print(f"[run_dbt] warning: .env.{env} not found at {env_path}, continuing using environment variables")

    dbt_dir = project_root / "dbt"
    dbt_env = os.environ.copy()
    # tell dbt to use the profiles.yml in the project dir (we keep profiles.yml in dbt/)
    dbt_env["DBT_PROFILES_DIR"] = str(dbt_dir)

    if dbt_cmd is None:
        dbt_cmd = ["dbt", command]

    printable = " ".join(shlex.quote(p) for p in dbt_cmd)
    print("Running:", printable)
    result = subprocess.run(
        dbt_cmd,
        env=dbt_env,
        cwd=dbt_dir,
        capture_output=True,
        text=True
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise SystemExit(f"dbt {command} failed with code {result.returncode}")

def main(argv):
    parser = argparse.ArgumentParser(description="Run dbt with project-local profiles and .env.<env>.")
    parser.add_argument("command", nargs="?", default="run", help="dbt command (run|test|compile|seed|snapshot|docs|ls)")
    parser.add_argument("--env", "-e", default="local", help=".env file suffix (default: local)")
    parser.add_argument("--select", "-s", help="dbt --select selector (e.g. stg_gameweeks+)")
    parser.add_argument("--models", "-m", help="alias for --select")
    parser.add_argument("--threads", "-t", type=int, help="dbt --threads value")
    parser.add_argument("--data", action="store_true", help="pass --data to dbt test")
    parser.add_argument("--schema", action="store_true", help="pass --schema to dbt test")
    parser.add_argument("--full-refresh", action="store_true", help="pass --full-refresh to dbt run")
    parser.add_argument("--vars", "-v", help="vars YAML/JSON string to pass to dbt (--vars '{...}')")
    parser.add_argument("dbt_args", nargs=argparse.REMAINDER, help="extra dbt CLI args (place after --)")

    args = parser.parse_args(argv)

    dbt_cmd = build_dbt_cmd(args.command, args)
    run_dbt(args.command, args.env, dbt_cmd)

if __name__ == "__main__":
    main(sys.argv[1:])