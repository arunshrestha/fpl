import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

def run_dbt(command: str = "run", env: str = "dev"):
    """
    Run a dbt CLI command using a project-local profiles.yml and environment variables from .env.{env}.
    Usage: python run_dbt.py run --env dev
    """
    # Project root
    project_root = Path(os.getenv("FPL_PROJECT_ROOT", Path(__file__).resolve().parent.parent))

    # Load environment variables from .env.{env}
    env_path = project_root / f".env.{env}"
    if not env_path.exists():
        raise FileNotFoundError(f".env.{env} file not found at {env_path}")
    load_dotenv(env_path)

    # dbt project folder
    dbt_dir = project_root / "dbt"

    # Use project-local profiles.yml
    dbt_env = os.environ.copy()
    dbt_env["DBT_PROFILES_DIR"] = str(dbt_dir)

    # Build dbt command
    dbt_cmd = ["dbt"] + command.split()

    # Run dbt
    result = subprocess.run(
        dbt_cmd,
        env=dbt_env,
        cwd=dbt_dir,
        capture_output=True,
        text=True
    )

    # Print stdout
    print(result.stdout)

    # Handle errors
    if result.returncode != 0:
        print(result.stderr)
        raise SystemExit(f"dbt {command} failed with code {result.returncode}")

if __name__ == "__main__":
    # Parse command and environment from CLI arguments
    cmd = "run"
    env = "dev"
    args = sys.argv[1:]
    if args:
        cmd = args[0]
        if "--env" in args:
            env_idx = args.index("--env")
            if len(args) > env_idx + 1:
                env = args[env_idx + 1]
    run_dbt(cmd, env)