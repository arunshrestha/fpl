import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

def run_flyway(command: str = "migrate", env: str = "dev"):
    """
    Run a Flyway command with environment variables loaded from the specified .env file.
    Usage: python run_flyway.py migrate --env dev
    """
    project_root = Path(os.getenv("FPL_PROJECT_ROOT", Path(__file__).resolve().parent.parent))
    env_file = project_root / f".env.{env}"
    if not env_file.exists():
        raise FileNotFoundError(f"Environment file {env_file} not found.")
    load_dotenv(env_file)

    # Prevent flyway clean in production
    if command == "clean" and env != "dev":
        raise SystemExit("Flyway clean is disabled in production!")

    flyway_conf = project_root / "db/flyway/conf/flyway.conf"
    flyway_cmd = [
        "flyway",
        command,
        "-configFiles=" + str(flyway_conf)
    ]

    result = subprocess.run(flyway_cmd, env=os.environ, capture_output=True, text=True)

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise SystemExit(f"Flyway {command} failed")

if __name__ == "__main__":
    # Parse command and environment from CLI arguments
    cmd = "migrate"
    env = "dev"
    args = sys.argv[1:]
    if args:
        cmd = args[0]
        if "--env" in args:
            env_idx = args.index("--env")
            if len(args) > env_idx + 1:
                env = args[env_idx + 1]
    run_flyway(cmd, env)