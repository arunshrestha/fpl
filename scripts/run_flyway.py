"""
Run Flyway migrations locally using DATABASE_URL from .env.local.
Blocks destructive commands in staging/prod for safety (though this script is local-only).
"""

import subprocess
from pathlib import Path
import os

from config.settings import APP_ENV

DISALLOWED_COMMANDS = {"clean"}

# -------------------------------
# Load local environment variables
# -------------------------------
if APP_ENV == "local":
    from dotenv import load_dotenv

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[run_flyway] Loaded environment from {env_path}")
    else:
        print(f"[run_flyway] Warning: {env_path} not found, relying on existing env vars")

# -------------------------------
# Main Flyway runner
# -------------------------------
def main(command: str = "migrate"):
    command = command.lower()

    # Safety check
    if command in DISALLOWED_COMMANDS and APP_ENV in {"staging", "prod"}:
        raise SystemExit(f"[ERROR] Flyway '{command}' is disabled in {APP_ENV} environment")

    project_root = Path(__file__).resolve().parent.parent
    flyway_conf = project_root / "db" / "flyway" / "conf" / "flyway.conf"

    # Use JDBC URL from environment
    jdbc_url = os.getenv("FLYWAY_JDBC_URL")
    if not jdbc_url:
        raise SystemExit("[ERROR] FLYWAY_JDBC_URL not set in environment")

    cmd = ["flyway", command, f"-configFiles={flyway_conf}", f"-url={jdbc_url}"]

    print(f"[flyway] env={APP_ENV} command={command}")
    print(f"[flyway] running: {' '.join(cmd)}")

    subprocess.run(cmd, check=True, env=os.environ, text=True)


# -------------------------------
# CLI entrypoint
# -------------------------------
if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "migrate"
    main(cmd)
