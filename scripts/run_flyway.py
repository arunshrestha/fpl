import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

def run_flyway(command: str = "migrate", env: str = "local"):
    """
    Run Flyway migrations safely depending on environment.
    
    Local: loads .env.local and allows all commands.
    Staging/Prod: expects env vars to be set (CI/CD secrets). Prevents destructive commands.
    
    Usage: python -m scripts.run_flyway migrate --env local
    """
    # 1️⃣ Determine project root
    project_root = Path(__file__).parent.parent

    # 2️⃣ Load local .env only for local environment
    if env == "local":
        env_file = project_root / ".env.local"
        if env_file.exists():
            load_dotenv(env_file)
        else:
            raise FileNotFoundError(f"Local env file not found at {env_file}")

    # 3️⃣ Prevent destructive commands in staging/prod
    if env in ["staging", "prod"] and command in ["clean"]:
        raise SystemExit(f"Flyway '{command}' is disabled in {env} environment!")

    # 4️⃣ Determine Flyway config file
    flyway_conf = project_root / "db" / "flyway" / "conf" / f"flyway_{env}.conf"
    if not flyway_conf.exists():
        raise FileNotFoundError(f"Flyway config file not found: {flyway_conf}")

    # 5️⃣ Build and run Flyway command
    flyway_cmd = [
        "flyway",
        command,
        f"-configFiles={flyway_conf}"
    ]

    result = subprocess.run(flyway_cmd, env=os.environ, capture_output=True, text=True)

    # 6️⃣ Print Flyway output
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise SystemExit(f"Flyway '{command}' failed with exit code {result.returncode}")

if __name__ == "__main__":
    # Default command/env
    cmd = "migrate"
    env = "local"

    # Parse CLI arguments
    args = sys.argv[1:]
    if args:
        cmd = args[0]
        if "--env" in args:
            env_idx = args.index("--env")
            if len(args) > env_idx + 1:
                env = args[env_idx + 1]

    run_flyway(cmd, env)
