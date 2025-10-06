from sqlalchemy import create_engine, text
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Determine environment (default: dev)
env = "dev"
if len(sys.argv) > 1 and sys.argv[1] in ("dev", "prod"):
    env = sys.argv[1]

# Load environment variables from .env.{env}
project_root = Path(__file__).resolve().parent.parent
env_file = project_root / f".env.{env}"
if not env_file.exists():
    raise FileNotFoundError(f"Environment file {env_file} not found.")
load_dotenv(env_file)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = int(os.getenv("POSTGRES_PORT", 5432))
DB_NAME = os.getenv("POSTGRES_DB")

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Test connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW();"))
        print(f"✅ Connection successful to {env}! Current database timestamp:", result.fetchone()[0])
except Exception as e:
    print(f"❌ Connection failed to {env}:", e)