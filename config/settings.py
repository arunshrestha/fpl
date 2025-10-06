import os

# Determine environment (default: dev)
ENV = os.getenv("FPL_ENV", "dev")

DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "raw_schema": os.getenv("RAW_SCHEMA"),
    "raw_staging_schema": os.getenv("RAW_STAGING_SCHEMA"),
}