"""
ETL orchestration script: extract → transform → load.
"""

from .extract import fetch_bootstrap_static, fetch_fixtures
from .transform import transform_bootstrap_static, transform_fixtures
from .load import upsert_dataframe
from .utils import prepare_load_df
from db.postgres_client import get_engine
from config.db_config import get_schema

def run_etl(env: str = "local"):
    """
    Run the ETL pipeline for the given environment.

    Parameters:
    - env: "local", "staging", or "prod"
    """
    # Note: scripts should set ENV before importing this module; get_engine reads ENV
    engine = get_engine()

    # Extract
    bootstrap_data = fetch_bootstrap_static()
    fixtures_data = fetch_fixtures()

    # Transform
    bootstrap_row_df = transform_bootstrap_static(bootstrap_data)
    fixtures_df = transform_fixtures(fixtures_data)

    # Prepare for loading (JSONB columns, etc.)
    bootstrap_row_df = prepare_load_df(
        bootstrap_row_df,
        json_cols=['data']
    )

    # Resolve schemas from central config
    temp_schema = get_schema("staging_tmp")
    target_schema = get_schema("target")

    # Load: bootstrap
    upsert_dataframe(
        df=bootstrap_row_df,
        table_name="bootstrap_static",
        engine=engine,
        unique_key=None,
        staging_schema=temp_schema,
        target_schema=target_schema
    )

    # Load: fixtures
    upsert_dataframe(
        df=fixtures_df,
        table_name="fixtures",
        engine=engine,
        unique_key="id",
        staging_schema=temp_schema,
        target_schema=target_schema
    )