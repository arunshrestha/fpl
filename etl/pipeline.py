"""
ETL orchestration script: extract → transform → load.
"""

import logging

from db.postgres_client import get_engine
from config import schemas

from .extract import fetch_bootstrap_static, fetch_fixtures
from .transform import transform_bootstrap_static, transform_fixtures
from .load import upsert_dataframe
from .utils import prepare_load_df

logger = logging.getLogger(__name__)


def run_etl() -> None:
    """
    Run the ETL pipeline.

    The target environment is determined entirely by DATABASE_URL.
    """
    engine = get_engine()

    logger.info("Starting ETL pipeline")

    # --------------------
    # Extract
    # --------------------
    bootstrap_data = fetch_bootstrap_static()
    fixtures_data = fetch_fixtures()

    # --------------------
    # Transform
    # --------------------
    bootstrap_df = transform_bootstrap_static(bootstrap_data)
    fixtures_df = transform_fixtures(fixtures_data)

    # --------------------
    # Prepare for loading
    # --------------------
    bootstrap_df = prepare_load_df(
        bootstrap_df,
        json_cols=["data"],
    )

    # --------------------
    # Load: bootstrap_static
    # --------------------
    upsert_dataframe(
        df=bootstrap_df,
        table_name="bootstrap_static",
        engine=engine,
        unique_key="id",
        staging_schema=schemas.RAW_STAGING_TMP,
        target_schema=schemas.RAW,
    )

    # --------------------
    # Load: fixtures
    # --------------------
    upsert_dataframe(
        df=fixtures_df,
        table_name="fixtures",
        engine=engine,
        unique_key="id",
        staging_schema=schemas.RAW_STAGING_TMP,
        target_schema=schemas.RAW,
    )

    logger.info("ETL pipeline completed successfully")
