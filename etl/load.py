import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import schemas

logger = logging.getLogger(__name__)


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    *,
    unique_key: Optional[str] = None,
    staging_schema: str = schemas.RAW_STAGING_TMP,
    target_schema: str = schemas.RAW,
    batch_size: int = 5_000,
) -> None:
    """
    Load a DataFrame into a staging table, then merge into the target table.

    - Uses TRUNCATE + INSERT for staging
    - Uses INSERT ... ON CONFLICT for upsert
    """

    if df is None or df.empty:
        logger.info("No data to load into %s.%s", target_schema, table_name)
        return

    # Ensure DataFrame
    if isinstance(df, dict):
        df = pd.DataFrame([df])

    columns = list(df.columns)
    quoted_cols = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(f":{c}" for c in columns)

    staging_table = f"{staging_schema}.{table_name}"
    target_table = f"{target_schema}.{table_name}"

    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        # 1️⃣ Clear staging
        conn.execute(text(f"TRUNCATE TABLE {staging_table};"))

        # 2️⃣ Load staging
        insert_sql = text(
            f"INSERT INTO {staging_table} ({quoted_cols}) VALUES ({placeholders})"
        )

        for i in range(0, len(rows), batch_size):
            conn.execute(insert_sql, rows[i : i + batch_size])

        # 3️⃣ Merge into target
        if unique_key:
            update_cols = [c for c in columns if c != unique_key]

            set_clause = ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in update_cols
            )

            merge_sql = f"""
                INSERT INTO {target_table} ({quoted_cols})
                SELECT {quoted_cols} FROM {staging_table}
                ON CONFLICT ("{unique_key}")
                DO UPDATE SET {set_clause};
            """
            conn.execute(text(merge_sql))
            logger.info("Upserted %d rows into %s", len(rows), target_table)
        else:
            insert_sql = f"""
                INSERT INTO {target_table} ({quoted_cols})
                SELECT {quoted_cols} FROM {staging_table};
            """
            conn.execute(text(insert_sql))
            logger.info("Inserted %d rows into %s", len(rows), target_table)
