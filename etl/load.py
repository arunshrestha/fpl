import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
from config.db_config import get_schema

def upsert_dataframe(
    df,
    table_name: str,
    engine: Engine,
    unique_key: str = None,
    staging_schema: str = None,   # e.g., raw_staging_tmp
    target_schema: str = None,    # e.g., raw
    batch_size: int = 5000
):
    """
    Generic upsert function: loads a pandas DataFrame into a staging schema and merges into target schema.
    """

    # Use defaults from config
    if staging_schema is None:
        staging_schema = get_schema("staging_tmp")
    if target_schema is None:
        target_schema = get_schema("target")

    # Convert dict to DataFrame
    if isinstance(df, dict):
        df = pd.DataFrame([df])

    if df is None or getattr(df, "empty", False):
        logging.info(f"No data to upsert into {target_schema}.{table_name}.")
        return

    cols = list(df.columns)
    # quote column names to avoid SQL issues with casing or reserved words
    cols_str = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join([f":{c}" for c in cols])

    # Fully qualified table names
    staging_table = f"{staging_schema}.{table_name}"
    target_table = f"{target_schema}.{table_name}"

    try:
        with engine.begin() as conn:
            # Load into staging table
            conn.execute(text(f"TRUNCATE TABLE {staging_table};"))

            insert_sql = text(f"INSERT INTO {staging_table} ({cols_str}) VALUES ({placeholders})")
            rows = df.to_dict(orient="records")
            for i in range(0, len(rows), batch_size):
                conn.execute(insert_sql, rows[i : i + batch_size])

            # Merge into target table
            if unique_key:
                update_cols = [c for c in cols if c != unique_key]
                if update_cols:
                    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                else:
                    set_clause = ""
                merge_sql = f"""
                    INSERT INTO {target_table} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                    ON CONFLICT ("{unique_key}") DO UPDATE SET {set_clause};
                """
                conn.execute(text(merge_sql))
                logging.info("Upserted %d rows into %s", len(rows), target_table)
            else:
                insert_from_staging = f"""
                    INSERT INTO {target_table} ({cols_str})
                    SELECT {cols_str} FROM {staging_table};
                """
                conn.execute(text(insert_from_staging))
                logging.info("Inserted %d rows into %s (append)", len(rows), target_table)

    except Exception as e:
        logging.error("Upsert/insert failed for %s.%s: %s", target_schema, table_name, e)
        raise