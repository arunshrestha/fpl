import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine
from config.settings import DB_CONFIG

def upsert_dataframe(
    df,
    table_name: str,
    engine: Engine,
    unique_key: str,
    staging_schema: str = None,
    target_schema: str = None,
    batch_size: int = 5000
):
    """
    Production-ready upsert of a pandas DataFrame into a Postgres table via staging.

    Parameters:
    - df: pandas DataFrame
    - table_name: base table name (no schema), e.g. 'fixtures'
    - engine: SQLAlchemy Engine
    - unique_key: primary key column for ON CONFLICT
    - staging_schema: staging schema (from env/config if not provided)
    - target_schema: target schema (from env/config if not provided)
    - batch_size: number of rows per batch insert
    """
    # Use schemas from config if not provided
    if staging_schema is None:
        staging_schema = DB_CONFIG.get("raw_staging_schema", "raw_staging")
    if target_schema is None:
        target_schema = DB_CONFIG.get("raw_schema", "raw")

    if isinstance(df, dict):
        import pandas as pd
        df = pd.DataFrame([df])

    if df.empty:
        logging.info(f"No data to upsert into {target_schema}.{table_name}.")
        return

    cols = list(df.columns)
    cols_str = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])

    staging_table = f"{staging_schema}.{table_name}"
    target_table = f"{target_schema}.{table_name}"

    try:
        with engine.begin() as conn:
            # clear staging and bulk insert into staging table
            conn.execute(text(f"TRUNCATE TABLE {staging_table};"))

            insert_sql = text(f"INSERT INTO {staging_table} ({cols_str}) VALUES ({placeholders})")
            rows = df.to_dict(orient="records")
            for i in range(0, len(rows), batch_size):
                conn.execute(insert_sql, rows[i : i + batch_size])

            # merge from staging -> target
            if unique_key:
                update_cols = [c for c in cols if c != unique_key]
                set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols]) if update_cols else ""
                merge_sql = f"""
                    INSERT INTO {target_table} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                    ON CONFLICT ({unique_key}) DO UPDATE SET {set_clause};
                """
                conn.execute(text(merge_sql))
                logging.info("Upserted %d rows into %s", len(rows), target_table)
            else:
                # insert-only append
                insert_from_staging = f"""
                    INSERT INTO {target_table} ({cols_str})
                    SELECT {cols_str} FROM {staging_table};
                """
                conn.execute(text(insert_from_staging))
                logging.info("Inserted %d rows into %s (append)", len(rows), target_table)

    except Exception as e:
        logging.error("Upsert/insert failed for %s.%s: %s", target_schema, table_name, e)
        raise