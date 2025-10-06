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

    if df.empty:
        logging.info(f"No data to upsert into {target_schema}.{table_name}.")
        return

    staging_table = f"{staging_schema}.{table_name}"
    target_table = f"{target_schema}.{table_name}"

    cols = df.columns.tolist()
    update_cols = [c for c in cols if c != unique_key]
    set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    cols_str = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])  # named params for safe binding

    try:
        with engine.begin() as conn:
            # 1️⃣ Truncate staging
            conn.execute(text(f"TRUNCATE TABLE {staging_table};"))
            logging.info(f"Truncated staging table {staging_table}.")

            # 2️⃣ Batched bulk insert
            rows = df.to_dict(orient="records")
            insert_sql = text(f"INSERT INTO {staging_table} ({cols_str}) VALUES ({placeholders})")

            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                conn.execute(insert_sql, batch)
                logging.info(f"Inserted batch {i//batch_size + 1} with {len(batch)} rows into {staging_table}.")

            # 3️⃣ Merge into target
            merge_sql = f"""
            INSERT INTO {target_table} ({cols_str})
            SELECT {cols_str} FROM {staging_table}
            ON CONFLICT ({unique_key}) DO UPDATE SET {set_clause};
            """
            conn.execute(text(merge_sql))
            logging.info(f"Upserted {len(rows)} rows into {target_table}.")

    except Exception as e:
        logging.error(f"Upsert failed for {target_table}: {e}")
        raise