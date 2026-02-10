import json
import pandas as pd
from typing import Iterable, Optional

def serialize_json_columns(df: pd.DataFrame, json_cols: Iterable[str]) -> pd.DataFrame:
    for col in json_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

def prepare_load_df(
    obj,
    json_cols: Optional[Iterable[str]] = None,
    date_cols: Optional[Iterable[str]] = None,
    enforce_cols: Optional[Iterable[str]] = None,
    int_cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Normalize input into a pandas DataFrame ready for load:
    - obj may be a DataFrame, a dict (single row) or list-of-dicts.
    - serializes json_cols to JSON strings (for JSONB).
    - parses date_cols with pandas.to_datetime.
    - ensures enforce_cols exist (fills with None if missing).
    """
    # Normalize input
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
    elif isinstance(obj, dict):
        df = pd.DataFrame([obj])
    else:
        # assume list-of-dicts
        df = pd.DataFrame(obj)

    # Ensure required columns exist
    if enforce_cols:
        for c in enforce_cols:
            if c not in df.columns:
                df[c] = None

    # Serialize JSON
    if json_cols:
        df = serialize_json_columns(df, json_cols)

    # Parse dates
    if date_cols:
        for c in date_cols:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors='coerce')

    # Enforce integer columns (nullable-safe)
    if int_cols:
        for c in int_cols:
            if c in df.columns:
                df[c] = (
                    pd.to_numeric(df[c], errors="coerce")
                    .astype("Int64")   # pandas nullable int
                )

    # NaN -> None for psycopg2
    df = df.where(pd.notnull(df), None)

    return df