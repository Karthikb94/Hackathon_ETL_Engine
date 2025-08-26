import os
import polars as pl
from .exceptions import ETLError

def read_parquet_file(file_path: str) -> pl.DataFrame:
    if not os.path.exists(file_path):
        raise ETLError(f"Input parquet file not found: {file_path}")
    try:
        df = pl.read_parquet(file_path)
        return df
    except Exception as e:
        raise ETLError(f"Failed to read parquet: {e}") from e
