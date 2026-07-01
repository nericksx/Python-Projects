# src/duckdb_conn.py

"""
DuckDB connection and export helpers.

This module owns low-level DuckDB mechanics for the local pipeline target:
opening connections, writing pandas DataFrames to tables, and exporting tables
to Parquet.

Higher-level load orchestration lives in load.py.
"""

from pathlib import Path

import duckdb
import pandas as pd


def _quote_identifier(name: str) -> str:
    """
    Quote a DuckDB SQL identifier.

    Parameters
    ----------
    name : str
        Table or column identifier.

    Returns
    -------
    str
        Double-quoted SQL identifier.

    Raises
    ------
    ValueError
        If the identifier is empty.
    """
    if not name.strip():
        raise ValueError("SQL identifier cannot be empty")

    return '"' + name.replace('"', '""') + '"'


def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Open a DuckDB connection.

    Parameters
    ----------
    db_path : str
        Path to the DuckDB database file.

    Returns
    -------
    duckdb.DuckDBPyConnection
        Open DuckDB connection.
    """
    return duckdb.connect(db_path)


def write_df(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Replace a DuckDB table with the contents of a pandas DataFrame.

    This function currently uses CREATE OR REPLACE TABLE, so it has full-refresh
    semantics. Append/upsert behavior should be implemented explicitly as part
    of the append-load strategy.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    df : pd.DataFrame
        DataFrame to write.
    table_name : str
        Destination table name.

    Returns
    -------
    None
    """
    safe_table = _quote_identifier(table_name)

    conn.register("tmp_df", df)
    try:
        conn.execute(f"CREATE OR REPLACE TABLE {safe_table} AS SELECT * FROM tmp_df")
    finally:
        conn.unregister("tmp_df")


def export_table_to_parquet(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    out_path: str,
) -> None:
    """
    Export a DuckDB table to a Parquet file.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    table_name : str
        Source DuckDB table name.
    out_path : str
        Destination Parquet file path.

    Returns
    -------
    None
    """
    safe_table = _quote_identifier(table_name)

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    safe_path = str(out).replace("'", "''")

    conn.execute(
        f"COPY {safe_table} TO '{safe_path}' (FORMAT PARQUET, OVERWRITE TRUE)"
    )