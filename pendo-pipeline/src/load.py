# src/load.py

"""
Load transformed pipeline tables to DuckDB and Parquet.

This module owns local persistence for transformed tables. It writes all tables
to DuckDB and exports selected reporting/debug tables to Parquet for downstream
use.
"""

from pathlib import Path

import pandas as pd

from paths import DB_DIR, DUCKDB_PATH

from duckdb_conn import get_connection, write_df, export_table_to_parquet


PARQUET_EXPORTS = [
    "ux_lite_registry",
    "guide_sessions",
    "guide_session_population_rows",
    "ux_lite_local_events",
    "ux_lite_local_responses",
    "ux_lite_local_comments",
    "pendo_app_mau_rolling_30d",
    "guides_raw",
]


def load_all(
    tables: dict[str, pd.DataFrame],
    *,
    db_path: Path | str = DUCKDB_PATH,
) -> None:
    """
    Write transformed tables to DuckDB and export selected tables to Parquet.

    Parameters
    ----------
    tables : dict[str, pd.DataFrame]
        Transformed output tables keyed by table name.
    db_path : Path | str = DUCKDB_PATH
        Local DuckDB database path.

    Returns
    -------
    None

    Raises
    ------
    TypeError
        If any table value is not a pandas DataFrame.
    """
    db_path = Path(db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection(str(db_path))

    try:
        for table_name, df in tables.items():
            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    f"Expected DataFrame for table {table_name}, "
                    f"got {type(df).__name__}"
                )

            write_df(conn, df, table_name)
            print(f"Wrote table: {table_name} ({len(df)} rows)")

        for table_name in PARQUET_EXPORTS:
            if table_name in tables:
                parquet_path = DB_DIR / f"{table_name}.parquet"
                export_table_to_parquet(
                    conn,
                    table_name,
                    str(parquet_path),
                )
                print(f"Exported Parquet: {parquet_path}")

    finally:
        conn.close()