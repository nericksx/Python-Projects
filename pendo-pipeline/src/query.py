# src/query.py

import sys
from pathlib import Path

import duckdb
import pandas as pd

from paths import DUCKDB_PATH


def run_query(sql: str, db_path: Path | str = DUCKDB_PATH) -> pd.DataFrame:
    db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(
            f"DuckDB database does not exist: {db_path}. "
            "Run the pipeline first to create it."
        )

    conn = duckdb.connect(str(db_path))
    try:
        return conn.execute(sql).fetchdf()
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            sql = f.read()
    else:
        sql = """
        SELECT
            createdByJobRole,
            COUNT(*) AS n
        FROM guides_raw
        GROUP BY 1
        ORDER BY n DESC
        """

    df = run_query(sql)
    print(df.to_string(index=False))
    print(f"\nRows returned: {len(df)}")