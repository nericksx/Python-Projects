# src/query.py

import sys
import duckdb
import pandas as pd

DB_PATH = "src/db/pendo.duckdb"


def run_query(sql: str) -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH)
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
