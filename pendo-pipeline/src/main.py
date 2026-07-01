# src/main.py

"""
Command-line entry point for the Pendo pipeline.

This module orchestrates the pipeline run:
1. parse command-line options
2. extract raw Pendo data
3. transform raw outputs into analytics tables
4. load/export transformed tables
5. optionally print local debug previews
"""

import argparse

import pandas as pd

from extract import make_client, extract_all_two_phase
from load import load_all
from transform.transform import transform_all


PREVIEW_TABLES = [
    "guides_raw",
    "guide_sessions",
    "ux_lite_local_events",
    "ux_lite_local_responses",
]


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    parse_known_args() is used so notebook/IPython-injected arguments do not
    break local pipeline runs.
    """
    parser = argparse.ArgumentParser(description="Run the Pendo pipeline.")
    parser.add_argument(
        "--mode",
        choices=["incremental", "full-refresh"],
        default="incremental",
        help="Run incremental pull by default, or full-refresh to rebuild history.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print local transform/load preview output.",
    )

    args, _ = parser.parse_known_args()
    return args


def print_table_previews(tables: dict[str, object], table_names: list[str]) -> None:
    """
    Print small dataframe previews for local smoke testing.

    Missing or non-dataframe tables are skipped so preview output does not fail
    an otherwise successful pipeline run.
    """
    for name in table_names:
        table = tables.get(name)

        if table is None:
            print(f"[main] preview skipped missing table: {name}")
            continue

        if not isinstance(table, pd.DataFrame):
            print(
                f"[main] preview skipped non-DataFrame table: "
                f"{name} ({type(table).__name__})"
            )
            continue

        print(f"\n=== {name} ===")
        print(table.head(10).to_string(index=False))
        print(f"Rows: {len(table)} | Cols: {len(table.columns)}")


def main() -> int:
    """
    Run the Pendo pipeline.

    Returns
    -------
    int
        Process exit code. Zero indicates success.
    """
    args = parse_args()

    print(f"[main] running mode={args.mode}")

    client = make_client()

    # TODO: pass mode/config into extraction once extract.py supports
    # incremental vs full-refresh behavior.
    raw = extract_all_two_phase(client)

    tables = transform_all(raw, debug=args.debug)
    load_all(tables)

    if args.debug:
        print_table_previews(tables, PREVIEW_TABLES)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())