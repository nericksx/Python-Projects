# src/run_debug_table.py
"""
Run and write one pipeline table without running the full Pendo pipeline.

This is a local/debug helper. Put this file next to src/main.py and run it
from the src directory so imports match the main pipeline:

    python run_debug_table.py --target mau --preview
    python run_debug_table.py --target mau --output-dir ../data/debug --preview
    python run_debug_table.py --target mau --output-file ../data/debug/mau_rolling_30d.parquet
    python run_debug_table.py --target mau --app-id 5511429746786304 --app-sub CAE --preview

You can also point it at any callable that returns a DataFrame, list[dict],
or dict[str, DataFrame/list[dict]]:

    python run_debug_table.py \
        --call pendo.pulls.mau:pull_mau \
        --needs-client \
        --output-file ../data/debug/mau_rolling_30d.parquet \
        --preview

If the callable returns a dict of tables, use --table-key to choose which one:

    python run_debug_table.py \
        --call some.module:some_function \
        --table-key my_table \
        --output-file ../data/debug/my_table.parquet
"""

from __future__ import annotations

import argparse
import importlib
import inspect
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from extract import make_client
from pendo.client import PendoClient
from pendo.config import BASE_URL
from pendo.partitions import PARTITIONS


@dataclass(frozen=True)
class DebugTarget:
    """Configuration for one debuggable pipeline output."""

    callable_path: str
    output_filename: str
    description: str
    needs_client: bool = False
    default_kwargs: dict[str, Any] = field(default_factory=dict)
    key_columns: tuple[str, ...] = ()


DEBUG_TARGETS: dict[str, DebugTarget] = {
    "mau": DebugTarget(
        callable_path="pendo.pulls.mau:pull_mau",
        output_filename="pendo_app_mau_rolling_30d.parquet",
        description="Pull only Pendo rolling-30-day app-level MAU.",
        needs_client=True,
        default_kwargs={"days": 30},
        key_columns=(
            "app_id",
            "app_sub",
            "period_start",
            "period_end",
            "mau_grain",
        ),
    ),
    # Add more one-table debug targets here as the pipeline grows, e.g.:
    # "guide_sessions": DebugTarget(
    #     callable_path="some.module:build_guide_sessions",
    #     output_filename="guide_sessions.parquet",
    #     description="Build only guide_sessions.",
    #     needs_client=False,
    #     key_columns=("guide_id", "visitor_id", "session_id"),
    # ),
}


def parse_args() -> argparse.Namespace:
    """Parse debug-runner command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run and write one pipeline table for local debugging."
    )
    parser.add_argument(
        "--target",
        default="mau",
        help="Named debug target to run. Defaults to mau.",
    )
    parser.add_argument(
        "--list-targets",
        action="store_true",
        help="List configured debug targets and exit.",
    )
    parser.add_argument(
        "--call",
        dest="callable_path",
        help=(
            "Override --target with a specific callable in module:function form. "
            "The callable must return a DataFrame, list[dict], or dict of tables."
        ),
    )
    parser.add_argument(
        "--needs-client",
        action="store_true",
        help="Pass make_client() as the first argument to the callable.",
    )
    parser.add_argument(
        "--table-key",
        help="When the callable returns a dict of tables, write this table key.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/debug",
        help="Directory for parquet output when --output-file is not supplied.",
    )
    parser.add_argument(
        "--output-file",
        help="Full parquet output path. Overrides --output-dir and target filename.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Optional rolling-day count, passed only if the callable accepts days.",
    )
    parser.add_argument(
        "--months-back",
        type=int,
        default=None,
        help=(
            "Compatibility option for older pull functions. Passed only if the "
            "callable accepts months_back."
        ),
    )
    parser.add_argument(
        "--app-id",
        default=None,
        help="Optional Pendo app ID to pass to callables that accept app_id.",
    )
    parser.add_argument(
        "--app-ids",
        default=None,
        help=(
            "Optional comma-separated Pendo app IDs to pass to callables that "
            "accept app_ids. Useful for pulling a small validated app set."
        ),
    )
    parser.add_argument(
        "--app-sub",
        default=None,
        help=(
            "Optional Pendo app subscription/partition key (for example CAE or SAE). "
            "Passed to make_client() when that factory supports a matching parameter."
        ),
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Print dataframe preview, shape, columns, and duplicate-key check.",
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=10,
        help="Number of rows to print with --preview. Defaults to 10.",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Run and preview, but do not write parquet.",
    )

    args, _ = parser.parse_known_args()
    return args


def list_targets() -> None:
    """Print available named debug targets."""
    print("Configured debug targets:")
    for name, target in sorted(DEBUG_TARGETS.items()):
        print(f"  {name}: {target.description}")
        print(f"      callable: {target.callable_path}")
        print(f"      output:   {target.output_filename}")


def import_callable(callable_path: str) -> Callable[..., Any]:
    """Import a callable from a module:function string."""
    if ":" not in callable_path:
        raise ValueError(
            f"callable path must use module:function format, got {callable_path!r}"
        )

    module_name, function_name = callable_path.split(":", 1)
    module = importlib.import_module(module_name)
    func = getattr(module, function_name)

    if not callable(func):
        raise TypeError(f"{callable_path!r} did not resolve to a callable")

    return func


def supported_kwargs(
    func: Callable[..., Any],
    requested_kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Return only kwargs accepted by func unless func accepts **kwargs."""
    signature = inspect.signature(func)
    parameters = signature.parameters

    accepts_kwargs = any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in parameters.values()
    )
    if accepts_kwargs:
        return {k: v for k, v in requested_kwargs.items() if v is not None}

    return {
        key: value
        for key, value in requested_kwargs.items()
        if value is not None and key in parameters
    }


def make_debug_client(app_sub: str | None = None) -> Any:
    """Create a Pendo client for the requested debug partition.

    make_client() is intentionally the pipeline's base/default client, and in
    this project it currently resolves to SAE. Partition-specific debug runs
    should use pendo.partitions.PARTITIONS, which is the same configuration the
    partition-aware pulls use.
    """
    if app_sub is None:
        print("[debug] creating default client via extract.make_client()")
        return make_client()

    requested = app_sub.strip().upper()
    matching_partitions = [
        partition
        for partition in PARTITIONS
        if str(getattr(partition, "name", "")).strip().upper() == requested
    ]

    if not matching_partitions:
        available = ", ".join(
            str(getattr(partition, "name", "<unnamed>")) for partition in PARTITIONS
        )
        raise ValueError(
            f"No partition named {app_sub!r} found in pendo.partitions.PARTITIONS. "
            f"Available partitions: {available}"
        )

    partition = matching_partitions[0]

    # Be deliberately tolerant of Partition field names so this debug runner
    # does not need to change every time the config dataclass is tidied.
    host = (
        getattr(partition, "host", None)
        or getattr(partition, "base_url", None)
        or BASE_URL
    )
    key = getattr(partition, "api_key", None) or getattr(partition, "key", None)
    timeout_sec = getattr(partition, "timeout_sec", 90)

    if not key:
        raise ValueError(
            f"Partition {app_sub!r} exists but has no api_key/key value. "
            "Check pendo.config environment variables for this partition."
        )

    print(f"[debug] creating partition client app_sub={requested!r}")
    return PendoClient(host=host, key=key, timeout_sec=timeout_sec)


def run_callable(
    func: Callable[..., Any],
    *,
    needs_client: bool,
    kwargs: dict[str, Any],
    app_sub: str | None = None,
) -> Any:
    """Run the target callable with an optional Pendo client."""
    if needs_client:
        client = make_debug_client(app_sub=app_sub)
        return func(client, **kwargs)

    return func(**kwargs)


def table_to_dataframe(result: Any, table_key: str | None = None) -> pd.DataFrame:
    """Normalize a callable result to one pandas DataFrame."""
    if isinstance(result, pd.DataFrame):
        return result.copy()

    if isinstance(result, list):
        return pd.DataFrame(result)

    if isinstance(result, dict):
        if table_key is None:
            if len(result) == 1:
                table_key = next(iter(result))
            else:
                keys = ", ".join(sorted(str(key) for key in result.keys()))
                raise ValueError(
                    "Callable returned multiple tables. Re-run with --table-key. "
                    f"Available keys: {keys}"
                )

        if table_key not in result:
            keys = ", ".join(sorted(str(key) for key in result.keys()))
            raise KeyError(f"table_key {table_key!r} not found. Available keys: {keys}")

        return table_to_dataframe(result[table_key])

    raise TypeError(
        "Callable must return a pandas DataFrame, list[dict], or dict of tables. "
        f"Got {type(result).__name__}."
    )


def output_path_for(args: argparse.Namespace, target: DebugTarget | None) -> Path:
    """Resolve parquet output path."""
    if args.output_file:
        return Path(args.output_file)

    filename = target.output_filename if target else "debug_table.parquet"
    return Path(args.output_dir) / filename


def preview_dataframe(
    df: pd.DataFrame,
    *,
    preview_rows: int,
    key_columns: tuple[str, ...] = (),
) -> None:
    """Print dataframe preview and optional duplicate-key validation."""
    print("\n=== debug output preview ===")
    if df.empty:
        print("DataFrame is empty.")
    else:
        print(df.head(preview_rows).to_string(index=False))

    print(f"\nRows: {len(df)} | Cols: {len(df.columns)}")
    print("Columns:")
    for col in df.columns:
        print(f"  - {col}")

    available_keys = [col for col in key_columns if col in df.columns]
    missing_keys = [col for col in key_columns if col not in df.columns]

    if missing_keys:
        print(f"\n[debug] duplicate check skipped missing key columns: {missing_keys}")

    if available_keys and len(available_keys) == len(key_columns):
        duplicates = (
            df.groupby(available_keys, dropna=False)
            .size()
            .reset_index(name="row_count")
            .query("row_count > 1")
        )
        if duplicates.empty:
            print(f"\n[debug] duplicate check passed for keys: {available_keys}")
        else:
            print(f"\n[debug] duplicate check FAILED for keys: {available_keys}")
            print(duplicates.head(preview_rows).to_string(index=False))


def main() -> int:
    """Run a single debug target and optionally write it to parquet."""
    args = parse_args()

    if args.list_targets:
        list_targets()
        return 0

    target = DEBUG_TARGETS.get(args.target)
    if args.callable_path:
        callable_path = args.callable_path
        needs_client = args.needs_client
        output_target = None
        default_kwargs: dict[str, Any] = {}
        key_columns: tuple[str, ...] = ()
        print(f"[debug] running custom callable={callable_path}")
    else:
        if target is None:
            valid_targets = ", ".join(sorted(DEBUG_TARGETS))
            raise KeyError(
                f"Unknown target {args.target!r}. Valid targets: {valid_targets}. "
                "Use --call module:function for a custom target."
            )
        callable_path = target.callable_path
        needs_client = target.needs_client
        output_target = target
        default_kwargs = dict(target.default_kwargs)
        key_columns = target.key_columns
        print(f"[debug] running target={args.target} callable={callable_path}")

    func = import_callable(callable_path)

    parsed_app_ids = None
    if args.app_ids:
        parsed_app_ids = [part.strip() for part in args.app_ids.split(",") if part.strip()]

    requested_kwargs = {
        **default_kwargs,
        "days": args.days,
        "months_back": args.months_back,
        "app_id": args.app_id,
        "app_ids": parsed_app_ids,
        "app_sub": args.app_sub,
    }
    kwargs = supported_kwargs(func, requested_kwargs)
    if kwargs:
        print(f"[debug] kwargs={kwargs}")

    result = run_callable(
        func,
        needs_client=needs_client,
        kwargs=kwargs,
        app_sub=args.app_sub,
    )
    df = table_to_dataframe(result, table_key=args.table_key)

    if args.preview:
        preview_dataframe(
            df,
            preview_rows=args.preview_rows,
            key_columns=key_columns,
        )

    output_path = output_path_for(args, output_target)

    if args.no_write:
        print("\n[debug] --no-write set; parquet was not written")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"\n[debug] wrote {len(df)} rows to {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
