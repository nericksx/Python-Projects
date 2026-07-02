# scripts/build_app_registry.py

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from paths import APP_REGISTRY_CSV, APP_REGISTRY_PARQUET  # noqa: E402


EXPECTED_COLUMNS = [
    "include_in_xm_reporting",
    "platform",
    "app_sub",
    "portfolio",
    "common_name",
    "dashboard_app_name",
    "pendo_app_name",
    "app_contact",
    "install_status",
    "jedi_dashboard_status",
    "last_validation_date",
    "pendo_app_id",
    "notes",
]

REQUIRED_COLUMNS = {
    "include_in_xm_reporting",
    "platform",
    "app_sub",
    "portfolio",
    "common_name",
    "dashboard_app_name",
    "pendo_app_name",
    "install_status",
    "pendo_app_id",
}

VALID_APP_SUBS = {"SAE", "CAE"}

TRUE_VALUES = {"true", "t", "yes", "y", "1"}
FALSE_VALUES = {"false", "f", "no", "n", "0"}


def normalize_column_name(value: str) -> str:
    value = str(value).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def parse_bool(value: str) -> bool:
    clean = str(value).strip().lower()

    if clean in TRUE_VALUES:
        return True

    if clean in FALSE_VALUES:
        return False

    raise ValueError(f"Invalid include_in_xm_reporting value: {value!r}")


def fail(errors: list[str]) -> None:
    print("\nRegistry validation failed:\n", file=sys.stderr)

    for error in errors:
        print(f"- {error}", file=sys.stderr)

    print("", file=sys.stderr)
    sys.exit(1)


def build_registry(input_path: Path, output_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        fail([f"Input CSV does not exist: {input_path}"])

    df = pd.read_csv(
        input_path,
        dtype=str,
        keep_default_na=False,
    )

    original_columns = list(df.columns)
    normalized_columns = [normalize_column_name(col) for col in df.columns]
    df.columns = normalized_columns

    errors: list[str] = []

    duplicate_columns = sorted(
        {
            col
            for col in df.columns
            if list(df.columns).count(col) > 1
        }
    )

    if duplicate_columns:
        errors.append(f"Duplicate normalized column names: {duplicate_columns}")
        errors.append(f"Original CSV columns were: {original_columns}")

    missing_columns = sorted(REQUIRED_COLUMNS - set(df.columns))

    if missing_columns:
        errors.append(f"Missing required columns: {missing_columns}")

    if errors:
        fail(errors)

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    df = df.loc[~(df == "").all(axis=1)].copy()

    df["app_sub"] = df["app_sub"].str.upper()
    df["pendo_app_id"] = df["pendo_app_id"].astype(str).str.strip()

    try:
        df["include_in_xm_reporting"] = df["include_in_xm_reporting"].apply(parse_bool)
    except ValueError as exc:
        errors.append(str(exc))

    bad_app_subs = sorted(
        set(df.loc[~df["app_sub"].isin(VALID_APP_SUBS), "app_sub"])
    )

    if bad_app_subs:
        errors.append(
            f"Invalid app_sub values. Expected SAE or CAE. Found: {bad_app_subs}"
        )

    has_id = df["pendo_app_id"] != ""
    bad_ids = df.loc[
        has_id & ~df["pendo_app_id"].str.match(r"^-?\d+$"),
        ["dashboard_app_name", "app_sub", "pendo_app_id"],
    ]

    if not bad_ids.empty:
        errors.append(
            "Malformed pendo_app_id values:\n"
            + bad_ids.to_string(index=False)
        )

    included = df["include_in_xm_reporting"] == True

    required_for_included = [
        "platform",
        "app_sub",
        "portfolio",
        "common_name",
        "dashboard_app_name",
        "pendo_app_name",
        "install_status",
        "pendo_app_id",
    ]

    for col in required_for_included:
        missing = df.loc[
            included & (df[col] == ""),
            ["dashboard_app_name", "common_name", "app_sub", "pendo_app_id"],
        ]

        if not missing.empty:
            errors.append(
                f"Included rows missing {col}:\n"
                + missing.to_string(index=False)
            )

    populated_ids = df[df["pendo_app_id"] != ""].copy()
    duplicate_id_sub = populated_ids[
        populated_ids.duplicated(
            subset=["pendo_app_id", "app_sub"],
            keep=False,
        )
    ]

    if not duplicate_id_sub.empty:
        errors.append(
            "Duplicate pendo_app_id + app_sub pairs:\n"
            + duplicate_id_sub[
                [
                    "dashboard_app_name",
                    "pendo_app_name",
                    "app_sub",
                    "pendo_app_id",
                    "include_in_xm_reporting",
                ]
            ].to_string(index=False)
        )

    whitespace_issues = []

    for col in df.columns:
        as_text = df[col].astype(str)
        if not as_text.equals(as_text.str.strip()):
            whitespace_issues.append(col)

    if whitespace_issues:
        errors.append(
            f"Columns still have leading/trailing whitespace: {whitespace_issues}"
        )

    if errors:
        fail(errors)

    ordered_columns = [col for col in EXPECTED_COLUMNS if col in df.columns]
    extra_columns = [col for col in df.columns if col not in ordered_columns]
    df = df[ordered_columns + extra_columns]

    df = df.sort_values(
        by=[
            "include_in_xm_reporting",
            "portfolio",
            "dashboard_app_name",
            "platform",
            "app_sub",
        ],
        ascending=[False, True, True, True, True],
    ).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build validated XM Pendo app registry parquet from CSV."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=APP_REGISTRY_CSV,
        help="Source CSV path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=APP_REGISTRY_PARQUET,
        help="Output parquet path.",
    )

    args = parser.parse_args()

    df = build_registry(args.input, args.output)
    included = df[df["include_in_xm_reporting"]]

    print(f"Built registry: {args.output}")
    print(f"Rows: {len(df)}")
    print(f"Included rows: {len(included)}")

    print("\nIncluded apps by app_sub:")
    print(included.groupby("app_sub").size().to_string())

    print("\nIncluded apps by portfolio:")
    print(included.groupby("portfolio").size().to_string())

    print("\nIncluded apps by platform:")
    print(included.groupby("platform").size().to_string())


if __name__ == "__main__":
    main()