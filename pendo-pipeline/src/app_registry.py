# src/app_registry.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from paths import APP_REGISTRY_PARQUET


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


TRUE_VALUES = {"true", "t", "yes", "y", "1"}
FALSE_VALUES = {"false", "f", "no", "n", "0"}


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    clean = str(value).strip().lower()

    if clean in TRUE_VALUES:
        return True

    if clean in FALSE_VALUES:
        return False

    raise ValueError(f"Invalid include_in_xm_reporting value: {value!r}")


def load_app_registry(
    registry_path: Path | str = APP_REGISTRY_PARQUET,
) -> pd.DataFrame:
    """
    Load the validated XM Pendo app registry.

    The registry is the pipeline source of truth for:
    - app inclusion
    - app subscription routing
    - dashboard/reporting app names
    - portfolio assignment
    - Pendo app IDs
    """
    registry_path = Path(registry_path)

    if not registry_path.exists():
        raise FileNotFoundError(
            f"App registry parquet does not exist: {registry_path}. "
            "Run scripts/build_app_registry.py first."
        )

    df = pd.read_parquet(registry_path)

    missing = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"Registry missing required columns: {missing}")

    df = df.copy()

    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype).startswith("string"):
            df[col] = df[col].astype(str).str.strip()

    df["include_in_xm_reporting"] = df["include_in_xm_reporting"].apply(_parse_bool)
    df["app_sub"] = df["app_sub"].astype(str).str.strip().str.upper()
    df["pendo_app_id"] = df["pendo_app_id"].astype(str).str.strip()

    return df


def included_xm_apps(
    registry_path: Path | str = APP_REGISTRY_PARQUET,
) -> pd.DataFrame:
    """
    Return registry rows that should participate in current XM dashboard reporting.
    """
    df = load_app_registry(registry_path)

    included = df[
        (df["include_in_xm_reporting"])
        & (df["pendo_app_id"] != "")
    ].copy()

    return included.reset_index(drop=True)