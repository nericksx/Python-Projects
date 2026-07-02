# src/pendo/pulls/registry_mau.py

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from app_registry import included_xm_apps
from .mau import DEFAULT_ROLLING_DAYS, pull_mau
from .partitioned import make_client
from ..partitions import PARTITIONS, Partition


REGISTRY_METADATA_COLUMNS = [
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
    "notes",
]


def _clients_by_app_sub(
    partitions: Iterable[Partition] = PARTITIONS,
) -> dict[str, Any]:
    """
    Build one Pendo client per configured app subscription.
    """
    return {
        partition.name.upper(): make_client(partition)
        for partition in partitions
    }


def _clean_value(value: Any) -> Any:
    """
    Convert blank/NaN-ish registry metadata to None for cleaner downstream rows.
    """
    if value is None:
        return None

    if pd.isna(value):
        return None

    clean = str(value).strip()
    return clean if clean != "" else None


def pull_registry_mau(
    *,
    registry_path: Path | str | None = None,
    days: int = DEFAULT_ROLLING_DAYS,
    partitions: Iterable[Partition] = PARTITIONS,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Pull rolling 30-day app-level MAU for included XM registry apps.

    Uses the app registry to decide:
    - which apps are included
    - which app_sub/client to use
    - which Pendo app ID to query
    - which dashboard/reporting metadata to stamp onto rows

    One bad app should not fail the whole pull. If an app fails, return an
    error row with mau_pull_status='error' and mau_pull_error populated.
    """
    if registry_path is None:
        apps = included_xm_apps()
    else:
        apps = included_xm_apps(registry_path)

    if limit is not None:
        apps = apps.head(limit).copy()

    if apps.empty:
        print("[registry_mau] no included registry apps found")
        return []

    clients = _clients_by_app_sub(partitions)

    missing_subs = sorted(set(apps["app_sub"]) - set(clients))
    if missing_subs:
        raise ValueError(
            f"Registry references app_sub values with no configured client: {missing_subs}"
        )

    all_rows: list[dict[str, Any]] = []

    for app_sub, group in apps.groupby("app_sub", sort=True):
        group = group.copy()

        print(
            f"[registry_mau] pulling {len(group)} app(s) "
            f"for app_sub={app_sub}: "
            f"{', '.join(group['dashboard_app_name'].astype(str).head(5))}"
            f"{'...' if len(group) > 5 else ''}"
        )

        client = clients[app_sub]

        for _, registry_row in group.iterrows():
            pendo_app_id = str(registry_row["pendo_app_id"]).strip()
            dashboard_app_name = str(registry_row["dashboard_app_name"]).strip()

            print(
                f"[registry_mau] pulling MAU "
                f"app_sub={app_sub} "
                f"dashboard_app={dashboard_app_name} "
                f"pendo_app_id={pendo_app_id}"
            )

            try:
                mau_rows = pull_mau(
                    client,
                    app_ids=[pendo_app_id],
                    app_sub=app_sub,
                    days=days,
                )

                if not mau_rows:
                    mau_rows = [
                        {
                            "app_id": pendo_app_id,
                            "app_sub": app_sub,
                            "pendo_app_id": pendo_app_id,
                            "mau": None,
                            "mau_pull_status": "empty",
                            "mau_pull_error": None,
                        }
                    ]

                for row in mau_rows:
                    row["mau_pull_status"] = "success"
                    row["mau_pull_error"] = None
                    row["pendo_app_id"] = pendo_app_id
                    row["app_id"] = pendo_app_id

                    for col in REGISTRY_METADATA_COLUMNS:
                        if col in registry_row:
                            row[col] = _clean_value(registry_row[col])

                    row["reporting_app"] = (
                        _clean_value(registry_row.get("dashboard_app_name"))
                        or _clean_value(registry_row.get("common_name"))
                    )

                    all_rows.append(row)

            except Exception as exc:
                error_row = {
                    "app_id": pendo_app_id,
                    "pendo_app_id": pendo_app_id,
                    "app_sub": app_sub,
                    "mau": None,
                    "mau_definition": "unique visitors who sent event data",
                    "mau_window": "rolling_30_day",
                    "mau_grain": "app_level",
                    "billing_basis": True,
                    "source": "Pendo Aggregation API events",
                    "window_days": days,
                    "includes_current_day": True,
                    "mau_pull_status": "error",
                    "mau_pull_error": str(exc),
                }

                for col in REGISTRY_METADATA_COLUMNS:
                    if col in registry_row:
                        error_row[col] = _clean_value(registry_row[col])

                error_row["reporting_app"] = (
                    _clean_value(registry_row.get("dashboard_app_name"))
                    or _clean_value(registry_row.get("common_name"))
                )

                print(
                    f"[registry_mau] ERROR "
                    f"app_sub={app_sub} "
                    f"dashboard_app={dashboard_app_name} "
                    f"pendo_app_id={pendo_app_id}: {exc}"
                )

                all_rows.append(error_row)

    return all_rows