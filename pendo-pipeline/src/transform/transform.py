# src/transform/transform.py

"""
Top-level transform orchestration for the Pendo pipeline.

This module converts raw extracted Pendo outputs into analytics-ready tables.
Endpoint pulls preserve raw source data and app_sub lineage; transform modules
apply UX-Lite business rules, derive sessions/responses, and prepare output
tables for loading/reporting.
"""

from typing import Any

import pandas as pd

from pendo.pulls.guides import guides_to_df
from pendo.pulls.visitor_population import build_population_rows_for_sessions
from app_registry import included_xm_apps, load_app_registry
from transform.ux_lite import (
    attach_population_to_sessions,
    build_comment_events,
    build_guide_sessions,
    build_poll_lookup,
    build_score_events,
    build_ux_lite_registry,
    build_ux_lite_responses,
)

MAU_TABLE = "pendo_app_mau_rolling_30d"
APP_REGISTRY_TABLE = "xm_pendo_app_registry"

def _rows_to_df(value: Any) -> pd.DataFrame:
    """
    Normalize common raw pull output shapes into a dataframe.

    Pull outputs may be:
    - {"results": [...]} from a raw Pendo aggregation response
    - a list of row dictionaries after pull normalization/partition stamping
    - an unsupported/empty shape, which becomes an empty dataframe
    """
    if isinstance(value, dict):
        rows = value.get("results", [])
        return pd.json_normalize(rows) if isinstance(rows, list) else pd.DataFrame()

    if isinstance(value, list):
        return pd.json_normalize(value)

    return pd.DataFrame()


def _require_raw_keys(raw: dict[str, object], keys: list[str]) -> None:
    """
    Validate that expected raw extract outputs are present.

    Raises
    ------
    KeyError
        If any required raw output key is missing.
    """
    missing = [key for key in keys if key not in raw]
    if missing:
        raise KeyError(f"transform_all missing required raw outputs: {missing}")


def _attach_guide_names_to_comments(
    comment_events: pd.DataFrame,
    registry: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach guideName to comment events using guideId + app_sub lineage.

    Comment events already carry appId from the poll event row. This merge only
    attaches guideName so it does not create appId_x/appId_y suffixes or cross
    subscription metadata bleed.
    """
    if comment_events.empty:
        return comment_events

    required_comment_cols = {"guideId", "app_sub"}
    required_registry_cols = {"guideId", "app_sub", "guideName"}

    if not required_comment_cols.issubset(comment_events.columns):
        raise ValueError(
            "comment_events missing required columns for guide-name merge: "
            f"{sorted(required_comment_cols - set(comment_events.columns))}"
        )

    if registry.empty or not required_registry_cols.issubset(registry.columns):
        comment_events = comment_events.copy()
        comment_events["guideName"] = pd.NA
        return comment_events

    guide_names = (
        registry[["guideId", "app_sub", "guideName"]]
        .copy()
        .assign(
            guideId=lambda df: df["guideId"].astype(str),
            app_sub=lambda df: df["app_sub"].astype(str).str.strip(),
        )
        .drop_duplicates(["guideId", "app_sub"])
    )

    comments = comment_events.copy()
    comments["guideId"] = comments["guideId"].astype(str)
    comments["app_sub"] = comments["app_sub"].astype(str).str.strip()

    return comments.merge(
        guide_names,
        on=["guideId", "app_sub"],
        how="left",
    )


def _print_debug_summary(
    *,
    registry: pd.DataFrame,
    guide_sessions: pd.DataFrame,
    pop_df: pd.DataFrame,
) -> None:
    """Print a compact transform summary for local smoke testing."""
    print("registry rows:", len(registry))
    print("guide sessions rows:", len(guide_sessions))

    session_preview_cols = [
        "guideSessionId",
        "guideName",
        "appId",
        "app_sub",
        "reportingStart",
        "reportingEnd",
        "responseCount",
        "population",
    ]
    existing_session_cols = [
        col for col in session_preview_cols if col in guide_sessions.columns
    ]

    if existing_session_cols and not guide_sessions.empty:
        print(
            guide_sessions[existing_session_cols]
            .head(20)
            .to_string(index=False)
        )

    if "guideName" in guide_sessions.columns:
        orderup = guide_sessions.loc[
            guide_sessions["guideName"].eq("OU: UX-Lite (app)")
        ].copy()

        if not orderup.empty:
            print("\n=== OrderUp sessions: ===")
            orderup_cols = [
                "guideId",
                "guideSessionId",
                "reportingStart",
                "reportingEnd",
                "responseCount",
            ]
            existing_orderup_cols = [
                col for col in orderup_cols if col in orderup.columns
            ]
            print(orderup[existing_orderup_cols].sort_values("reportingStart"))

    pop_preview_cols = [
        "guideSessionId",
        "visitorId",
        "appId",
        "eventCount",
        "firstTime",
        "lastTime",
    ]
    existing_pop_cols = [col for col in pop_preview_cols if col in pop_df.columns]

    if existing_pop_cols and not pop_df.empty:
        print(pop_df[existing_pop_cols].head(20).to_string(index=False))


def _filter_to_included_registry_apps(
    df: pd.DataFrame,
    app_registry: pd.DataFrame,
    *,
    app_col: str = "appId",
) -> pd.DataFrame:
    """
    Filter a dataframe to apps included in the XM Pendo app registry.

    Matching uses Pendo app ID + app_sub so default app IDs like -323232
    remain subscription-specific.
    """
    if df.empty:
        return df

    required_df_cols = {app_col, "app_sub"}
    missing_df_cols = required_df_cols - set(df.columns)

    if missing_df_cols:
        raise ValueError(
            "Cannot filter dataframe to XM registry; missing columns: "
            f"{sorted(missing_df_cols)}"
        )

    required_registry_cols = {
        "include_in_xm_reporting",
        "pendo_app_id",
        "app_sub",
    }
    missing_registry_cols = required_registry_cols - set(app_registry.columns)

    if missing_registry_cols:
        raise ValueError(
            "Cannot filter to XM registry; registry missing columns: "
            f"{sorted(missing_registry_cols)}"
        )

    included_keys = (
        app_registry.loc[
            app_registry["include_in_xm_reporting"].astype(bool),
            ["pendo_app_id", "app_sub"],
        ]
        .copy()
        .assign(
            pendo_app_id=lambda x: x["pendo_app_id"].astype(str).str.strip(),
            app_sub=lambda x: x["app_sub"].astype(str).str.strip().str.upper(),
        )
        .rename(columns={"pendo_app_id": app_col})
        .drop_duplicates([app_col, "app_sub"])
    )

    working = df.copy()
    working[app_col] = working[app_col].astype(str).str.strip()
    working["app_sub"] = working["app_sub"].astype(str).str.strip().str.upper()

    return working.merge(
        included_keys,
        on=[app_col, "app_sub"],
        how="inner",
    )


def transform_all(raw: dict[str, object], *, debug: bool = False) -> dict[str, pd.DataFrame]:
    """
    Transform raw Pendo pipeline outputs into loadable analytics tables.

    Parameters
    ----------
    raw : dict[str, object]
        Raw extract outputs keyed by pull name.
    debug : bool, default False
        When True, print a compact local smoke-test summary.

    Returns
    -------
    dict[str, object]
        Analytics-ready tables keyed by output table name.
    """
    _require_raw_keys(
        raw,
        [
            "guides",
            "aggregations",
            "ux_lite_poll_events",
            MAU_TABLE,
        ],
    )

    tables: dict[str, pd.DataFrame] = {}

    app_registry = load_app_registry()
    tables[APP_REGISTRY_TABLE] = app_registry

    guides = raw["guides"]
    if not isinstance(guides, list):
        raise TypeError(f'Expected raw["guides"] to be a list, got {type(guides).__name__}')

    tables["guides_raw"] = guides_to_df(guides)

    # GuideSeen rollup acts as the observed-activity truth set for registry
    # eligibility. Accept both raw {"results": [...]} and normalized row-list
    # shapes so this transform works regardless of extraction normalization.
    guide_seen = _rows_to_df(raw["aggregations"])

    registry = build_ux_lite_registry(guides, guide_seen)

    registry = _filter_to_included_registry_apps(
        registry,
        app_registry,
        app_col="appId",
    )

    poll_lookup = build_poll_lookup(registry)

    poll_events = raw["ux_lite_poll_events"]
    if not isinstance(poll_events, list):
        raise TypeError(
            'Expected raw["ux_lite_poll_events"] to be a list, '
            f"got {type(poll_events).__name__}"
        )

    score_events = build_score_events(poll_events, poll_lookup)
    comment_events = build_comment_events(poll_events, poll_lookup)

    guide_sessions = build_guide_sessions(
        registry=registry,
        score_events=score_events,
        gap_days=10,
    )

    # Population pulls are app_sub/partition-aware inside visitor_population.py.
    # Passing client=None makes the transform layer avoid reaching back into extract
    # just to create a fallback client.
    pop_df = build_population_rows_for_sessions(
        client=None,
        guide_sessions=guide_sessions,
    )
    guide_sessions = attach_population_to_sessions(guide_sessions, pop_df)

    comment_events = _attach_guide_names_to_comments(comment_events, registry)

    responses = build_ux_lite_responses(
        score_events=score_events,
        guide_sessions=guide_sessions,
        registry=registry,
    )

    mau_df = _rows_to_df(raw[MAU_TABLE])

    tables["ux_lite_registry"] = registry
    tables["guide_sessions"] = guide_sessions
    tables["guide_session_population_rows"] = pop_df
    tables["ux_lite_local_events"] = score_events
    tables["ux_lite_local_responses"] = responses
    tables["ux_lite_local_comments"] = comment_events
    tables[MAU_TABLE] = mau_df

    if debug:
        _print_debug_summary(
            registry=registry,
            guide_sessions=guide_sessions,
            pop_df=pop_df,
        )

    return tables