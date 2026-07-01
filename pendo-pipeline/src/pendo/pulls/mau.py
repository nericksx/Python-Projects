# src/pendo/pulls/mau.py
"""
Pull Pendo app-level MAU using the confirmed Pendo UI definition.

Business definition
-------------------
Pendo MAU is a rolling 30-day count of unique visitors who sent event data.
For THD's application-level MAU/billing model, the metric is scoped to one
Pendo app at a time. A single visitor can therefore count once for each app
they use.

Implementation rule
-------------------
Use the Pendo Aggregation API `events` source, not `pageEvents`.

The earlier pipeline estimated MAU from pageEvents grouped by app/month. That
was useful usage context, but it did not match the Pendo UI MAU metric. The UI
metric is broader: a visitor is active when they send any tracked Pendo event,
including page views, feature clicks, track events, guide interactions, and
other click/interaction events that Pendo records.

Current-window behavior
-----------------------
The Pendo UI current MAU includes the current calendar day up to the most
recently processed events. This means the value can change during the workday.
To mirror the UI behavior, this module uses Pendo's relative dayRange syntax:

    first = "now()"
    count = -30

The date fields emitted by this module are metadata for traceability. The
source-of-truth window is the Pendo timeSeries expression stored in:

    pendo_timeseries_period
    pendo_timeseries_first
    pendo_timeseries_count

Validated reference case
------------------------
On 2026-06-25, Delivery Tracker was validated against Leo/Pendo UI using:

    app_id  = 5511429746786304
    app_sub = CAE
    source  = events

Leo/Pendo UI reported approximately 6.0M current MAU and the API pull returned
approximately 6.13M, confirming the correct metric scale/source. Small
point-in-time differences are expected because current MAU includes today's
processed activity and changes during the day.

Non-goals
---------
This module does not calculate:

- calendar-month MAU
- month-to-date MAU
- YTD MAU
- subscription-level de-duplicated MAU
- de-duplicated portfolio users
- portfolio rollups

Source grain
------------
Downstream transforms/modeling may join app metadata such as reporting app and
portfolio, but this pull should preserve the source grain:

    one row per app_id / app_sub / period_start / period_end / mau_grain

For production, prefer passing explicit app IDs from the validated app/domain
mapping and creating the correct partition client from PARTITIONS. Do not rely
on the default/base make_client() for partition-specific apps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import pandas as pd

from ..client import PendoClient
from ..endpoint_factory import get_endpoint
from ..endpoints import AggregationEndpoint


MAU_WINDOW = "rolling_30_day"
MAU_GRAIN = "app_level"
MAU_DEFINITION = "unique visitors who sent event data"
MAU_SOURCE = "Pendo Aggregation API events"
DEFAULT_ROLLING_DAYS = 30


@dataclass(frozen=True)
class RollingMauWindow:
    """
    Rolling day window used for Pendo current MAU validation.

    The Pendo UI current MAU includes the current calendar day up to the most
    recently processed events. To match that behavior, the aggregation payload
    uses Pendo's relative dayRange form:

        first = "now()"
        count = -30

    For a pull run on 2026-06-25, this corresponds to the UI date label:

        2026-05-27_to_2026-06-25

    period_end is the actual pull timestamp and is not a closed calendar-day
    boundary. period_start is a best-effort timestamp approximation used for
    metadata/debugging; the source-of-truth window is the Pendo timeSeries
    expression stored in pendo_timeseries_first and pendo_timeseries_count.
    """

    period_start: datetime
    period_end: datetime
    label: str
    days: int = DEFAULT_ROLLING_DAYS
    pendo_timeseries_first: str = "now()"
    pendo_timeseries_count: int = -DEFAULT_ROLLING_DAYS
    includes_current_day: bool = True


# Backward-compatible alias for imports that may still reference MonthWindow.
# This is not a calendar-month window anymore.
MonthWindow = RollingMauWindow


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def to_ms(dt: datetime) -> int:
    """
    Convert a timezone-aware datetime to Unix epoch milliseconds.

    Retained for compatibility with older imports/tests. The current MAU pull
    uses Pendo's relative `now()` dayRange expression rather than epoch ms.
    """
    if dt.tzinfo is None:
        raise ValueError("dt must be timezone-aware")
    return int(dt.timestamp() * 1000)


def build_rolling_mau_window(
    now: datetime | None = None,
    days: int = DEFAULT_ROLLING_DAYS,
) -> RollingMauWindow:
    """
    Build metadata for the Pendo UI-style current MAU window.

    Parameters
    ----------
    now : datetime | None, optional
        Reference datetime. Defaults to the current UTC datetime. Must be
        timezone-aware when provided.
    days : int, default 30
        Number of rolling days to request from Pendo.

    Returns
    -------
    RollingMauWindow
        Metadata describing the Pendo relative rolling MAU window.

    Raises
    ------
    ValueError
        If days is not positive or now is timezone-naive.
    """
    if days <= 0:
        raise ValueError(f"days must be positive, got {days}")

    now = now or utc_now()
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    now = now.astimezone(timezone.utc)

    # This is metadata. The aggregation payload uses first="now()", count=-days.
    # For UI-style date labeling, a 30-day inclusive label ending today starts
    # 29 calendar dates before today.
    end_date = now.date()
    start_date = end_date - timedelta(days=days - 1)

    label = f"{start_date.isoformat()}_to_{end_date.isoformat()}"

    # Use calendar-day metadata to match the UI date label. The exact source
    # expression remains pendo_timeseries_first="now()" and count=-days.
    period_start = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)

    return RollingMauWindow(
        period_start=period_start,
        period_end=now,
        label=label,
        days=days,
        pendo_timeseries_first="now()",
        pendo_timeseries_count=-days,
        includes_current_day=True,
    )


def build_month_windows(
    months_back: int,
    now: datetime | None = None,
) -> list[RollingMauWindow]:
    """
    Deprecated compatibility wrapper.

    The old MAU pull built calendar-month windows. That is no longer aligned to
    Pendo's MAU definition. This wrapper intentionally returns one rolling
    30-day window so older imports fail less dramatically while the downstream
    pipeline is updated.
    """
    if months_back <= 0:
        raise ValueError(f"months_back must be positive, got {months_back}")
    return [build_rolling_mau_window(now=now)]


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    """
    Extract row lists from common Pendo aggregation response shapes.

    Pendo aggregation responses usually use "results", but this helper also
    checks "result" and "data" so the MAU pull is resilient to response-shape
    differences.
    """
    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            return payload["results"]
        if isinstance(payload.get("result"), list):
            return payload["result"]
        if isinstance(payload.get("data"), list):
            return payload["data"]
        # Some reduce responses come back as a single object.
        return [payload]

    if isinstance(payload, list):
        return payload

    return []


# Kept for compatibility with pulls/__init__.py, which imports this helper
# under the name mau_results_to_rows.
def results_to_rows(resp: Any) -> list[dict[str, Any]]:
    """Normalize a Pendo aggregation response into result rows."""
    return _extract_rows(resp)


def _client_metadata(
    client: PendoClient,
    *,
    app_sub: str | None = None,
    subscription_id: str | None = None,
) -> dict[str, Any]:
    """
    Best-effort extraction of partition metadata from the client.

    The PendoClient is intentionally lightweight and may not carry partition
    metadata. Debug and partition-aware callers can pass app_sub explicitly so
    output rows retain the subscription/partition that produced the metric.
    """
    inferred_app_sub = getattr(
        client,
        "app_sub",
        getattr(
            client,
            "partition",
            getattr(client, "partition_name", None),
        ),
    )

    inferred_subscription_id = getattr(
        client,
        "subscription_id",
        getattr(client, "subscription", None),
    )

    return {
        "app_sub": app_sub or inferred_app_sub,
        "subscription_id": subscription_id or inferred_subscription_id,
    }


def _normalize_app_ids(app_ids: Iterable[str] | None) -> list[str]:
    """Normalize an optional iterable of app IDs into clean strings."""
    if not app_ids:
        return []
    return [str(app_id).strip() for app_id in app_ids if str(app_id).strip()]


def build_mau_aggregation_body(
    *,
    days: int = DEFAULT_ROLLING_DAYS,
    app_id: str | None = None,
    use_reduce: bool | None = None,
) -> dict[str, Any]:
    """
    Build the Pendo aggregation payload for rolling app-level MAU.

    Parameters
    ----------
    days : int, default 30
        Number of rolling days to request from Pendo. Sent as count=-days.
    app_id : str | None, optional
        Specific Pendo app ID to query. If omitted, the payload queries all
        expanded app IDs.
    use_reduce : bool | None, optional
        When True, use Leo/Pendo's app-specific reduce shape:
            reduce uniqueVisitors count visitorId
        When False, group by appId and visitorId so Python can de-duplicate.
        Defaults to True for a specific app_id and False for all apps.

    Returns
    -------
    dict[str, Any]
        Pendo aggregation payload.
    """
    if days <= 0:
        raise ValueError(f"days must be positive, got {days}")

    clean_app_id = str(app_id).strip() if app_id is not None else None
    app_selector = clean_app_id or 'expandAppIds("*")'

    if use_reduce is None:
        use_reduce = clean_app_id is not None

    pipeline: list[dict[str, Any]] = [
        {
            "source": {
                # Use `events` to match Pendo UI MAU. `pageEvents` undercounts
                # apps where feature clicks, track events, guides, or other
                # interactions make visitors active.
                "events": {"appId": app_selector},
                "timeSeries": {
                    "period": "dayRange",
                    "first": "now()",
                    "count": -days,
                },
            }
        }
    ]

    if use_reduce:
        # Leo/Pendo-provided shape for matching the UI current MAU value for a
        # specific app. This is much smaller than returning one row per visitor.
        pipeline.append(
            {
                "reduce": {
                    "uniqueVisitors": {
                        "count": "visitorId",
                    }
                }
            }
        )
    else:
        # Fallback/all-app shape. This can return a very large response because
        # it asks Pendo for one row per app/visitor pair, but it avoids relying
        # on grouped distinct-count syntax while we validate the exact API shape.
        pipeline.append({"group": {"group": ["appId", "visitorId"]}})

    return {
        "response": {"mimeType": "application/json"},
        "request": {"pipeline": pipeline},
    }


def _extract_reduced_mau(result_rows: list[dict[str, Any]]) -> int | None:
    """Extract uniqueVisitors from a reduce-style Pendo response if present."""
    if not result_rows:
        return None

    for row in result_rows:
        if not isinstance(row, dict):
            continue

        value = row.get("uniqueVisitors")
        if isinstance(value, dict):
            # Be defensive in case Pendo wraps aggregation values.
            for key in ("value", "count", "num", "total"):
                if key in value:
                    value = value[key]
                    break

        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

    return None


def _summarize_app_level_mau(
    result_rows: list[dict[str, Any]],
    *,
    app_id: str | None = None,
) -> pd.DataFrame:
    """
    Convert Pendo aggregation rows into one MAU row per appId.

    Supports two response shapes:
    1. App-specific reduce response with a `uniqueVisitors` value.
    2. Grouped response with one row per appId/visitorId pair.
    """
    clean_app_id = str(app_id).strip() if app_id is not None else None

    reduced_mau = _extract_reduced_mau(result_rows)
    if reduced_mau is not None and clean_app_id is not None:
        return pd.DataFrame([{"appId": clean_app_id, "mau": reduced_mau}])

    result_df = pd.DataFrame(result_rows)
    if result_df.empty:
        return pd.DataFrame(columns=["appId", "mau"])

    required_cols = ["appId", "visitorId"]
    missing = [col for col in required_cols if col not in result_df.columns]
    if missing:
        raise ValueError(f"Pendo MAU response missing required columns: {missing}")

    app_visitor_df = result_df[required_cols].dropna().drop_duplicates()
    if app_visitor_df.empty:
        return pd.DataFrame(columns=["appId", "mau"])

    return (
        app_visitor_df.groupby("appId", dropna=False)
        .size()
        .reset_index(name="mau")
    )


def _pull_one_app_mau(
    endpoint: AggregationEndpoint,
    *,
    app_id: str,
    days: int,
) -> pd.DataFrame:
    """Pull one app's UI-style MAU using Pendo's reduce shape."""
    body = build_mau_aggregation_body(days=days, app_id=app_id, use_reduce=True)
    payload = endpoint.run(body)
    result_rows = _extract_rows(payload)
    return _summarize_app_level_mau(result_rows, app_id=app_id)


def pull_mau(
    client: PendoClient,
    months_back: int = 1,
    *,
    now: datetime | None = None,
    days: int = DEFAULT_ROLLING_DAYS,
    app_id: str | None = None,
    app_ids: Iterable[str] | None = None,
    app_sub: str | None = None,
    subscription_id: str | None = None,
) -> list[dict[str, Any]]:
    """
    Pull Pendo app-level rolling-30-day MAU.

    Parameters
    ----------
    client : PendoClient
        Pendo client for one subscription/partition.
    months_back : int, default 1
        Deprecated. Accepted for compatibility with existing callers, but not
        used to build monthly windows.
    now : datetime | None, optional keyword-only
        Reference datetime for metadata construction. Defaults to current UTC.
        The Pendo query itself uses first="now()", count=-days.
    days : int, default 30, optional keyword-only
        Number of rolling days to request from Pendo.
    app_id : str | None, optional keyword-only
        Specific app ID to validate, using the Pendo/Leo reduce query shape.
    app_ids : Iterable[str] | None, optional keyword-only
        Optional list of app IDs to pull one-by-one using the reduce query
        shape. This is preferred over all-app group mode when a validated app
        mapping is available.

    Returns
    -------
    list[dict[str, Any]]
        App-level rolling MAU rows with explicit window, grain, definition, and
        pull metadata.
    """
    if months_back != 1:
        print(
            "[mau] months_back is deprecated and ignored; "
            f"using {days}-day rolling window instead"
        )

    endpoint = get_endpoint("aggregations", client)
    if not isinstance(endpoint, AggregationEndpoint):
        raise TypeError(f"Expected AggregationEndpoint, got {type(endpoint).__name__}")

    pulled_at = utc_now()
    window = build_rolling_mau_window(now=now or pulled_at, days=days)
    client_meta = _client_metadata(
        client,
        app_sub=app_sub,
        subscription_id=subscription_id,
    )

    clean_app_ids = _normalize_app_ids(app_ids)
    if app_id is not None:
        clean_app_ids = [str(app_id).strip()]

    app_scope = ",".join(clean_app_ids) if clean_app_ids else 'expandAppIds("*")'
    print(
        "[mau] pulling app-level MAU "
        f"source=events "
        f"window={window.label} "
        f"pendo_timeSeries_first={window.pendo_timeseries_first} "
        f"pendo_timeSeries_count={window.pendo_timeseries_count} "
        f"includes_current_day={window.includes_current_day} "
        f"app_scope={app_scope} "
        f"app_sub={client_meta.get('app_sub')}"
    )

    if clean_app_ids:
        summaries = [
            _pull_one_app_mau(endpoint, app_id=one_app_id, days=window.days)
            for one_app_id in clean_app_ids
        ]
        summary = pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame()
    else:
        print(
            "[mau] WARNING: no app_id/app_ids supplied; using all-app events "
            "group mode, which may return a large response. Prefer app_ids from "
            "the validated app mapping for production runs."
        )
        body = build_mau_aggregation_body(days=window.days, app_id=None, use_reduce=False)
        payload = endpoint.run(body)
        result_rows = _extract_rows(payload)
        if not result_rows:
            print(f"[mau] no rows for rolling window {window.label}")
            return []
        summary = _summarize_app_level_mau(result_rows)

    if summary.empty:
        print(f"[mau] no usable MAU rows for rolling window {window.label}")
        return []

    rows: list[dict[str, Any]] = []
    for row in summary.to_dict(orient="records"):
        source_app_id = row.get("appId")
        rows.append(
            {
                # Source/app grain
                "app_id": str(source_app_id) if source_app_id is not None else None,
                "app_sub": client_meta.get("app_sub"),
                "subscription_id": client_meta.get("subscription_id"),

                # Metric
                "mau": int(row.get("mau", 0)),
                "mau_definition": MAU_DEFINITION,
                "mau_window": MAU_WINDOW,
                "mau_grain": MAU_GRAIN,
                "billing_basis": True,
                "source": MAU_SOURCE,

                # Window metadata. The source-of-truth Pendo window is the
                # relative dayRange expression below.
                "period_label": window.label,
                "period_start": window.period_start.isoformat(),
                "period_end": window.period_end.isoformat(),
                "period_end_inclusive": window.period_end.isoformat(),
                "window_days": window.days,
                "includes_current_day": window.includes_current_day,
                "pendo_timeseries_period": "dayRange",
                "pendo_timeseries_first": window.pendo_timeseries_first,
                "pendo_timeseries_count": window.pendo_timeseries_count,
                "pulled_at": pulled_at.isoformat(),

                # App metadata placeholders filled downstream.
                "reporting_app": None,
                "portfolio": None,
            }
        )

    return rows
