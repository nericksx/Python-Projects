# src/pendo/pulls/visitor_population.py

"""
Pull visitor population rows for UX-Lite guide sessions.

Population is calculated from Pendo pageEvents as the unique visitorId count for
a given appId and guide-session reporting window.

Important:
Guide sessions can come from multiple Pendo subscriptions. This module uses
app_sub lineage to choose the correct Pendo client for each session. That keeps
population denominators aligned with the same subscription that produced the
guide/session data.
"""

import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from ..client import PendoClient
from ..endpoint_factory import get_endpoint
from ..endpoints import AggregationEndpoint
from ..partitions import PARTITIONS, Partition
from .partitioned import make_client


POPULATION_LIMIT = 300_000

POPULATION_COLUMNS = [
    "guideSessionId",
    "guideId",
    "guideName",
    "app_sub",
    "appId",
    "visitorId",
    "eventCount",
    "firstTime",
    "lastTime",
    "reportingStart",
    "reportingEndExclusive",
]


def _empty_population_df() -> pd.DataFrame:
    """Return an empty population dataframe with the expected schema."""
    return pd.DataFrame(columns=POPULATION_COLUMNS)


def _norm_app_sub(value: Any) -> str:
    """
    Normalize an app_sub value for dictionary lookup.

    app_sub values come from Partition.name and are expected to be stable labels
    such as "SAE" or "CAE". Normalizing here avoids mismatches from whitespace
    or casing differences.
    """
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    return str(value).strip().upper()


def _to_utc_timestamp(ts: Any) -> pd.Timestamp:
    """
    Convert a timestamp-like value to a timezone-aware UTC pandas Timestamp.

    Naive timestamps are treated as UTC because pipeline reporting windows are
    built as UTC timestamps.
    """
    if pd.isna(ts):
        raise ValueError("Timestamp is null")

    stamp = pd.Timestamp(ts)

    if stamp.tzinfo is None:
        return stamp.tz_localize("UTC")

    return stamp.tz_convert("UTC")


def _ts_to_ms(ts: Any) -> int:
    """Convert a timestamp-like value to UTC epoch milliseconds."""
    stamp = _to_utc_timestamp(ts)
    return int(stamp.timestamp() * 1000)


def _window_days(start_ts: Any, end_ts: Any) -> int:
    """
    Calculate the number of Pendo dayRange days for a reporting window.

    Uses ceil so partial-day windows do not under-pull.
    """
    start = _to_utc_timestamp(start_ts)
    end = _to_utc_timestamp(end_ts)

    seconds = (end - start).total_seconds()
    if seconds <= 0:
        raise ValueError(f"Window end must be after start: start={start}, end={end}")

    return max(math.ceil(seconds / 86400), 1)


def _results_to_rows(resp: Any) -> list[dict[str, Any]]:
    """
    Normalize a Pendo aggregation response into result rows.

    Pendo aggregation calls usually return {"results": [...]}, but this helper
    also accepts an already-normalized list.
    """
    if isinstance(resp, dict):
        rows = resp.get("results", [])
        return rows if isinstance(rows, list) else []

    if isinstance(resp, list):
        return resp

    return []


def _clients_by_app_sub_from_partitions(
    partitions: Sequence[Partition],
) -> dict[str, PendoClient]:
    """
    Build one PendoClient per configured partition.

    Returns
    -------
    dict[str, PendoClient]
        Mapping from normalized app_sub / partition name to configured client.
    """
    clients: dict[str, PendoClient] = {}

    for partition in partitions:
        app_sub = _norm_app_sub(partition.name)
        if not app_sub:
            continue

        clients[app_sub] = make_client(partition)

    return clients


def _normalize_client_mapping(
    clients_by_app_sub: Mapping[str, PendoClient],
) -> dict[str, PendoClient]:
    """
    Normalize an externally provided app_sub -> client mapping.
    """
    return {
        _norm_app_sub(app_sub): client
        for app_sub, client in clients_by_app_sub.items()
        if _norm_app_sub(app_sub)
    }


def _resolve_clients_by_app_sub(
    *,
    clients_by_app_sub: Mapping[str, PendoClient] | None,
    partitions: Sequence[Partition] | None,
) -> dict[str, PendoClient]:
    """
    Resolve the client mapping used for population pulls.

    Explicit clients_by_app_sub wins. Otherwise, build clients from configured
    PARTITIONS so population pulls are subscription-aware by default.
    """
    if clients_by_app_sub is not None:
        return _normalize_client_mapping(clients_by_app_sub)

    if partitions is None:
        partitions = PARTITIONS

    return _clients_by_app_sub_from_partitions(partitions)


def pull_population_for_window(
    client: PendoClient,
    *,
    app_id: int | str,
    start_ts: Any,
    end_ts: Any,
    pop_limit: int = POPULATION_LIMIT,
) -> Any:
    """
    Pull one row per active visitor for a given app and reporting window.

    Population can then be calculated as nunique(visitorId).

    Parameters
    ----------
    client : PendoClient
        Configured Pendo client for the relevant subscription.
    app_id : int | str
        Pendo application ID.
    start_ts : Any
        Reporting window start timestamp.
    end_ts : Any
        Reporting window exclusive end timestamp.
    pop_limit : int, default POPULATION_LIMIT
        Maximum rows returned by Pendo aggregation.

    Returns
    -------
    Any
        Raw Pendo aggregation response.

    Raises
    ------
    ValueError
        If app_id or pop_limit is invalid.
    TypeError
        If the endpoint factory does not return an AggregationEndpoint.
    """
    if pd.isna(app_id) or str(app_id).strip() == "":
        raise ValueError("app_id is null or empty")

    if pop_limit <= 0:
        raise ValueError(f"pop_limit must be positive, got {pop_limit}")

    endpoint = get_endpoint("aggregations", client)
    if not isinstance(endpoint, AggregationEndpoint):
        raise TypeError(f"Expected AggregationEndpoint, got {type(endpoint).__name__}")

    end_ms = _ts_to_ms(end_ts)
    days = _window_days(start_ts, end_ts)

    payload = {
        "response": {"mimeType": "application/json"},
        "request": {
            "pipeline": [
                {
                    "source": {
                        "timeSeries": {
                            "period": "dayRange",
                            "first": end_ms,
                            "count": -days,
                        },
                        "pageEvents": {"appId": app_id},
                    }
                },
                {
                    "group": {
                        "group": ["appId", "visitorId"],
                        "fields": {
                            "eventCount": {"count": None},
                            "firstTime": {"min": "firstTime"},
                            "lastTime": {"max": "lastTime"},
                        },
                    }
                },
                {"limit": pop_limit},
            ]
        },
    }

    return endpoint.run(payload)


def build_population_rows_for_sessions(
    client: PendoClient | None,
    guide_sessions: pd.DataFrame,
    *,
    clients_by_app_sub: Mapping[str, PendoClient] | None = None,
    partitions: Sequence[Partition] | None = None,
    continue_on_error: bool = True,
) -> pd.DataFrame:
    """
    Pull active visitor rows for each guide session.

    For each guide session, this function:
    1. reads the session's app_sub
    2. chooses the matching Pendo client for that subscription
    3. pulls pageEvent visitors for the session's appId/reporting window
    4. stamps guide/session metadata onto each returned visitor row

    The first client argument is retained for backward compatibility with the
    existing transform layer. Normal population pulls use clients resolved by
    app_sub from PARTITIONS or clients_by_app_sub.

    Parameters
    ----------
    client : PendoClient | None
        Backward-compatibility fallback client. Prefer app_sub-specific clients
        built from partitions or supplied through clients_by_app_sub.
    guide_sessions : pd.DataFrame
        Guide-session dataframe with guideSessionId, app_sub, appId,
        reportingStart, and reportingEndExclusive.
    clients_by_app_sub : Mapping[str, PendoClient] | None, optional
        Optional explicit client mapping. Useful for tests or custom runtime
        configuration.
    partitions : Sequence[Partition] | None, optional
        Optional partition list. Defaults to configured PARTITIONS.
    continue_on_error : bool, default True
        When True, log failed session pulls and continue. When False, re-raise
        the exception.

    Returns
    -------
    pd.DataFrame
        Combined population rows. Population can be calculated by counting
        distinct visitorId per guideSessionId.

    Raises
    ------
    ValueError
        If guide_sessions is missing required columns.
    """
    if guide_sessions.empty:
        return _empty_population_df()

    required_cols = [
        "guideSessionId",
        "guideId",
        "guideName",
        "app_sub",
        "appId",
        "reportingStart",
        "reportingEndExclusive",
    ]
    missing = [col for col in required_cols if col not in guide_sessions.columns]
    if missing:
        raise ValueError(f"guide_sessions missing required columns: {missing}")

    resolved_clients = _resolve_clients_by_app_sub(
        clients_by_app_sub=clients_by_app_sub,
        partitions=partitions,
    )

    if not resolved_clients and client is not None:
        # This should only happen in unusual/test situations where PARTITIONS is
        # not available. Normal pipeline runs should use app_sub-specific clients.
        print(
            "[population] WARNING no app_sub-specific clients were resolved; "
            "falling back to the single provided client"
        )

    rows: list[pd.DataFrame] = []

    for _, session in guide_sessions.iterrows():
        session_id = session["guideSessionId"]
        app_sub = _norm_app_sub(session.get("app_sub"))
        app_id = session["appId"]
        start_ts = session["reportingStart"]
        end_ts = session["reportingEndExclusive"]

        if not app_sub:
            message = f"[population] missing app_sub for session {session_id}"
            if continue_on_error:
                print(message)
                continue
            raise ValueError(message)

        session_client = resolved_clients.get(app_sub)

        if session_client is None:
            if resolved_clients:
                message = (
                    f"[population] no Pendo client configured for "
                    f"app_sub={app_sub} session={session_id}"
                )
                if continue_on_error:
                    print(message)
                    continue
                raise KeyError(message)

            # Backward-compatibility fallback only when no app_sub mapping exists.
            session_client = client

        if session_client is None:
            message = (
                f"[population] no Pendo client available for "
                f"app_sub={app_sub} session={session_id}"
            )
            if continue_on_error:
                print(message)
                continue
            raise ValueError(message)

        if pd.isna(app_id) or pd.isna(start_ts) or pd.isna(end_ts):
            continue

        try:
            resp = pull_population_for_window(
                session_client,
                app_id=app_id,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except Exception as exc:
            message = (
                f"[population] failed for {session_id} "
                f"app_sub={app_sub} appId={app_id}: {exc}"
            )
            if continue_on_error:
                print(message)
                continue
            raise RuntimeError(message) from exc

        result_rows = _results_to_rows(resp)
        df = pd.json_normalize(result_rows)

        if df.empty:
            continue

        if len(df) >= POPULATION_LIMIT:
            print(
                f"[population] WARNING {session_id}: returned {len(df)} rows, "
                f"which may indicate truncation at POPULATION_LIMIT={POPULATION_LIMIT}"
            )

        df["guideSessionId"] = session_id
        df["guideId"] = session.get("guideId")
        df["guideName"] = session.get("guideName")
        df["app_sub"] = app_sub
        df["reportingStart"] = start_ts
        df["reportingEndExclusive"] = end_ts

        rows.append(df)

        if "visitorId" in df.columns:
            print(
                f"[population] {session_id} app_sub={app_sub}: "
                f"{df['visitorId'].astype(str).nunique()} visitors"
            )

    if not rows:
        return _empty_population_df()

    out = pd.concat(rows, ignore_index=True)

    if "visitorId" in out.columns:
        out["visitorId"] = out["visitorId"].astype(str)
    if "appId" in out.columns:
        out["appId"] = out["appId"].astype(str)
    if "app_sub" in out.columns:
        out["app_sub"] = out["app_sub"].astype(str)

    for col in POPULATION_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    return out[POPULATION_COLUMNS].reset_index(drop=True)