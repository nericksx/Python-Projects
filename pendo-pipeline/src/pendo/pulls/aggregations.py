# src/pendo/pulls/aggregations.py
"""
Pull-layer module for Pendo guideSeen aggregation data.

Pull modules are where we define:
- what to request from Pendo
- how to call the endpoint wrapper
- optional light shaping/normalization of returned data

Pull modules should NOT:
- store secrets or environment config; config.py handles that
- implement HTTP mechanics; client.py handles that

This module does not pull poll responses or comments. It pulls guide activity:
which guides were seen, for which apps, during a recent lookback window.

The transform layer uses this as a truth set to detect real guide activity and
avoid relying only on guide metadata fields like state, publishedAt, or
showsAfter.
"""

from typing import Any

from ..client import PendoClient
from ..endpoint_factory import get_endpoint
from ..endpoints import AggregationEndpoint


def results_to_rows(resp: Any) -> list[dict[str, Any]]:
    """
    Normalize a Pendo aggregation response into result rows.

    Pendo aggregation calls usually return a dictionary with a "results" list,
    but this helper also accepts a raw list for convenience.

    Parameters
    ----------
    resp : Any
        Raw response returned by the Pendo aggregation endpoint.

    Returns
    -------
    list[dict[str, Any]]
        Aggregation result rows. Unexpected response shapes return an empty
        list so callers can safely combine partitioned results.
    """
    if isinstance(resp, dict):
        rows = resp.get("results", [])
        return rows if isinstance(rows, list) else []

    if isinstance(resp, list):
        return resp

    return []


def pull_aggregations(client: PendoClient, *, first_ms: int, days: int) -> Any:
    """
    Pull guideSeen activity rollups for a lookback window.

    This aggregation is used as an activity truth set for guides. Guide metadata
    alone is not reliable because teams may reuse guides, rename them, disable
    them, or move them back to draft after a measurement run.

    Parameters
    ----------
    client : PendoClient
        Configured Pendo client for one subscription/partition.
    first_ms : int
        Window end timestamp in UTC epoch milliseconds.
    days : int
        Positive lookback window length in days. The Pendo payload uses
        count=-days to walk backward from first_ms.

    Returns
    -------
    Any
        Raw aggregation response from Pendo, usually {"results": [...]}.

    Raises
    ------
    TypeError
        If the endpoint factory does not return an AggregationEndpoint.
    ValueError
        If first_ms or days are not positive.

    Notes
    -----
    Output grain is one row per guideId/appId combination with firstSeenAt,
    lastSeenAt, and seenCount.
    """
    if first_ms <= 0:
        raise ValueError(
            f"first_ms must be a positive epoch millisecond timestamp, got {first_ms}"
        )

    if days <= 0:
        raise ValueError(f"days must be positive, got {days}")

    endpoint = get_endpoint("aggregations", client)
    if not isinstance(endpoint, AggregationEndpoint):
        raise TypeError(f"Expected AggregationEndpoint, got {type(endpoint).__name__}")

    payload = {
        "response": {"mimeType": "application/json"},
        "request": {
            "pipeline": [
                {
                    "source": {
                        "timeSeries": {
                            "period": "dayRange",
                            "first": first_ms,
                            "count": -days,
                        },
                        # Critical: expandAppIds("*") is required so the
                        # aggregation returns guide events across all accessible
                        # apps. Without it, app-level guideSeen activity may be
                        # missing or incorrectly scoped.
                        "guideEvents": {"appId": 'expandAppIds("*")'},
                    }
                },
                {"filter": 'type=="guideSeen"'},
                {
                    "group": {
                        "group": ["guideId", "appId"],
                        "fields": {
                            "firstSeenAt": {"min": "browserTime"},
                            "lastSeenAt": {"max": "browserTime"},
                            "seenCount": {"count": None},
                        },
                    }
                },
                {"sort": ["appId", "guideId"]},
            ]
        },
    }

    return endpoint.run(payload)