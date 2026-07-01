# src/pendo/pulls/ux_lite_poll_events.py

"""
Pull raw Pendo pollEvents rows for one subscription and one time window.

This module owns the low-level pollEvents aggregation request only. It does not
build the UX-Lite registry, filter to UX-Lite poll IDs, loop across partitions,
or apply UX-Lite scoring/classification.

Registry targeting and multi-partition chunking happen in
ux_lite_poll_events_phase2.py.
"""

from typing import Any

from pendo.client import PendoClient
from pendo.endpoint_factory import get_endpoint
from pendo.endpoints import AggregationEndpoint


def _results_to_rows(resp: Any) -> list[dict[str, Any]]:
    """
    Normalize a Pendo aggregation response into row dictionaries.

    Pendo aggregation calls usually return {"results": [...]}, but this helper
    also accepts an already-normalized list of rows.

    Parameters
    ----------
    resp : Any
        Raw response returned by the Pendo aggregation endpoint.

    Returns
    -------
    list[dict[str, Any]]
        Result rows. Unsupported response shapes return an empty list.
    """
    if isinstance(resp, dict):
        rows = resp.get("results", [])
        return rows if isinstance(rows, list) else []

    if isinstance(resp, list):
        return resp

    return []


def pull_ux_lite_poll_events(
    client: PendoClient,
    *,
    first_ms: int,
    days: int,
) -> list[dict[str, Any]]:
    """
    Pull raw pollEvents rows for one subscription and one time window chunk.

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
    list[dict[str, Any]]
        Raw poll event rows returned by Pendo.

    Raises
    ------
    TypeError
        If the endpoint factory does not return an AggregationEndpoint.
    ValueError
        If first_ms or days are not positive.

    Notes
    -----
    This function intentionally does not filter to UX-Lite poll IDs. The
    phase-2 pull uses the UX-Lite registry to filter rows by app_sub and pollId.
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

    payload: dict[str, Any] = {
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
                        "pollEvents": {"appId": 'expandAppIds("*")'},
                    }
                }
            ]
        },
    }

    resp = endpoint.run(payload)
    return _results_to_rows(resp)