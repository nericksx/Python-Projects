# src/pendo/pulls/guides.py
"""
Pull-layer module for Pendo Guides.

Pull modules are where we define:
- what to request from Pendo
- how to call the endpoint wrapper
- optional light shaping/normalization of returned data

Pull modules should NOT:
- store secrets or environment config; config.py handles that
- implement HTTP mechanics; client.py handles that

Guides are pulled early because they are the lookup layer for UX-Lite:
- guide IDs identify relevant guide experiences
- poll IDs identify ease/usefulness/comment questions
- timing/state fields help reconstruct measurement windows
- guide names and app IDs support registry review and debugging

This file does not decide which guides count as UX-Lite. It only retrieves
and lightly flattens guide metadata for downstream transforms.
"""

from typing import Any

import pandas as pd

from ..client import PendoClient
from ..endpoint_factory import get_endpoint
from ..endpoints import GuideEndpoint
from ..utils import normalize_job_role, normalize_unset


def pull_guides(client: PendoClient, *, app_id: int | None = None) -> list[dict[str, Any]]:
    """
    Pull expanded guide metadata from the Pendo Guides endpoint.

    Parameters
    ----------
    client : PendoClient
        Configured Pendo client for one subscription/partition.
    app_id : int | None, optional
        Optional Pendo application ID. When provided, limits the pull to guides
        for that app. The normal pipeline leaves this unset to pull all
        accessible guides.

    Returns
    -------
    list[dict[str, Any]]
        Raw guide dictionaries returned by Pendo.

    Raises
    ------
    TypeError
        If the endpoint factory does not return a GuideEndpoint, or if Pendo
        returns a response shape other than a list.

    Notes
    -----
    This function pulls guide metadata only. It does not decide which guides are
    UX-Lite guides. UX-Lite filtering and classification happen later in the
    transform layer.
    """
    endpoint = get_endpoint("guides", client)
    if not isinstance(endpoint, GuideEndpoint):
        raise TypeError(f"Expected GuideEndpoint, got {type(endpoint).__name__}")

    # expand="*" asks Pendo for full guide metadata. UX-Lite transforms need
    # expanded fields such as poll IDs, state, createdByUser, and showsAfter,
    # which are not included in the default guide list response.
    params: dict[str, Any] = {"expand": "*"}

    if app_id is not None:
        params["appId"] = app_id

    data = endpoint.list_guides(params=params)

    if not isinstance(data, list):
        raise TypeError(
            f"Expected list from /api/v1/guide, got {type(data).__name__}: "
            f"{str(data)[:200]}"
        )

    return data


def guides_to_df(guides: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Flatten raw Pendo guide metadata into a curated dataframe.

    This keeps guide fields useful for registry building, debugging, and
    reporting, while leaving UX-Lite classification and poll mapping to the
    transform layer.

    Parameters
    ----------
    guides : list[dict[str, Any]]
        Raw guide dictionaries returned by pull_guides().

    Returns
    -------
    pd.DataFrame
        One row per guide with selected metadata fields.

    Raises
    ------
    TypeError
        If any guide item is not a dictionary.
    """
    rows: list[dict[str, Any]] = []

    for guide in guides:
        if not isinstance(guide, dict):
            raise TypeError(f"Expected guide dict, got {type(guide).__name__}")

        # createdByUser is sometimes missing/null, so default to an empty dict
        # before reading nested creator fields.
        created = guide.get("createdByUser") or {}

        # Keep multiple Pendo timing fields because guide state alone is not
        # enough to determine whether a guide is active, scheduled, expired, or
        # returned to draft after a measurement window.
        shows_after = guide.get("showsAfter")
        published_at = guide.get("publishedAt")
        first_eligible = guide.get("currentFirstEligibleToBeSeenAt")

        rows.append(
            {
                "guideId": guide.get("id"),
                "guideName": guide.get("name"),
                "appId": guide.get("appId"),
                "state": guide.get("state"),
                "launchMethod": guide.get("launchMethod"),
                "isMultiStep": guide.get("isMultiStep"),
                "createdAt": guide.get("createdAt"),
                "publishedAt": published_at,
                "showsAfter": shows_after,
                "currentFirstEligibleToBeSeenAt": first_eligible,
                "expiresAfter": guide.get("expiresAfter"),
                "publishedEver": guide.get("publishedEver"),
                "createdByFirst": normalize_unset(created.get("first")),
                "createdByLast": normalize_unset(created.get("last")),
                "createdByJobRole": normalize_job_role(created.get("jobRole")),
            }
        )

    return pd.DataFrame(rows)
