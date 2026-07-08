# src/pendo/pulls/ux_lite_poll_events_phase2.py

"""
Phase-2 UX-Lite poll event pull.

This module uses phase-1 guide metadata and guideSeen aggregation output to
build the UX-Lite registry, identify valid poll IDs by app_sub, and then retain
only poll events that match that registry.

The phase-2 structure prevents blind poll-event pulls from being interpreted
without guide context and preserves source-subscription lineage.
"""

import math
from typing import Any

import pandas as pd

from pendo.partitions import PARTITIONS
from pendo.pulls.partitioned import pull_all
from pendo.pulls.ux_lite_poll_events import pull_ux_lite_poll_events
from transform.ux_lite import build_ux_lite_registry
from datetime import datetime, timezone


CUTOFF_DATE = datetime(2026, 2, 2, tzinfo=timezone.utc)
CUTOFF_MS = int(CUTOFF_DATE.timestamp() * 1000)
CHUNK_DAYS = 30
MS_PER_DAY = 24 * 60 * 60 * 1000


def _aggregation_rows(aggregations: Any) -> list[dict[str, Any]]:
    """
    Extract guideSeen rows from supported phase-1 aggregation shapes.

    Phase-1 may store the raw aggregation payload as {"results": [...]} or an
    already-normalized list of rows.

    Parameters
    ----------
    aggregations : Any
        Raw or normalized phase-1 guideSeen aggregation output.

    Returns
    -------
    list[dict[str, Any]]
        GuideSeen aggregation rows. Unsupported shapes return an empty list.
    """
    if isinstance(aggregations, dict):
        rows = aggregations.get("results", [])
        return rows if isinstance(rows, list) else []

    if isinstance(aggregations, list):
        return aggregations

    return []


def _poll_ids_by_app_sub(
    *,
    guides: list[dict[str, Any]],
    aggregations: Any,
) -> dict[str, set[str]]:
    """
    Build the set of UX-Lite poll IDs to retain for each Pendo subscription.

    The UX-Lite registry is built separately for each app_sub so that poll
    events are only matched against poll IDs from the same source subscription.
    GuideSeen aggregation rows are filtered by app_sub when that column is
    available; otherwise, all aggregation rows are passed through for backward
    compatibility with unstamped inputs.

    Parameters
    ----------
    guides : list[dict[str, Any]]
        Raw guide records, expected to include app_sub lineage.
    aggregations : Any
        Raw or normalized GuideSeen aggregation output used by
        build_ux_lite_registry().

    Returns
    -------
    dict[str, set[str]]
        Mapping of app_sub to the UX-Lite usability, usefulness, and comment
        poll IDs found in that subscription's registry.
    """
    guide_seen_df = pd.json_normalize(_aggregation_rows(aggregations))

    poll_ids_by_sub: dict[str, set[str]] = {}

    app_subs = sorted(
        {
            str(guide.get("app_sub")).strip()
            for guide in guides
            if isinstance(guide, dict) and guide.get("app_sub")
        }
    )

    for app_sub in app_subs:
        guides_sub = [
            guide
            for guide in guides
            if (
                isinstance(guide, dict)
                and str(guide.get("app_sub", "")).strip() == app_sub
            )
        ]

        if "app_sub" in guide_seen_df.columns:
            guide_seen_app_sub = guide_seen_df["app_sub"].astype(str).str.strip()
            guide_seen_sub = guide_seen_df[guide_seen_app_sub.eq(app_sub)]
        else:
            # Older aggregation inputs may not be stamped with app_sub. In that
            # case, pass all guideSeen rows into the registry builder for
            # backward compatibility.
            guide_seen_sub = guide_seen_df

        registry = build_ux_lite_registry(guides_sub, guide_seen_sub)

        poll_ids: set[str] = set()
        for col in ["pollId_usability", "pollId_usefulness", "pollId_comment"]:
            if col in registry.columns:
                values = registry[col].dropna().astype(str).str.strip()
                poll_ids |= set(values[values.ne("")])

        poll_ids_by_sub[app_sub] = poll_ids
        print(
            f"[ux_lite_phase2] app_sub={app_sub} "
            f"registry_rows={len(registry)} pollIds={len(poll_ids)}"
        )

    return poll_ids_by_sub


def pull_ux_lite_poll_events_for_registry(
    *,
    guides: list[dict[str, Any]],
    aggregations: Any,
    cutoff_ms: int = CUTOFF_MS,
    chunk_days: int = CHUNK_DAYS,
) -> list[dict[str, Any]]:
    """
    Pull UX-Lite poll events for poll IDs present in the current UX-Lite registry.

    This is a phase-2 pull. It depends on earlier guide and GuideSeen aggregation
    pulls so the UX-Lite registry can identify the valid usability, usefulness,
    and comment poll IDs for each app_sub. Poll events are pulled across all
    configured partitions in date chunks, then filtered to keep only rows whose
    app_sub and pollId match the registry.

    Parameters
    ----------
    guides : list[dict[str, Any]]
        Phase-1 raw guide records, expected to include app_sub lineage.
    aggregations : Any
        Phase-1 GuideSeen aggregation output, either a raw {"results": [...]}
        payload or a normalized list of rows.
    cutoff_ms : int, default CUTOFF_MS
        Earliest event timestamp to pull, in UTC epoch milliseconds.
    chunk_days : int, default CHUNK_DAYS
        Maximum number of days to request per backward chunk.

    Returns
    -------
    list[dict[str, Any]]
        Poll event rows whose app_sub and pollId match the UX-Lite registry.

    Raises
    ------
    ValueError
        If cutoff_ms or chunk_days are invalid.
    """
    if cutoff_ms <= 0:
        raise ValueError(
            f"cutoff_ms must be a positive epoch millisecond timestamp, got {cutoff_ms}"
        )

    if chunk_days <= 0:
        raise ValueError(f"chunk_days must be positive, got {chunk_days}")

    poll_ids_by_sub = _poll_ids_by_app_sub(guides=guides, aggregations=aggregations)

    if not any(poll_ids_by_sub.values()):
        print("[ux_lite_phase2] no registry poll IDs found; skipping poll event pull")
        return []

    end_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
    cursor = end_ms
    kept_all: list[dict[str, Any]] = []

    # Walk backward in fixed-size chunks so historical pulls do not require one
    # large request. The final chunk may be shorter than chunk_days so the pull
    # does not intentionally reach before cutoff_ms.
    while cursor > cutoff_ms:
        chunk_start = max(cutoff_ms, cursor - chunk_days * MS_PER_DAY)
        chunk_days_this = max(1, math.ceil((cursor - chunk_start) / MS_PER_DAY))

        chunk_rows = pull_all(
            lambda client: pull_ux_lite_poll_events(
                client,
                first_ms=cursor,
                days=chunk_days_this,
            ),
            partitions=PARTITIONS,
        )

        kept_rows: list[dict[str, Any]] = []

        for row in chunk_rows:
            if not isinstance(row, dict):
                continue

            app_sub = row.get("app_sub")
            poll_id = row.get("pollId")

            if not app_sub or poll_id is None:
                continue

            # Keep only poll events whose pollId belongs to the UX-Lite registry
            # for the same subscription. This prevents pollId collisions or
            # cross-subscription bleed.
            wanted = poll_ids_by_sub.get(str(app_sub).strip(), set())
            if str(poll_id).strip() in wanted:
                kept_rows.append(row)

        print(
            f"[ux_lite_phase2] chunk "
            f"{pd.to_datetime(chunk_start, unit='ms', utc=True).date()} → "
            f"{pd.to_datetime(cursor, unit='ms', utc=True).date()} "
            f"days={chunk_days_this} rows={len(chunk_rows)} kept={len(kept_rows)}"
        )

        kept_all.extend(kept_rows)
        cursor = chunk_start

    return kept_all