# src/pendo/pulls/partitioned.py

"""
Helpers for running the same pull across multiple Pendo subscriptions.

Most endpoint-specific pull functions know how to pull data from one Pendo
client. This file handles the multi-subscription layer:

1. Build the correct client for each partition.
2. Run the pull function once per partition.
3. Stamp every returned row/item with app_sub for lineage.
4. Return one combined list.

Keep endpoint-specific request logic out of this file.
"""

from typing import Any, Callable

from pendo.client import PendoClient
from pendo.partitions import Partition


def make_client(partition: Partition) -> PendoClient:
    """
    Build a PendoClient for one configured Pendo partition/subscription.

    Each partition has its own host and API key. Pulls should use this client
    so data is fetched from the correct subscription.
    """
    return PendoClient(host=partition.host, key=partition.api_key)


def pull_all(
    pull_fn: Callable[[PendoClient], Any],
    *,
    partitions: list[Partition],
) -> list[Any]:
    """
    Run a single-partition pull function against every configured partition.

    Parameters
    ----------
    pull_fn : Callable[[PendoClient], Any]
        Function that accepts a PendoClient and returns raw data for one
        subscription.
    partitions : list[Partition]
        Pendo subscriptions/partitions to pull from.

    Returns
    -------
    list[Any]
        Combined list of raw rows/items. Each output item is stamped with
        app_sub so downstream transforms can trace the row back to its source
        subscription.

    Notes
    -----
    - If pull_fn returns a list of dict rows, app_sub is merged into each row.
    - If pull_fn returns non-dict rows or a single payload, the value is
      wrapped as {"app_sub": ..., "data": ...}.
    """
    out: list[Any] = []

    for partition in partitions:
        client = make_client(partition)
        result = pull_fn(client)

        # app_sub is lineage, not a reporting dimension. It lets downstream
        # transforms trace each row back to the Pendo subscription/partition it
        # came from, such as SAE or CAE.
        app_sub = partition.name

        if isinstance(result, list):
            items_added = 0

            for row in result:
                if isinstance(row, dict):
                    # Partition lineage is authoritative if row already includes
                    # an app_sub key.
                    out.append({**row, "app_sub": app_sub})
                else:
                    out.append({"app_sub": app_sub, "data": row})

                items_added += 1

            print(f"[pull] app_sub={app_sub} rows/items={items_added}")
        else:
            out.append({"app_sub": app_sub, "data": result})
            print(f"[pull] app_sub={app_sub} payload")

    print(f"[pull] total rows/items={len(out)}")
    return out