# src/pendo/partitions.py
"""
Configured Pendo subscriptions/partitions.

The pipeline pulls data from multiple Pendo subscriptions, currently SAE and CAE.
Each Partition contains the connection info needed to build a PendoClient for
that subscription.

partitioned.pull_all(...) loops over PARTITIONS, runs the same pull once per
subscription, and stamps returned rows with app_sub=partition.name for lineage.

To add another subscription, add its API key to config and add another Partition
to PARTITIONS. Endpoint-specific pull code should not hardcode subscription
names or API keys.
"""

from dataclasses import dataclass

from pendo.config import (
    BASE_URL,
    API_KEY_CAE,
    API_KEY_SAE,
)


# Partition config is immutable because these values should be created once
# from config and not changed during a run.
@dataclass(frozen=True)
class Partition:
    """
    Connection config and lineage label for one Pendo subscription.

    Parameters
    ----------
    name : str
        Short lineage label stamped into extracted rows as app_sub, such as
        "SAE" or "CAE". Treat this as a stable data value because downstream
        transforms and reports may rely on it.
    host : str
        Pendo API base URL.
    api_key : str
        API key for this specific Pendo subscription.
    """

    name: str
    host: str
    api_key: str


# Order is mainly used for deterministic pull/log output. Downstream logic should
# rely on app_sub lineage values, not list position.
PARTITIONS: list[Partition] = [
    Partition(
        name="SAE",
        host=BASE_URL,
        api_key=API_KEY_SAE,
    ),
    Partition(
        name="CAE",
        host=BASE_URL,
        api_key=API_KEY_CAE,
    ),
]