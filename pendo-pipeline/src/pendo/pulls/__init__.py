# src/pendo/pulls/__init__.py

from .registry_mau import pull_registry_mau


"""
Registry of independent phase-1 pulls.

extract.py loops over this dictionary and calls each value as func(client).
The client argument is accepted for a consistent interface, but most entries
ignore it and use pull_all(..., partitions=PARTITIONS) so each Pendo
subscription gets its own correctly configured client.

Add new independent raw pulls here. Add pulls that depend on these outputs
to pendo/pulls/dependent.py instead.
"""

from collections.abc import Callable
from pendo.client import PendoClient
from pendo.partitions import PARTITIONS
from pendo.pulls.partitioned import pull_all
from pendo.pulls.aggregations import pull_aggregations, results_to_rows
from pendo.pulls.guides import pull_guides
from pendo.pulls.mau import pull_mau, results_to_rows as mau_results_to_rows
from pendo.util.time import now_ms 


PULL_FUNCTIONS: dict[str, Callable[[PendoClient], object]] = {
    "guides": lambda _client: pull_all(pull_guides, partitions=PARTITIONS),


    "aggregations": lambda _client: {
        "results": pull_all(
            lambda c: results_to_rows(
                pull_aggregations(
                    c,
                    first_ms=now_ms(),
                    days=120,
                )
            ),
            partitions=PARTITIONS,
        )
    },

     "pendo_app_mau_rolling_30d": lambda _client: {
        "results": pull_registry_mau()
    },
}