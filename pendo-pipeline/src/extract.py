# src/extract.py
"""
Orchestrator only.

This file intentionally contains no API request shapes (no payloads/params) and no
data-shaping logic. It just creates a PendoClient (from config) and calls the
pull functions in pendo/pulls/.

See pendo/pulls/pulls_[endpoint].py for request parameters and extraction logic.
"""
from pendo.client import PendoClient
from pendo.config import BASE_URL, API_KEY_SAE
from pendo.pulls.dependent import DEPENDENT_PULLS
from pendo.pulls import PULL_FUNCTIONS #<-- add additional pull in pulls/__init__.py


def make_client() -> PendoClient:
    """
    Create the base Pendo client used by the extraction orchestrator.

    Note:
        Most phase-1 pulls create their own per-subscription clients using
        PARTITIONS. This client mainly keeps the pull function interface
        consistent.
    """
    return PendoClient(host=BASE_URL, key=API_KEY_SAE, timeout_sec=90)

def extract_all(client: PendoClient) -> dict[str, object]:
    raw: dict[str, object] = {}
    for name, func in PULL_FUNCTIONS.items():
        print(f"[extract] pulling {name}...")
        raw[name] = func(client)
        try:
            n = len(raw[name])  # works for list/dict/df
            print(f"[extract] pulled {name}: {n}")
        except TypeError:
            print(f"[extract] pulled {name}")
    if name == "aggregations" and isinstance(raw[name], dict):
        results = raw[name].get("results", [])
        if isinstance(results, list):
            print(f"[extract] aggregations results rows: {len(results)}")
    
    return raw

def extract_all_two_phase(client: PendoClient) -> dict[str, object]:
    """
    Run the extraction workflow in two phases.

    Phase 1 pulls independent raw data such as guides and aggregations.

    Phase 2 runs dependent pulls that need phase-1 outputs. This is required
    for UX Lite because we first need guide/poll metadata to identify the
    correct guide IDs and poll IDs before pulling or assembling UX Lite
    score and comment data.

    This avoids hardcoding guide IDs or poll IDs in the pipeline.
    """
    raw = extract_all(client)  # phase 1

    for dep in DEPENDENT_PULLS:  # phase 2
        print(f"[extract] pulling dependent {dep.name}...")
        raw[dep.name] = dep.run(raw)

        try:
            n = len(raw[dep.name])  # works for list/dict/df
            print(f"[extract] pulled dependent {dep.name}: {n}")
        except TypeError:
            print(f"[extract] pulled dependent {dep.name}")

    return raw