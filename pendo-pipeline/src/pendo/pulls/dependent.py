# src/pendo/pulls/dependent.py
"""
Registry for phase-2 dependent pulls.

Phase-1 pulls are independent and are registered in pendo/pulls/__init__.py.
Phase-2 pulls live here because they need phase-1 outputs before they can run.

Current example:
- ux_lite_poll_events needs raw guide metadata and guideSeen aggregation rows
  so it can identify the UX-Lite guide/poll registry before pulling targeted
  poll event data.

Add new entries here when a pull depends on earlier raw outputs. Add fully
independent pulls to PULL_FUNCTIONS instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pendo.pulls.ux_lite_poll_events_phase2 import pull_ux_lite_poll_events_for_registry


RawPulls = dict[str, Any]


@dataclass(frozen=True)
class DependentPull:
    """
    Registry entry for a phase-2 pull.

    Parameters
    ----------
    name : str
        Output name for the dependent pull.
    run : Callable[[RawPulls], Any]
        Function that receives the phase-1 raw output dictionary and returns the
        dependent pull output.

    Notes
    -----
    Dependent pulls use earlier outputs, such as guides or aggregations, to
    decide what additional data needs to be pulled.
    """

    name: str
    run: Callable[[RawPulls], Any]


def require_raw_keys(raw: RawPulls, keys: list[str], *, pull_name: str) -> None:
    """
    Validate that required phase-1 outputs are available for a dependent pull.

    Parameters
    ----------
    raw : RawPulls
        Phase-1 raw output dictionary.
    keys : list[str]
        Required phase-1 output keys.
    pull_name : str
        Name of the dependent pull being prepared.

    Raises
    ------
    KeyError
        If any required phase-1 output is missing.
    """
    missing = [key for key in keys if key not in raw]
    if missing:
        raise KeyError(f"Missing required phase-1 outputs for {pull_name}: {missing}")


def run_ux_lite_poll_events(raw: RawPulls) -> Any:
    """
    Run the UX-Lite phase-2 poll event pull using phase-1 outputs.

    The guides and aggregations keys must match PULL_FUNCTIONS output names in
    pendo/pulls/__init__.py.
    """
    require_raw_keys(
        raw,
        ["guides", "aggregations"],
        pull_name="ux_lite_poll_events",
    )

    return pull_ux_lite_poll_events_for_registry(
        guides=raw["guides"],
        aggregations=raw["aggregations"],
    )


DEPENDENT_PULLS: list[DependentPull] = [
    DependentPull(
        name="ux_lite_poll_events",
        run=run_ux_lite_poll_events,
    ),
]