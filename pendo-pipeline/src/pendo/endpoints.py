# src/pendo/endpoints.py
"""
pendo/endpoints.py

Endpoint wrappers.

Defines one small class per Pendo endpoint (guide, aggregation, etc.). Each class:
- knows the endpoint path
- exposes a thin method that calls PendoClient.get/post

Endpoint wrappers should remain "dumb":
- no payload building
- no business logic
- no environment/config loading

Payloads and params belong in pendo/pulls/.
"""

from dataclasses import dataclass
from typing import Any

from .client import PendoClient


@dataclass(frozen=True)
class GuideEndpoint:
    """
    Thin wrapper for the Pendo Guides endpoint.

    This class knows:
    - the endpoint path
    - which HTTP method to use

    It does NOT:
    - build query parameters
    - apply filters
    - shape or normalize response data
    """
    client: PendoClient
    path: str = "/api/v1/guide"

    def list_guides(self, *, params: dict[str, Any] | None = None) -> Any:
        """
        Retrieve guides from the Pendo Guides API.

        Parameters
        ----------
        params : dict[str, Any] | None, optional
            Optional query parameters to pass through unchanged to the Guides API.

        Returns
        -------
        Any
            Raw JSON response from the Guides endpoint.
        """
        return self.client.get(self.path, params=params)
    

@dataclass(frozen=True)
class AggregationEndpoint:
    """
    Thin wrapper for the Pendo Aggregation endpoint.

    Aggregation endpoints are POST-based and require a structured pipeline
    payload. Payload construction is intentionally handled outside this class.
    """
    client: PendoClient
    path: str = "/api/v1/aggregation"

    def run(self, payload: dict[str, Any]) -> Any:
        """
        Execute an aggregation pipeline request.

        Parameters
        ----------
        payload : dict[str, Any]
            Prebuilt aggregation pipeline payload to pass through unchanged.

        Returns
        -------
        Any
            Raw JSON response from the Aggregation API.
        """
        return self.client.post(self.path, json=payload)
    
# Add others later, same pattern:
# PageEndpoint, FeatureEndpoint, SegmentEndpoint, ReportEndpoint, etc.
