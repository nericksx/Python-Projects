# src/pendo/endpoint_factory.py
"""
Endpoint factory.

Maps a short endpoint name, such as "guides" or "aggregations", to an endpoint
wrapper instance, given a configured PendoClient.

Pull modules use this so they do not need to construct endpoint wrapper classes
directly.
"""

from typing import Literal

from .client import PendoClient
from .endpoints import AggregationEndpoint, GuideEndpoint

# Keep these names aligned with calls in pull modules.
# Example: guides.py calls get_endpoint("guides", client), so EndpointName
# and the branches below must use "guides", not "guide".
EndpointName = Literal["guides", "aggregations"]


def get_endpoint(name: EndpointName, client: PendoClient) -> GuideEndpoint | AggregationEndpoint:
    """
    Return an endpoint wrapper instance for the requested Pendo endpoint.

    Parameters
    ----------
    name : EndpointName
        Short endpoint identifier used by pull modules.
    client : PendoClient
        Configured Pendo client for one subscription/partition.

    Returns
    -------
    GuideEndpoint | AggregationEndpoint
        Endpoint wrapper bound to the provided client.

    Raises
    ------
    ValueError
        If the endpoint name is not recognized.
    """
    if name == "guides":
        return GuideEndpoint(client)

    if name == "aggregations":
        return AggregationEndpoint(client)

    raise ValueError(f"Unknown endpoint name: {name}")