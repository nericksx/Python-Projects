# src/pendo/client.py
"""
Pendo HTTP client and authentication layer.

This is the only place that should know:
- how to build full Pendo URLs
- which auth headers Pendo expects
- how GET/POST requests are executed
- how API errors are formatted

This file should not contain endpoint-specific paths, payloads, params,
poll logic, guide logic, or UX-Lite business rules. Those belong in pulls/
and transforms/.
"""

from dataclasses import dataclass
from typing import Any

import os
import requests


def _ssl_verify_from_env() -> bool | str:
    """
    Determine how Requests should verify SSL certificates.

    Default behavior is secure: verify SSL certificates.

    Supported escape hatches:
    - REQUESTS_CA_BUNDLE=/path/to/company-ca.pem
      Best corporate-network fix. Tells Requests which CA bundle to trust.

    - SSL_CERT_FILE=/path/to/company-ca.pem
      Common Python/OpenSSL-compatible alternative.

    - PENDO_VERIFY_SSL=false
      Local/dev-only workaround for corporate SSL inspection issues.
      Do not use for production/shared runs unless explicitly approved.
    """
    ca_bundle = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
    if ca_bundle:
        return ca_bundle

    verify_ssl = os.getenv("PENDO_VERIFY_SSL", "true").strip().lower()
    if verify_ssl in {"false", "0", "no", "n"}:
        return False

    return True


# PendoClient is immutable because host, key, timeout, and SSL verification
# are connection config. A pull should create or receive the correct client,
# then treat it as read-only.
@dataclass(frozen=True)
class PendoClient:
    """
    Lightweight HTTP client for the Pendo API.

    Encapsulates:
    - base host URL
    - integration key authentication
    - shared GET/POST request helpers
    - consistent error handling
    - SSL verification configuration

    Each Pendo subscription gets its own PendoClient, created from a Partition.

    This class should not contain endpoint-specific logic or payload construction.
    """

    host: str
    key: str
    timeout_sec: int = 90
    verify_ssl: bool | str = True

    @classmethod
    def from_env(
        cls,
        *,
        host: str,
        key: str,
        timeout_sec: int = 90,
    ) -> "PendoClient":
        """
        Create a PendoClient using SSL verification settings from environment.

        This keeps corporate cert/proxy handling out of endpoint-specific code.
        """
        return cls(
            host=host,
            key=key,
            timeout_sec=timeout_sec,
            verify_ssl=_ssl_verify_from_env(),
        )

    @property
    def headers(self) -> dict[str, str]:
        """Build Pendo auth headers in one place for all requests."""
        return {
            "x-pendo-integration-key": self.key,
            "content-type": "application/json",
        }

    def url(self, path: str) -> str:
        """
        Build a full Pendo API URL from a relative endpoint path.

        Leading/trailing slashes are normalized so callers can pass either
        "/api/..." or "api/..." safely.
        """
        return f"{self.host.rstrip('/')}/{path.lstrip('/')}"

    def get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """
        Execute a GET request against a Pendo API endpoint.

        Parameters
        ----------
        path : str
            API path relative to the Pendo host, for example "/api/v1/guide".
        params : dict[str, Any] | None, optional
            Query parameters to include with the request.

        Returns
        -------
        Any
            Parsed JSON response returned by the Pendo API.

        Raises
        ------
        requests.HTTPError
            If the API returns a non-2xx response.
        """
        resp = requests.get(
            self.url(path),
            headers=self.headers,
            params=params,
            timeout=self.timeout_sec,
            verify=self.verify_ssl,
        )

        if not resp.ok:
            raise requests.HTTPError(
                f"{resp.status_code} {resp.reason} for {resp.url}\n{resp.text[:1500]}",
                response=resp,
            )

        return resp.json()

    def post(self, path: str, *, json: dict[str, Any]) -> Any:
        """
        Execute a POST request against a Pendo API endpoint.

        Parameters
        ----------
        path : str
            API path relative to the Pendo host, for example "/api/v1/aggregation".
        json : dict[str, Any]
            JSON payload to send in the request body.

        Returns
        -------
        Any
            Parsed JSON response returned by the Pendo API.

        Raises
        ------
        requests.HTTPError
            If the API returns a non-2xx response.
        """
        resp = requests.post(
            self.url(path),
            headers=self.headers,
            json=json,
            timeout=self.timeout_sec,
            verify=self.verify_ssl,
        )

        if not resp.ok:
            # Include enough response text to debug bad payloads/auth/permissions,
            # but truncate to avoid overwhelming logs if the response is huge.
            raise requests.HTTPError(
                f"{resp.status_code} {resp.reason} for {resp.url}\n{resp.text[:1500]}",
                response=resp,
            )

        return resp.json()