"""
GTFS-Realtime HTTP resource. Wraps httpx with sensible defaults: timeout, retries,
and a polite User-Agent identifying QuantumLane to upstream maintainers.

Retry policy:
    Transient errors (timeout, connection error, 5xx) → up to 3 attempts with
    exponential backoff. 4xx fails fast — those are bugs in our request, not flakes.
"""
from __future__ import annotations

import httpx
import structlog
from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

log = structlog.get_logger(__name__)


def _is_transient(exc: BaseException) -> bool:
    """Return True if the exception represents a retryable failure."""
    if isinstance(exc, httpx.TimeoutException | httpx.ConnectError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return False


class GTFSRTResource(ConfigurableResource):
    """HTTP client tuned for fetching GTFS-RT protobuf feeds."""

    timeout_seconds: float = 15.0
    user_agent: str = "QuantumLane/0.1"
    max_retries: int = 3

    _client: httpx.Client | None = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = httpx.Client(
            timeout=self.timeout_seconds,
            headers={"User-Agent": self.user_agent, "Accept": "application/x-protobuf"},
            follow_redirects=True,
        )

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def fetch_protobuf(self, url: str) -> bytes:
        """
        Fetch a GTFS-RT feed and return the raw protobuf bytes.
        Retries on transient errors. Raises on permanent failures.
        """
        if self._client is None:
            raise RuntimeError("HTTP client not initialized.")

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception(_is_transient),
            reraise=True,
        )
        def _do() -> bytes:
            assert self._client is not None
            response = self._client.get(url)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            # TTC sometimes returns application/octet-stream; both are fine. Reject HTML — usually an error page.
            if "html" in content_type.lower():
                raise httpx.HTTPStatusError(
                    f"Got HTML instead of protobuf from {url}. Likely an upstream error page.",
                    request=response.request,
                    response=response,
                )
            return response.content

        body = _do()
        log.debug("fetched_feed", url=url, bytes=len(body))
        return body
