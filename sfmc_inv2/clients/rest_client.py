"""REST API client for SFMC.

Provides both synchronous and asynchronous HTTP operations with:
- Automatic retry with exponential backoff
- Token refresh on 401 errors
- Rate limit handling (429)
- Consistent response format
"""

import asyncio
import logging
import time
from typing import Any, Callable, Optional

import httpx

from ..core.config import SFMCConfig, get_config
from .auth import TokenManager, get_token_manager

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # Base delay in seconds
RETRY_BACKOFF = 2.0  # Exponential backoff multiplier
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_RATE_LIMIT_DELAY = 60.0  # Default delay for 429 when no Retry-After header


class RESTClient:
    """REST API client for SFMC.

    Supports both sync and async operations with automatic retry and token refresh.
    """

    def __init__(
        self,
        config: Optional[SFMCConfig] = None,
        token_manager: Optional[TokenManager] = None,
    ):
        """Initialize the REST client.

        Args:
            config: SFMC configuration. If None, loads from environment.
            token_manager: Token manager instance. If None, uses default.
        """
        self._config = config or get_config()
        self._token_manager = token_manager or get_token_manager(config)
        self._debug = self._config.rest_debug

    @property
    def base_url(self) -> str:
        """Get the REST API base URL."""
        return self._config.rest_url

    def _log_request(self, method: str, url: str, **kwargs: Any) -> None:
        """Log request details if debug is enabled."""
        if self._debug:
            logger.debug(f"REST {method} {url}")
            if "json" in kwargs:
                logger.debug(f"Request body: {kwargs['json']}")
            if "params" in kwargs:
                logger.debug(f"Query params: {kwargs['params']}")

    def _log_response(self, response: httpx.Response) -> None:
        """Log response details if debug is enabled."""
        if self._debug:
            logger.debug(f"Response status: {response.status_code}")
            try:
                logger.debug(f"Response body: {response.text[:1000]}")
            except Exception:
                pass

    def _build_headers(self, token: str) -> dict[str, str]:
        """Build request headers with authorization."""
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _handle_response(self, response: httpx.Response) -> dict[str, Any]:
        """Process response and return standardized format."""
        self._log_response(response)

        result: dict[str, Any] = {
            "ok": response.is_success,
            "status_code": response.status_code,
        }

        try:
            data = response.json()
            result["data"] = data
        except Exception:
            result["data"] = response.text

        if not response.is_success:
            result["error"] = response.text

        return result

    def _get_retry_delay(self, response: httpx.Response, attempt: int) -> float:
        """Calculate delay before retry."""
        if response.status_code == 429:
            # Honor Retry-After header if present
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return float(retry_after)
                except ValueError:
                    pass
            return DEFAULT_RATE_LIMIT_DELAY

        # Exponential backoff for other errors
        return RETRY_DELAY * (RETRY_BACKOFF**attempt)

    def request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make a synchronous REST API request with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            path: API path (e.g., "/automation/v1/automations")
            **kwargs: Additional arguments passed to httpx.Client.request

        Returns:
            Dict with keys: ok, status_code, data, and optionally error
        """
        url = f"{self.base_url}{path}"
        self._log_request(method, url, **kwargs)

        last_error: Optional[Exception] = None

        for attempt in range(MAX_RETRIES):
            token = self._token_manager.get_token()
            headers = self._build_headers(token)

            try:
                with httpx.Client(timeout=60) as client:
                    response = client.request(
                        method,
                        url,
                        headers=headers,
                        **kwargs,
                    )

                    # Handle 401 - token expired
                    if response.status_code == 401:
                        logger.debug("Got 401, refreshing token")
                        self._token_manager.force_refresh()
                        continue

                    # Handle retryable errors
                    if response.status_code in RETRYABLE_STATUS_CODES:
                        delay = self._get_retry_delay(response, attempt)
                        logger.debug(
                            f"Got {response.status_code}, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})"
                        )
                        time.sleep(delay)
                        continue

                    return self._handle_response(response)

            except httpx.TimeoutException as e:
                last_error = e
                logger.debug(f"Request timeout (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

            except httpx.RequestError as e:
                last_error = e
                logger.debug(f"Request error: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

        # All retries exhausted
        return {
            "ok": False,
            "status_code": 0,
            "error": str(last_error) if last_error else "Max retries exceeded",
        }

    async def request_async(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make an asynchronous REST API request with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            path: API path (e.g., "/automation/v1/automations")
            **kwargs: Additional arguments passed to httpx.AsyncClient.request

        Returns:
            Dict with keys: ok, status_code, data, and optionally error
        """
        url = f"{self.base_url}{path}"
        self._log_request(method, url, **kwargs)

        last_error: Optional[Exception] = None

        for attempt in range(MAX_RETRIES):
            token = self._token_manager.get_token()
            headers = self._build_headers(token)

            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    response = await client.request(
                        method,
                        url,
                        headers=headers,
                        **kwargs,
                    )

                    # Handle 401 - token expired
                    if response.status_code == 401:
                        logger.debug("Got 401, refreshing token")
                        self._token_manager.force_refresh()
                        continue

                    # Handle retryable errors
                    if response.status_code in RETRYABLE_STATUS_CODES:
                        delay = self._get_retry_delay(response, attempt)
                        logger.debug(
                            f"Got {response.status_code}, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{MAX_RETRIES})"
                        )
                        await asyncio.sleep(delay)
                        continue

                    return self._handle_response(response)

            except httpx.TimeoutException as e:
                last_error = e
                logger.debug(f"Request timeout (attempt {attempt + 1}/{MAX_RETRIES})")
                await asyncio.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

            except httpx.RequestError as e:
                last_error = e
                logger.debug(f"Request error: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
                await asyncio.sleep(RETRY_DELAY * (RETRY_BACKOFF**attempt))

        # All retries exhausted
        return {
            "ok": False,
            "status_code": 0,
            "error": str(last_error) if last_error else "Max retries exceeded",
        }

    # Convenience methods
    def get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make a GET request."""
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make a POST request."""
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make a PUT request."""
        return self.request("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make a PATCH request."""
        return self.request("PATCH", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make a DELETE request."""
        return self.request("DELETE", path, **kwargs)

    async def get_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make an async GET request."""
        return await self.request_async("GET", path, **kwargs)

    async def post_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make an async POST request."""
        return await self.request_async("POST", path, **kwargs)

    async def put_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make an async PUT request."""
        return await self.request_async("PUT", path, **kwargs)

    async def patch_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make an async PATCH request."""
        return await self.request_async("PATCH", path, **kwargs)

    async def delete_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Make an async DELETE request."""
        return await self.request_async("DELETE", path, **kwargs)


# Module-level convenience functions
_default_client: Optional[RESTClient] = None


def get_rest_client(config: Optional[SFMCConfig] = None) -> RESTClient:
    """Get or create the default REST client."""
    global _default_client
    if _default_client is None:
        _default_client = RESTClient(config)
    return _default_client


def rest_get(path: str, **kwargs: Any) -> dict[str, Any]:
    """Make a GET request using the default client."""
    return get_rest_client().get(path, **kwargs)


def rest_post(path: str, **kwargs: Any) -> dict[str, Any]:
    """Make a POST request using the default client."""
    return get_rest_client().post(path, **kwargs)


def rest_put(path: str, **kwargs: Any) -> dict[str, Any]:
    """Make a PUT request using the default client."""
    return get_rest_client().put(path, **kwargs)


def rest_patch(path: str, **kwargs: Any) -> dict[str, Any]:
    """Make a PATCH request using the default client."""
    return get_rest_client().patch(path, **kwargs)


def rest_delete(path: str, **kwargs: Any) -> dict[str, Any]:
    """Make a DELETE request using the default client."""
    return get_rest_client().delete(path, **kwargs)
