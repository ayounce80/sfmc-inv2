"""Authentication and token management for SFMC APIs.

Provides thread-safe token caching with automatic refresh and single-flight
coordination to prevent thundering herd on 401 errors.
"""

import threading
import time
from dataclasses import dataclass
from typing import Optional

import httpx

from ..core.config import SFMCConfig, get_config

# Buffer time before token expiry (seconds)
TOKEN_EXPIRY_BUFFER_SECONDS = 60


@dataclass
class TokenCache:
    """Cached access token with expiration tracking."""

    access_token: str
    expires_at: float  # Unix timestamp
    account_id: Optional[str] = None

    def is_expired(self) -> bool:
        """Check if token is expired or about to expire."""
        return time.time() >= (self.expires_at - TOKEN_EXPIRY_BUFFER_SECONDS)


class TokenManager:
    """Thread-safe token manager with automatic refresh.

    Implements double-checked locking and single-flight pattern to prevent
    multiple simultaneous token refreshes.
    """

    def __init__(self, config: Optional[SFMCConfig] = None):
        """Initialize the token manager.

        Args:
            config: SFMC configuration. If None, loads from environment.
        """
        self._config = config or get_config()
        self._token_cache: Optional[TokenCache] = None
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._refresh_in_progress = False

    @property
    def config(self) -> SFMCConfig:
        """Get the SFMC configuration."""
        return self._config

    def get_token(self) -> str:
        """Get a valid access token, refreshing if necessary.

        Returns:
            Valid access token string.

        Raises:
            httpx.HTTPStatusError: If token request fails.
        """
        # Fast path: check without lock
        if self._token_cache and not self._token_cache.is_expired():
            return self._token_cache.access_token

        # Slow path: acquire lock and refresh
        with self._lock:
            # Double-check after acquiring lock
            if self._token_cache and not self._token_cache.is_expired():
                return self._token_cache.access_token

            # Wait if another thread is refreshing
            while self._refresh_in_progress:
                self._condition.wait()
                # Check if refresh completed successfully
                if self._token_cache and not self._token_cache.is_expired():
                    return self._token_cache.access_token

            # Perform refresh
            self._refresh_in_progress = True

        try:
            self._do_refresh()
        finally:
            with self._condition:
                self._refresh_in_progress = False
                self._condition.notify_all()

        return self._token_cache.access_token  # type: ignore

    def force_refresh(self) -> str:
        """Force a token refresh, coordinating with other threads.

        Used when a 401 error is received to refresh the token.

        Returns:
            New access token string.
        """
        with self._condition:
            # If refresh already in progress, wait for it
            while self._refresh_in_progress:
                self._condition.wait()
                # Another thread may have refreshed, check if valid
                if self._token_cache and not self._token_cache.is_expired():
                    return self._token_cache.access_token

            self._refresh_in_progress = True

        try:
            self._do_refresh()
        finally:
            with self._condition:
                self._refresh_in_progress = False
                self._condition.notify_all()

        return self._token_cache.access_token  # type: ignore

    def _do_refresh(self) -> None:
        """Perform the actual token refresh."""
        payload = {
            "grant_type": "client_credentials",
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
        }

        if self._config.account_id:
            payload["account_id"] = int(self._config.account_id)

        with httpx.Client(timeout=30) as client:
            response = client.post(self._config.auth_url, json=payload)
            response.raise_for_status()
            data = response.json()

        expires_in = data.get("expires_in", 1200)
        self._token_cache = TokenCache(
            access_token=data["access_token"],
            expires_at=time.time() + expires_in,
            account_id=self._config.account_id,
        )

    def invalidate(self) -> None:
        """Invalidate the current token cache."""
        with self._lock:
            self._token_cache = None

    def get_client_id(self) -> Optional[str]:
        """Get the account/MID for the current token."""
        if self._token_cache:
            return self._token_cache.account_id
        return self._config.account_id


# Module-level singleton instance
_default_manager: Optional[TokenManager] = None
_manager_lock = threading.Lock()


def get_token_manager(config: Optional[SFMCConfig] = None) -> TokenManager:
    """Get or create the default token manager.

    Args:
        config: SFMC configuration. Only used if creating a new manager.

    Returns:
        TokenManager instance.
    """
    global _default_manager
    if _default_manager is None:
        with _manager_lock:
            if _default_manager is None:
                _default_manager = TokenManager(config)
    return _default_manager


def get_token(config: Optional[SFMCConfig] = None) -> str:
    """Convenience function to get a valid access token.

    Args:
        config: SFMC configuration. Only used if creating a new manager.

    Returns:
        Valid access token string.
    """
    return get_token_manager(config).get_token()


def reset_token_manager() -> None:
    """Reset the default token manager. Useful for testing."""
    global _default_manager
    with _manager_lock:
        _default_manager = None
