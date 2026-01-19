"""Adaptive rate limiter for API request management.

Implements progressive backoff on failures and gradual recovery on success,
with semaphore-based concurrency control.
"""

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExtractorStats:
    """Statistics for an extractor's API usage."""

    consecutive_failures: int = 0
    consecutive_successes: int = 0
    total_requests: int = 0
    total_failures: int = 0
    current_delay: float = 0.0
    last_request_time: float = 0.0


class AdaptiveRateLimiter:
    """Adaptive rate limiter with progressive backoff and recovery.

    Features:
    - Per-extractor failure/success tracking
    - Progressive backoff on consecutive failures
    - Gradual recovery after consecutive successes
    - Semaphore-based concurrency control
    """

    def __init__(
        self,
        max_concurrent: int = 3,
        base_delay: float = 0.3,
        max_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
        recovery_threshold: int = 3,
    ):
        """Initialize the rate limiter.

        Args:
            max_concurrent: Maximum concurrent requests.
            base_delay: Base delay between requests in seconds.
            max_delay: Maximum delay cap in seconds.
            backoff_multiplier: Multiplier for backoff on failures.
            recovery_threshold: Consecutive successes needed to reduce delay.
        """
        self._max_concurrent = max_concurrent
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._backoff_multiplier = backoff_multiplier
        self._recovery_threshold = recovery_threshold

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._sync_semaphore = threading.Semaphore(max_concurrent)
        self._stats: dict[str, ExtractorStats] = {}
        self._lock = threading.Lock()

        # Global API stress indicator
        self._global_failures = 0
        self._global_stress_multiplier = 1.0

    def acquire(self, extractor_name: str) -> None:
        """Acquire a slot and apply appropriate delay (sync version).

        Args:
            extractor_name: Name of the extractor requesting.
        """
        self._sync_semaphore.acquire()

        with self._lock:
            stats = self._get_or_create_stats(extractor_name)

            # Calculate effective delay
            delay = self._calculate_delay(stats)
            if delay > 0:
                time.sleep(delay)

            stats.last_request_time = time.time()
            stats.total_requests += 1

    async def acquire_async(self, extractor_name: str) -> None:
        """Acquire a slot and apply appropriate delay (async version).

        Args:
            extractor_name: Name of the extractor requesting.
        """
        await self._semaphore.acquire()

        with self._lock:
            stats = self._get_or_create_stats(extractor_name)

            # Calculate effective delay
            delay = self._calculate_delay(stats)

        if delay > 0:
            await asyncio.sleep(delay)

        with self._lock:
            stats.last_request_time = time.time()
            stats.total_requests += 1

    def release(self, extractor_name: str, success: bool) -> None:
        """Release a slot and update statistics (sync version).

        Args:
            extractor_name: Name of the extractor.
            success: Whether the request was successful.
        """
        with self._lock:
            self._update_stats(extractor_name, success)

        self._sync_semaphore.release()

    async def release_async(self, extractor_name: str, success: bool) -> None:
        """Release a slot and update statistics (async version).

        Args:
            extractor_name: Name of the extractor.
            success: Whether the request was successful.
        """
        with self._lock:
            self._update_stats(extractor_name, success)

        self._semaphore.release()

    def _get_or_create_stats(self, extractor_name: str) -> ExtractorStats:
        """Get or create stats for an extractor."""
        if extractor_name not in self._stats:
            self._stats[extractor_name] = ExtractorStats(current_delay=self._base_delay)
        return self._stats[extractor_name]

    def _calculate_delay(self, stats: ExtractorStats) -> float:
        """Calculate the delay to apply before a request."""
        base = stats.current_delay * self._global_stress_multiplier

        # Minimum time since last request
        if stats.last_request_time > 0:
            elapsed = time.time() - stats.last_request_time
            if elapsed < base:
                return base - elapsed

        return 0.0

    def _update_stats(self, extractor_name: str, success: bool) -> None:
        """Update statistics after a request."""
        stats = self._get_or_create_stats(extractor_name)

        if success:
            stats.consecutive_failures = 0
            stats.consecutive_successes += 1

            # Reduce delay after consecutive successes
            if stats.consecutive_successes >= self._recovery_threshold:
                stats.current_delay = max(
                    self._base_delay,
                    stats.current_delay / self._backoff_multiplier,
                )
                stats.consecutive_successes = 0

            # Reduce global stress
            self._global_failures = max(0, self._global_failures - 1)
            if self._global_failures == 0:
                self._global_stress_multiplier = 1.0

        else:
            stats.consecutive_successes = 0
            stats.consecutive_failures += 1
            stats.total_failures += 1

            # Increase delay on failure
            stats.current_delay = min(
                self._max_delay,
                stats.current_delay * self._backoff_multiplier,
            )

            # Track global stress
            self._global_failures += 1
            if self._global_failures > 5:
                self._global_stress_multiplier = min(
                    3.0,
                    self._global_stress_multiplier * 1.2,
                )

    def get_status(self) -> dict[str, Any]:
        """Get current rate limiter status."""
        with self._lock:
            return {
                "max_concurrent": self._max_concurrent,
                "global_stress_multiplier": self._global_stress_multiplier,
                "global_failures": self._global_failures,
                "extractors": {
                    name: {
                        "current_delay": stats.current_delay,
                        "consecutive_failures": stats.consecutive_failures,
                        "consecutive_successes": stats.consecutive_successes,
                        "total_requests": stats.total_requests,
                        "total_failures": stats.total_failures,
                    }
                    for name, stats in self._stats.items()
                },
            }

    def reset(self, extractor_name: Optional[str] = None) -> None:
        """Reset statistics.

        Args:
            extractor_name: Specific extractor to reset, or None for all.
        """
        with self._lock:
            if extractor_name:
                if extractor_name in self._stats:
                    self._stats[extractor_name] = ExtractorStats(
                        current_delay=self._base_delay
                    )
            else:
                self._stats.clear()
                self._global_failures = 0
                self._global_stress_multiplier = 1.0


# Context managers for cleaner usage
class RateLimitContext:
    """Sync context manager for rate limiting."""

    def __init__(self, limiter: AdaptiveRateLimiter, extractor_name: str):
        self._limiter = limiter
        self._extractor_name = extractor_name
        self._success = True

    def __enter__(self) -> "RateLimitContext":
        self._limiter.acquire(self._extractor_name)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._success = exc_type is None
        self._limiter.release(self._extractor_name, self._success)

    def mark_failure(self) -> None:
        """Mark the current request as failed."""
        self._success = False


class AsyncRateLimitContext:
    """Async context manager for rate limiting."""

    def __init__(self, limiter: AdaptiveRateLimiter, extractor_name: str):
        self._limiter = limiter
        self._extractor_name = extractor_name
        self._success = True

    async def __aenter__(self) -> "AsyncRateLimitContext":
        await self._limiter.acquire_async(self._extractor_name)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._success = exc_type is None
        await self._limiter.release_async(self._extractor_name, self._success)

    def mark_failure(self) -> None:
        """Mark the current request as failed."""
        self._success = False
