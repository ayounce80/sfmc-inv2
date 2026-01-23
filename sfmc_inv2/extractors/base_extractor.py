"""Base extractor pattern for SFMC object extraction.

Provides a template for implementing domain-specific extractors with
consistent fetch -> enrich -> transform pipeline.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generic, Optional, TypeVar

from pydantic import BaseModel

from ..cache.cache_manager import CacheManager, CacheType, get_cache_manager
from ..clients.rest_client import RESTClient, get_rest_client
from ..clients.soap_client import SOAPClient, get_soap_client
from ..types.inventory import ExtractionError
from ..types.relationships import RelationshipEdge, RelationshipGraph, RelationshipType

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


@dataclass
class ExtractorOptions:
    """Options for configuring extractor behavior."""

    # Pagination
    page_size: int = 500
    max_pages: int = 100

    # Performance
    max_concurrent: int = 5
    delay_between_requests: float = 0.0

    # Filtering
    include_details: bool = True
    include_content: bool = False

    # Progress
    progress_callback: Optional[Callable[[str, int, int], None]] = None

    # Custom options (extractor-specific)
    custom: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractorResult:
    """Result of an extraction operation."""

    extractor_name: str
    success: bool = False
    items: list[dict[str, Any]] = field(default_factory=list)
    errors: list[ExtractionError] = field(default_factory=list)
    relationships: list[RelationshipEdge] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    item_count: int = 0
    pages_fetched: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Update item count from items list."""
        if self.items:
            self.item_count = len(self.items)

    def add_error(
        self,
        error_type: str,
        message: str,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add an error to the result."""
        self.errors.append(
            ExtractionError(
                extractor=self.extractor_name,
                error_type=error_type,
                message=message,
                details=details,
            )
        )

    def add_relationship(
        self,
        source_id: str,
        source_type: str,
        target_id: str,
        target_type: str,
        relationship_type: RelationshipType,
        source_name: Optional[str] = None,
        target_name: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add a relationship edge to the result."""
        self.relationships.append(
            RelationshipEdge(
                source_id=source_id,
                source_type=source_type,
                source_name=source_name,
                target_id=target_id,
                target_type=target_type,
                target_name=target_name,
                relationship_type=relationship_type,
                metadata=metadata,
            )
        )

    @property
    def duration_seconds(self) -> float:
        """Get extraction duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0


class BaseExtractor(ABC, Generic[T]):
    """Abstract base class for SFMC object extractors.

    Implements a fetch -> enrich -> transform pipeline with:
    - Automatic cache warming
    - Progress reporting
    - Error collection
    - Relationship extraction
    """

    # Override in subclasses
    name: str = "base"
    description: str = "Base extractor"
    object_type: str = "object"

    # Cache types needed by this extractor
    required_caches: list[CacheType] = []

    # Multi-BU support: True if this extractor should aggregate across child BUs
    # Objects like journeys live on child BUs, while shared resources are on parent
    supports_multi_bu: bool = False

    def __init__(
        self,
        rest_client: Optional[RESTClient] = None,
        soap_client: Optional[SOAPClient] = None,
        cache_manager: Optional[CacheManager] = None,
    ):
        """Initialize the extractor.

        Args:
            rest_client: REST client instance.
            soap_client: SOAP client instance.
            cache_manager: Cache manager instance.
        """
        self._rest = rest_client or get_rest_client()
        self._soap = soap_client or get_soap_client()
        self._cache = cache_manager or get_cache_manager()

    async def extract(self, options: Optional[ExtractorOptions] = None) -> ExtractorResult:
        """Execute the full extraction pipeline.

        Args:
            options: Extraction options.

        Returns:
            ExtractorResult with extracted items and metadata.
        """
        options = options or ExtractorOptions()
        result = ExtractorResult(extractor_name=self.name)

        try:
            # Warm required caches
            await self._warm_caches(options)

            # Fetch raw data
            self._report_progress(options, "Fetching", 0, 0)
            raw_items = await self.fetch_data(options)
            result.pages_fetched = getattr(self, "_pages_fetched", 1)

            # Enrich data
            self._report_progress(options, "Enriching", 0, len(raw_items))
            enriched_items = await self.enrich_data(raw_items, options, result)

            # Transform to output format
            self._report_progress(options, "Transforming", 0, len(enriched_items))
            result.items = self.transform_data(enriched_items, options)
            result.item_count = len(result.items)

            # Extract relationships
            self._report_progress(options, "Analyzing relationships", 0, 0)
            await self.extract_relationships(enriched_items, result)

            result.success = True

        except Exception as e:
            logger.exception(f"Extraction failed for {self.name}")
            result.add_error("ExtractionError", str(e))
            result.success = False

        result.completed_at = datetime.now()
        return result

    def extract_sync(self, options: Optional[ExtractorOptions] = None) -> ExtractorResult:
        """Synchronous wrapper for extract().

        Args:
            options: Extraction options.

        Returns:
            ExtractorResult with extracted items and metadata.
        """
        return asyncio.run(self.extract(options))

    @abstractmethod
    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch raw data from SFMC APIs.

        Override in subclasses to implement specific fetching logic.

        Args:
            options: Extraction options.

        Returns:
            List of raw object dictionaries.
        """
        ...

    async def enrich_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
        result: ExtractorResult,
    ) -> list[dict[str, Any]]:
        """Enrich raw data with additional information.

        Default implementation adds breadcrumb paths. Override to add
        more enrichment.

        Args:
            items: Raw items from fetch_data.
            options: Extraction options.
            result: Result object for error collection.

        Returns:
            List of enriched items.
        """
        enriched = []
        for i, item in enumerate(items):
            try:
                enriched_item = await self.enrich_item(item, options)
                enriched.append(enriched_item)

                if options.progress_callback and (i + 1) % 100 == 0:
                    self._report_progress(options, "Enriching", i + 1, len(items))

            except Exception as e:
                logger.warning(f"Failed to enrich item: {e}")
                result.add_error(
                    "EnrichmentError",
                    str(e),
                    {"item_id": item.get("id", item.get("ID", "unknown"))},
                )
                enriched.append(item)  # Keep unenriched

        return enriched

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich a single item.

        Override in subclasses for item-specific enrichment.

        Args:
            item: Raw item dictionary.
            options: Extraction options.

        Returns:
            Enriched item dictionary.
        """
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform enriched data to output format.

        Default implementation returns items as-is. Override for
        custom transformation.

        Args:
            items: Enriched items.
            options: Extraction options.

        Returns:
            List of transformed items.
        """
        return items

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships between objects.

        Override in subclasses to identify relationships.

        Args:
            items: Enriched items.
            result: Result object to add relationships to.
        """
        pass

    async def _warm_caches(self, options: ExtractorOptions) -> None:
        """Warm required caches before extraction."""
        if not self.required_caches:
            return

        self._report_progress(options, "Warming caches", 0, len(self.required_caches))

        # Cache warming is synchronous, run in thread pool
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._cache.warm,
            self.required_caches,
        )

    def _report_progress(
        self,
        options: ExtractorOptions,
        stage: str,
        current: int,
        total: int,
    ) -> None:
        """Report progress via callback if provided."""
        if options.progress_callback:
            options.progress_callback(f"{self.name}: {stage}", current, total)

    def get_breadcrumb(
        self,
        folder_id: Optional[str],
        cache_type: CacheType,
    ) -> str:
        """Get breadcrumb path for a folder ID."""
        return self._cache.get_breadcrumb(folder_id, cache_type)
