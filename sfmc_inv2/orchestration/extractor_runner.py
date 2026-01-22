"""Extractor runner for parallel execution of extractors.

Provides async parallel execution with configurable concurrency,
progress reporting, and error collection.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

from ..extractors import EXTRACTORS, ExtractorOptions, ExtractorResult, get_extractor
from ..extractors.base_extractor import BaseExtractor
from ..types.inventory import InventoryStatistics, ExtractorStats as StatsModel
from ..types.relationships import RelationshipGraph
from .rate_limiter import AdaptiveRateLimiter

logger = logging.getLogger(__name__)


@dataclass
class RunnerConfig:
    """Configuration for extractor runner."""

    # Execution
    max_concurrent_extractors: int = 3
    max_concurrent_requests: int = 5

    # Extraction options
    page_size: int = 500
    max_pages: int = 100
    include_details: bool = True
    include_content: bool = False

    # Rate limiting
    base_delay: float = 0.3
    max_delay: float = 60.0

    # Progress
    progress_callback: Optional[Callable[[str, int, int, str], None]] = None


@dataclass
class RunnerResult:
    """Result of running multiple extractors."""

    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    extractors_run: list[str] = field(default_factory=list)
    results: dict[str, ExtractorResult] = field(default_factory=dict)
    relationship_graph: RelationshipGraph = field(default_factory=RelationshipGraph)

    @property
    def success(self) -> bool:
        """Check if all extractors succeeded."""
        return all(r.success for r in self.results.values())

    @property
    def partial_success(self) -> bool:
        """Check if at least one extractor succeeded."""
        return any(r.success for r in self.results.values())

    @property
    def duration_seconds(self) -> float:
        """Get total run duration."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    def get_statistics(self) -> InventoryStatistics:
        """Generate statistics from results."""
        stats = InventoryStatistics(
            extractors_run=len(self.extractors_run),
            total_duration_seconds=self.duration_seconds,
        )

        for name, result in self.results.items():
            if result.success:
                stats.extractors_succeeded += 1
                stats.total_objects += result.item_count
            else:
                stats.extractors_failed += 1

            stats.by_object_type[name] = result.item_count

            stats.by_extractor[name] = StatsModel(
                name=name,
                status="completed" if result.success else "failed",
                items_extracted=result.item_count,
                duration_seconds=result.duration_seconds,
                errors=result.errors,
            )

        stats.total_relationships = len(self.relationship_graph.edges)

        return stats


class ExtractorRunner:
    """Runs multiple extractors with parallel execution support.

    Features:
    - Async parallel execution with configurable concurrency
    - Rate limiting across all extractors
    - Progress reporting
    - Error collection without abort
    - Relationship graph merging
    """

    def __init__(self, config: Optional[RunnerConfig] = None):
        """Initialize the runner.

        Args:
            config: Runner configuration.
        """
        self._config = config or RunnerConfig()
        self._rate_limiter = AdaptiveRateLimiter(
            max_concurrent=self._config.max_concurrent_requests,
            base_delay=self._config.base_delay,
            max_delay=self._config.max_delay,
        )

    async def run(
        self,
        extractor_names: list[str],
        custom_options: Optional[dict[str, Any]] = None,
    ) -> RunnerResult:
        """Run specified extractors in parallel.

        Args:
            extractor_names: List of extractor names to run.
            custom_options: Additional options per extractor.

        Returns:
            RunnerResult with all extraction results.
        """
        result = RunnerResult(extractors_run=extractor_names)
        custom_options = custom_options or {}

        # Create semaphore for extractor concurrency
        semaphore = asyncio.Semaphore(self._config.max_concurrent_extractors)

        async def run_single(name: str) -> tuple[str, ExtractorResult]:
            async with semaphore:
                self._report_progress(name, 0, 0, "Starting")

                try:
                    extractor_class = get_extractor(name)
                    extractor = extractor_class()

                    options = self._build_options(name, custom_options.get(name, {}))
                    extractor_result = await extractor.extract(options)

                    status = "Completed" if extractor_result.success else "Failed"
                    self._report_progress(
                        name, extractor_result.item_count, extractor_result.item_count, status
                    )

                    return name, extractor_result

                except Exception as e:
                    logger.exception(f"Extractor {name} failed with exception")
                    error_result = ExtractorResult(extractor_name=name, success=False)
                    error_result.add_error("RunnerError", str(e))
                    error_result.completed_at = datetime.now()
                    self._report_progress(name, 0, 0, "Error")
                    return name, error_result

        # Run all extractors
        tasks = [run_single(name) for name in extractor_names]
        completed = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        for item in completed:
            if isinstance(item, Exception):
                logger.error(f"Task exception: {item}")
                continue

            name, extractor_result = item
            result.results[name] = extractor_result

            # Merge relationships into graph
            for edge in extractor_result.relationships:
                result.relationship_graph.edges.append(edge)

        # Detect orphaned objects using RelationshipBuilder
        self._detect_orphans(result)

        # Calculate relationship stats
        result.relationship_graph.calculate_stats()
        result.completed_at = datetime.now()

        return result

    def run_sync(
        self,
        extractor_names: list[str],
        custom_options: Optional[dict[str, Any]] = None,
    ) -> RunnerResult:
        """Synchronous wrapper for run().

        Args:
            extractor_names: List of extractor names to run.
            custom_options: Additional options per extractor.

        Returns:
            RunnerResult with all extraction results.
        """
        return asyncio.run(self.run(extractor_names, custom_options))

    async def run_sequential(
        self,
        extractor_names: list[str],
        custom_options: Optional[dict[str, Any]] = None,
    ) -> RunnerResult:
        """Run extractors sequentially (one at a time).

        Args:
            extractor_names: List of extractor names to run.
            custom_options: Additional options per extractor.

        Returns:
            RunnerResult with all extraction results.
        """
        result = RunnerResult(extractors_run=extractor_names)
        custom_options = custom_options or {}

        for name in extractor_names:
            self._report_progress(name, 0, 0, "Starting")

            try:
                extractor_class = get_extractor(name)
                extractor = extractor_class()

                options = self._build_options(name, custom_options.get(name, {}))
                extractor_result = await extractor.extract(options)

                result.results[name] = extractor_result

                # Merge relationships
                for edge in extractor_result.relationships:
                    result.relationship_graph.edges.append(edge)

                status = "Completed" if extractor_result.success else "Failed"
                self._report_progress(
                    name, extractor_result.item_count, extractor_result.item_count, status
                )

            except Exception as e:
                logger.exception(f"Extractor {name} failed with exception")
                error_result = ExtractorResult(extractor_name=name, success=False)
                error_result.add_error("RunnerError", str(e))
                error_result.completed_at = datetime.now()
                result.results[name] = error_result
                self._report_progress(name, 0, 0, "Error")

        # Detect orphaned objects
        self._detect_orphans(result)

        result.relationship_graph.calculate_stats()
        result.completed_at = datetime.now()

        return result

    def _build_options(
        self, extractor_name: str, custom: dict[str, Any]
    ) -> ExtractorOptions:
        """Build ExtractorOptions for an extractor."""
        # Progress callback wrapper to include extractor name
        def progress_wrapper(stage: str, current: int, total: int) -> None:
            self._report_progress(extractor_name, current, total, stage)

        return ExtractorOptions(
            page_size=custom.get("page_size", self._config.page_size),
            max_pages=custom.get("max_pages", self._config.max_pages),
            max_concurrent=custom.get("max_concurrent", self._config.max_concurrent_requests),
            include_details=custom.get("include_details", self._config.include_details),
            include_content=custom.get("include_content", self._config.include_content),
            progress_callback=progress_wrapper,
            custom=custom,
        )

    def _report_progress(
        self, extractor: str, current: int, total: int, stage: str
    ) -> None:
        """Report progress via callback."""
        if self._config.progress_callback:
            self._config.progress_callback(extractor, current, total, stage)

    def _detect_orphans(self, result: RunnerResult) -> None:
        """Detect orphaned objects across all extraction results.

        Uses RelationshipBuilder to index objects and identify orphans
        based on relationship rules.
        """
        # Import here to avoid circular import
        from ..output.relationship_builder import RelationshipBuilder

        builder = RelationshipBuilder()

        # Map extractor names to object types used in relationships
        # The relationship edges use singular, lowercase type names
        type_mapping = {
            "automations": "automation",
            "data_extensions": "data_extension",
            "queries": "query",
            "journeys": "journey",
            "scripts": "script",
            "imports": "import",
            "data_extracts": "data_extract",
            "filters": "filter",
            "file_transfers": "file_transfer",
            "assets": "asset",
            "folders": "folder",
            "event_definitions": "event_definition",
            "classic_emails": "classic_email",
            "triggered_sends": "triggered_send",
            "lists": "list",
            "sender_profiles": "sender_profile",
            "delivery_profiles": "delivery_profile",
            "send_classifications": "send_classification",
            "templates": "template",
            "account": "account",
        }

        # Index all extracted objects
        for extractor_name, extractor_result in result.results.items():
            if not extractor_result.success:
                continue

            object_type = type_mapping.get(extractor_name, extractor_name)
            builder.index_objects(
                extractor_result.items,
                object_type,
                id_field="id",
            )

        # Merge relationship edges
        builder.merge_edges(result.relationship_graph.edges)

        # Detect orphans
        builder.detect_all_orphans()

        # Copy orphans to result graph
        for orphan in builder.graph.orphans:
            result.relationship_graph.orphans.append(orphan)


# Preset configurations
PRESETS = {
    "quick": {
        "extractors": ["automations", "data_extensions"],
        "description": "Quick overview - automations and data extensions only",
    },
    "full": {
        "extractors": [
            "automations", "data_extensions", "queries", "journeys",
            "scripts", "imports", "data_extracts", "filters", "file_transfers",
            "assets",
        ],
        "description": "Full REST inventory - all REST-based object types",
    },
    "automation": {
        "extractors": [
            "automations", "queries", "scripts", "imports",
            "data_extracts", "filters", "file_transfers",
        ],
        "description": "Automation focus - all Automation Studio activities",
    },
    "messaging": {
        "extractors": [
            "classic_emails", "triggered_sends", "sender_profiles",
            "delivery_profiles", "send_classifications",
        ],
        "description": "Messaging focus - email sending infrastructure (SOAP)",
    },
    "content": {
        "extractors": ["data_extensions", "queries", "assets"],
        "description": "Content focus - data extensions, queries, and Content Builder assets",
    },
    "journey": {
        "extractors": ["journeys", "data_extensions", "event_definitions"],
        "description": "Journey focus - journeys, entry events, and related data extensions",
    },
    "core": {
        "extractors": [
            # Existing
            "automations", "data_extensions", "queries", "journeys",
            # Phase 1 - Automation Activities
            "scripts", "imports", "data_extracts", "filters", "file_transfers",
            # Phase 2 - Content
            "assets", "folders", "event_definitions",
            # Phase 3 - Messaging (SOAP)
            "classic_emails", "triggered_sends", "lists",
            "sender_profiles", "delivery_profiles", "send_classifications",
            "templates", "account",
        ],
        "description": "Core inventory - all 20 extractors covering operational SFMC objects",
    },
}


def get_preset(name: str) -> dict[str, Any]:
    """Get a preset configuration by name."""
    if name not in PRESETS:
        raise ValueError(f"Unknown preset: {name}. Available: {list(PRESETS.keys())}")
    return PRESETS[name]


def list_presets() -> dict[str, str]:
    """List available presets with descriptions."""
    return {name: preset["description"] for name, preset in PRESETS.items()}
