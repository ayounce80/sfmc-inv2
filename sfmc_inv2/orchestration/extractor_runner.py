"""Extractor runner for parallel execution of extractors.

Provides async parallel execution with configurable concurrency,
progress reporting, and error collection.

Supports dependency-aware extraction ordering via the ExtractionPlanner
to ensure dependencies are extracted before dependents.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

from ..core.config import SFMCConfig, get_config, get_config_with_account
from ..clients.rest_client import RESTClient
from ..clients.soap_client import SOAPClient
from ..clients.auth import TokenManager
from ..extractors import EXTRACTORS, ExtractorOptions, ExtractorResult, get_extractor
from ..extractors.base_extractor import BaseExtractor
from ..types.inventory import InventoryStatistics, ExtractorStats as StatsModel
from ..types.relationships import RelationshipGraph
from .extraction_planner import ExtractionPlan, ExtractionPlanner, plan_extraction
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

    # Dependency-aware extraction (Phase 6)
    use_extraction_planner: bool = False  # Enable topological ordering
    include_dependencies: bool = True  # Include dependency types as cache-only
    cache_only_types: list[str] = field(default_factory=list)  # Types to cache but not output

    # Multi-BU support: run extractors with supports_multi_bu=True across all child BUs
    enable_multi_bu: bool = True  # Enable multi-BU aggregation
    child_bu_ids: list[str] = field(default_factory=list)  # Override child BUs (or use config)


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
    - Dependency-aware extraction ordering (via ExtractionPlanner)
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
        self._planner = ExtractionPlanner(
            include_dependencies=self._config.include_dependencies
        )
        self._current_plan: Optional[ExtractionPlan] = None
        self._base_config = get_config()

    def _get_child_bu_ids(self) -> list[str]:
        """Get child BU IDs to use for multi-BU extraction."""
        # Use explicit config first, then fall back to SFMCConfig
        if self._config.child_bu_ids:
            return self._config.child_bu_ids
        return self._base_config.child_account_ids

    async def _run_extractor_for_bu(
        self,
        name: str,
        account_id: str,
        options: ExtractorOptions,
    ) -> ExtractorResult:
        """Run an extractor targeting a specific Business Unit.

        Creates BU-specific clients and tags results with source BU.

        Args:
            name: Extractor name.
            account_id: Target BU MID.
            options: Extraction options.

        Returns:
            ExtractorResult with items tagged with source BU.
        """
        # Create BU-specific configuration
        bu_config = get_config_with_account(account_id)

        # Create BU-specific clients
        token_manager = TokenManager(bu_config)
        rest_client = RESTClient(bu_config, token_manager)
        soap_client = SOAPClient(bu_config, token_manager)

        # Create extractor with BU-specific clients
        extractor_class = get_extractor(name)
        extractor = extractor_class(
            rest_client=rest_client,
            soap_client=soap_client,
        )

        # Run extraction
        extractor_result = await extractor.extract(options)

        # Tag all items with source BU
        for item in extractor_result.items:
            item["_sourceBuMid"] = account_id

        # Also tag relationships
        for edge in extractor_result.relationships:
            if edge.metadata is None:
                edge.metadata = {}
            edge.metadata["_sourceBuMid"] = account_id

        return extractor_result

    async def _run_multi_bu_extractor(
        self,
        name: str,
        options: ExtractorOptions,
    ) -> ExtractorResult:
        """Run an extractor across all configured BUs and merge results.

        Args:
            name: Extractor name.
            options: Extraction options.

        Returns:
            Merged ExtractorResult from all BUs.
        """
        child_bu_ids = self._get_child_bu_ids()
        if not child_bu_ids:
            # No child BUs, run on parent only
            extractor_class = get_extractor(name)
            extractor = extractor_class()
            return await extractor.extract(options)

        # Run on all child BUs (skip parent for child-BU-specific objects like journeys)
        all_results = []
        for bu_id in child_bu_ids:
            try:
                logger.info(f"Running {name} on BU {bu_id}")
                bu_result = await self._run_extractor_for_bu(name, bu_id, options)
                all_results.append(bu_result)
            except Exception as e:
                logger.error(f"Failed to run {name} on BU {bu_id}: {e}")

        # Merge all results
        merged = ExtractorResult(extractor_name=name, success=True)

        for bu_result in all_results:
            merged.items.extend(bu_result.items)
            merged.relationships.extend(bu_result.relationships)
            merged.errors.extend(bu_result.errors)
            merged.pages_fetched += bu_result.pages_fetched

            if not bu_result.success:
                merged.success = False  # Mark as partial failure

        merged.item_count = len(merged.items)
        merged.completed_at = datetime.now()
        merged.metadata["multi_bu"] = True
        merged.metadata["bu_count"] = len(all_results)

        return merged

    def get_extraction_plan(
        self,
        extractor_names: list[str],
    ) -> ExtractionPlan:
        """Get the extraction plan for the given extractors.

        Uses the ExtractionPlanner to determine dependency order.

        Args:
            extractor_names: List of extractor names to plan.

        Returns:
            ExtractionPlan with ordered steps.
        """
        return self._planner.plan(extractor_names)

    async def run(
        self,
        extractor_names: list[str],
        custom_options: Optional[dict[str, Any]] = None,
    ) -> RunnerResult:
        """Run specified extractors in parallel.

        When use_extraction_planner is enabled, extractors run in
        topological order based on dependencies.

        Args:
            extractor_names: List of extractor names to run.
            custom_options: Additional options per extractor.

        Returns:
            RunnerResult with all extraction results.
        """
        # Apply extraction planner if enabled
        if self._config.use_extraction_planner:
            plan = self.get_extraction_plan(extractor_names)
            self._current_plan = plan
            # Use all extractors from plan (includes dependencies)
            ordered_names = plan.all_extractor_names
            # Track which are cache-only
            cache_only_set = set(plan.cache_only_extractor_names)
            logger.info(
                f"Using extraction plan: {len(plan.steps)} steps "
                f"({len(plan.output_extractor_names)} output, "
                f"{len(plan.cache_only_extractor_names)} cache-only)"
            )
        else:
            ordered_names = extractor_names
            cache_only_set = set(self._config.cache_only_types)

        result = RunnerResult(extractors_run=extractor_names)
        custom_options = custom_options or {}

        # Create semaphore for extractor concurrency
        semaphore = asyncio.Semaphore(self._config.max_concurrent_extractors)

        async def run_single(name: str) -> tuple[str, ExtractorResult]:
            async with semaphore:
                self._report_progress(name, 0, 0, "Starting")

                try:
                    extractor_class = get_extractor(name)
                    options = self._build_options(name, custom_options.get(name, {}))

                    # Check if extractor supports multi-BU and multi-BU is enabled
                    supports_multi_bu = getattr(extractor_class, "supports_multi_bu", False)
                    child_bu_ids = self._get_child_bu_ids()

                    if (
                        self._config.enable_multi_bu
                        and supports_multi_bu
                        and child_bu_ids
                    ):
                        # Run across all child BUs
                        logger.info(f"Running {name} across {len(child_bu_ids)} child BUs")
                        extractor_result = await self._run_multi_bu_extractor(name, options)
                    else:
                        # Run on default BU only
                        extractor = extractor_class()
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

        # Run extractors - use dependency layers if planner enabled
        if self._config.use_extraction_planner and self._current_plan:
            # Get types from plan steps
            plan_types = {step.type_name for step in self._current_plan.steps}
            layers = self._planner.get_dependency_layers(plan_types)

            # Build type-to-extractor mapping for this plan
            type_to_extractor = {
                step.type_name: step.extractor_name
                for step in self._current_plan.steps
            }

            logger.info(f"Executing {len(layers)} dependency layers")

            completed = []
            for layer_idx, layer_types in enumerate(layers):
                # Convert types to extractor names
                layer_extractors = [
                    type_to_extractor[t]
                    for t in layer_types
                    if t in type_to_extractor
                ]

                if not layer_extractors:
                    continue

                logger.debug(
                    f"Layer {layer_idx}: {layer_extractors}"
                )

                # Run this layer in parallel
                layer_tasks = [run_single(name) for name in layer_extractors]
                layer_results = await asyncio.gather(*layer_tasks, return_exceptions=True)
                completed.extend(layer_results)
        else:
            # Non-planner mode: run all concurrently (original behavior)
            tasks = [run_single(name) for name in ordered_names]
            completed = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        for item in completed:
            if isinstance(item, Exception):
                logger.error(f"Task exception: {item}")
                continue

            name, extractor_result = item

            # Skip adding to results if cache-only (but still merge relationships)
            is_cache_only = name in cache_only_set
            if not is_cache_only:
                result.results[name] = extractor_result
            else:
                # Mark as cache-only in extractor result metadata
                extractor_result.metadata["cache_only"] = True
                logger.debug(f"Extractor {name} completed (cache-only)")

            # Merge relationships into graph (from all extractors)
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
            # REST - Automation Studio
            "automations", "queries", "scripts", "imports",
            "data_extracts", "filters", "file_transfers",
            # REST - Data & Content
            "data_extensions", "assets", "folders", "event_definitions",
            # REST - Journey Builder
            "journeys",
            # SOAP - Messaging
            "classic_emails", "triggered_sends", "lists",
            "sender_profiles", "delivery_profiles", "send_classifications",
            "templates", "account",
        ],
        "description": "Full inventory - all 20 extractors (REST and SOAP)",
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
}


def get_preset(name: str) -> dict[str, Any]:
    """Get a preset configuration by name."""
    if name not in PRESETS:
        raise ValueError(f"Unknown preset: {name}. Available: {list(PRESETS.keys())}")
    return PRESETS[name]


def list_presets() -> dict[str, str]:
    """List available presets with descriptions."""
    return {name: preset["description"] for name, preset in PRESETS.items()}


def run_with_planner(
    extractor_names: list[str],
    config: Optional[RunnerConfig] = None,
    include_dependencies: bool = True,
) -> RunnerResult:
    """Run extractors with dependency-aware ordering.

    Convenience function that enables the extraction planner
    for topological ordering based on type dependencies.

    Args:
        extractor_names: List of extractor names to run.
        config: Optional runner configuration.
        include_dependencies: Include dependency types as cache-only.

    Returns:
        RunnerResult with all extraction results.
    """
    run_config = config or RunnerConfig()
    run_config.use_extraction_planner = True
    run_config.include_dependencies = include_dependencies

    runner = ExtractorRunner(run_config)
    return runner.run_sync(extractor_names)


def get_extraction_order(extractor_names: list[str]) -> list[str]:
    """Get the extraction order for given extractors.

    Uses the ExtractionPlanner to determine dependency order.

    Args:
        extractor_names: List of extractor names.

    Returns:
        Ordered list of extractor names (dependencies first).
    """
    planner = ExtractionPlanner(include_dependencies=True)
    plan = planner.plan(extractor_names)
    return plan.all_extractor_names
