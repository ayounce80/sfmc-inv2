"""Extraction planner for dependency-aware extraction ordering.

Provides topological sorting of extractors based on type dependencies
to ensure dependencies are extracted before dependents.
"""

from dataclasses import dataclass, field
from typing import Optional

from ..types.type_registry import (
    TYPE_REGISTRY,
    get_type_by_extractor,
    get_type_definition,
    get_type_to_extractor_map,
)


@dataclass
class ExtractionStep:
    """A single step in the extraction plan."""

    type_name: str  # Type name (e.g., "data_extension")
    extractor_name: str  # Extractor name (e.g., "data_extensions")
    cache_only: bool = False  # If True, cache but don't write to output
    reason: str = ""  # Why this step was added (for debugging)


@dataclass
class ExtractionPlan:
    """Complete extraction plan with ordered steps."""

    steps: list[ExtractionStep] = field(default_factory=list)
    requested_types: list[str] = field(default_factory=list)
    dependency_types: list[str] = field(default_factory=list)

    @property
    def all_extractor_names(self) -> list[str]:
        """Get all extractor names in order."""
        return [step.extractor_name for step in self.steps]

    @property
    def output_extractor_names(self) -> list[str]:
        """Get extractor names that produce output (not cache_only)."""
        return [step.extractor_name for step in self.steps if not step.cache_only]

    @property
    def cache_only_extractor_names(self) -> list[str]:
        """Get extractor names that are cache_only."""
        return [step.extractor_name for step in self.steps if step.cache_only]


class ExtractionPlanner:
    """Plans extraction order based on type dependencies.

    Uses topological sorting to ensure dependencies are extracted
    before the types that depend on them.
    """

    def __init__(self, include_dependencies: bool = True):
        """Initialize the planner.

        Args:
            include_dependencies: If True, automatically include dependency
                types as cache_only steps. If False, only plan requested types.
        """
        self._include_dependencies = include_dependencies
        self._type_to_extractor = get_type_to_extractor_map()

    def plan(
        self,
        requested_extractors: list[str],
        exclude_cache_only: Optional[list[str]] = None,
    ) -> ExtractionPlan:
        """Create an extraction plan for the requested extractors.

        Args:
            requested_extractors: List of extractor names to run.
            exclude_cache_only: Optional list of types to exclude from
                cache_only dependency loading.

        Returns:
            ExtractionPlan with topologically sorted steps.

        Example:
            Input: ["automations", "journeys"]
            Output plan steps:
            1. folder (cache_only=True)
            2. data_extension (cache_only=True)
            3. query (cache_only=True)
            4. script (cache_only=True)
            5. import (cache_only=True)
            6. data_extract (cache_only=True)
            7. filter (cache_only=True)
            8. file_transfer (cache_only=True)
            9. event_definition (cache_only=True)
            10. automations (cache_only=False)
            11. triggered_send (cache_only=True)
            12. journeys (cache_only=False)
        """
        exclude_cache_only = exclude_cache_only or []
        plan = ExtractionPlan()

        # Convert extractor names to type names
        requested_types = set()
        for extractor_name in requested_extractors:
            type_def = get_type_by_extractor(extractor_name)
            if type_def:
                requested_types.add(type_def.name)
            else:
                # Unknown extractor, skip
                continue

        plan.requested_types = list(requested_types)

        # Collect all dependencies
        all_types = set(requested_types)
        dependency_types = set()

        if self._include_dependencies:
            to_process = list(requested_types)
            processed = set()

            while to_process:
                type_name = to_process.pop(0)
                if type_name in processed:
                    continue
                processed.add(type_name)

                type_def = get_type_definition(type_name)
                if type_def:
                    for dep in type_def.dependencies:
                        all_types.add(dep)
                        if dep not in requested_types:
                            dependency_types.add(dep)
                        if dep not in processed:
                            to_process.append(dep)

        plan.dependency_types = list(dependency_types)

        # Topological sort
        sorted_types = self._topological_sort(all_types)

        # Build plan steps
        for type_name in sorted_types:
            type_def = get_type_definition(type_name)
            if not type_def:
                continue

            is_requested = type_name in requested_types
            is_excluded = type_name in exclude_cache_only

            if is_requested:
                reason = "Requested by user"
                cache_only = False
            elif is_excluded:
                # Skip excluded dependency types
                continue
            else:
                reason = "Dependency"
                cache_only = True

            plan.steps.append(
                ExtractionStep(
                    type_name=type_name,
                    extractor_name=type_def.extractor_name,
                    cache_only=cache_only,
                    reason=reason,
                )
            )

        return plan

    def get_dependency_layers(self, types: set[str]) -> list[list[str]]:
        """Group types into dependency layers for phased execution.

        Types in each layer have all their dependencies satisfied by
        previous layers. Types within a layer can execute in parallel.

        Uses Kahn's algorithm to produce layers rather than a flat list.

        Args:
            types: Set of type names to organize into layers.

        Returns:
            List of layers, where each layer is a list of type names.
            Layer 0 contains types with no dependencies.
            Layer N contains types whose dependencies are all in layers 0..N-1.

        Example:
            Input: {"automation", "query", "folder", "data_extension"}
            Output: [
                ["folder"],                    # Layer 0: no deps
                ["data_extension"],            # Layer 1: depends on folder
                ["query"],                     # Layer 2: depends on DE, folder
                ["automation"]                 # Layer 3: depends on query, etc.
            ]
        """
        # Build adjacency list and in-degree count
        in_degree: dict[str, int] = {t: 0 for t in types}
        graph: dict[str, list[str]] = {t: [] for t in types}

        for type_name in types:
            type_def = get_type_definition(type_name)
            if type_def:
                for dep in type_def.dependencies:
                    if dep in types:
                        graph[dep].append(type_name)
                        in_degree[type_name] += 1

        layers: list[list[str]] = []

        # Start with types that have no dependencies (in-degree 0)
        current_layer = sorted([t for t in types if in_degree[t] == 0])

        while current_layer:
            layers.append(current_layer)

            # Find next layer: types whose deps are now all processed
            next_layer = []
            for type_name in current_layer:
                for dependent in graph[type_name]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_layer.append(dependent)

            # Sort for determinism
            current_layer = sorted(next_layer)

        # Handle any remaining types (cycles - shouldn't happen)
        processed = {t for layer in layers for t in layer}
        remaining = sorted([t for t in types if t not in processed])
        if remaining:
            layers.append(remaining)

        return layers

    def _topological_sort(self, types: set[str]) -> list[str]:
        """Perform topological sort on types based on dependencies.

        Uses Kahn's algorithm for topological sorting.

        Args:
            types: Set of type names to sort.

        Returns:
            List of type names in dependency order (dependencies first).
        """
        # Build adjacency list and in-degree count
        # Edge from A to B means B depends on A
        in_degree: dict[str, int] = {t: 0 for t in types}
        graph: dict[str, list[str]] = {t: [] for t in types}

        for type_name in types:
            type_def = get_type_definition(type_name)
            if type_def:
                for dep in type_def.dependencies:
                    if dep in types:
                        graph[dep].append(type_name)
                        in_degree[type_name] += 1

        # Start with types that have no dependencies
        queue = [t for t in types if in_degree[t] == 0]
        # Sort for deterministic output
        queue.sort()

        result = []

        while queue:
            # Process in sorted order for determinism
            current = queue.pop(0)
            result.append(current)

            # Add new items with 0 in-degree
            new_items = []
            for dependent in graph[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    new_items.append(dependent)

            # Sort new items for determinism
            new_items.sort()
            queue.extend(new_items)
            queue.sort()

        # Handle any cycles (shouldn't happen with valid registry)
        remaining = [t for t in types if t not in result]
        if remaining:
            # Add remaining in sorted order (cycle detected)
            remaining.sort()
            result.extend(remaining)

        return result

    def get_extraction_order(
        self,
        requested_extractors: list[str],
    ) -> list[str]:
        """Get just the ordered list of extractor names.

        Simpler interface when you don't need the full plan details.

        Args:
            requested_extractors: List of extractor names to run.

        Returns:
            Ordered list of all extractor names including dependencies.
        """
        plan = self.plan(requested_extractors)
        return plan.all_extractor_names

    def validate_dependencies(
        self,
        extractor_names: list[str],
    ) -> dict[str, list[str]]:
        """Check if all dependencies are satisfied for given extractors.

        Args:
            extractor_names: List of extractor names.

        Returns:
            Dict mapping extractor names to list of missing dependencies.
            Empty dict means all dependencies satisfied.
        """
        missing: dict[str, list[str]] = {}

        extractor_set = set(extractor_names)

        for extractor_name in extractor_names:
            type_def = get_type_by_extractor(extractor_name)
            if not type_def:
                continue

            missing_deps = []
            for dep_type in type_def.dependencies:
                dep_def = get_type_definition(dep_type)
                if dep_def and dep_def.extractor_name not in extractor_set:
                    missing_deps.append(dep_def.extractor_name)

            if missing_deps:
                missing[extractor_name] = missing_deps

        return missing


def plan_extraction(
    requested_extractors: list[str],
    include_dependencies: bool = True,
) -> ExtractionPlan:
    """Convenience function to create an extraction plan.

    Args:
        requested_extractors: List of extractor names to run.
        include_dependencies: If True, include dependency types.

    Returns:
        ExtractionPlan with topologically sorted steps.
    """
    planner = ExtractionPlanner(include_dependencies=include_dependencies)
    return planner.plan(requested_extractors)


def get_extraction_order(requested_extractors: list[str]) -> list[str]:
    """Convenience function to get extraction order.

    Args:
        requested_extractors: List of extractor names to run.

    Returns:
        Ordered list of extractor names.
    """
    planner = ExtractionPlanner(include_dependencies=True)
    return planner.get_extraction_order(requested_extractors)
