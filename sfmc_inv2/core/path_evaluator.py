"""JSON path expression evaluator for dependency graph traversal.

Supports Accenture-style dependency graph path expressions for
data-driven relationship extraction.

Supported patterns:
- "field" - Direct field access
- "field.subfield" - Nested access
- "array[]" - Array iteration (returns all elements)
- "array[].field" - Field from each array element
- "field=value" - Filter by exact value match
- "array[].field=value" - Filter array elements by field value

Examples:
    evaluate_paths(automation, ["steps[].activities[].objectTypeId=300"])
    -> Returns all activities with objectTypeId=300

    evaluate_paths(journey, ["triggers[].metaData.eventDefinitionId"])
    -> Returns all eventDefinitionId values from triggers
"""

import re
from typing import Any, Iterator, Union


class PathEvaluator:
    """Evaluates JSON path expressions against objects."""

    # Pattern for parsing path segments
    # Matches: fieldName, fieldName[], fieldName=value
    SEGMENT_PATTERN = re.compile(r"^([^.\[\]=]+)(\[\])?(?:=(.+))?$")

    def evaluate(
        self,
        obj: Union[dict, list, Any],
        path: str,
    ) -> list[Any]:
        """Evaluate a path expression against an object.

        Args:
            obj: Object to evaluate against.
            path: Path expression to evaluate.

        Returns:
            List of matching values (may be empty).

        Examples:
            >>> evaluator = PathEvaluator()
            >>> obj = {"steps": [{"activities": [{"id": 1}, {"id": 2}]}]}
            >>> evaluator.evaluate(obj, "steps[].activities[].id")
            [1, 2]
        """
        if not path or obj is None:
            return []

        results = []
        for value in self._evaluate_path(obj, path):
            if value is not None:
                results.append(value)

        return results

    def evaluate_all(
        self,
        obj: Union[dict, list, Any],
        paths: list[str],
    ) -> list[Any]:
        """Evaluate multiple path expressions and combine results.

        Args:
            obj: Object to evaluate against.
            paths: List of path expressions.

        Returns:
            Combined list of all matching values (deduplicated).
        """
        results = []
        seen = set()

        for path in paths:
            for value in self.evaluate(obj, path):
                # Dedup by string representation for hashable comparison
                value_key = str(value) if isinstance(value, dict) else value
                if value_key not in seen:
                    seen.add(value_key)
                    results.append(value)

        return results

    def evaluate_with_context(
        self,
        obj: Union[dict, list, Any],
        path: str,
    ) -> list[tuple[Any, dict[str, Any]]]:
        """Evaluate a path and return values with their parent context.

        Useful for extracting related fields from the same parent object.

        Args:
            obj: Object to evaluate against.
            path: Path expression to evaluate.

        Returns:
            List of (value, parent_dict) tuples.
        """
        if not path or obj is None:
            return []

        results = []
        for value, context in self._evaluate_path_with_context(obj, path, {}):
            if value is not None:
                results.append((value, context))

        return results

    def _evaluate_path(
        self,
        obj: Union[dict, list, Any],
        path: str,
    ) -> Iterator[Any]:
        """Internal path evaluation generator."""
        segments = path.split(".")
        yield from self._evaluate_segments(obj, segments)

    def _evaluate_path_with_context(
        self,
        obj: Union[dict, list, Any],
        path: str,
        context: dict[str, Any],
    ) -> Iterator[tuple[Any, dict[str, Any]]]:
        """Internal path evaluation with context tracking."""
        segments = path.split(".")
        yield from self._evaluate_segments_with_context(obj, segments, context)

    def _evaluate_segments(
        self,
        obj: Union[dict, list, Any],
        segments: list[str],
    ) -> Iterator[Any]:
        """Recursively evaluate path segments."""
        if not segments:
            yield obj
            return

        if obj is None:
            return

        segment = segments[0]
        remaining = segments[1:]

        # Parse the segment
        match = self.SEGMENT_PATTERN.match(segment)
        if not match:
            return

        field_name = match.group(1)
        is_array = match.group(2) == "[]"
        filter_value = match.group(3)

        # Handle list iteration
        if isinstance(obj, list):
            for item in obj:
                yield from self._evaluate_segments(item, segments)
            return

        # Handle dict field access
        if isinstance(obj, dict):
            value = obj.get(field_name)

            if value is None:
                return

            # Apply filter if present
            if filter_value is not None:
                # Filter mode: check if value matches
                if self._matches_value(value, filter_value):
                    yield from self._evaluate_segments(obj, remaining) if remaining else [obj]
                return

            # Array iteration mode
            if is_array:
                if isinstance(value, list):
                    for item in value:
                        yield from self._evaluate_segments(item, remaining)
                return

            # Simple field access
            yield from self._evaluate_segments(value, remaining)

    def _evaluate_segments_with_context(
        self,
        obj: Union[dict, list, Any],
        segments: list[str],
        context: dict[str, Any],
    ) -> Iterator[tuple[Any, dict[str, Any]]]:
        """Recursively evaluate with context tracking."""
        if not segments:
            yield obj, context
            return

        if obj is None:
            return

        segment = segments[0]
        remaining = segments[1:]

        match = self.SEGMENT_PATTERN.match(segment)
        if not match:
            return

        field_name = match.group(1)
        is_array = match.group(2) == "[]"
        filter_value = match.group(3)

        if isinstance(obj, list):
            for item in obj:
                yield from self._evaluate_segments_with_context(item, segments, context)
            return

        if isinstance(obj, dict):
            # Update context with current object
            new_context = {**context, **obj}
            value = obj.get(field_name)

            if value is None:
                return

            if filter_value is not None:
                if self._matches_value(value, filter_value):
                    if remaining:
                        yield from self._evaluate_segments_with_context(obj, remaining, new_context)
                    else:
                        yield obj, new_context
                return

            if is_array:
                if isinstance(value, list):
                    for item in value:
                        yield from self._evaluate_segments_with_context(item, remaining, new_context)
                return

            yield from self._evaluate_segments_with_context(value, remaining, new_context)

    def _matches_value(self, value: Any, expected: str) -> bool:
        """Check if a value matches the expected string representation.

        Handles numeric comparisons for activity type IDs.
        """
        # Try numeric comparison first
        try:
            expected_num = int(expected)
            if isinstance(value, (int, float)):
                return value == expected_num
            if isinstance(value, str) and value.isdigit():
                return int(value) == expected_num
        except ValueError:
            pass

        # Fall back to string comparison
        return str(value) == expected


# Module-level convenience functions
_default_evaluator = PathEvaluator()


def evaluate_path(obj: Any, path: str) -> list[Any]:
    """Evaluate a single path expression.

    Args:
        obj: Object to evaluate against.
        path: Path expression.

    Returns:
        List of matching values.
    """
    return _default_evaluator.evaluate(obj, path)


def evaluate_paths(obj: Any, paths: list[str]) -> list[Any]:
    """Evaluate multiple path expressions.

    Args:
        obj: Object to evaluate against.
        paths: List of path expressions.

    Returns:
        Combined list of matching values.
    """
    return _default_evaluator.evaluate_all(obj, paths)


def evaluate_path_with_context(obj: Any, path: str) -> list[tuple[Any, dict[str, Any]]]:
    """Evaluate a path and return values with parent context.

    Args:
        obj: Object to evaluate against.
        path: Path expression.

    Returns:
        List of (value, context) tuples.
    """
    return _default_evaluator.evaluate_with_context(obj, path)


def find_activities_by_type(
    automation: dict[str, Any],
    activity_type_id: int,
) -> list[dict[str, Any]]:
    """Find all activities of a specific type in an automation.

    Convenience function for common automation analysis.

    Args:
        automation: Automation object.
        activity_type_id: Activity type ID to find (e.g., 300 for Query).

    Returns:
        List of activity dicts matching the type.
    """
    path = f"steps[].activities[].objectTypeId={activity_type_id}"
    results = _default_evaluator.evaluate_with_context(automation, path)
    return [context for _, context in results]


def extract_dependency_refs(
    obj: dict[str, Any],
    dependency_paths: dict[str, list[str]],
) -> dict[str, list[Any]]:
    """Extract dependency references from an object using registry paths.

    Args:
        obj: Object to analyze.
        dependency_paths: Dict mapping dep type to list of paths.
            Example: {"data_extension": ["targetKey", "queryText"]}

    Returns:
        Dict mapping dep type to list of found references.
    """
    results: dict[str, list[Any]] = {}

    for dep_type, paths in dependency_paths.items():
        refs = evaluate_paths(obj, paths)
        if refs:
            results[dep_type] = refs

    return results
