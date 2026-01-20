"""Relationship builder for analyzing object dependencies.

Builds a relationship graph from extraction results and identifies
orphaned objects that appear unused.
"""

import logging
import re
from collections import defaultdict
from typing import Any, Optional, Set

from ..types.relationships import (
    OrphanedObject,
    RelationshipEdge,
    RelationshipGraph,
    RelationshipType,
)

logger = logging.getLogger(__name__)


class RelationshipBuilder:
    """Builds and analyzes relationships between SFMC objects."""

    def __init__(self):
        """Initialize the relationship builder."""
        self._graph = RelationshipGraph()
        self._object_index: dict[str, dict[str, Any]] = {}  # type -> {id -> object}
        self._reference_counts: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )  # type -> {id -> count}

    @property
    def graph(self) -> RelationshipGraph:
        """Get the relationship graph."""
        return self._graph

    def index_objects(
        self,
        objects: list[dict[str, Any]],
        object_type: str,
        id_field: str = "id",
    ) -> None:
        """Index objects for relationship lookups.

        Args:
            objects: List of objects to index.
            object_type: Type of objects (e.g., "data_extension").
            id_field: Field name containing the object ID.
        """
        if object_type not in self._object_index:
            self._object_index[object_type] = {}

        for obj in objects:
            obj_id = obj.get(id_field)
            if obj_id:
                self._object_index[object_type][str(obj_id)] = obj

    def add_edge(
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
        """Add a relationship edge to the graph.

        Also tracks reference counts for orphan detection.
        """
        self._graph.add_edge(
            source_id=source_id,
            source_type=source_type,
            source_name=source_name,
            target_id=target_id,
            target_type=target_type,
            target_name=target_name,
            relationship_type=relationship_type,
            metadata=metadata,
        )

        # Track that target is referenced
        self._reference_counts[target_type][target_id] += 1

    def merge_edges(self, edges: list[RelationshipEdge]) -> None:
        """Merge edges from extraction results.

        Args:
            edges: List of relationship edges to merge.
        """
        for edge in edges:
            self._graph.edges.append(edge)
            self._reference_counts[edge.target_type][edge.target_id] += 1

    def find_orphans(
        self,
        object_type: str,
        must_be_referenced_by: Optional[list[str]] = None,
    ) -> list[OrphanedObject]:
        """Find objects that appear to be orphaned (unreferenced).

        Args:
            object_type: Type of objects to check for orphans.
            must_be_referenced_by: Optional list of source types that should
                reference this object type for it to be considered used.

        Returns:
            List of orphaned objects.
        """
        orphans = []
        objects = self._object_index.get(object_type, {})

        for obj_id, obj in objects.items():
            ref_count = self._reference_counts[object_type].get(obj_id, 0)

            if ref_count == 0:
                # Check if it's referenced by required types
                is_orphan = True

                if must_be_referenced_by:
                    # Check edges for any reference from required types
                    for edge in self._graph.edges:
                        if (
                            edge.target_id == obj_id
                            and edge.target_type == object_type
                            and edge.source_type in must_be_referenced_by
                        ):
                            is_orphan = False
                            break

                if is_orphan:
                    orphans.append(
                        OrphanedObject(
                            id=obj_id,
                            object_type=object_type,
                            name=obj.get("name", "Unknown"),
                            folder_path=obj.get("folderPath"),
                            reason="Not referenced by any other object",
                            last_modified=obj.get("modifiedDate"),
                        )
                    )

        return orphans

    def detect_all_orphans(self) -> None:
        """Detect orphans across all indexed object types.

        Updates the graph's orphans list.
        """
        # Define which object types should be referenced
        orphan_rules = {
            "query": ["automation"],  # Queries should be used in automations
            "data_extension": ["automation", "query", "journey", "import"],
        }

        for object_type, required_refs in orphan_rules.items():
            orphans = self.find_orphans(object_type, required_refs)
            for orphan in orphans:
                self._graph.add_orphan(
                    id=orphan.id,
                    object_type=orphan.object_type,
                    name=orphan.name,
                    reason=orphan.reason,
                    folder_path=orphan.folder_path,
                    last_modified=orphan.last_modified,
                )

    def analyze_sql_dependencies(
        self,
        sql: str,
        source_id: str,
        source_name: str,
    ) -> list[str]:
        """Analyze SQL query to find Data Extension dependencies.

        Args:
            sql: SQL query text.
            source_id: ID of the query.
            source_name: Name of the query.

        Returns:
            List of referenced DE names.
        """
        de_names: Set[str] = set()

        # Pattern for FROM/JOIN clauses
        patterns = [
            r"\bFROM\s+\[?([^\s\[\],]+)\]?",
            r"\bJOIN\s+\[?([^\s\[\],]+)\]?",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, sql, re.IGNORECASE):
                name = match.group(1).strip()
                if name and not self._is_system_table(name):
                    de_names.add(name)

                    # Add edge (by name since we may not have ID)
                    self.add_edge(
                        source_id=source_id,
                        source_type="query",
                        source_name=source_name,
                        target_id=name,  # Using name as ID
                        target_type="data_extension",
                        target_name=name,
                        relationship_type=RelationshipType.QUERY_READS_DE,
                        metadata={"resolved_by_name": True},
                    )

        return list(de_names)

    def _is_system_table(self, name: str) -> bool:
        """Check if a table name is a system table."""
        name_lower = name.lower()
        return (
            name_lower.startswith("_")
            or name_lower.startswith("sys")
            or name_lower in {"dual", "subscribers", "subscriberattributes"}
        )

    def get_dependencies_for(
        self, object_id: str, object_type: str
    ) -> list[RelationshipEdge]:
        """Get all objects that a given object depends on.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            List of edges where this object is the source.
        """
        return [
            edge
            for edge in self._graph.edges
            if edge.source_id == object_id and edge.source_type == object_type
        ]

    def get_dependents_for(
        self, object_id: str, object_type: str
    ) -> list[RelationshipEdge]:
        """Get all objects that depend on a given object.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            List of edges where this object is the target.
        """
        return [
            edge
            for edge in self._graph.edges
            if edge.target_id == object_id and edge.target_type == object_type
        ]

    def calculate_stats(self) -> None:
        """Calculate and update graph statistics."""
        self._graph.calculate_stats()

    def to_dict(self) -> dict[str, Any]:
        """Export the graph as a dictionary."""
        return self._graph.model_dump()
