"""Relationship builder for analyzing object dependencies.

Builds a relationship graph from extraction results and identifies
orphaned objects that appear unused.

Multi-BU Support:
- Tracks shared resources via `fromParentBU` metadata on edges
- Detects shared DEs by ENT. prefix and folder path conventions
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


# Patterns indicating shared/enterprise resources
SHARED_PREFIXES = ("ENT.", "_ENT.", "Shared_", "Enterprise_")
SHARED_FOLDER_KEYWORDS = ("shared", "enterprise", "parent", "global")


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
        # Define which object types should be referenced and by what
        # Key = object type, Value = list of source types that should reference it
        orphan_rules = {
            # Automation activities - should be used in automations
            "query": ["automation"],
            "script": ["automation"],
            "import": ["automation"],
            "data_extract": ["automation"],
            "file_transfer": ["automation"],
            "filter": ["automation", "journey"],

            # Data extensions - widely used across the platform
            "data_extension": [
                "automation", "query", "journey", "import", "filter",
                "data_extract", "event_definition", "triggered_send",
            ],

            # Messaging infrastructure
            "email": ["automation", "journey", "triggered_send"],
            "classic_email": ["automation", "journey", "triggered_send"],
            "asset": ["email", "asset", "journey"],  # Content blocks used in emails/other assets
            "content_block": ["email", "asset"],

            # Triggered send dependencies
            "list": ["triggered_send", "journey"],
            "sender_profile": ["send_classification", "triggered_send"],
            "delivery_profile": ["send_classification", "triggered_send"],
            "send_classification": ["triggered_send"],

            # Journey entry events
            "event_definition": ["journey"],
        }

        for object_type, required_refs in orphan_rules.items():
            if object_type not in self._object_index:
                continue  # Skip if no objects of this type were indexed

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
        source_account_id: Optional[str] = None,
    ) -> list[str]:
        """Analyze SQL query to find Data Extension dependencies.

        Detects shared/enterprise DEs by naming conventions and adds
        appropriate metadata to relationship edges.

        Args:
            sql: SQL query text.
            source_id: ID of the query.
            source_name: Name of the query.
            source_account_id: Account ID of the source object (for BU tracking).

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

                    # Detect if this is a shared/enterprise DE
                    is_shared = self._is_shared_resource_name(name)

                    # Build metadata
                    metadata: dict[str, Any] = {"resolved_by_name": True}
                    if is_shared:
                        metadata["isShared"] = True
                        metadata["fromParentBU"] = True

                    if source_account_id:
                        metadata["sourceAccountId"] = source_account_id

                    # Add edge (by name since we may not have ID)
                    self.add_edge(
                        source_id=source_id,
                        source_type="query",
                        source_name=source_name,
                        target_id=name,  # Using name as ID
                        target_type="data_extension",
                        target_name=name,
                        relationship_type=RelationshipType.QUERY_READS_DE,
                        metadata=metadata,
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

    def _is_shared_resource_name(self, name: str) -> bool:
        """Check if a resource name indicates a shared/enterprise resource.

        Detects shared resources by common SFMC naming conventions:
        - ENT. prefix (standard SFMC convention for enterprise DEs)
        - _ENT. prefix (alternative convention)
        - Shared_ or Enterprise_ prefixes (custom conventions)

        Args:
            name: Resource name to check.

        Returns:
            True if the name indicates a shared resource.
        """
        if not name:
            return False

        # Check for known shared prefixes
        for prefix in SHARED_PREFIXES:
            if name.startswith(prefix):
                return True

        return False

    def _is_shared_resource(self, item: dict[str, Any]) -> bool:
        """Check if an item is a shared resource from parent BU.

        Detects shared resources by:
        1. `_fromParentBU` flag (set by cache manager)
        2. ENT. prefix in name (SFMC convention)
        3. Shared folder keywords in path

        Args:
            item: Item dictionary to check.

        Returns:
            True if the item appears to be a shared resource.
        """
        # Check explicit flag
        if item.get("_fromParentBU"):
            return True

        # Check name for shared prefixes
        name = item.get("name", "")
        if self._is_shared_resource_name(name):
            return True

        # Check folder path for shared indicators
        folder_path = item.get("folderPath", "")
        if folder_path:
            path_lower = folder_path.lower()
            for keyword in SHARED_FOLDER_KEYWORDS:
                if keyword in path_lower:
                    return True

        return False

    def add_edge_with_bu_tracking(
        self,
        source_id: str,
        source_type: str,
        target_id: str,
        target_type: str,
        relationship_type: RelationshipType,
        source_name: Optional[str] = None,
        target_name: Optional[str] = None,
        target_item: Optional[dict[str, Any]] = None,
        source_account_id: Optional[str] = None,
        additional_metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add a relationship edge with automatic BU tracking.

        This method automatically detects shared resources and adds
        appropriate metadata to the edge.

        Args:
            source_id: Source object ID.
            source_type: Source object type.
            target_id: Target object ID.
            target_type: Target object type.
            relationship_type: Type of relationship.
            source_name: Optional source object name.
            target_name: Optional target object name.
            target_item: Optional full target item dict for shared detection.
            source_account_id: Account ID of source object.
            additional_metadata: Additional metadata to include.
        """
        metadata: dict[str, Any] = additional_metadata.copy() if additional_metadata else {}

        # Detect shared resource
        is_shared = False
        from_parent_bu = False

        if target_item:
            if self._is_shared_resource(target_item):
                is_shared = True
                from_parent_bu = target_item.get("_fromParentBU", False)
        elif target_name:
            is_shared = self._is_shared_resource_name(target_name)
            from_parent_bu = is_shared  # Assume from parent if shared by name

        if is_shared:
            metadata["isShared"] = True
        if from_parent_bu:
            metadata["fromParentBU"] = True
        if source_account_id:
            metadata["sourceAccountId"] = source_account_id

        self.add_edge(
            source_id=source_id,
            source_type=source_type,
            source_name=source_name,
            target_id=target_id,
            target_type=target_type,
            target_name=target_name,
            relationship_type=relationship_type,
            metadata=metadata if metadata else None,
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

    def get_objects_used_by(self, source_type: str) -> set[str]:
        """Get all object IDs that are targets of a given source type.

        Args:
            source_type: Type of source objects (e.g., "automation").

        Returns:
            Set of target object IDs used by the source type.
        """
        used_ids = set()
        for edge in self._graph.edges:
            if edge.source_type == source_type:
                used_ids.add(edge.target_id)
        return used_ids

    def get_objects_not_used_by(
        self,
        object_type: str,
        source_types: list[str],
    ) -> list[dict[str, Any]]:
        """Find objects not referenced by any of the specified source types.

        Args:
            object_type: Type of objects to check.
            source_types: List of source types that should reference the objects.

        Returns:
            List of objects not used by any of the source types.
        """
        # Get all target IDs from the specified source types
        used_ids: set[str] = set()
        for edge in self._graph.edges:
            if edge.source_type in source_types and edge.target_type == object_type:
                used_ids.add(edge.target_id)

        # Find objects not in the used set
        objects = self._object_index.get(object_type, {})
        not_used = []
        for obj_id, obj in objects.items():
            if obj_id not in used_ids:
                not_used.append(obj)
        return not_used

    def get_usage_count(self, object_id: str, object_type: str) -> int:
        """Count how many times an object is referenced as a target.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            Number of times the object is referenced.
        """
        return self._reference_counts[object_type].get(object_id, 0)

    def get_sources_for_target(
        self,
        target_id: str,
        target_type: str,
        source_type: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Get all source objects that reference a target object.

        Args:
            target_id: ID of the target object.
            target_type: Type of the target object.
            source_type: Optional filter for source type.

        Returns:
            List of dicts with source info (id, type, name).
        """
        sources = []
        for edge in self._graph.edges:
            if edge.target_id == target_id and edge.target_type == target_type:
                if source_type is None or edge.source_type == source_type:
                    sources.append({
                        "id": edge.source_id,
                        "type": edge.source_type,
                        "name": edge.source_name,
                        "relationship": edge.relationship_type.value,
                    })
        return sources

    def get_object_by_id(
        self,
        object_id: str,
        object_type: str,
    ) -> Optional[dict[str, Any]]:
        """Get an indexed object by ID and type.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            Object dict or None if not found.
        """
        return self._object_index.get(object_type, {}).get(object_id)

    def get_all_objects(self, object_type: str) -> list[dict[str, Any]]:
        """Get all indexed objects of a given type.

        Args:
            object_type: Type of objects to retrieve.

        Returns:
            List of all objects of that type.
        """
        return list(self._object_index.get(object_type, {}).values())

    def generate_deletion_impact_report(
        self,
        object_id: str,
        object_type: str,
        max_depth: int = 3,
    ) -> dict[str, Any]:
        """Generate a report showing what would be affected if an object were deleted.

        Performs a recursive traversal to find all objects that depend on the
        target object, directly or transitively.

        Args:
            object_id: ID of the object to analyze.
            object_type: Type of the object.
            max_depth: Maximum depth to traverse (default 3).

        Returns:
            Report dict with:
            - object: The target object info
            - direct_dependents: Objects directly depending on this object
            - transitive_dependents: Objects indirectly affected (by depth level)
            - summary: Counts by object type
        """
        # Get the target object
        target_obj = self.get_object_by_id(object_id, object_type)

        report = {
            "object": {
                "id": object_id,
                "type": object_type,
                "name": target_obj.get("name") if target_obj else None,
            },
            "direct_dependents": [],
            "transitive_dependents": {},
            "summary": {
                "total_affected": 0,
                "by_type": {},
                "by_depth": {},
            },
        }

        # Track visited to avoid cycles
        visited: set[tuple[str, str]] = set()
        visited.add((object_id, object_type))

        # Find direct dependents
        direct_edges = self.get_dependents_for(object_id, object_type)
        for edge in direct_edges:
            dependent_info = {
                "id": edge.source_id,
                "type": edge.source_type,
                "name": edge.source_name,
                "relationship": edge.relationship_type.value,
            }
            report["direct_dependents"].append(dependent_info)
            visited.add((edge.source_id, edge.source_type))

        # Track counts
        report["summary"]["by_depth"]["1"] = len(direct_edges)

        # Find transitive dependents (up to max_depth)
        current_level = [(e.source_id, e.source_type, e.source_name) for e in direct_edges]

        for depth in range(2, max_depth + 1):
            next_level = []
            depth_key = str(depth)
            report["transitive_dependents"][depth_key] = []

            for src_id, src_type, src_name in current_level:
                edges = self.get_dependents_for(src_id, src_type)
                for edge in edges:
                    key = (edge.source_id, edge.source_type)
                    if key not in visited:
                        visited.add(key)
                        dependent_info = {
                            "id": edge.source_id,
                            "type": edge.source_type,
                            "name": edge.source_name,
                            "relationship": edge.relationship_type.value,
                            "via": {
                                "id": src_id,
                                "type": src_type,
                                "name": src_name,
                            },
                        }
                        report["transitive_dependents"][depth_key].append(dependent_info)
                        next_level.append((edge.source_id, edge.source_type, edge.source_name))

            report["summary"]["by_depth"][depth_key] = len(report["transitive_dependents"][depth_key])
            current_level = next_level

            if not current_level:
                break

        # Calculate summary
        all_affected: list[dict[str, Any]] = list(report["direct_dependents"])
        for depth_items in report["transitive_dependents"].values():
            all_affected.extend(depth_items)

        report["summary"]["total_affected"] = len(all_affected)

        # Count by type
        type_counts: dict[str, int] = {}
        for item in all_affected:
            obj_type = item["type"]
            type_counts[obj_type] = type_counts.get(obj_type, 0) + 1
        report["summary"]["by_type"] = type_counts

        return report

    def get_deletion_impact_summary(
        self,
        object_id: str,
        object_type: str,
    ) -> str:
        """Get a human-readable summary of deletion impact.

        Args:
            object_id: ID of the object to analyze.
            object_type: Type of the object.

        Returns:
            Formatted string summarizing the impact.
        """
        report = self.generate_deletion_impact_report(object_id, object_type)

        lines = []
        obj_name = report["object"]["name"] or object_id
        lines.append(f"Deletion Impact: {object_type} '{obj_name}'")
        lines.append("=" * 50)

        total = report["summary"]["total_affected"]
        if total == 0:
            lines.append("No other objects depend on this object.")
            lines.append("This object can be safely deleted.")
        else:
            lines.append(f"Total affected objects: {total}")
            lines.append("")
            lines.append("By type:")
            for obj_type, count in sorted(report["summary"]["by_type"].items()):
                lines.append(f"  - {obj_type}: {count}")
            lines.append("")
            lines.append("By dependency depth:")
            for depth, count in sorted(report["summary"]["by_depth"].items()):
                if count > 0:
                    depth_label = "direct" if depth == "1" else f"depth {depth}"
                    lines.append(f"  - {depth_label}: {count}")
            lines.append("")
            lines.append("Direct dependents:")
            for dep in report["direct_dependents"][:10]:  # Limit to first 10
                lines.append(f"  - [{dep['type']}] {dep['name'] or dep['id']}")
            if len(report["direct_dependents"]) > 10:
                lines.append(f"  ... and {len(report['direct_dependents']) - 10} more")

        return "\n".join(lines)
