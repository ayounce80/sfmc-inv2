"""Dependency tree visualization for SFMC objects.

Generates hierarchical dependency views showing what an object uses
(dependencies) or what uses an object (dependents).

Output formats:
- Dictionary tree structure (for JSON export)
- Text tree (for CLI display)
- Flat list (for CSV export)
"""

from dataclasses import dataclass, field
from typing import Any, Optional

from ..types.relationships import RelationshipEdge, RelationshipGraph


@dataclass
class DependencyNode:
    """Node in the dependency tree."""

    id: str
    object_type: str
    name: Optional[str] = None
    relationship_type: Optional[str] = None
    is_shared: bool = False
    from_parent_bu: bool = False
    depth: int = 0
    children: list["DependencyNode"] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "id": self.id,
            "type": self.object_type,
            "name": self.name,
        }

        if self.relationship_type:
            result["relationship"] = self.relationship_type

        if self.is_shared:
            result["isShared"] = True

        if self.from_parent_bu:
            result["fromParentBU"] = True

        if self.children:
            result["dependencies"] = [child.to_dict() for child in self.children]

        return result


class DependencyTreeBuilder:
    """Builds dependency trees from relationship graphs."""

    def __init__(self, graph: RelationshipGraph):
        """Initialize with a relationship graph.

        Args:
            graph: RelationshipGraph containing edges.
        """
        self._graph = graph
        self._edges_by_source: dict[tuple[str, str], list[RelationshipEdge]] = {}
        self._edges_by_target: dict[tuple[str, str], list[RelationshipEdge]] = {}

        # Index edges for fast lookup
        self._index_edges()

    def _index_edges(self) -> None:
        """Index edges by source and target for fast lookup."""
        for edge in self._graph.edges:
            # Index by source
            source_key = (edge.source_id, edge.source_type)
            if source_key not in self._edges_by_source:
                self._edges_by_source[source_key] = []
            self._edges_by_source[source_key].append(edge)

            # Index by target
            target_key = (edge.target_id, edge.target_type)
            if target_key not in self._edges_by_target:
                self._edges_by_target[target_key] = []
            self._edges_by_target[target_key].append(edge)

    def build_dependency_tree(
        self,
        object_id: str,
        object_type: str,
        object_name: Optional[str] = None,
        max_depth: int = 5,
    ) -> DependencyNode:
        """Build a tree showing what this object depends on.

        Args:
            object_id: ID of the root object.
            object_type: Type of the root object.
            object_name: Optional name of the root object.
            max_depth: Maximum depth to traverse.

        Returns:
            DependencyNode tree structure.
        """
        visited: set[tuple[str, str]] = set()
        return self._build_deps_recursive(
            object_id, object_type, object_name, 0, max_depth, visited
        )

    def build_dependent_tree(
        self,
        object_id: str,
        object_type: str,
        object_name: Optional[str] = None,
        max_depth: int = 5,
    ) -> DependencyNode:
        """Build a tree showing what depends on this object.

        Args:
            object_id: ID of the root object.
            object_type: Type of the root object.
            object_name: Optional name of the root object.
            max_depth: Maximum depth to traverse.

        Returns:
            DependencyNode tree structure.
        """
        visited: set[tuple[str, str]] = set()
        return self._build_dependents_recursive(
            object_id, object_type, object_name, 0, max_depth, visited
        )

    def _build_deps_recursive(
        self,
        object_id: str,
        object_type: str,
        object_name: Optional[str],
        depth: int,
        max_depth: int,
        visited: set[tuple[str, str]],
    ) -> DependencyNode:
        """Recursively build dependency tree."""
        node = DependencyNode(
            id=object_id,
            object_type=object_type,
            name=object_name,
            depth=depth,
        )

        key = (object_id, object_type)
        if key in visited or depth >= max_depth:
            return node

        visited.add(key)

        # Find all objects this one depends on (outgoing edges)
        edges = self._edges_by_source.get(key, [])

        for edge in edges:
            metadata = edge.metadata or {}
            child = self._build_deps_recursive(
                edge.target_id,
                edge.target_type,
                edge.target_name,
                depth + 1,
                max_depth,
                visited,
            )
            child.relationship_type = edge.relationship_type.value
            child.is_shared = metadata.get("isShared", False)
            child.from_parent_bu = metadata.get("fromParentBU", False)
            node.children.append(child)

        return node

    def _build_dependents_recursive(
        self,
        object_id: str,
        object_type: str,
        object_name: Optional[str],
        depth: int,
        max_depth: int,
        visited: set[tuple[str, str]],
    ) -> DependencyNode:
        """Recursively build dependent tree."""
        node = DependencyNode(
            id=object_id,
            object_type=object_type,
            name=object_name,
            depth=depth,
        )

        key = (object_id, object_type)
        if key in visited or depth >= max_depth:
            return node

        visited.add(key)

        # Find all objects that depend on this one (incoming edges)
        edges = self._edges_by_target.get(key, [])

        for edge in edges:
            metadata = edge.metadata or {}
            child = self._build_dependents_recursive(
                edge.source_id,
                edge.source_type,
                edge.source_name,
                depth + 1,
                max_depth,
                visited,
            )
            child.relationship_type = edge.relationship_type.value
            child.is_shared = metadata.get("isShared", False)
            child.from_parent_bu = metadata.get("fromParentBU", False)
            node.children.append(child)

        return node

    def to_text_tree(
        self,
        root: DependencyNode,
        indent: str = "  ",
        prefix: str = "",
        is_last: bool = True,
    ) -> str:
        """Convert dependency tree to text representation.

        Args:
            root: Root node of the tree.
            indent: Indentation string.
            prefix: Current line prefix.
            is_last: Whether this is the last sibling.

        Returns:
            Multi-line text representation.
        """
        lines = []

        # Build current node line
        connector = "└── " if is_last else "├── "
        type_badge = f"[{root.object_type}]"
        name_display = root.name or root.id

        flags = []
        if root.is_shared:
            flags.append("shared")
        if root.from_parent_bu:
            flags.append("parent-BU")

        flag_str = f" ({', '.join(flags)})" if flags else ""
        rel_str = f" via {root.relationship_type}" if root.relationship_type else ""

        if root.depth == 0:
            lines.append(f"{type_badge} {name_display}")
        else:
            lines.append(f"{prefix}{connector}{type_badge} {name_display}{rel_str}{flag_str}")

        # Build children
        child_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(root.children):
            child_is_last = i == len(root.children) - 1
            lines.append(self.to_text_tree(child, indent, child_prefix, child_is_last))

        return "\n".join(lines)

    def to_flat_list(
        self,
        root: DependencyNode,
        include_root: bool = True,
    ) -> list[dict[str, Any]]:
        """Convert dependency tree to flat list.

        Useful for CSV export or tabular display.

        Args:
            root: Root node of the tree.
            include_root: Whether to include the root node.

        Returns:
            List of dicts with id, type, name, depth, relationship, etc.
        """
        results = []

        def traverse(node: DependencyNode, parent_id: Optional[str] = None) -> None:
            item = {
                "id": node.id,
                "type": node.object_type,
                "name": node.name,
                "depth": node.depth,
                "relationship": node.relationship_type,
                "is_shared": node.is_shared,
                "from_parent_bu": node.from_parent_bu,
                "parent_id": parent_id,
            }
            results.append(item)

            for child in node.children:
                traverse(child, node.id)

        if include_root:
            traverse(root)
        else:
            for child in root.children:
                traverse(child, root.id)

        return results


def generate_dependency_tree(
    graph: RelationshipGraph,
    object_id: str,
    object_type: str,
    object_name: Optional[str] = None,
    max_depth: int = 5,
) -> dict[str, Any]:
    """Generate a dependency tree for an object.

    Convenience function that returns a dictionary tree structure.

    Args:
        graph: RelationshipGraph to analyze.
        object_id: ID of the object.
        object_type: Type of the object.
        object_name: Optional name of the object.
        max_depth: Maximum depth to traverse.

    Returns:
        Dictionary tree structure showing dependencies.

    Example output:
        {
            "object": {"id": "...", "type": "automation", "name": "My Auto"},
            "dependencies": [
                {
                    "id": "...",
                    "type": "query",
                    "name": "My Query",
                    "relationship": "automation_contains_query",
                    "dependencies": [
                        {"type": "data_extension", "name": "Source_DE", ...}
                    ]
                }
            ]
        }
    """
    builder = DependencyTreeBuilder(graph)
    tree = builder.build_dependency_tree(object_id, object_type, object_name, max_depth)

    return {
        "object": {
            "id": object_id,
            "type": object_type,
            "name": object_name,
        },
        "dependencies": [child.to_dict() for child in tree.children],
    }


def generate_dependent_tree(
    graph: RelationshipGraph,
    object_id: str,
    object_type: str,
    object_name: Optional[str] = None,
    max_depth: int = 5,
) -> dict[str, Any]:
    """Generate a tree showing what depends on an object.

    Args:
        graph: RelationshipGraph to analyze.
        object_id: ID of the object.
        object_type: Type of the object.
        object_name: Optional name of the object.
        max_depth: Maximum depth to traverse.

    Returns:
        Dictionary tree structure showing dependents.
    """
    builder = DependencyTreeBuilder(graph)
    tree = builder.build_dependent_tree(object_id, object_type, object_name, max_depth)

    return {
        "object": {
            "id": object_id,
            "type": object_type,
            "name": object_name,
        },
        "dependents": [child.to_dict() for child in tree.children],
    }


def print_dependency_tree(
    graph: RelationshipGraph,
    object_id: str,
    object_type: str,
    object_name: Optional[str] = None,
    max_depth: int = 5,
) -> str:
    """Generate a text representation of the dependency tree.

    Args:
        graph: RelationshipGraph to analyze.
        object_id: ID of the object.
        object_type: Type of the object.
        object_name: Optional name of the object.
        max_depth: Maximum depth to traverse.

    Returns:
        Text tree representation.
    """
    builder = DependencyTreeBuilder(graph)
    tree = builder.build_dependency_tree(object_id, object_type, object_name, max_depth)
    return builder.to_text_tree(tree)
