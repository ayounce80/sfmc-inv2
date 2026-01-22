"""Cross-BU impact analysis for shared resources.

Analyzes shared resource usage across business units in Enterprise 2.0
accounts. Identifies:
- Shared resources from parent BU
- Which child BUs reference each shared resource
- Impact if shared resources were modified/deleted
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

from ..types.relationships import RelationshipEdge, RelationshipGraph


@dataclass
class SharedResource:
    """A resource shared from parent BU."""

    id: str
    object_type: str
    name: Optional[str] = None
    parent_account_id: Optional[str] = None
    referencing_bus: set[str] = field(default_factory=set)
    reference_count: int = 0
    references: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "type": self.object_type,
            "name": self.name,
            "parentAccountId": self.parent_account_id,
            "referencingBUs": list(self.referencing_bus),
            "referenceCount": self.reference_count,
            "references": self.references,
        }


@dataclass
class CrossBUReport:
    """Report on cross-BU resource usage."""

    parent_account_id: Optional[str] = None
    child_account_ids: list[str] = field(default_factory=list)
    shared_resources: list[SharedResource] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "parentAccountId": self.parent_account_id,
            "childAccountIds": self.child_account_ids,
            "sharedResources": [r.to_dict() for r in self.shared_resources],
            "summary": self.summary,
        }


class CrossBUAnalyzer:
    """Analyzes cross-BU resource usage from relationship data."""

    # Patterns indicating shared resources
    SHARED_PREFIXES = ("ENT.", "_ENT.", "Shared_", "Enterprise_")
    SHARED_FOLDER_KEYWORDS = ("shared", "enterprise", "parent", "global")

    def __init__(self, graph: RelationshipGraph):
        """Initialize with a relationship graph.

        Args:
            graph: RelationshipGraph containing edges with BU metadata.
        """
        self._graph = graph

    def analyze(
        self,
        parent_account_id: Optional[str] = None,
        child_account_ids: Optional[list[str]] = None,
    ) -> CrossBUReport:
        """Analyze cross-BU resource usage.

        Identifies shared resources and tracks which BUs reference them.

        Args:
            parent_account_id: Parent BU MID (optional, detected from edges).
            child_account_ids: List of child BU MIDs (optional).

        Returns:
            CrossBUReport with shared resource analysis.
        """
        report = CrossBUReport(
            parent_account_id=parent_account_id,
            child_account_ids=child_account_ids or [],
        )

        # Track shared resources
        shared_resources: dict[tuple[str, str], SharedResource] = {}

        # Analyze edges for shared resource references
        for edge in self._graph.edges:
            metadata = edge.metadata or {}

            # Check if target is a shared resource
            is_shared = (
                metadata.get("isShared", False)
                or metadata.get("fromParentBU", False)
                or self._is_shared_by_name(edge.target_name)
            )

            if is_shared:
                key = (edge.target_id, edge.target_type)

                if key not in shared_resources:
                    shared_resources[key] = SharedResource(
                        id=edge.target_id,
                        object_type=edge.target_type,
                        name=edge.target_name,
                        parent_account_id=metadata.get("_parentAccountId", parent_account_id),
                    )

                resource = shared_resources[key]
                resource.reference_count += 1

                # Track which BU made this reference
                source_bu = metadata.get("sourceAccountId")
                if source_bu:
                    resource.referencing_bus.add(source_bu)

                # Track reference details
                resource.references.append({
                    "sourceId": edge.source_id,
                    "sourceType": edge.source_type,
                    "sourceName": edge.source_name,
                    "relationship": edge.relationship_type.value,
                    "sourceAccountId": source_bu,
                })

        # Convert to list and sort by reference count
        report.shared_resources = sorted(
            shared_resources.values(),
            key=lambda r: r.reference_count,
            reverse=True,
        )

        # Build summary
        report.summary = self._build_summary(report)

        return report

    def _is_shared_by_name(self, name: Optional[str]) -> bool:
        """Check if a resource name indicates it's shared."""
        if not name:
            return False

        for prefix in self.SHARED_PREFIXES:
            if name.startswith(prefix):
                return True

        return False

    def _build_summary(self, report: CrossBUReport) -> dict[str, Any]:
        """Build summary statistics for the report."""
        # Count by type
        by_type: dict[str, int] = defaultdict(int)
        for resource in report.shared_resources:
            by_type[resource.object_type] += 1

        # Most referenced resources
        top_referenced = [
            {
                "id": r.id,
                "type": r.object_type,
                "name": r.name,
                "referenceCount": r.reference_count,
            }
            for r in report.shared_resources[:10]
        ]

        # BUs with most shared resource usage
        bu_usage: dict[str, int] = defaultdict(int)
        for resource in report.shared_resources:
            for bu in resource.referencing_bus:
                bu_usage[bu] += 1

        bu_ranking = sorted(
            [{"accountId": bu, "sharedResourceCount": count} for bu, count in bu_usage.items()],
            key=lambda x: x["sharedResourceCount"],
            reverse=True,
        )

        return {
            "totalSharedResources": len(report.shared_resources),
            "totalReferences": sum(r.reference_count for r in report.shared_resources),
            "byType": dict(by_type),
            "topReferenced": top_referenced,
            "buUsageRanking": bu_ranking,
        }

    def generate_impact_report(
        self,
        resource_id: str,
        resource_type: str,
    ) -> dict[str, Any]:
        """Generate impact report for a specific shared resource.

        Shows what would be affected if the shared resource were
        modified or deleted.

        Args:
            resource_id: ID of the shared resource.
            resource_type: Type of the shared resource.

        Returns:
            Impact report dictionary.
        """
        # Find all edges where this resource is the target
        affected_edges = [
            edge for edge in self._graph.edges
            if edge.target_id == resource_id and edge.target_type == resource_type
        ]

        # Group by source type and BU
        by_source_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_bu: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for edge in affected_edges:
            metadata = edge.metadata or {}
            ref_info = {
                "id": edge.source_id,
                "type": edge.source_type,
                "name": edge.source_name,
                "relationship": edge.relationship_type.value,
            }

            by_source_type[edge.source_type].append(ref_info)

            bu = metadata.get("sourceAccountId", "unknown")
            by_bu[bu].append(ref_info)

        # Build resource info
        resource_info = {
            "id": resource_id,
            "type": resource_type,
        }

        # Try to get name from edges
        for edge in affected_edges:
            if edge.target_name:
                resource_info["name"] = edge.target_name
                break

        return {
            "resource": resource_info,
            "totalAffected": len(affected_edges),
            "bySourceType": {
                stype: {
                    "count": len(refs),
                    "references": refs,
                }
                for stype, refs in by_source_type.items()
            },
            "byBusinessUnit": {
                bu: {
                    "count": len(refs),
                    "references": refs,
                }
                for bu, refs in by_bu.items()
            },
            "impactSummary": self._generate_impact_summary(by_source_type, by_bu),
        }

    def _generate_impact_summary(
        self,
        by_source_type: dict[str, list[dict[str, Any]]],
        by_bu: dict[str, list[dict[str, Any]]],
    ) -> str:
        """Generate human-readable impact summary."""
        lines = []

        total = sum(len(refs) for refs in by_source_type.values())

        if total == 0:
            return "This resource has no known dependencies. Safe to modify."

        lines.append(f"Modifying this resource would affect {total} object(s):")
        lines.append("")

        # By type summary
        lines.append("By object type:")
        for stype, refs in sorted(by_source_type.items(), key=lambda x: len(x[1]), reverse=True):
            lines.append(f"  - {stype}: {len(refs)}")

        # By BU summary
        if len(by_bu) > 1:
            lines.append("")
            lines.append("Across business units:")
            for bu, refs in sorted(by_bu.items(), key=lambda x: len(x[1]), reverse=True):
                bu_display = bu if bu != "unknown" else "Unknown/Current BU"
                lines.append(f"  - {bu_display}: {len(refs)}")

        return "\n".join(lines)


def generate_cross_bu_report(
    graph: RelationshipGraph,
    parent_account_id: Optional[str] = None,
    child_account_ids: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Generate a cross-BU impact report.

    Convenience function that returns a dictionary report.

    Args:
        graph: RelationshipGraph to analyze.
        parent_account_id: Parent BU MID (optional).
        child_account_ids: List of child BU MIDs (optional).

    Returns:
        Dictionary report of shared resource usage.

    Example output:
        {
            "parentAccountId": "12345",
            "childAccountIds": ["67890", "11111"],
            "sharedResources": [
                {
                    "id": "DE_123",
                    "type": "data_extension",
                    "name": "ENT.Subscribers",
                    "referencingBUs": ["67890", "11111"],
                    "referenceCount": 15,
                    "references": [...]
                }
            ],
            "summary": {
                "totalSharedResources": 10,
                "totalReferences": 45,
                "byType": {"data_extension": 8, "email": 2},
                "topReferenced": [...],
                "buUsageRanking": [...]
            }
        }
    """
    analyzer = CrossBUAnalyzer(graph)
    report = analyzer.analyze(parent_account_id, child_account_ids)
    return report.to_dict()


def get_shared_resource_impact(
    graph: RelationshipGraph,
    resource_id: str,
    resource_type: str,
) -> dict[str, Any]:
    """Get impact analysis for a specific shared resource.

    Args:
        graph: RelationshipGraph to analyze.
        resource_id: ID of the shared resource.
        resource_type: Type of the shared resource.

    Returns:
        Impact report dictionary.
    """
    analyzer = CrossBUAnalyzer(graph)
    return analyzer.generate_impact_report(resource_id, resource_type)


def list_shared_resources(
    graph: RelationshipGraph,
    resource_type: Optional[str] = None,
    min_reference_count: int = 0,
) -> list[dict[str, Any]]:
    """List all shared resources from the relationship graph.

    Args:
        graph: RelationshipGraph to analyze.
        resource_type: Optional filter by resource type.
        min_reference_count: Minimum reference count filter.

    Returns:
        List of shared resource summaries.
    """
    analyzer = CrossBUAnalyzer(graph)
    report = analyzer.analyze()

    results = []
    for resource in report.shared_resources:
        if resource_type and resource.object_type != resource_type:
            continue
        if resource.reference_count < min_reference_count:
            continue

        results.append({
            "id": resource.id,
            "type": resource.object_type,
            "name": resource.name,
            "referenceCount": resource.reference_count,
            "referencingBUs": list(resource.referencing_bus),
        })

    return results
