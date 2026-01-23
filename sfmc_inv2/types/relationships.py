"""Relationship graph models for tracking object dependencies."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class RelationshipType(str, Enum):
    """Types of relationships between SFMC objects."""

    # Automation relationships
    AUTOMATION_CONTAINS_QUERY = "automation_contains_query"
    AUTOMATION_CONTAINS_SCRIPT = "automation_contains_script"
    AUTOMATION_CONTAINS_IMPORT = "automation_contains_import"
    AUTOMATION_CONTAINS_EXTRACT = "automation_contains_extract"
    AUTOMATION_CONTAINS_TRANSFER = "automation_contains_transfer"
    AUTOMATION_CONTAINS_EMAIL = "automation_contains_email"
    AUTOMATION_CONTAINS_FILTER = "automation_contains_filter"
    AUTOMATION_CONTAINS_FIRE_EVENT = "automation_contains_fire_event"
    AUTOMATION_CONTAINS_SMS = "automation_contains_sms"
    AUTOMATION_CONTAINS_VERIFICATION = "automation_contains_verification"
    AUTOMATION_CONTAINS_WAIT = "automation_contains_wait"
    AUTOMATION_CONTAINS_REFRESH_GROUP = "automation_contains_refresh_group"
    AUTOMATION_CONTAINS_JOURNEY_ENTRY = "automation_contains_journey_entry"
    AUTOMATION_CONTAINS_SALESFORCE_SEND = "automation_contains_salesforce_send"
    AUTOMATION_CONTAINS_PUSH = "automation_contains_push"

    # Query relationships
    QUERY_READS_DE = "query_reads_de"
    QUERY_WRITES_DE = "query_writes_de"

    # Journey relationships
    JOURNEY_USES_DE = "journey_uses_de"
    JOURNEY_USES_EMAIL = "journey_uses_email"
    JOURNEY_USES_FILTER = "journey_uses_filter"
    JOURNEY_USES_AUTOMATION = "journey_uses_automation"
    JOURNEY_USES_EVENT = "journey_uses_event"
    JOURNEY_USES_SENDER_PROFILE = "journey_uses_sender_profile"
    JOURNEY_USES_DELIVERY_PROFILE = "journey_uses_delivery_profile"
    JOURNEY_USES_SEND_CLASSIFICATION = "journey_uses_send_classification"
    JOURNEY_USES_SMS = "journey_uses_sms"

    # Import relationships
    IMPORT_WRITES_DE = "import_writes_de"
    IMPORT_READS_FILE = "import_reads_file"

    # Data Extract relationships
    EXTRACT_READS_DE = "extract_reads_de"
    EXTRACT_WRITES_FILE = "extract_writes_file"

    # Filter relationships
    FILTER_READS_DE = "filter_reads_de"
    FILTER_WRITES_DE = "filter_writes_de"

    # Email/Asset relationships
    EMAIL_USES_DE = "email_uses_de"
    EMAIL_USES_CONTENT_BLOCK = "email_uses_content_block"
    CONTENT_BLOCK_USES_DE = "content_block_uses_de"
    ASSET_USES_CONTENT_BLOCK = "asset_uses_content_block"

    # Triggered Send relationships
    TRIGGERED_SEND_USES_EMAIL = "triggered_send_uses_email"
    TRIGGERED_SEND_USES_LIST = "triggered_send_uses_list"
    TRIGGERED_SEND_USES_SENDER_PROFILE = "triggered_send_uses_sender_profile"
    TRIGGERED_SEND_USES_DELIVERY_PROFILE = "triggered_send_uses_delivery_profile"
    TRIGGERED_SEND_USES_SEND_CLASSIFICATION = "triggered_send_uses_send_classification"

    # Send Classification relationships
    SEND_CLASSIFICATION_USES_SENDER_PROFILE = "send_classification_uses_sender_profile"
    SEND_CLASSIFICATION_USES_DELIVERY_PROFILE = "send_classification_uses_delivery_profile"

    # Event Definition relationships
    EVENT_DEFINITION_USES_DE = "event_definition_uses_de"

    # Folder relationships
    FOLDER_CONTAINS_FOLDER = "folder_contains_folder"

    # Script relationships
    SCRIPT_USES_DE = "script_uses_de"

    # CloudPage relationships (AMPscript DE references)
    CLOUDPAGE_WRITES_DE = "cloudpage_writes_de"
    CLOUDPAGE_READS_DE = "cloudpage_reads_de"

    # Generic
    REFERENCES = "references"
    CONTAINS = "contains"
    DEPENDS_ON = "depends_on"


class RelationshipEdge(BaseModel):
    """Edge in the relationship graph."""

    source_id: str = Field(description="Source object ID")
    source_type: str = Field(description="Source object type (e.g., 'automation')")
    source_name: Optional[str] = Field(default=None, description="Source object name")
    target_id: str = Field(description="Target object ID")
    target_type: str = Field(description="Target object type (e.g., 'query')")
    target_name: Optional[str] = Field(default=None, description="Target object name")
    relationship_type: RelationshipType = Field(description="Type of relationship")
    metadata: Optional[dict[str, Any]] = Field(default=None, description="Additional context")

    model_config = {"extra": "allow"}


class OrphanedObject(BaseModel):
    """Object that appears to be unused/orphaned."""

    id: str = Field(description="Object ID")
    object_type: str = Field(description="Object type")
    name: str = Field(description="Object name")
    folder_path: Optional[str] = Field(default=None)
    reason: str = Field(description="Why this object is considered orphaned")
    last_modified: Optional[str] = Field(default=None)


class RelationshipStats(BaseModel):
    """Statistics about the relationship graph."""

    total_edges: int = Field(default=0)
    total_nodes: int = Field(default=0)
    orphaned_count: int = Field(default=0)
    by_relationship_type: dict[str, int] = Field(default_factory=dict)
    by_source_type: dict[str, int] = Field(default_factory=dict)
    by_target_type: dict[str, int] = Field(default_factory=dict)
    most_connected: list[dict[str, Any]] = Field(
        default_factory=list, description="Top objects by connection count"
    )


class RelationshipGraph(BaseModel):
    """Complete relationship graph for the inventory."""

    edges: list[RelationshipEdge] = Field(default_factory=list)
    orphans: list[OrphanedObject] = Field(default_factory=list)
    stats: RelationshipStats = Field(default_factory=RelationshipStats)

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
        """Add an edge to the graph."""
        edge = RelationshipEdge(
            source_id=source_id,
            source_type=source_type,
            source_name=source_name,
            target_id=target_id,
            target_type=target_type,
            target_name=target_name,
            relationship_type=relationship_type,
            metadata=metadata,
        )
        self.edges.append(edge)

    def add_orphan(
        self,
        id: str,
        object_type: str,
        name: str,
        reason: str,
        folder_path: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> None:
        """Add an orphaned object to the graph."""
        orphan = OrphanedObject(
            id=id,
            object_type=object_type,
            name=name,
            reason=reason,
            folder_path=folder_path,
            last_modified=last_modified,
        )
        self.orphans.append(orphan)

    def get_edges_for_object(self, object_id: str) -> list[RelationshipEdge]:
        """Get all edges involving a specific object."""
        return [
            edge
            for edge in self.edges
            if edge.source_id == object_id or edge.target_id == object_id
        ]

    def get_dependents(self, object_id: str) -> list[RelationshipEdge]:
        """Get all objects that depend on this object."""
        return [edge for edge in self.edges if edge.target_id == object_id]

    def get_dependencies(self, object_id: str) -> list[RelationshipEdge]:
        """Get all objects this object depends on."""
        return [edge for edge in self.edges if edge.source_id == object_id]

    def calculate_stats(self) -> None:
        """Recalculate graph statistics."""
        node_ids = set()
        by_type: dict[str, int] = {}
        by_source: dict[str, int] = {}
        by_target: dict[str, int] = {}
        connection_counts: dict[str, int] = {}

        for edge in self.edges:
            node_ids.add(edge.source_id)
            node_ids.add(edge.target_id)

            rel_type = edge.relationship_type.value
            by_type[rel_type] = by_type.get(rel_type, 0) + 1
            by_source[edge.source_type] = by_source.get(edge.source_type, 0) + 1
            by_target[edge.target_type] = by_target.get(edge.target_type, 0) + 1

            connection_counts[edge.source_id] = connection_counts.get(edge.source_id, 0) + 1
            connection_counts[edge.target_id] = connection_counts.get(edge.target_id, 0) + 1

        # Find most connected nodes
        sorted_nodes = sorted(connection_counts.items(), key=lambda x: x[1], reverse=True)
        most_connected = [
            {"id": node_id, "connection_count": count} for node_id, count in sorted_nodes[:10]
        ]

        self.stats = RelationshipStats(
            total_edges=len(self.edges),
            total_nodes=len(node_ids),
            orphaned_count=len(self.orphans),
            by_relationship_type=by_type,
            by_source_type=by_source,
            by_target_type=by_target,
            most_connected=most_connected,
        )
