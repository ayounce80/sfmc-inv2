"""Output handling for inventory snapshots."""

from .snapshot_writer import SnapshotWriter, write_snapshot_sync
from .relationship_builder import RelationshipBuilder
from .csv_exporter import CSVExporter, export_to_csv, COLUMN_CONFIGS
from .dependency_tree import (
    DependencyTreeBuilder,
    DependencyNode,
    generate_dependency_tree,
    generate_dependent_tree,
    print_dependency_tree,
)
from .cross_bu_report import (
    CrossBUAnalyzer,
    CrossBUReport,
    SharedResource,
    generate_cross_bu_report,
    get_shared_resource_impact,
    list_shared_resources,
)

__all__ = [
    # Snapshot
    "SnapshotWriter",
    "write_snapshot_sync",
    # Relationships
    "RelationshipBuilder",
    # CSV Export
    "CSVExporter",
    "export_to_csv",
    "COLUMN_CONFIGS",
    # Dependency Tree
    "DependencyTreeBuilder",
    "DependencyNode",
    "generate_dependency_tree",
    "generate_dependent_tree",
    "print_dependency_tree",
    # Cross-BU Analysis
    "CrossBUAnalyzer",
    "CrossBUReport",
    "SharedResource",
    "generate_cross_bu_report",
    "get_shared_resource_impact",
    "list_shared_resources",
]
