"""Type definitions and Pydantic models."""

from .inventory import (
    InventorySnapshot,
    InventoryMetadata,
    InventoryStatistics,
    ExtractionError,
)
from .objects import (
    SFMCObject,
    Automation,
    AutomationStep,
    AutomationActivity,
    DataExtension,
    DataExtensionField,
    Query,
    Journey,
    JourneyActivity,
    Asset,
    Folder,
)
from .relationships import (
    RelationshipGraph,
    RelationshipEdge,
    RelationshipType,
)
from .type_registry import (
    TypeDefinition,
    TYPE_REGISTRY,
    get_type_definition,
    get_type_by_extractor,
    get_all_types,
    get_shared_types,
    get_dependencies,
    get_dependency_paths,
    get_extractor_to_type_map,
    get_type_to_extractor_map,
)

__all__ = [
    # Inventory
    "InventorySnapshot",
    "InventoryMetadata",
    "InventoryStatistics",
    "ExtractionError",
    # Objects
    "SFMCObject",
    "Automation",
    "AutomationStep",
    "AutomationActivity",
    "DataExtension",
    "DataExtensionField",
    "Query",
    "Journey",
    "JourneyActivity",
    "Asset",
    "Folder",
    # Relationships
    "RelationshipGraph",
    "RelationshipEdge",
    "RelationshipType",
    # Type Registry
    "TypeDefinition",
    "TYPE_REGISTRY",
    "get_type_definition",
    "get_type_by_extractor",
    "get_all_types",
    "get_shared_types",
    "get_dependencies",
    "get_dependency_paths",
    "get_extractor_to_type_map",
    "get_type_to_extractor_map",
]
