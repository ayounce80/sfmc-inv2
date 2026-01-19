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
]
