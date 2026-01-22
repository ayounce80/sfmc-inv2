"""Declarative type registry for SFMC object types.

Inspired by Accenture's sfmc-devtools MetadataTypeDefinitions.
Provides centralized type metadata including field mappings,
dependencies, and dependency graph paths for relationship extraction.
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class TypeDefinition:
    """Definition for an SFMC object type."""

    # Core identification
    name: str
    extractor_name: str  # Name used in EXTRACTORS registry

    # Field mappings
    id_field: str = "id"
    key_field: str = "customerKey"
    name_field: str = "name"

    # Dependencies for extraction ordering
    # Types that should be extracted before this type
    dependencies: list[str] = field(default_factory=list)

    # Dependency graph paths for relationship extraction
    # Maps target type to JSON path expressions where references occur
    # Example: {"data_extension": ["targetKey", "steps[].targetDE"]}
    dependency_graph: dict[str, list[str]] = field(default_factory=dict)

    # Whether this type can be shared from parent BU
    shared_from_parent: bool = False

    # Additional metadata
    api_type: str = "REST"  # REST or SOAP
    description: str = ""


# Comprehensive type registry
TYPE_REGISTRY: dict[str, TypeDefinition] = {
    # Folders - foundational, no dependencies
    "folder": TypeDefinition(
        name="folder",
        extractor_name="folders",
        id_field="id",
        key_field="id",
        name_field="name",
        dependencies=[],
        dependency_graph={},
        shared_from_parent=True,
        description="Folder hierarchy for organizing objects",
    ),
    # Data Extensions - can be shared from parent BU
    "data_extension": TypeDefinition(
        name="data_extension",
        extractor_name="data_extensions",
        id_field="id",
        key_field="customerKey",
        name_field="name",
        dependencies=["folder"],
        dependency_graph={},  # DEs don't reference other objects
        shared_from_parent=True,
        description="Data Extensions for storing tabular data",
    ),
    # Query Activity
    "query": TypeDefinition(
        name="query",
        extractor_name="queries",
        id_field="queryDefinitionId",
        key_field="key",
        name_field="name",
        dependencies=["data_extension", "folder"],
        dependency_graph={
            "data_extension": [
                "targetKey",
                "targetName",
                "queryText",  # SQL parsing extracts DE references
            ],
        },
        shared_from_parent=False,
        description="SQL Query Activities for data manipulation",
    ),
    # Script Activity
    "script": TypeDefinition(
        name="script",
        extractor_name="scripts",
        id_field="ssjsActivityId",
        key_field="key",
        name_field="name",
        dependencies=["data_extension", "folder"],
        dependency_graph={
            "data_extension": ["script"],  # SSJS code may reference DEs
        },
        shared_from_parent=False,
        description="SSJS Script Activities",
    ),
    # Import Activity
    "import": TypeDefinition(
        name="import",
        extractor_name="imports",
        id_field="importDefinitionId",
        key_field="key",
        name_field="name",
        dependencies=["data_extension", "folder"],
        dependency_graph={
            "data_extension": ["destinationObjectId", "destinationObjectKey"],
        },
        shared_from_parent=False,
        description="Import File Activities",
    ),
    # Data Extract Activity
    "data_extract": TypeDefinition(
        name="data_extract",
        extractor_name="data_extracts",
        id_field="dataExtractDefinitionId",
        key_field="key",
        name_field="name",
        dependencies=["data_extension", "folder"],
        dependency_graph={
            "data_extension": ["dataExtensionKey", "dataExtensionName"],
        },
        shared_from_parent=False,
        description="Data Extract Activities for exporting data",
    ),
    # File Transfer Activity
    "file_transfer": TypeDefinition(
        name="file_transfer",
        extractor_name="file_transfers",
        id_field="id",
        key_field="key",
        name_field="name",
        dependencies=["folder"],
        dependency_graph={},
        shared_from_parent=False,
        description="File Transfer Activities for FTP/SFTP operations",
    ),
    # Filter Activity
    "filter": TypeDefinition(
        name="filter",
        extractor_name="filters",
        id_field="filterDefinitionId",
        key_field="key",
        name_field="name",
        dependencies=["data_extension", "folder"],
        dependency_graph={
            "data_extension": ["sourceDataExtension", "destinationDataExtension"],
        },
        shared_from_parent=False,
        description="Filter Activities for data segmentation",
    ),
    # Event Definition
    "event_definition": TypeDefinition(
        name="event_definition",
        extractor_name="event_definitions",
        id_field="id",
        key_field="eventDefinitionKey",
        name_field="name",
        dependencies=["data_extension"],
        dependency_graph={
            "data_extension": ["dataExtensionId", "dataExtensionName"],
        },
        shared_from_parent=True,
        description="Journey Entry Event Definitions",
    ),
    # Automation
    "automation": TypeDefinition(
        name="automation",
        extractor_name="automations",
        id_field="id",
        key_field="key",
        name_field="name",
        dependencies=[
            "query",
            "script",
            "import",
            "data_extract",
            "filter",
            "file_transfer",
            "event_definition",
            "folder",
        ],
        dependency_graph={
            "query": ["steps[].activities[].objectTypeId=300"],
            "script": ["steps[].activities[].objectTypeId=423"],
            "import": ["steps[].activities[].objectTypeId=43"],
            "data_extract": ["steps[].activities[].objectTypeId=73"],
            "file_transfer": ["steps[].activities[].objectTypeId=53"],
            "filter": ["steps[].activities[].objectTypeId=303"],
            "event_definition": [
                "steps[].activities[].objectTypeId=749",
                "steps[].activities[].objectTypeId=952",
            ],
        },
        shared_from_parent=False,
        description="Automation Studio automations",
    ),
    # Journey
    "journey": TypeDefinition(
        name="journey",
        extractor_name="journeys",
        id_field="id",
        key_field="key",
        name_field="name",
        dependencies=[
            "event_definition",
            "data_extension",
            "triggered_send",
            "folder",
        ],
        dependency_graph={
            "event_definition": ["triggers[].metaData.eventDefinitionId"],
            "data_extension": [
                "activities[].configurationArguments.dataExtensionId",
                "activities[].configurationArguments.audienceDataExtensionId",
            ],
            "triggered_send": [
                "activities[].configurationArguments.triggeredSend.triggeredSendId"
            ],
        },
        shared_from_parent=False,
        description="Journey Builder journeys",
    ),
    # Classic Email (SOAP)
    "classic_email": TypeDefinition(
        name="classic_email",
        extractor_name="classic_emails",
        id_field="id",
        key_field="customerKey",
        name_field="name",
        dependencies=["folder"],
        dependency_graph={},
        shared_from_parent=True,
        api_type="SOAP",
        description="Classic Emails from Email Studio",
    ),
    # Triggered Send Definition (SOAP)
    "triggered_send": TypeDefinition(
        name="triggered_send",
        extractor_name="triggered_sends",
        id_field="ObjectID",
        key_field="CustomerKey",
        name_field="Name",
        dependencies=[
            "classic_email",
            "list",
            "sender_profile",
            "delivery_profile",
            "send_classification",
        ],
        dependency_graph={
            "classic_email": ["Email.ID"],
            "list": ["List.ID"],
            "sender_profile": ["SenderProfile.CustomerKey"],
            "delivery_profile": ["DeliveryProfile.CustomerKey"],
            "send_classification": ["SendClassification.CustomerKey"],
        },
        shared_from_parent=False,
        api_type="SOAP",
        description="Triggered Send Definitions",
    ),
    # Subscriber List (SOAP)
    "list": TypeDefinition(
        name="list",
        extractor_name="lists",
        id_field="ID",
        key_field="CustomerKey",
        name_field="ListName",
        dependencies=["folder"],
        dependency_graph={},
        shared_from_parent=True,
        api_type="SOAP",
        description="Subscriber Lists",
    ),
    # Sender Profile (SOAP)
    "sender_profile": TypeDefinition(
        name="sender_profile",
        extractor_name="sender_profiles",
        id_field="ObjectID",
        key_field="CustomerKey",
        name_field="Name",
        dependencies=[],
        dependency_graph={},
        shared_from_parent=True,
        api_type="SOAP",
        description="Sender Profiles for email sending",
    ),
    # Delivery Profile (SOAP)
    "delivery_profile": TypeDefinition(
        name="delivery_profile",
        extractor_name="delivery_profiles",
        id_field="ObjectID",
        key_field="CustomerKey",
        name_field="Name",
        dependencies=[],
        dependency_graph={},
        shared_from_parent=True,
        api_type="SOAP",
        description="Delivery Profiles for email sending",
    ),
    # Send Classification (SOAP)
    "send_classification": TypeDefinition(
        name="send_classification",
        extractor_name="send_classifications",
        id_field="ObjectID",
        key_field="CustomerKey",
        name_field="Name",
        dependencies=["sender_profile", "delivery_profile"],
        dependency_graph={
            "sender_profile": ["SenderProfile.CustomerKey"],
            "delivery_profile": ["DeliveryProfile.CustomerKey"],
        },
        shared_from_parent=True,
        api_type="SOAP",
        description="Send Classifications for email categorization",
    ),
    # Content Builder Asset
    "asset": TypeDefinition(
        name="asset",
        extractor_name="assets",
        id_field="id",
        key_field="customerKey",
        name_field="name",
        dependencies=["folder"],  # Uses content categories
        dependency_graph={
            "asset": ["content.blocks[].id"],  # Content blocks reference other assets
            "data_extension": ["data.email.legacy.legacyData.dataExtension"],
        },
        shared_from_parent=True,
        description="Content Builder assets (emails, blocks, templates)",
    ),
    # Template (SOAP)
    "template": TypeDefinition(
        name="template",
        extractor_name="templates",
        id_field="ID",
        key_field="CustomerKey",
        name_field="TemplateName",
        dependencies=["folder"],
        dependency_graph={},
        shared_from_parent=True,
        api_type="SOAP",
        description="Email Templates",
    ),
    # Account (special - metadata only)
    "account": TypeDefinition(
        name="account",
        extractor_name="account",
        id_field="ID",
        key_field="CustomerKey",
        name_field="Name",
        dependencies=[],
        dependency_graph={},
        shared_from_parent=False,
        api_type="SOAP",
        description="Account/Business Unit information",
    ),
}


def get_type_definition(type_name: str) -> Optional[TypeDefinition]:
    """Get the type definition for a given type name.

    Args:
        type_name: Name of the type (e.g., "automation", "data_extension").

    Returns:
        TypeDefinition or None if not found.
    """
    return TYPE_REGISTRY.get(type_name)


def get_type_by_extractor(extractor_name: str) -> Optional[TypeDefinition]:
    """Get the type definition for a given extractor name.

    Args:
        extractor_name: Name of the extractor (e.g., "automations", "data_extensions").

    Returns:
        TypeDefinition or None if not found.
    """
    for type_def in TYPE_REGISTRY.values():
        if type_def.extractor_name == extractor_name:
            return type_def
    return None


def get_all_types() -> list[str]:
    """Get list of all registered type names."""
    return list(TYPE_REGISTRY.keys())


def get_shared_types() -> list[str]:
    """Get list of types that can be shared from parent BU."""
    return [
        name for name, type_def in TYPE_REGISTRY.items() if type_def.shared_from_parent
    ]


def get_dependencies(type_name: str) -> list[str]:
    """Get dependencies for a type.

    Args:
        type_name: Name of the type.

    Returns:
        List of type names this type depends on.
    """
    type_def = get_type_definition(type_name)
    if type_def:
        return type_def.dependencies.copy()
    return []


def get_dependency_paths(type_name: str, dep_type: str) -> list[str]:
    """Get JSON paths where dependencies of a specific type occur.

    Args:
        type_name: Name of the source type.
        dep_type: Name of the dependency type to find paths for.

    Returns:
        List of JSON path expressions.
    """
    type_def = get_type_definition(type_name)
    if type_def:
        return type_def.dependency_graph.get(dep_type, [])
    return []


def get_extractor_to_type_map() -> dict[str, str]:
    """Get mapping from extractor names to type names."""
    return {
        type_def.extractor_name: type_def.name for type_def in TYPE_REGISTRY.values()
    }


def get_type_to_extractor_map() -> dict[str, str]:
    """Get mapping from type names to extractor names."""
    return {
        type_def.name: type_def.extractor_name for type_def in TYPE_REGISTRY.values()
    }
