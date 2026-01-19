"""Domain extractors for SFMC object types."""

from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

# Existing extractors
from .automation import AutomationExtractor
from .data_extension import DataExtensionExtractor
from .query import QueryExtractor
from .journey import JourneyExtractor

# Phase 1 - Automation Activities (REST)
from .script import ScriptExtractor
from .import_activity import ImportExtractor
from .data_extract import DataExtractExtractor
from .filter import FilterExtractor
from .file_transfer import FileTransferExtractor

# Phase 2 - Content & Structure (REST)
from .asset import AssetExtractor
from .folder import FolderExtractor
from .event_definition import EventDefinitionExtractor

# Phase 3 - Messaging Objects (SOAP)
from .classic_email import ClassicEmailExtractor
from .triggered_send import TriggeredSendExtractor
from .subscriber_list import ListExtractor
from .sender_profile import SenderProfileExtractor
from .delivery_profile import DeliveryProfileExtractor
from .send_classification import SendClassificationExtractor
from .template import TemplateExtractor
from .account import AccountExtractor

__all__ = [
    "BaseExtractor",
    "ExtractorOptions",
    "ExtractorResult",
    # Existing
    "AutomationExtractor",
    "DataExtensionExtractor",
    "QueryExtractor",
    "JourneyExtractor",
    # Phase 1 - Automation Activities
    "ScriptExtractor",
    "ImportExtractor",
    "DataExtractExtractor",
    "FilterExtractor",
    "FileTransferExtractor",
    # Phase 2 - Content
    "AssetExtractor",
    "FolderExtractor",
    "EventDefinitionExtractor",
    # Phase 3 - Messaging
    "ClassicEmailExtractor",
    "TriggeredSendExtractor",
    "ListExtractor",
    "SenderProfileExtractor",
    "DeliveryProfileExtractor",
    "SendClassificationExtractor",
    "TemplateExtractor",
    "AccountExtractor",
]

# Registry of available extractors
EXTRACTORS = {
    # Existing extractors
    "automations": AutomationExtractor,
    "data_extensions": DataExtensionExtractor,
    "queries": QueryExtractor,
    "journeys": JourneyExtractor,
    # Phase 1 - Automation Activities (REST)
    "scripts": ScriptExtractor,
    "imports": ImportExtractor,
    "data_extracts": DataExtractExtractor,
    "filters": FilterExtractor,
    "file_transfers": FileTransferExtractor,
    # Phase 2 - Content & Structure (REST)
    "assets": AssetExtractor,
    "folders": FolderExtractor,
    "event_definitions": EventDefinitionExtractor,
    # Phase 3 - Messaging Objects (SOAP)
    "classic_emails": ClassicEmailExtractor,
    "triggered_sends": TriggeredSendExtractor,
    "lists": ListExtractor,
    "sender_profiles": SenderProfileExtractor,
    "delivery_profiles": DeliveryProfileExtractor,
    "send_classifications": SendClassificationExtractor,
    "templates": TemplateExtractor,
    "account": AccountExtractor,
}


def get_extractor(name: str) -> type[BaseExtractor]:
    """Get extractor class by name."""
    if name not in EXTRACTORS:
        raise ValueError(f"Unknown extractor: {name}. Available: {list(EXTRACTORS.keys())}")
    return EXTRACTORS[name]


def list_extractors() -> list[str]:
    """Get list of available extractor names."""
    return list(EXTRACTORS.keys())
