"""Event Definition extractor for SFMC Journey Builder.

Extracts Journey entry event definitions (API events, data extension events, etc.).
"""

import logging
from typing import Any

from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class EventDefinitionExtractor(BaseExtractor):
    """Extractor for SFMC Event Definitions."""

    name = "event_definitions"
    description = "SFMC Journey Entry Event Definitions"
    object_type = "EventDefinition"

    # Event definitions are used by journeys and can exist on child BUs
    supports_multi_bu = True

    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch event definitions via REST API with pagination."""
        events = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/interaction/v1/eventDefinitions?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch event definitions page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            events.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(events), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return events

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform event definition data for output."""
        transformed = []

        for item in items:
            # Extract data extension info if present
            data_extension_id = None
            data_extension_name = None

            # Check various locations where DE info might be
            if item.get("dataExtensionId"):
                data_extension_id = item.get("dataExtensionId")
                data_extension_name = item.get("dataExtensionName")
            elif item.get("schema"):
                schema = item.get("schema", {})
                data_extension_id = schema.get("id")
                data_extension_name = schema.get("name")

            # Extract arguments/configuration
            arguments = item.get("arguments", {})

            output = {
                "id": item.get("id"),
                "name": item.get("name"),
                "eventDefinitionKey": item.get("eventDefinitionKey"),
                "description": item.get("description"),
                "type": item.get("type"),
                "mode": item.get("mode"),
                "status": item.get("status"),
                # Data Extension binding
                "dataExtensionId": data_extension_id,
                "dataExtensionName": data_extension_name,
                # Configuration
                "isVisibleInPicker": item.get("isVisibleInPicker"),
                "category": item.get("category"),
                # Schema fields
                "schemaId": item.get("schema", {}).get("id") if item.get("schema") else None,
                # Audit
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                "createdBy": item.get("createdBy"),
                "modifiedBy": item.get("modifiedBy"),
            }
            transformed.append(output)

        return transformed

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from event definitions to Data Extensions."""
        for item in items:
            event_id = item.get("id")
            event_name = item.get("name")

            if not event_id:
                continue

            # Data Extension relationship
            de_id = None
            de_name = None

            if item.get("dataExtensionId"):
                de_id = item.get("dataExtensionId")
                de_name = item.get("dataExtensionName")
            elif item.get("schema"):
                schema = item.get("schema", {})
                de_id = schema.get("id")
                de_name = schema.get("name")

            if de_id:
                result.add_relationship(
                    source_id=str(event_id),
                    source_type="event_definition",
                    source_name=event_name,
                    target_id=str(de_id),
                    target_type="data_extension",
                    target_name=de_name,
                    relationship_type=RelationshipType.EVENT_DEFINITION_USES_DE,
                    metadata={"usage": "entry_source"},
                )
