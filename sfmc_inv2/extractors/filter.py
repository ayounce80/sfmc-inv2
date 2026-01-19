"""Filter Activity extractor for SFMC.

Extracts Filter Activities from Automation Studio.
Identifies relationships to source and destination Data Extensions.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class FilterExtractor(BaseExtractor):
    """Extractor for SFMC Filter Activities."""

    name = "filters"
    description = "SFMC Filter Activities"
    object_type = "FilterDefinition"

    required_caches = [CacheType.FILTER_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch filters via REST API with pagination."""
        filters = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/automation/v1/filters?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch filters page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            filters.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(filters), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return filters

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich filter with breadcrumb path."""
        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.FILTER_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform filter data for output."""
        transformed = []

        for item in items:
            # Extract source and destination DE info
            source_de = item.get("sourceDataExtension", {})
            dest_de = item.get("destinationDataExtension", {})

            output = {
                "id": item.get("filterActivityId"),
                "name": item.get("name"),
                "customerKey": item.get("key"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                # Source Data Extension
                "sourceDataExtensionId": source_de.get("id") if source_de else None,
                "sourceDataExtensionName": source_de.get("name") if source_de else None,
                "sourceDataExtensionKey": source_de.get("key") if source_de else None,
                # Destination Data Extension
                "destinationDataExtensionId": dest_de.get("id") if dest_de else None,
                "destinationDataExtensionName": dest_de.get("name") if dest_de else None,
                "destinationDataExtensionKey": dest_de.get("key") if dest_de else None,
                # Filter definition
                "filterDefinitionId": item.get("filterDefinitionId"),
                # Status and dates
                "status": item.get("status"),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
            }
            transformed.append(output)

        return transformed

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from filters to Data Extensions."""
        for item in items:
            filter_id = item.get("filterActivityId")
            filter_name = item.get("name")

            if not filter_id:
                continue

            # Source DE relationship (reads from)
            source_de = item.get("sourceDataExtension", {})
            if source_de:
                de_id = source_de.get("id")
                de_name = source_de.get("name")
                if de_id:
                    result.add_relationship(
                        source_id=str(filter_id),
                        source_type="filter",
                        source_name=filter_name,
                        target_id=str(de_id),
                        target_type="data_extension",
                        target_name=de_name,
                        relationship_type=RelationshipType.DEPENDS_ON,
                        metadata={"usage": "source"},
                    )

            # Destination DE relationship (writes to)
            dest_de = item.get("destinationDataExtension", {})
            if dest_de:
                de_id = dest_de.get("id")
                de_name = dest_de.get("name")
                if de_id:
                    result.add_relationship(
                        source_id=str(filter_id),
                        source_type="filter",
                        source_name=filter_name,
                        target_id=str(de_id),
                        target_type="data_extension",
                        target_name=de_name,
                        relationship_type=RelationshipType.DEPENDS_ON,
                        metadata={"usage": "destination"},
                    )
