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
            # API returns sourceObjectId/destinationObjectId (DE IDs)
            # and resultDEName/resultDEKey for destination DE info
            output = {
                "id": item.get("filterActivityId"),
                "name": item.get("name"),
                "customerKey": item.get("customerKey") or item.get("key"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                # Source Data Extension (reads from)
                "sourceDataExtensionId": item.get("sourceObjectId"),
                "sourceDataExtensionName": item.get("sourceDEName"),
                "sourceDataExtensionKey": item.get("sourceDEKey"),
                # Destination Data Extension (writes to)
                "destinationDataExtensionId": item.get("destinationObjectId"),
                "destinationDataExtensionName": item.get("resultDEName"),
                "destinationDataExtensionKey": item.get("resultDEKey"),
                # Filter definition
                "filterDefinitionId": item.get("filterDefinitionId"),
                # Status and dates
                "status": item.get("status") or item.get("statusId"),
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
        """Extract relationships from filters to Data Extensions."""
        for item in items:
            filter_id = item.get("filterActivityId")
            filter_name = item.get("name")

            if not filter_id:
                continue

            # Source DE relationship (reads from)
            source_de_id = item.get("sourceObjectId")
            if source_de_id:
                result.add_relationship(
                    source_id=str(filter_id),
                    source_type="filter",
                    source_name=filter_name,
                    target_id=str(source_de_id),
                    target_type="data_extension",
                    target_name=item.get("sourceDEName"),
                    relationship_type=RelationshipType.FILTER_READS_DE,
                )

            # Destination DE relationship (writes to)
            dest_de_id = item.get("destinationObjectId")
            if dest_de_id:
                result.add_relationship(
                    source_id=str(filter_id),
                    source_type="filter",
                    source_name=filter_name,
                    target_id=str(dest_de_id),
                    target_type="data_extension",
                    target_name=item.get("resultDEName"),
                    relationship_type=RelationshipType.FILTER_WRITES_DE,
                )
