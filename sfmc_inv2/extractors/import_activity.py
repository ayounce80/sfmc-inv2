"""Import Activity extractor for SFMC.

Extracts Import File Activities from Automation Studio.
Identifies relationships to Data Extensions and file locations.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class ImportExtractor(BaseExtractor):
    """Extractor for SFMC Import File Activities."""

    name = "imports"
    description = "SFMC Import File Activities"
    object_type = "ImportDefinition"

    required_caches = [CacheType.IMPORT_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch imports via REST API with pagination."""
        imports = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/automation/v1/imports?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch imports page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            imports.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(imports), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return imports

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich import with breadcrumb path."""
        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.IMPORT_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform import data for output."""
        transformed = []

        for item in items:
            # Extract destination info
            destination_object = item.get("destinationObject", {})
            file_transfer_location = item.get("fileTransferLocation", {})

            output = {
                "id": item.get("importDefinitionId"),
                "name": item.get("name"),
                "customerKey": item.get("key"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                # Destination Data Extension
                "destinationId": destination_object.get("id") if destination_object else None,
                "destinationName": destination_object.get("name") if destination_object else None,
                "destinationKey": destination_object.get("key") if destination_object else None,
                # File transfer location
                "fileTransferLocationId": file_transfer_location.get("id") if file_transfer_location else None,
                "fileTransferLocationName": file_transfer_location.get("name") if file_transfer_location else None,
                # File settings
                "fileNamingPattern": item.get("fileNamingPattern"),
                "fileType": item.get("fileType"),
                "encoding": item.get("encoding"),
                "delimiter": item.get("delimiter"),
                "hasColumnHeader": item.get("hasColumnHeader"),
                # Update type
                "updateType": item.get("updateType"),
                "updateTypeName": item.get("updateTypeName"),
                # Status and dates
                "status": item.get("status"),
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
        """Extract relationships from imports to Data Extensions."""
        for item in items:
            import_id = item.get("importDefinitionId")
            import_name = item.get("name")

            if not import_id:
                continue

            # Destination DE relationship
            destination_object = item.get("destinationObject", {})
            if destination_object:
                de_id = destination_object.get("id")
                de_name = destination_object.get("name")
                if de_id:
                    result.add_relationship(
                        source_id=str(import_id),
                        source_type="import",
                        source_name=import_name,
                        target_id=str(de_id),
                        target_type="data_extension",
                        target_name=de_name,
                        relationship_type=RelationshipType.IMPORT_WRITES_DE,
                    )
