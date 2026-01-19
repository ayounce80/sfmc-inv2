"""Data Extract Activity extractor for SFMC.

Extracts Data Extract Activities from Automation Studio.
Identifies relationships to Data Extensions and output files.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class DataExtractExtractor(BaseExtractor):
    """Extractor for SFMC Data Extract Activities."""

    name = "data_extracts"
    description = "SFMC Data Extract Activities"
    object_type = "DataExtractDefinition"

    required_caches = [CacheType.DATAEXTRACT_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch data extracts via REST API with pagination."""
        extracts = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/automation/v1/dataextracts?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch data extracts page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            extracts.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(extracts), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return extracts

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich data extract with breadcrumb path."""
        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.DATAEXTRACT_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform data extract data for output."""
        transformed = []

        for item in items:
            # Extract type info
            data_extract_type = item.get("dataExtractType", {})

            output = {
                "id": item.get("dataExtractDefinitionId"),
                "name": item.get("name"),
                "customerKey": item.get("key"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                # Data extract type
                "dataExtractTypeId": data_extract_type.get("id") if data_extract_type else None,
                "dataExtractTypeName": data_extract_type.get("name") if data_extract_type else None,
                # File settings
                "fileNamingPattern": item.get("fileNamingPattern"),
                "fileSpec": item.get("fileSpec"),
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
        """Extract relationships from data extracts to Data Extensions."""
        for item in items:
            extract_id = item.get("dataExtractDefinitionId")
            extract_name = item.get("name")

            if not extract_id:
                continue

            # Source DE relationship (if specified in config)
            # Note: Data extract configuration varies by type
            # Tracking Extracts use tracking data, DE Extracts use a specific DE
            data_fields = item.get("dataFields", [])
            for field in data_fields:
                if field.get("dataExtension"):
                    de_info = field.get("dataExtension", {})
                    de_id = de_info.get("id")
                    de_name = de_info.get("name")
                    if de_id:
                        result.add_relationship(
                            source_id=str(extract_id),
                            source_type="data_extract",
                            source_name=extract_name,
                            target_id=str(de_id),
                            target_type="data_extension",
                            target_name=de_name,
                            relationship_type=RelationshipType.EXTRACT_READS_DE,
                        )
