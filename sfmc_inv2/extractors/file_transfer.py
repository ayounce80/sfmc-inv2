"""File Transfer Activity extractor for SFMC.

Extracts File Transfer Activities from Automation Studio.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class FileTransferExtractor(BaseExtractor):
    """Extractor for SFMC File Transfer Activities."""

    name = "file_transfers"
    description = "SFMC File Transfer Activities"
    object_type = "FileTransferDefinition"

    required_caches = [CacheType.FILETRANSFER_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch file transfers via REST API with pagination."""
        transfers = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/automation/v1/filetransfers?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch file transfers page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            transfers.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(transfers), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return transfers

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich file transfer with breadcrumb path."""
        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.FILETRANSFER_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform file transfer data for output."""
        transformed = []

        for item in items:
            # Extract file transfer location info
            file_transfer_location = item.get("fileTransferLocation", {})

            output = {
                "id": item.get("fileTransferDefinitionId"),
                "name": item.get("name"),
                "customerKey": item.get("key"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                # File transfer location
                "fileTransferLocationId": file_transfer_location.get("id") if file_transfer_location else None,
                "fileTransferLocationName": file_transfer_location.get("name") if file_transfer_location else None,
                "fileTransferLocationType": file_transfer_location.get("type") if file_transfer_location else None,
                # File settings
                "fileNamingPattern": item.get("fileNamingPattern"),
                "fileAction": item.get("fileAction"),
                "isCompressed": item.get("isCompressed"),
                "isEncrypted": item.get("isEncrypted"),
                # Status and dates
                "status": item.get("status"),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
            }
            transformed.append(output)

        return transformed
