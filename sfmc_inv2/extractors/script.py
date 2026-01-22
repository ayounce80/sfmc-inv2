"""Script Activity extractor for SFMC.

Extracts SSJS Script Activities from Automation Studio.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class ScriptExtractor(BaseExtractor):
    """Extractor for SFMC SSJS Script Activities."""

    name = "scripts"
    description = "SFMC SSJS Script Activities"
    object_type = "Script"

    required_caches = [CacheType.SCRIPT_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch scripts via REST API with pagination."""
        scripts = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/automation/v1/scripts?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch scripts page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            scripts.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(scripts), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return scripts

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich script with breadcrumb path."""
        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.SCRIPT_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform script data for output."""
        transformed = []

        for item in items:
            output = {
                "id": item.get("ssjsActivityId"),
                "name": item.get("name"),
                "customerKey": item.get("key"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                "script": item.get("script") if options.include_content else None,
                "status": item.get("status"),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                "createdBy": item.get("createdBy"),
                "modifiedBy": item.get("modifiedBy"),
            }
            transformed.append(output)

        return transformed
