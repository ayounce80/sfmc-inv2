"""Asset extractor for SFMC Content Builder.

Extracts Content Builder assets (emails, content blocks, images, etc.).
Uses the Asset API with pagination support.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)

# Asset type ID to name mapping (common types)
ASSET_TYPE_MAP = {
    # Email-related
    5: "HTML Email",
    207: "Template-Based Email",
    208: "Text-Only Email",
    209: "Email (Default)",
    # Content blocks
    195: "Content Block",
    196: "Code Snippet",
    197: "Text Content",
    198: "HTML Content",
    199: "Free Form Content",
    220: "Smart Capture Block",
    # Images and files
    20: "Image",
    22: "Document",
    23: "Audio",
    28: "Video",
    # Templates
    210: "Email Template",
    211: "Webpage",
    212: "Landing Page",
}


class AssetExtractor(BaseExtractor):
    """Extractor for SFMC Content Builder Assets."""

    name = "assets"
    description = "SFMC Content Builder Assets"
    object_type = "Asset"

    required_caches = [CacheType.CONTENT_CATEGORIES]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch assets via REST API with pagination.

        Uses POST /asset/v1/content/assets/query for filtering support.
        """
        assets = []
        page = 1
        self._pages_fetched = 0

        # Use simplified fields to avoid large payloads
        # Content is only included if include_content option is set
        fields = [
            "id", "customerKey", "name", "description",
            "assetType", "category", "status", "version",
            "createdDate", "modifiedDate", "createdBy", "modifiedBy",
        ]

        while page <= options.max_pages:
            # Use POST query endpoint for better control
            result = self._rest.post(
                "/asset/v1/content/assets/query",
                json={
                    "page": {
                        "page": page,
                        "pageSize": options.page_size,
                    },
                    "fields": fields,
                },
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch assets page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            assets.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(assets), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return assets

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich asset with breadcrumb path and type name."""
        # Add breadcrumb path from category
        category = item.get("category", {})
        if category:
            category_id = category.get("id")
            if category_id:
                item["folderPath"] = self.get_breadcrumb(
                    str(category_id), CacheType.CONTENT_CATEGORIES
                )

        # Resolve asset type name
        asset_type = item.get("assetType", {})
        if asset_type:
            type_id = asset_type.get("id")
            if type_id:
                item["assetTypeName"] = ASSET_TYPE_MAP.get(type_id, f"Type {type_id}")

        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform asset data for output."""
        transformed = []

        for item in items:
            asset_type = item.get("assetType", {})
            category = item.get("category", {})
            created_by = item.get("createdBy", {})
            modified_by = item.get("modifiedBy", {})

            output = {
                "id": item.get("id"),
                "name": item.get("name"),
                "customerKey": item.get("customerKey"),
                "description": item.get("description"),
                # Asset type
                "assetTypeId": asset_type.get("id") if asset_type else None,
                "assetTypeName": item.get("assetTypeName") or (asset_type.get("name") if asset_type else None),
                # Category/folder
                "categoryId": category.get("id") if category else None,
                "categoryName": category.get("name") if category else None,
                "folderPath": item.get("folderPath"),
                # Status
                "status": item.get("status"),
                "version": item.get("version"),
                # Audit
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                "createdBy": created_by.get("name") if created_by else None,
                "modifiedBy": modified_by.get("name") if modified_by else None,
            }
            transformed.append(output)

        return transformed

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from assets.

        Note: Full content block reference extraction requires parsing
        content, which is deferred to include_content scenarios.
        """
        for item in items:
            asset_id = item.get("id")
            asset_name = item.get("name")

            if not asset_id:
                continue

            # Check for referenced content blocks (if available in slots)
            slots = item.get("slots", {})
            for slot_name, slot_data in slots.items():
                if isinstance(slot_data, dict):
                    blocks = slot_data.get("blocks", [])
                    for block in blocks:
                        if isinstance(block, dict):
                            block_id = block.get("id")
                            block_name = block.get("name")
                            if block_id:
                                result.add_relationship(
                                    source_id=str(asset_id),
                                    source_type="asset",
                                    source_name=asset_name,
                                    target_id=str(block_id),
                                    target_type="content_block",
                                    target_name=block_name,
                                    relationship_type=RelationshipType.EMAIL_USES_CONTENT_BLOCK,
                                )
