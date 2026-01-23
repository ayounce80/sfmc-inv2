"""Asset extractor for SFMC Content Builder.

Extracts Content Builder assets (emails, content blocks, images, etc.).
Uses the Asset API with pagination support.

CloudPage Support:
    For CloudPage asset types (webpage, landing page, etc.), the extractor
    fetches full content to parse AMPscript for DE references. This enables
    detection of relationships like cloudpage_writes_de and cloudpage_reads_de.
"""

import logging
import re
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
    # CloudPages and web content
    205: "Webpage",
    246: "JSON Message",
    247: "CloudPages",
    248: "Microsite Collection",
    249: "Microsite Page",
}

# CloudPage asset types that should have content fetched for AMPscript parsing
# These are Content Builder CloudPages (not Web Studio classic pages)
CLOUDPAGE_ASSET_TYPES = {
    205,  # Webpage
    211,  # Webpage (alternate)
    212,  # Landing Page
    246,  # JSON Message
    247,  # CloudPages
    248,  # Microsite Collection
    249,  # Microsite Page
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
        For CloudPage asset types, fetches full content for AMPscript parsing.
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

        # Fetch full content for CloudPage types to enable AMPscript parsing
        # Gate behind include_content option or custom 'parse_cloudpages' flag
        if options.include_content or options.custom.get("parse_cloudpages", False):
            assets = await self._fetch_cloudpage_content(assets, options)

        return assets

    async def _fetch_cloudpage_content(
        self,
        assets: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Fetch full content for CloudPage assets to enable AMPscript parsing.

        Args:
            assets: List of assets from initial fetch.
            options: Extractor options.

        Returns:
            Assets with content populated for CloudPage types.
        """
        # Identify CloudPage assets that need content fetched
        cloudpage_assets = [
            a for a in assets
            if a.get("assetType", {}).get("id") in CLOUDPAGE_ASSET_TYPES
        ]

        if not cloudpage_assets:
            return assets

        logger.info(f"Fetching content for {len(cloudpage_assets)} CloudPage assets")

        # Create index for quick lookup
        assets_by_id = {a.get("id"): a for a in assets}

        # Fetch content for each CloudPage
        for i, asset in enumerate(cloudpage_assets):
            asset_id = asset.get("id")
            if not asset_id:
                continue

            result = self._rest.get(f"/asset/v1/content/assets/{asset_id}")

            if result.get("ok"):
                full_asset = result.get("data", {})
                # Merge content field into existing asset
                if "content" in full_asset:
                    assets_by_id[asset_id]["content"] = full_asset["content"]
                if "views" in full_asset:
                    assets_by_id[asset_id]["views"] = full_asset["views"]
            else:
                logger.warning(
                    f"Failed to fetch content for CloudPage {asset_id}: {result.get('error')}"
                )

            if (i + 1) % 10 == 0:
                self._report_progress(
                    options, "Fetching CloudPage content", i + 1, len(cloudpage_assets)
                )

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

    def _extract_ampscript_blocks(self, content: str) -> str:
        """Extract only AMPscript blocks from content.

        Extracts content from %%[ ... ]%% and %%= ... =%% blocks to avoid
        false positives from JavaScript or HTML that might contain similar
        function names.

        Args:
            content: Full HTML/AMPscript content.

        Returns:
            Concatenated AMPscript block contents.
        """
        if not content:
            return ""

        blocks = []

        # Match %%[ ... ]%% blocks (multiline)
        block_pattern = r'%%\[(.*?)\]%%'
        for match in re.finditer(block_pattern, content, re.DOTALL | re.IGNORECASE):
            blocks.append(match.group(1))

        # Match %%= ... =%% inline expressions
        inline_pattern = r'%%=(.*?)=%%'
        for match in re.finditer(inline_pattern, content, re.DOTALL | re.IGNORECASE):
            blocks.append(match.group(1))

        return "\n".join(blocks)

    def _parse_ampscript_de_refs(self, content: str) -> list[tuple[str, str]]:
        """Parse AMPscript content for DE references.

        Detects AMPscript functions that read from or write to Data Extensions:
        - Write: InsertDE, InsertData, UpdateDE, UpdateData, UpsertDE, UpsertData,
                 DeleteDE, DeleteData
        - Read: Lookup, LookupRows, LookupOrderedRows, LookupRowsCS, ClaimRow

        Only parses content within AMPscript blocks (%%[...]%% and %%=...=%%)
        to avoid false positives from JavaScript or HTML.

        Limitation:
            Only detects literal DE names in quotes (e.g., Lookup("My_DE", ...)).
            Dynamic/variable DE names (e.g., Lookup(@deName, ...)) cannot be
            resolved via static analysis and will not be captured.

        Args:
            content: AMPscript/HTML content to parse.

        Returns:
            List of (de_name, operation) tuples where operation is
            'insert', 'update', 'upsert', 'delete', or 'read'.
        """
        if not content:
            return []

        # Extract only AMPscript blocks to avoid false positives
        ampscript_content = self._extract_ampscript_blocks(content)
        if not ampscript_content:
            return []

        refs = []

        # Write operations: InsertDE("DE_Name", ...), UpsertData("DE_Name", ...)
        # Pattern matches: InsertDE, InsertData, UpdateDE, UpdateData,
        #                  UpsertDE, UpsertData, DeleteDE, DeleteData
        write_pattern = r'(Insert|Update|Upsert|Delete)D(?:E|ata)\s*\(\s*["\']([^"\']+)["\']'
        for match in re.finditer(write_pattern, ampscript_content, re.IGNORECASE):
            operation = match.group(1).lower()
            de_name = match.group(2)
            refs.append((de_name, operation))

        # Read operations: Lookup("DE_Name", ...), LookupRows("DE_Name", ...),
        #                  LookupOrderedRows("DE_Name", ...), LookupRowsCS, ClaimRow
        read_pattern = r'(?:Lookup(?:OrderedRows|RowsCS|Rows)?|ClaimRow)\s*\(\s*["\']([^"\']+)["\']'
        for match in re.finditer(read_pattern, ampscript_content, re.IGNORECASE):
            de_name = match.group(1)
            refs.append((de_name, "read"))

        return refs

    def _extract_content_text(self, item: dict[str, Any]) -> str:
        """Extract all text content from an asset for AMPscript parsing.

        Combines content from various locations in the asset structure.

        Args:
            item: Asset dictionary.

        Returns:
            Combined content string.
        """
        content_parts = []

        # Direct content field
        content = item.get("content")
        if content:
            content_parts.append(content)

        # Views (html, text, etc.)
        views = item.get("views", {})
        if isinstance(views, dict):
            for view_name, view_data in views.items():
                if isinstance(view_data, dict):
                    view_content = view_data.get("content")
                    if view_content:
                        content_parts.append(view_content)
                elif isinstance(view_data, str):
                    content_parts.append(view_data)

        return "\n".join(content_parts)

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from assets.

        For CloudPage types, parses AMPscript to detect DE reads and writes.
        """
        for item in items:
            asset_id = item.get("id")
            asset_name = item.get("name")
            asset_type_id = item.get("assetType", {}).get("id")

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

            # Parse CloudPage AMPscript for DE references
            if asset_type_id in CLOUDPAGE_ASSET_TYPES:
                content = self._extract_content_text(item)
                de_refs = self._parse_ampscript_de_refs(content)

                # Track unique DE names to avoid duplicate relationships
                seen_writes = set()
                seen_reads = set()

                for de_name, operation in de_refs:
                    if operation in ("insert", "update", "upsert", "delete"):
                        if de_name not in seen_writes:
                            seen_writes.add(de_name)
                            result.add_relationship(
                                source_id=str(asset_id),
                                source_type="asset",
                                source_name=asset_name,
                                target_id=de_name,  # Use DE name as ID (resolved by merge_edges)
                                target_type="data_extension",
                                target_name=de_name,
                                relationship_type=RelationshipType.CLOUDPAGE_WRITES_DE,
                                metadata={
                                    "operation": operation,
                                    "asset_type": "cloudpage",
                                    "resolved_by_name": True,  # Signal for ID resolution
                                },
                            )
                    elif operation == "read":
                        if de_name not in seen_reads:
                            seen_reads.add(de_name)
                            result.add_relationship(
                                source_id=str(asset_id),
                                source_type="asset",
                                source_name=asset_name,
                                target_id=de_name,  # Use DE name as ID (resolved by merge_edges)
                                target_type="data_extension",
                                target_name=de_name,
                                relationship_type=RelationshipType.CLOUDPAGE_READS_DE,
                                metadata={
                                    "operation": operation,
                                    "asset_type": "cloudpage",
                                    "resolved_by_name": True,  # Signal for ID resolution
                                },
                            )
