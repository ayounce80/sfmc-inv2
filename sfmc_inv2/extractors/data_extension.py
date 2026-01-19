"""Data Extension extractor for SFMC.

Extracts Data Extensions with their fields and metadata.
Uses category-based retrieval to avoid the $search requirement.
Supports parallel field retrieval for performance.
"""

import asyncio
import logging
from typing import Any, Optional

from ..cache.cache_manager import CacheType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class DataExtensionExtractor(BaseExtractor):
    """Extractor for SFMC Data Extensions.

    Uses the category-based endpoint /data/v1/customobjects/category/{categoryId}
    to retrieve all DEs without requiring a search parameter.
    """

    name = "data_extensions"
    description = "SFMC Data Extensions with fields"
    object_type = "DataExtension"

    required_caches = [CacheType.DE_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch data extensions by iterating through all categories.

        Strategy:
        1. Query category 0 first (contains all DEs including shared)
        2. Query all other DE folders to catch any that might be missed
        3. Deduplicate by customerKey
        """
        self._pages_fetched = 0

        # Get all DE folder categories
        categories = await self._fetch_de_categories(options)
        logger.info(f"Found {len(categories)} DE categories to query")

        # Start with category 0 (contains all DEs)
        all_categories = [{"categoryId": 0}] + categories

        # Dedupe by key
        all_des: dict[str, dict[str, Any]] = {}

        for i, category in enumerate(all_categories):
            category_id = category.get("categoryId")
            self._report_progress(
                options, f"Fetching category {i+1}/{len(all_categories)}", i, len(all_categories)
            )

            des = await self._fetch_des_by_category(category_id, options)
            for de in des:
                de_key = de.get("key") or de.get("customerKey")
                if de_key and de_key not in all_des:
                    # Keep categoryId from original query for breadcrumb
                    if "categoryId" not in de or not de.get("categoryId"):
                        de["categoryId"] = category_id
                    all_des[de_key] = de

            logger.debug(f"Category {category_id}: {len(des)} DEs, total unique: {len(all_des)}")

        logger.info(f"Retrieved {len(all_des)} unique data extensions")
        return list(all_des.values())

    async def _fetch_de_categories(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch all data extension folder categories."""
        categories = []
        page = 1

        while page <= options.max_pages:
            params = {
                "$filter": "categorytype eq dataextension",
                "$pageSize": options.page_size,
                "$page": page,
            }
            result = self._rest.get("/automation/v1/folders", params=params)

            if not result.get("ok"):
                logger.error(f"Failed to fetch DE folders: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])
            if not items:
                break

            categories.extend(items)

            if len(items) < options.page_size:
                break
            page += 1

        return categories

    async def _fetch_des_by_category(
        self, category_id: int, options: ExtractorOptions
    ) -> list[dict[str, Any]]:
        """Fetch data extensions for a specific category with pagination."""
        all_items = []
        page = 1
        page_size = 25  # SFMC default for this endpoint

        while page <= options.max_pages:
            result = self._rest.get(
                f"/data/v1/customobjects/category/{category_id}",
                params={"$pageSize": page_size, "$page": page},
            )

            if not result.get("ok"):
                error = result.get("error", "")
                # 404 is expected for empty categories
                if "404" not in str(error):
                    logger.warning(f"Failed to fetch DEs for category {category_id}: {error}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            all_items.extend(items)
            self._pages_fetched += 1

            total_count = data.get("count", 0)
            if len(all_items) >= total_count:
                break

            page += 1

        return all_items

    async def enrich_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
        result: ExtractorResult,
    ) -> list[dict[str, Any]]:
        """Enrich DEs with fields and breadcrumbs, using parallel requests."""
        enriched = []

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(options.max_concurrent)

        async def enrich_single(item: dict[str, Any], index: int) -> dict[str, Any]:
            async with semaphore:
                try:
                    enriched_item = await self.enrich_item(item, options)
                    return enriched_item
                except Exception as e:
                    logger.warning(f"Failed to enrich DE: {e}")
                    result.add_error(
                        "EnrichmentError",
                        str(e),
                        {"de_key": item.get("customerKey", "unknown")},
                    )
                    return item

        # Run enrichment in parallel
        tasks = [enrich_single(item, i) for i, item in enumerate(items)]

        # Process with progress reporting
        for i, coro in enumerate(asyncio.as_completed(tasks)):
            enriched_item = await coro
            enriched.append(enriched_item)

            if options.progress_callback and (i + 1) % 50 == 0:
                self._report_progress(options, "Enriching", i + 1, len(items))

        return enriched

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich DE with fields and breadcrumb path."""
        de_id = item.get("id")

        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.DE_FOLDERS
            )

        # Fetch fields if requested (uses DE ID, not key)
        if options.include_details and de_id:
            skip_fields = options.custom.get("skip_fields", False)
            if not skip_fields:
                fields = await self._fetch_de_fields(de_id)
                if fields is not None:
                    item["fields"] = fields

        return item

    async def _fetch_de_fields(self, de_id: str) -> Optional[list[dict[str, Any]]]:
        """Fetch fields for a Data Extension by ID.

        Endpoint: GET /data/v1/customobjects/{id}/fields
        """
        # Run sync request in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self._rest.get,
            f"/data/v1/customobjects/{de_id}/fields",
        )

        if result.get("ok"):
            data = result.get("data", {})
            return data.get("fields", [])

        logger.debug(f"Failed to fetch fields for DE {de_id}: {result.get('error')}")
        return None

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform DE data for output.

        API property names: id, name, key (customerKey), description, categoryId,
        isSendable, isTestable, sendableCustomObjectField, sendableSubscriberField,
        rowCount, createdDate, modifiedDate, dataRetentionProperties, fieldCount
        """
        transformed = []

        for item in items:
            fields = item.get("fields", [])
            retention = item.get("dataRetentionProperties", {})

            output = {
                "id": item.get("id"),
                "name": item.get("name"),
                # API uses "key" but we normalize to customerKey
                "customerKey": item.get("key") or item.get("customerKey"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                "isSendable": item.get("isSendable", False),
                "isTestable": item.get("isTestable", False),
                # API uses "sendableCustomObjectField" not "sendableDataExtensionField"
                "sendableDataExtensionField": item.get("sendableCustomObjectField")
                or item.get("sendableDataExtensionField"),
                "sendableSubscriberField": item.get("sendableSubscriberField"),
                "rowCount": item.get("rowCount"),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                # Retention properties are nested in dataRetentionProperties
                "deleteAtEndOfRetentionPeriod": retention.get(
                    "isDeleteAtEndOfRetentionPeriod", False
                ),
                "resetRetentionPeriodOnImport": retention.get(
                    "isResetRetentionPeriodOnImport", False
                ),
                "isRowBasedRetention": retention.get("isRowBasedRetention", False),
                "fieldCount": item.get("fieldCount") or len(fields),
                "fields": self._transform_fields(fields),
                "primaryKeyFields": [
                    f.get("name") for f in fields if f.get("isPrimaryKey")
                ],
            }
            transformed.append(output)

        return transformed

    def _transform_fields(
        self, fields: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Transform field data for output.

        API returns fields with properties: name, type, length, isPrimaryKey,
        isNullable, ordinal, description, maskType, storageType, etc.
        """
        transformed = []

        for field in fields:
            transformed.append({
                "name": field.get("name"),
                # API uses "type" not "fieldType"
                "fieldType": field.get("type") or field.get("fieldType"),
                # API uses "length" not "maxLength"
                "maxLength": field.get("length") or field.get("maxLength"),
                "isPrimaryKey": field.get("isPrimaryKey", False),
                # API uses "isNullable" (inverse of required)
                "isRequired": not field.get("isNullable", True),
                "defaultValue": field.get("defaultValue"),
                "ordinal": field.get("ordinal"),
                "description": field.get("description"),
                "scale": field.get("scale"),
                "maskType": field.get("maskType"),
                "storageType": field.get("storageType"),
            })

        return transformed
