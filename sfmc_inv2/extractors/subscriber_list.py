"""List extractor for SFMC.

Extracts subscriber list definitions via SOAP API.
Note: This extracts list metadata only, not subscriber data.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class ListExtractor(BaseExtractor):
    """Extractor for SFMC List definitions.

    Note: This extracts list metadata (structure, classification),
    NOT subscriber data. Subscriber data extraction is out of scope.
    """

    name = "lists"
    description = "SFMC Subscriber List Definitions (SOAP)"
    object_type = "List"

    SOAP_OBJECT_TYPE = "List"

    SOAP_PROPERTIES = [
        "ID",
        "ObjectID",
        "CustomerKey",
        "ListName",
        "Description",
        "Category",
        "Type",
        "ListClassification",
        "AutomatedEmail.ID",
        "CreatedDate",
        "ModifiedDate",
    ]

    required_caches = [CacheType.LIST_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch lists via SOAP API."""
        self._pages_fetched = 0

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=self.SOAP_PROPERTIES,
            max_pages=options.max_pages,
        )

        if not result.get("ok"):
            logger.error(f"Failed to fetch lists: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} lists")

        return objects

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich list with breadcrumb path."""
        category = item.get("Category")
        if category:
            item["folderPath"] = self.get_breadcrumb(
                str(category), CacheType.LIST_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform list data for output."""
        transformed = []

        for item in items:
            automated_email = item.get("AutomatedEmail", {})

            output = {
                "id": item.get("ID"),
                "objectId": item.get("ObjectID"),
                "listName": item.get("ListName"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                "type": item.get("Type"),
                "listClassification": item.get("ListClassification"),
                # Folder
                "category": item.get("Category"),
                "folderPath": item.get("folderPath"),
                # Automated email (if linked)
                "automatedEmailId": automated_email.get("ID") if isinstance(automated_email, dict) else None,
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed
