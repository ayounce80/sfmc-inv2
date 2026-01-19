"""Classic Email extractor for SFMC.

Extracts classic email definitions (non-Content Builder) via SOAP API.
These are distinct from Content Builder email assets.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class ClassicEmailExtractor(BaseExtractor):
    """Extractor for SFMC Classic Email definitions.

    Note: This extracts classic (non-Content Builder) emails via SOAP.
    For Content Builder emails, use the AssetExtractor.
    """

    name = "classic_emails"
    description = "SFMC Classic Email Definitions (SOAP)"
    object_type = "Email"

    # SOAP object type for retrieval
    SOAP_OBJECT_TYPE = "Email"

    # Properties to retrieve (excludes HTMLBody/TextBody by default for performance)
    SOAP_PROPERTIES = [
        "ID",
        "ObjectID",
        "CustomerKey",
        "Name",
        "Subject",
        "Status",
        "CategoryID",
        "IsHTMLPaste",
        "CharacterSet",
        "HasDynamicSubjectLine",
        "PreHeader",
        "CreatedDate",
        "ModifiedDate",
    ]

    # Additional properties when include_content is True
    CONTENT_PROPERTIES = [
        "HTMLBody",
        "TextBody",
    ]

    required_caches = [CacheType.EMAIL_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch classic emails via SOAP API."""
        self._pages_fetched = 0

        # Determine which properties to retrieve
        properties = list(self.SOAP_PROPERTIES)
        if options.include_content:
            properties.extend(self.CONTENT_PROPERTIES)

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=properties,
            max_pages=options.max_pages,
        )

        if not result.get("ok"):
            logger.error(f"Failed to fetch classic emails: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} classic emails")

        return objects

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich email with breadcrumb path."""
        # Add breadcrumb path
        category_id = item.get("CategoryID")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.EMAIL_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform classic email data for output.

        Normalizes SOAP field names to consistent format.
        """
        transformed = []

        for item in items:
            output = {
                # Normalize ID fields
                "id": item.get("ID"),
                "objectId": item.get("ObjectID"),
                "name": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                # Email properties
                "subject": item.get("Subject"),
                "status": item.get("Status"),
                "isHTMLPaste": item.get("IsHTMLPaste") == "true" if isinstance(item.get("IsHTMLPaste"), str) else item.get("IsHTMLPaste"),
                "characterSet": item.get("CharacterSet"),
                "hasDynamicSubjectLine": item.get("HasDynamicSubjectLine") == "true" if isinstance(item.get("HasDynamicSubjectLine"), str) else item.get("HasDynamicSubjectLine"),
                "preHeader": item.get("PreHeader"),
                "hasPreheader": bool(item.get("PreHeader")),
                # Folder
                "categoryId": item.get("CategoryID"),
                "folderPath": item.get("folderPath"),
                # Content (if requested)
                "htmlBody": item.get("HTMLBody") if options.include_content else None,
                "textBody": item.get("TextBody") if options.include_content else None,
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed
