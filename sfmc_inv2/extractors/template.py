"""Template extractor for SFMC.

Extracts classic email template definitions via SOAP API.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class TemplateExtractor(BaseExtractor):
    """Extractor for SFMC Classic Email Templates."""

    name = "templates"
    description = "SFMC Classic Email Templates (SOAP)"
    object_type = "Template"

    SOAP_OBJECT_TYPE = "Template"

    SOAP_PROPERTIES = [
        "ID",
        "ObjectID",
        "CustomerKey",
        "TemplateName",
        "TemplateSubject",
        "CategoryID",
        "ActiveFlag",
        "Align",
        "BackgroundColor",
        "BorderColor",
        "BorderWidth",
        "Cellpadding",
        "Cellspacing",
        "Width",
        "IsBlank",
        "IsTemplateSubjectLocked",
        "CreatedDate",
        "ModifiedDate",
    ]

    # Additional properties when include_content is True
    CONTENT_PROPERTIES = [
        "LayoutHTML",
        "HeaderContent.ID",
        "HeaderContent.Content",
    ]

    required_caches = [CacheType.TEMPLATE_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch templates via SOAP API."""
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
            logger.error(f"Failed to fetch templates: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} templates")

        return objects

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich template with breadcrumb path."""
        category_id = item.get("CategoryID")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.TEMPLATE_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform template data for output."""
        transformed = []

        for item in items:
            header_content = item.get("HeaderContent", {})

            output = {
                "id": item.get("ID"),
                "objectId": item.get("ObjectID"),
                "templateName": item.get("TemplateName"),
                "customerKey": item.get("CustomerKey"),
                "templateSubject": item.get("TemplateSubject"),
                "isActive": item.get("ActiveFlag") == "true" if isinstance(item.get("ActiveFlag"), str) else item.get("ActiveFlag"),
                "isBlank": item.get("IsBlank") == "true" if isinstance(item.get("IsBlank"), str) else item.get("IsBlank"),
                "isTemplateSubjectLocked": item.get("IsTemplateSubjectLocked") == "true" if isinstance(item.get("IsTemplateSubjectLocked"), str) else item.get("IsTemplateSubjectLocked"),
                # Layout settings
                "align": item.get("Align"),
                "backgroundColor": item.get("BackgroundColor"),
                "borderColor": item.get("BorderColor"),
                "borderWidth": item.get("BorderWidth"),
                "cellpadding": item.get("Cellpadding"),
                "cellspacing": item.get("Cellspacing"),
                "width": item.get("Width"),
                # Folder
                "categoryId": item.get("CategoryID"),
                "folderPath": item.get("folderPath"),
                # Content (if requested)
                "layoutHTML": item.get("LayoutHTML") if options.include_content else None,
                "headerContentId": header_content.get("ID") if isinstance(header_content, dict) else None,
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed
