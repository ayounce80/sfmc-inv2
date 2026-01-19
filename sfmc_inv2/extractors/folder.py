"""Folder extractor for SFMC Automation Studio.

Extracts folder structure for all Automation Studio object types.
"""

import logging
from typing import Any

from ..clients.soap_client import build_simple_filter
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)

# Content types for Automation Studio folders
AUTOMATION_CONTENT_TYPES = [
    "automations",
    "queryactivity",
    "ssjsactivity",
    "importactivity",
    "dataextractactivity",
    "filetransferactivity",
    "filteractivity",
    "dataextension",
]


class FolderExtractor(BaseExtractor):
    """Extractor for SFMC Automation Studio Folders.

    Extracts folder hierarchies via SOAP DataFolder object.
    """

    name = "folders"
    description = "SFMC Automation Studio Folders"
    object_type = "DataFolder"

    # No cache warming needed - we're extracting the folders themselves
    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch folders via SOAP API.

        Retrieves folders for all Automation Studio content types.
        """
        all_folders = []
        self._pages_fetched = 0

        for content_type in AUTOMATION_CONTENT_TYPES:
            self._report_progress(
                options,
                f"Fetching {content_type} folders",
                len(all_folders),
                0,
            )

            filter_xml = build_simple_filter("ContentType", "equals", content_type)

            result = self._soap.retrieve_all_pages(
                object_type="DataFolder",
                properties=[
                    "ID",
                    "ObjectID",
                    "CustomerKey",
                    "Name",
                    "ParentFolder.ID",
                    "ParentFolder.Name",
                    "ContentType",
                    "Description",
                    "IsActive",
                    "IsEditable",
                    "AllowChildren",
                    "CreatedDate",
                    "ModifiedDate",
                ],
                filter_xml=filter_xml,
                max_pages=options.max_pages,
            )

            if result.get("ok"):
                folders = result.get("objects", [])
                # Tag each folder with its content type
                for folder in folders:
                    folder["_contentType"] = content_type
                all_folders.extend(folders)
                self._pages_fetched += result.get("pages_retrieved", 1)
            else:
                logger.warning(f"Failed to fetch {content_type} folders: {result.get('error')}")

        logger.info(f"Retrieved {len(all_folders)} folders across all content types")
        return all_folders

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform folder data for output."""
        transformed = []

        for item in items:
            parent_folder = item.get("ParentFolder", {})

            output = {
                "id": item.get("ID"),
                "objectId": item.get("ObjectID"),
                "name": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                "contentType": item.get("ContentType") or item.get("_contentType"),
                "parentId": parent_folder.get("ID") if isinstance(parent_folder, dict) else None,
                "parentName": parent_folder.get("Name") if isinstance(parent_folder, dict) else None,
                "isActive": item.get("IsActive") == "true" if isinstance(item.get("IsActive"), str) else item.get("IsActive", True),
                "isEditable": item.get("IsEditable") == "true" if isinstance(item.get("IsEditable"), str) else item.get("IsEditable", True),
                "allowChildren": item.get("AllowChildren") == "true" if isinstance(item.get("AllowChildren"), str) else item.get("AllowChildren", True),
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed
