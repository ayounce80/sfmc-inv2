"""Automation extractor for SFMC automations.

Extracts automations with their steps, activities, and schedules.
Identifies relationships to queries, scripts, DEs, and other objects.
"""

import asyncio
import logging
from typing import Any, Optional

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)

# Activity type ID to name mapping
ACTIVITY_TYPE_MAP = {
    42: "Refresh Group",
    43: "Import File",
    45: "Transfer File (Legacy)",
    53: "File Transfer",
    73: "Data Extract",
    84: "Report Definition",
    300: "Query Activity",
    303: "Filter Activity",
    423: "Script Activity",
    425: "Verification Activity",
    427: "Wait Activity",
    667: "Journey Entry Injection",
    724: "Fire Event",
    725: "Exclusion Script",
    726: "Predictive Intelligence Recommendations",
    733: "Send Email",
    736: "Triggered Send Refresh",
    749: "Fire Entry Event",
    771: "Salesforce Send",
    783: "Send SMS",
    1101: "Audience Studio Segment Refresh",
}

# Automation status ID to name mapping
AUTOMATION_STATUS_MAP = {
    -1: "Error",
    0: "Building",
    1: "Ready",
    2: "Running",
    3: "Paused",
    4: "Stopped",
    5: "Scheduled",
    6: "Awaiting Trigger",
    7: "InactiveTrigger",
    8: "Skipped",
}


class AutomationExtractor(BaseExtractor):
    """Extractor for SFMC Automations."""

    name = "automations"
    description = "SFMC Automations with steps and activities"
    object_type = "Automation"

    required_caches = [
        CacheType.AUTOMATION_FOLDERS,
        CacheType.QUERIES,
        CacheType.SCRIPTS,
    ]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch automations via REST API with pagination."""
        automations = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/automation/v1/automations?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch automations page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            automations.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(automations), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return automations

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich automation with detail, breadcrumb, and resolved names."""
        automation_id = item.get("id")

        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.AUTOMATION_FOLDERS
            )

        # Resolve status name
        status = item.get("status")
        if status is not None:
            item["statusName"] = AUTOMATION_STATUS_MAP.get(status, f"Unknown ({status})")

        # Fetch detailed info if requested
        if options.include_details and automation_id:
            detail = await self._fetch_automation_detail(automation_id)
            if detail:
                item["steps"] = detail.get("steps", [])
                item["schedule"] = detail.get("schedule")
                item["notifications"] = detail.get("notifications")
                item["lastRunTime"] = detail.get("lastRunTime")
                item["lastRunStatus"] = detail.get("lastRunStatus")

                # Enrich steps and activities
                for step in item.get("steps", []):
                    for activity in step.get("activities", []):
                        self._enrich_activity(activity)

        return item

    async def _fetch_automation_detail(
        self, automation_id: str
    ) -> Optional[dict[str, Any]]:
        """Fetch detailed automation info including steps."""
        result = self._rest.get(f"/automation/v1/automations/{automation_id}")

        if result.get("ok"):
            return result.get("data", {})

        logger.debug(f"Failed to fetch automation detail {automation_id}: {result.get('error')}")
        return None

    def _enrich_activity(self, activity: dict[str, Any]) -> None:
        """Enrich activity with resolved type name and object info."""
        activity_type_id = activity.get("activityTypeId")
        if activity_type_id is not None:
            activity["activityTypeName"] = ACTIVITY_TYPE_MAP.get(
                activity_type_id, f"Unknown ({activity_type_id})"
            )

        # Resolve object references based on activity type
        object_id = activity.get("objectId")
        if object_id:
            if activity_type_id == 300:  # Query
                queries = self._cache.get_queries()
                query = queries.get(str(object_id), {})
                activity["queryName"] = query.get("name")
                activity["targetDataExtensionId"] = query.get("targetId")
                activity["targetDataExtensionName"] = query.get("targetName")

            elif activity_type_id == 423:  # Script
                scripts = self._cache.get_scripts()
                script = scripts.get(str(object_id), {})
                activity["scriptName"] = script.get("name")

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform automation data for output."""
        transformed = []

        for item in items:
            output = {
                "id": item.get("id"),
                "name": item.get("name"),
                "description": item.get("description"),
                "customerKey": item.get("key"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                "status": item.get("status"),
                "statusName": item.get("statusName"),
                "isActive": item.get("isActive"),
                "type": item.get("type"),
                "typeId": item.get("typeId"),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                "lastRunTime": item.get("lastRunTime"),
                "lastRunStatus": item.get("lastRunStatus"),
                "schedule": item.get("schedule"),
                "notifications": item.get("notifications"),
                "steps": item.get("steps", []),
                "stepCount": len(item.get("steps", [])),
                "activityCount": sum(
                    len(step.get("activities", []))
                    for step in item.get("steps", [])
                ),
            }
            transformed.append(output)

        return transformed

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from automations to other objects."""
        for item in items:
            automation_id = item.get("id")
            automation_name = item.get("name")

            if not automation_id:
                continue

            for step in item.get("steps", []):
                for activity in step.get("activities", []):
                    activity_type_id = activity.get("activityTypeId")
                    object_id = activity.get("objectId")

                    if not object_id:
                        continue

                    # Map activity types to relationship types
                    rel_type_map = {
                        300: (RelationshipType.AUTOMATION_CONTAINS_QUERY, "query"),
                        423: (RelationshipType.AUTOMATION_CONTAINS_SCRIPT, "script"),
                        43: (RelationshipType.AUTOMATION_CONTAINS_IMPORT, "import"),
                        73: (RelationshipType.AUTOMATION_CONTAINS_EXTRACT, "data_extract"),
                        53: (RelationshipType.AUTOMATION_CONTAINS_TRANSFER, "file_transfer"),
                        733: (RelationshipType.AUTOMATION_CONTAINS_EMAIL, "email"),
                        303: (RelationshipType.AUTOMATION_CONTAINS_FILTER, "filter"),
                    }

                    if activity_type_id in rel_type_map:
                        rel_type, target_type = rel_type_map[activity_type_id]
                        result.add_relationship(
                            source_id=automation_id,
                            source_type="automation",
                            source_name=automation_name,
                            target_id=str(object_id),
                            target_type=target_type,
                            target_name=activity.get("name"),
                            relationship_type=rel_type,
                        )

                        # For queries, also track the target DE relationship
                        if activity_type_id == 300:
                            target_de_id = activity.get("targetDataExtensionId")
                            if target_de_id:
                                result.add_relationship(
                                    source_id=str(object_id),
                                    source_type="query",
                                    source_name=activity.get("queryName"),
                                    target_id=str(target_de_id),
                                    target_type="data_extension",
                                    target_name=activity.get("targetDataExtensionName"),
                                    relationship_type=RelationshipType.QUERY_WRITES_DE,
                                )
