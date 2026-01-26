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
# Based on mcdev (sfmc-devtools) authoritative activityTypeMapping
# Reference: https://github.com/Accenture/sfmc-devtools
# See: lib/metadataTypes/definitions/Automation.definition.js
ACTIVITY_TYPE_MAP = {
    42: "Email Send",  # emailSend in mcdev
    43: "Import File",  # importFile in mcdev
    45: "Refresh Group",  # refreshGroup in mcdev
    53: "File Transfer",  # fileTransfer in mcdev
    73: "Data Extract",  # dataExtract in mcdev
    84: "Report Definition",  # reportDefinition in mcdev
    300: "Query Activity",  # query in mcdev
    303: "Filter Activity",  # filter in mcdev
    423: "Script Activity",  # script in mcdev
    425: "Data Factory Utility",  # dataFactoryUtility in mcdev (UI-only ELT)
    427: "Build Audience",  # UI-only
    467: "Wait Activity",  # wait in mcdev
    667: "Journey Entry Injection",
    724: "Refresh Mobile Filtered List",  # refreshMobileFilteredList in mcdev
    725: "SMS",  # sms in mcdev
    726: "Import Mobile Contact",  # importMobileContact in mcdev
    733: "Journey Entry (Legacy)",  # journeyEntryOld in mcdev
    736: "Push Notification",  # push in mcdev
    749: "Fire Event",  # fireEvent in mcdev
    771: "Salesforce Send",
    783: "Send SMS (v2)",  # Alternative SMS activity type
    952: "Journey Entry",  # journeyEntry in mcdev
    1000: "Verification Activity",  # verification in mcdev
    1010: "Interaction Studio Data",  # interactionStudioData in mcdev
    1101: "Interactions",  # interactions in mcdev (Audience Studio)
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


def parse_schedule_type(schedule: Optional[dict[str, Any]]) -> str:
    """Parse schedule data into a human-readable schedule type.

    Parses the icalRecur field (e.g., 'FREQ=DAILY;COUNT=1;INTERVAL=1') and
    other schedule properties into a readable string.
    """
    if not schedule:
        return ""

    # Check schedule status first
    status = schedule.get("scheduleStatus", "")
    if status == "none":
        return ""  # Not scheduled

    ical = schedule.get("icalRecur", "")
    if not ical:
        # Check for trigger-based schedules
        type_id = schedule.get("typeId")
        if type_id == 2:  # File drop trigger
            return "Triggered (File Drop)"
        return ""

    # Parse icalRecur string into a dict
    parts = {}
    for part in ical.split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            parts[key] = value

    freq = parts.get("FREQ", "").lower()
    interval = parts.get("INTERVAL", "1")
    count = parts.get("COUNT")
    byday = parts.get("BYDAY")
    bymonthday = parts.get("BYMONTHDAY")

    # Build human-readable string
    if freq == "minutely":
        base = f"Every {interval} minute(s)" if interval != "1" else "Every minute"
    elif freq == "hourly":
        base = f"Every {interval} hour(s)" if interval != "1" else "Hourly"
    elif freq == "daily":
        base = f"Every {interval} day(s)" if interval != "1" else "Daily"
    elif freq == "weekly":
        base = f"Every {interval} week(s)" if interval != "1" else "Weekly"
        if byday:
            days = {"MO": "Mon", "TU": "Tue", "WE": "Wed", "TH": "Thu",
                    "FR": "Fri", "SA": "Sat", "SU": "Sun"}
            day_list = [days.get(d, d) for d in byday.split(",")]
            base += f" ({', '.join(day_list)})"
    elif freq == "monthly":
        base = f"Every {interval} month(s)" if interval != "1" else "Monthly"
        if bymonthday:
            base += f" (day {bymonthday})"
    elif freq == "yearly":
        base = f"Every {interval} year(s)" if interval != "1" else "Yearly"
    else:
        base = freq.capitalize() if freq else "Unknown"

    # Add count if it's a one-time schedule
    if count == "1":
        base = "Once"

    return base


class AutomationExtractor(BaseExtractor):
    """Extractor for SFMC Automations."""

    name = "automations"
    description = "SFMC Automations with steps and activities"
    object_type = "Automation"

    # Automations can exist on child BUs, so aggregate across all configured BUs
    supports_multi_bu = True

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
        # API returns objectTypeId (not activityTypeId)
        activity_type_id = activity.get("objectTypeId")
        if activity_type_id is not None:
            activity["activityTypeName"] = ACTIVITY_TYPE_MAP.get(
                activity_type_id, f"Unknown ({activity_type_id})"
            )

        # API returns activityObjectId (not objectId)
        object_id = activity.get("activityObjectId")
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
            schedule = item.get("schedule")
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
                "scheduleType": parse_schedule_type(schedule),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                "createdBy": item.get("createdBy"),
                "modifiedBy": item.get("modifiedBy"),
                "lastRunTime": item.get("lastRunTime"),
                "lastRunStatus": item.get("lastRunStatus"),
                "schedule": schedule,
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
                    # API returns objectTypeId (not activityTypeId) and
                    # activityObjectId (not objectId)
                    activity_type_id = activity.get("objectTypeId")
                    object_id = activity.get("activityObjectId")

                    if not object_id:
                        continue

                    # Map activity types to relationship types
                    # Based on mcdev authoritative activityTypeMapping
                    rel_type_map = {
                        # Core activities with external references
                        300: (RelationshipType.AUTOMATION_CONTAINS_QUERY, "query"),
                        423: (RelationshipType.AUTOMATION_CONTAINS_SCRIPT, "script"),
                        43: (RelationshipType.AUTOMATION_CONTAINS_IMPORT, "import"),
                        73: (RelationshipType.AUTOMATION_CONTAINS_EXTRACT, "data_extract"),
                        53: (RelationshipType.AUTOMATION_CONTAINS_TRANSFER, "file_transfer"),
                        303: (RelationshipType.AUTOMATION_CONTAINS_FILTER, "filter"),
                        # Email activities
                        42: (RelationshipType.AUTOMATION_CONTAINS_EMAIL, "email"),  # emailSend
                        # Event and entry activities
                        749: (RelationshipType.AUTOMATION_CONTAINS_FIRE_EVENT, "event_definition"),  # fireEvent
                        667: (RelationshipType.AUTOMATION_CONTAINS_JOURNEY_ENTRY, "event_definition"),
                        733: (RelationshipType.AUTOMATION_CONTAINS_JOURNEY_ENTRY, "event_definition"),  # journeyEntryOld
                        952: (RelationshipType.AUTOMATION_CONTAINS_JOURNEY_ENTRY, "event_definition"),  # journeyEntry
                        # Send activities
                        725: (RelationshipType.AUTOMATION_CONTAINS_SMS, "sms_definition"),  # sms
                        783: (RelationshipType.AUTOMATION_CONTAINS_SMS, "sms_definition"),  # sms v2
                        771: (RelationshipType.AUTOMATION_CONTAINS_SALESFORCE_SEND, "salesforce_campaign"),
                        736: (RelationshipType.AUTOMATION_CONTAINS_PUSH, "push_definition"),  # push
                        # Refresh activities
                        724: (RelationshipType.AUTOMATION_CONTAINS_REFRESH_GROUP, "group"),  # refreshGroup
                        # Activities with inline config (no external references)
                        467: (RelationshipType.AUTOMATION_CONTAINS_WAIT, "wait"),
                        1000: (RelationshipType.AUTOMATION_CONTAINS_VERIFICATION, "verification"),
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

                    # Extract relationships from targetDataExtensions array
                    # This array is present for imports, queries, and filters
                    target_des = activity.get("targetDataExtensions", [])
                    for target_de in target_des:
                        de_id = target_de.get("id")
                        de_name = target_de.get("name")
                        if not de_id:
                            continue

                        # Map activity type to appropriate writes relationship
                        if activity_type_id == 43:  # Import
                            result.add_relationship(
                                source_id=str(object_id),
                                source_type="import",
                                source_name=activity.get("name"),
                                target_id=str(de_id),
                                target_type="data_extension",
                                target_name=de_name,
                                relationship_type=RelationshipType.IMPORT_WRITES_DE,
                            )
                        elif activity_type_id == 300:  # Query
                            result.add_relationship(
                                source_id=str(object_id),
                                source_type="query",
                                source_name=activity.get("name"),
                                target_id=str(de_id),
                                target_type="data_extension",
                                target_name=de_name,
                                relationship_type=RelationshipType.QUERY_WRITES_DE,
                            )
                        elif activity_type_id == 303:  # Filter
                            result.add_relationship(
                                source_id=str(object_id),
                                source_type="filter",
                                source_name=activity.get("name"),
                                target_id=str(de_id),
                                target_type="data_extension",
                                target_name=de_name,
                                relationship_type=RelationshipType.FILTER_WRITES_DE,
                            )
