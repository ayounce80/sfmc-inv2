"""Journey extractor for SFMC Journey Builder.

Extracts journeys with their activities, triggers, and goals.
Identifies relationships to DEs, emails, and automations.
"""

import asyncio
import logging
from typing import Any, Optional

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)

# Journey status values
JOURNEY_STATUS_MAP = {
    "Draft": "Draft",
    "Published": "Published",
    "ScheduledToPublish": "Scheduled to Publish",
    "Running": "Running",
    "Paused": "Paused",
    "Stopped": "Stopped",
    "Deleted": "Deleted",
}


class JourneyExtractor(BaseExtractor):
    """Extractor for SFMC Journeys."""

    name = "journeys"
    description = "SFMC Journey Builder Journeys"
    object_type = "Journey"

    required_caches = [CacheType.DE_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch journeys via REST API with pagination."""
        journeys = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/interaction/v1/interactions?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch journeys page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            journeys.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(journeys), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return journeys

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich journey with detail and resolved names."""
        journey_id = item.get("id")

        # Fetch detailed info if requested
        if options.include_details and journey_id:
            detail = await self._fetch_journey_detail(journey_id)
            if detail:
                item.update({
                    "triggers": detail.get("triggers", []),
                    "activities": detail.get("activities", []),
                    "goals": detail.get("goals", []),
                    "entryMode": detail.get("entryMode"),
                    "definitionId": detail.get("definitionId"),
                    "workflowApiVersion": detail.get("workflowApiVersion"),
                    "stats": detail.get("stats"),
                })

        # Resolve status name
        status = item.get("status")
        if status:
            item["statusName"] = JOURNEY_STATUS_MAP.get(status, status)

        return item

    async def _fetch_journey_detail(
        self, journey_id: str
    ) -> Optional[dict[str, Any]]:
        """Fetch detailed journey info including activities."""
        # Run sync request in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self._rest.get,
            f"/interaction/v1/interactions/{journey_id}",
        )

        if result.get("ok"):
            return result.get("data", {})

        logger.debug(f"Failed to fetch journey detail {journey_id}: {result.get('error')}")
        return None

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform journey data for output."""
        transformed = []

        for item in items:
            activities = item.get("activities", [])
            triggers = item.get("triggers", [])
            goals = item.get("goals", [])

            output = {
                "id": item.get("id"),
                "name": item.get("name"),
                "key": item.get("key"),
                "description": item.get("description"),
                "version": item.get("version"),
                "status": item.get("status"),
                "statusName": item.get("statusName"),
                "definitionId": item.get("definitionId"),
                "workflowApiVersion": item.get("workflowApiVersion"),
                "entryMode": item.get("entryMode"),
                "channel": item.get("channel"),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                "lastPublishedDate": item.get("lastPublishedDate"),
                "triggers": self._transform_triggers(triggers),
                "triggerCount": len(triggers),
                "activities": self._transform_activities(activities) if options.include_details else [],
                "activityCount": len(activities),
                "goals": self._transform_goals(goals),
                "goalCount": len(goals),
                "stats": item.get("stats"),
            }
            transformed.append(output)

        return transformed

    def _transform_triggers(
        self, triggers: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Transform trigger data."""
        transformed = []
        for trigger in triggers:
            # eventDefinitionKey/Id are nested in metaData
            meta = trigger.get("metaData", {})
            transformed.append({
                "id": trigger.get("id"),
                "key": trigger.get("key"),
                "name": trigger.get("name"),
                "type": trigger.get("type"),
                "eventDefinitionId": meta.get("eventDefinitionId"),
                "eventDefinitionKey": meta.get("eventDefinitionKey"),
            })
        return transformed

    def _transform_activities(
        self, activities: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Transform activity data."""
        transformed = []
        for activity in activities:
            transformed.append({
                "id": activity.get("id"),
                "key": activity.get("key"),
                "name": activity.get("name"),
                "type": activity.get("type"),
                "configurationUrl": activity.get("configurationUrl"),
                "outcomeCount": len(activity.get("outcomes", [])),
            })
        return transformed

    def _transform_goals(
        self, goals: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Transform goal data."""
        transformed = []
        for goal in goals:
            transformed.append({
                "name": goal.get("name"),
                "description": goal.get("description"),
                "metric": goal.get("metric"),
                "target": goal.get("target"),
            })
        return transformed

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from journeys to other objects."""
        for item in items:
            journey_id = item.get("id")
            journey_name = item.get("name")

            if not journey_id:
                continue

            # Process triggers for event definition and DE relationships
            for trigger in item.get("triggers", []):
                # Event definition relationship (from metaData)
                meta = trigger.get("metaData", {})
                event_def_id = meta.get("eventDefinitionId")
                event_def_key = meta.get("eventDefinitionKey")

                if event_def_id:
                    result.add_relationship(
                        source_id=str(journey_id),
                        source_type="journey",
                        source_name=journey_name,
                        target_id=str(event_def_id),
                        target_type="event_definition",
                        target_name=trigger.get("name"),
                        relationship_type=RelationshipType.JOURNEY_USES_EVENT,
                        metadata={"eventDefinitionKey": event_def_key},
                    )

                # Event-triggered journeys may also reference a DE directly
                config_args = trigger.get("configurationArguments", {})
                de_key = config_args.get("eventDataConfig", {}).get("deKey")

                if de_key:
                    result.add_relationship(
                        source_id=str(journey_id),
                        source_type="journey",
                        source_name=journey_name,
                        target_id=de_key,
                        target_type="data_extension",
                        target_name=de_key,
                        relationship_type=RelationshipType.JOURNEY_USES_DE,
                        metadata={"usage": "entry_event"},
                    )

            # Process activities for various relationships
            for activity in item.get("activities", []):
                activity_type = activity.get("type", "")
                config_args = activity.get("configurationArguments", {})

                # Email activities
                if "email" in activity_type.lower() or activity_type == "EMAILV2":
                    email_id = config_args.get("triggeredSend", {}).get("emailId")
                    if email_id:
                        result.add_relationship(
                            source_id=str(journey_id),
                            source_type="journey",
                            source_name=journey_name,
                            target_id=str(email_id),
                            target_type="email",
                            relationship_type=RelationshipType.JOURNEY_USES_EMAIL,
                        )

                # Filter activities
                if activity_type == "ENGAGMENTSPLIT" or "filter" in activity_type.lower():
                    filter_id = config_args.get("filterId")
                    if filter_id:
                        result.add_relationship(
                            source_id=str(journey_id),
                            source_type="journey",
                            source_name=journey_name,
                            target_id=str(filter_id),
                            target_type="filter",
                            relationship_type=RelationshipType.JOURNEY_USES_FILTER,
                        )

                # Update Contact Data activities (write to DE)
                if activity_type == "UPDATECONTACTDATA":
                    de_key = config_args.get("deKey")
                    if de_key:
                        result.add_relationship(
                            source_id=str(journey_id),
                            source_type="journey",
                            source_name=journey_name,
                            target_id=de_key,
                            target_type="data_extension",
                            relationship_type=RelationshipType.JOURNEY_USES_DE,
                            metadata={"usage": "update_contact"},
                        )

                # Fire Automation activities
                if activity_type == "FIREAUTOMATION":
                    automation_id = config_args.get("automationId")
                    if automation_id:
                        result.add_relationship(
                            source_id=str(journey_id),
                            source_type="journey",
                            source_name=journey_name,
                            target_id=str(automation_id),
                            target_type="automation",
                            relationship_type=RelationshipType.JOURNEY_USES_AUTOMATION,
                        )
