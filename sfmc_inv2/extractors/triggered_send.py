"""Triggered Send Definition extractor for SFMC.

Extracts Triggered Send Definitions via SOAP API.
Identifies relationships to emails, lists, and profiles.
"""

import logging
from typing import Any

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class TriggeredSendExtractor(BaseExtractor):
    """Extractor for SFMC Triggered Send Definitions."""

    name = "triggered_sends"
    description = "SFMC Triggered Send Definitions (SOAP)"
    object_type = "TriggeredSendDefinition"

    # Triggered sends can exist on child BUs
    supports_multi_bu = True

    SOAP_OBJECT_TYPE = "TriggeredSendDefinition"

    SOAP_PROPERTIES = [
        "ObjectID",
        "CustomerKey",
        "Name",
        "Description",
        "TriggeredSendStatus",
        "Email.ID",
        # "Email.Name",  # Fails at enterprise level
        "List.ID",
        # "List.ListName",  # Fails at enterprise level
        "SendClassification.CustomerKey",
        # "SendClassification.Name",  # Fails at enterprise level
        "SenderProfile.CustomerKey",
        # "SenderProfile.Name",  # Fails at enterprise level
        "DeliveryProfile.CustomerKey",
        # "DeliveryProfile.Name",  # Fails at enterprise level
        "CategoryID",
        "FromName",
        "FromAddress",
        "BccEmail",
        "EmailSubject",
        "DynamicEmailSubject",
        "IsMultipart",
        "IsWrapped",
        "AutoAddSubscribers",
        "AutoUpdateSubscribers",
        "Priority",
        "CreatedDate",
        "ModifiedDate",
    ]

    required_caches = [CacheType.TRIGGERED_SEND_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch triggered send definitions via SOAP API."""
        self._pages_fetched = 0

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=self.SOAP_PROPERTIES,
            max_pages=options.max_pages,
        )

        if not result.get("ok"):
            logger.error(f"Failed to fetch triggered sends: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} triggered send definitions")

        return objects

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich triggered send with breadcrumb path."""
        category_id = item.get("CategoryID")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.TRIGGERED_SEND_FOLDERS
            )
        return item

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform triggered send data for output."""
        transformed = []

        for item in items:
            # Extract nested objects
            email = item.get("Email", {})
            list_obj = item.get("List", {})
            send_class = item.get("SendClassification", {})
            sender_profile = item.get("SenderProfile", {})
            delivery_profile = item.get("DeliveryProfile", {})

            output = {
                "id": item.get("ObjectID"),
                "objectId": item.get("ObjectID"),
                "name": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                "status": item.get("TriggeredSendStatus"),
                # Email reference
                "emailId": email.get("ID") if isinstance(email, dict) else None,
                "emailName": email.get("Name") if isinstance(email, dict) else None,
                # List reference
                "listId": list_obj.get("ID") if isinstance(list_obj, dict) else None,
                "listName": list_obj.get("ListName") if isinstance(list_obj, dict) else None,
                # Send classification
                "sendClassificationKey": send_class.get("CustomerKey") if isinstance(send_class, dict) else None,
                "sendClassificationName": send_class.get("Name") if isinstance(send_class, dict) else None,
                # Sender profile
                "senderProfileKey": sender_profile.get("CustomerKey") if isinstance(sender_profile, dict) else None,
                "senderProfileName": sender_profile.get("Name") if isinstance(sender_profile, dict) else None,
                # Delivery profile
                "deliveryProfileKey": delivery_profile.get("CustomerKey") if isinstance(delivery_profile, dict) else None,
                "deliveryProfileName": delivery_profile.get("Name") if isinstance(delivery_profile, dict) else None,
                # Send settings
                "fromName": item.get("FromName"),
                "fromAddress": item.get("FromAddress"),
                "bccEmail": item.get("BccEmail"),
                "emailSubject": item.get("EmailSubject"),
                "dynamicEmailSubject": item.get("DynamicEmailSubject"),
                "isMultipart": item.get("IsMultipart") == "true" if isinstance(item.get("IsMultipart"), str) else item.get("IsMultipart"),
                "isWrapped": item.get("IsWrapped") == "true" if isinstance(item.get("IsWrapped"), str) else item.get("IsWrapped"),
                "autoAddSubscribers": item.get("AutoAddSubscribers") == "true" if isinstance(item.get("AutoAddSubscribers"), str) else item.get("AutoAddSubscribers"),
                "autoUpdateSubscribers": item.get("AutoUpdateSubscribers") == "true" if isinstance(item.get("AutoUpdateSubscribers"), str) else item.get("AutoUpdateSubscribers"),
                "priority": item.get("Priority"),
                # Folder
                "categoryId": item.get("CategoryID"),
                "folderPath": item.get("folderPath"),
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from triggered sends to other objects."""
        for item in items:
            ts_id = item.get("ObjectID")
            ts_name = item.get("Name")

            if not ts_id:
                continue

            # Email relationship
            email = item.get("Email", {})
            if isinstance(email, dict) and email.get("ID"):
                result.add_relationship(
                    source_id=str(ts_id),
                    source_type="triggered_send",
                    source_name=ts_name,
                    target_id=str(email.get("ID")),
                    target_type="email",
                    target_name=email.get("Name"),
                    relationship_type=RelationshipType.TRIGGERED_SEND_USES_EMAIL,
                )

            # List relationship
            list_obj = item.get("List", {})
            if isinstance(list_obj, dict) and list_obj.get("ID"):
                result.add_relationship(
                    source_id=str(ts_id),
                    source_type="triggered_send",
                    source_name=ts_name,
                    target_id=str(list_obj.get("ID")),
                    target_type="list",
                    target_name=list_obj.get("ListName"),
                    relationship_type=RelationshipType.TRIGGERED_SEND_USES_LIST,
                )

            # Sender profile relationship
            sender_profile = item.get("SenderProfile", {})
            if isinstance(sender_profile, dict) and sender_profile.get("CustomerKey"):
                result.add_relationship(
                    source_id=str(ts_id),
                    source_type="triggered_send",
                    source_name=ts_name,
                    target_id=sender_profile.get("CustomerKey"),
                    target_type="sender_profile",
                    target_name=sender_profile.get("Name"),
                    relationship_type=RelationshipType.TRIGGERED_SEND_USES_SENDER_PROFILE,
                )

            # Delivery profile relationship
            delivery_profile = item.get("DeliveryProfile", {})
            if isinstance(delivery_profile, dict) and delivery_profile.get("CustomerKey"):
                result.add_relationship(
                    source_id=str(ts_id),
                    source_type="triggered_send",
                    source_name=ts_name,
                    target_id=delivery_profile.get("CustomerKey"),
                    target_type="delivery_profile",
                    target_name=delivery_profile.get("Name"),
                    relationship_type=RelationshipType.TRIGGERED_SEND_USES_DELIVERY_PROFILE,
                )

            # Send classification relationship
            send_class = item.get("SendClassification", {})
            if isinstance(send_class, dict) and send_class.get("CustomerKey"):
                result.add_relationship(
                    source_id=str(ts_id),
                    source_type="triggered_send",
                    source_name=ts_name,
                    target_id=send_class.get("CustomerKey"),
                    target_type="send_classification",
                    target_name=send_class.get("Name"),
                    relationship_type=RelationshipType.TRIGGERED_SEND_USES_SEND_CLASSIFICATION,
                )
