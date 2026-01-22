"""Send Classification extractor for SFMC.

Extracts Send Classification definitions via SOAP API.
Identifies relationships to sender and delivery profiles.
"""

import logging
from typing import Any

from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class SendClassificationExtractor(BaseExtractor):
    """Extractor for SFMC Send Classifications."""

    name = "send_classifications"
    description = "SFMC Send Classifications (SOAP)"
    object_type = "SendClassification"

    SOAP_OBJECT_TYPE = "SendClassification"

    SOAP_PROPERTIES = [
        "ObjectID",
        "CustomerKey",
        "Name",
        "Description",
        "SenderProfile.ObjectID",
        "SenderProfile.CustomerKey",
        # "SenderProfile.Name",  # Fails at enterprise level
        "DeliveryProfile.ObjectID",
        "DeliveryProfile.CustomerKey",
        # "DeliveryProfile.Name",  # Fails at enterprise level
        # "HonorPublicationListOptOutsForTransactionalSends",  # Fails at enterprise level
        # "SendPriority",  # Fails at enterprise level
        "CreatedDate",
        "ModifiedDate",
    ]

    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch send classifications via SOAP API."""
        self._pages_fetched = 0

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=self.SOAP_PROPERTIES,
            max_pages=options.max_pages,
        )

        if not result.get("ok"):
            logger.error(f"Failed to fetch send classifications: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} send classifications")

        return objects

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform send classification data for output."""
        transformed = []

        for item in items:
            sender_profile = item.get("SenderProfile", {})
            delivery_profile = item.get("DeliveryProfile", {})

            output = {
                "objectId": item.get("ObjectID"),
                "name": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                # Sender profile reference
                "senderProfileId": sender_profile.get("ObjectID") if isinstance(sender_profile, dict) else None,
                "senderProfileKey": sender_profile.get("CustomerKey") if isinstance(sender_profile, dict) else None,
                "senderProfileName": sender_profile.get("Name") if isinstance(sender_profile, dict) else None,
                # Delivery profile reference
                "deliveryProfileId": delivery_profile.get("ObjectID") if isinstance(delivery_profile, dict) else None,
                "deliveryProfileKey": delivery_profile.get("CustomerKey") if isinstance(delivery_profile, dict) else None,
                "deliveryProfileName": delivery_profile.get("Name") if isinstance(delivery_profile, dict) else None,
                # Settings
                "honorPublicationListOptOuts": item.get("HonorPublicationListOptOutsForTransactionalSends") == "true" if isinstance(item.get("HonorPublicationListOptOutsForTransactionalSends"), str) else item.get("HonorPublicationListOptOutsForTransactionalSends"),
                "sendPriority": item.get("SendPriority"),
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
        """Extract relationships from send classifications to profiles."""
        for item in items:
            sc_id = item.get("ObjectID")
            sc_name = item.get("Name")

            if not sc_id:
                continue

            # Sender profile relationship
            sender_profile = item.get("SenderProfile", {})
            if isinstance(sender_profile, dict):
                sp_key = sender_profile.get("CustomerKey")
                sp_name = sender_profile.get("Name")
                if sp_key:
                    result.add_relationship(
                        source_id=str(sc_id),
                        source_type="send_classification",
                        source_name=sc_name,
                        target_id=sp_key,
                        target_type="sender_profile",
                        target_name=sp_name,
                        relationship_type=RelationshipType.SEND_CLASSIFICATION_USES_SENDER_PROFILE,
                    )

            # Delivery profile relationship
            delivery_profile = item.get("DeliveryProfile", {})
            if isinstance(delivery_profile, dict):
                dp_key = delivery_profile.get("CustomerKey")
                dp_name = delivery_profile.get("Name")
                if dp_key:
                    result.add_relationship(
                        source_id=str(sc_id),
                        source_type="send_classification",
                        source_name=sc_name,
                        target_id=dp_key,
                        target_type="delivery_profile",
                        target_name=dp_name,
                        relationship_type=RelationshipType.SEND_CLASSIFICATION_USES_DELIVERY_PROFILE,
                    )
