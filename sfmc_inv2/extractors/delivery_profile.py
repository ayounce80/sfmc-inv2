"""Delivery Profile extractor for SFMC.

Extracts Delivery Profile definitions via SOAP API.
"""

import logging
from typing import Any

from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class DeliveryProfileExtractor(BaseExtractor):
    """Extractor for SFMC Delivery Profiles."""

    name = "delivery_profiles"
    description = "SFMC Delivery Profiles (SOAP)"
    object_type = "DeliveryProfile"

    SOAP_OBJECT_TYPE = "DeliveryProfile"

    SOAP_PROPERTIES = [
        "ObjectID",
        "CustomerKey",
        "Name",
        "Description",
        "SourceAddressType",
        "PrivateIP",
        "DomainType",
        "PrivateDomain",
        "HeaderSalutationSource",
        "HeaderContentArea.ID",
        "FooterSalutationSource",
        "FooterContentArea.ID",
        "SubscriberLevelPrivateDomain",
        "SMIMESignatureCertificateCustomerKey",
        "SMIMEEncryptionCertificateCustomerKey",
        "CreatedDate",
        "ModifiedDate",
    ]

    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch delivery profiles via SOAP API."""
        self._pages_fetched = 0

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=self.SOAP_PROPERTIES,
            max_pages=options.max_pages,
        )

        if not result.get("ok"):
            logger.error(f"Failed to fetch delivery profiles: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} delivery profiles")

        return objects

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform delivery profile data for output."""
        transformed = []

        for item in items:
            header_content = item.get("HeaderContentArea", {})
            footer_content = item.get("FooterContentArea", {})

            output = {
                "objectId": item.get("ObjectID"),
                "name": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                # IP/Domain settings
                "sourceAddressType": item.get("SourceAddressType"),
                "privateIP": item.get("PrivateIP"),
                "domainType": item.get("DomainType"),
                "privateDomain": item.get("PrivateDomain"),
                "subscriberLevelPrivateDomain": item.get("SubscriberLevelPrivateDomain") == "true" if isinstance(item.get("SubscriberLevelPrivateDomain"), str) else item.get("SubscriberLevelPrivateDomain"),
                # Header/Footer
                "headerSalutationSource": item.get("HeaderSalutationSource"),
                "headerContentAreaId": header_content.get("ID") if isinstance(header_content, dict) else None,
                "footerSalutationSource": item.get("FooterSalutationSource"),
                "footerContentAreaId": footer_content.get("ID") if isinstance(footer_content, dict) else None,
                # S/MIME
                "smimeSignatureCertificateKey": item.get("SMIMESignatureCertificateCustomerKey"),
                "smimeEncryptionCertificateKey": item.get("SMIMEEncryptionCertificateCustomerKey"),
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed
