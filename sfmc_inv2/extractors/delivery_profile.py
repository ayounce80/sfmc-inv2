"""Delivery Profile extractor for SFMC.

Extracts Delivery Profile definitions via REST API.
Note: The API only exposes basic metadata (id, key, name, description, dates).
Detailed configuration (IP, domain, headers/footers) is not available via API.
"""

import logging
from typing import Any

from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class DeliveryProfileExtractor(BaseExtractor):
    """Extractor for SFMC Delivery Profiles.

    Uses the legacy REST endpoint as the SOAP DeliveryProfile object type
    is not directly retrievable in most SFMC instances.
    """

    name = "delivery_profiles"
    description = "SFMC Delivery Profiles (REST)"
    object_type = "DeliveryProfile"

    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch delivery profiles via REST API.

        Note: This endpoint only returns basic metadata. Detailed delivery
        profile configuration is not exposed via API.
        """
        self._pages_fetched = 0

        result = self._rest.get("/legacy/v1/beta/messaging/deliverypolicy/")

        if not result.get("ok"):
            logger.error(f"Failed to fetch delivery profiles: {result.get('error')}")
            return []

        data = result.get("data", {})

        # Response uses 'entry' as the items field
        items = data.get("entry", [])
        self._pages_fetched = 1

        logger.info(f"Retrieved {len(items)} delivery profiles")
        return items

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform delivery profile data for output.

        Note: The API only provides limited metadata. Detailed configuration
        (IP address, domain, headers, footers) is not available.
        """
        transformed = []

        for item in items:
            output = {
                "id": item.get("id"),
                "customerKey": item.get("key"),
                "name": item.get("name"),
                "description": item.get("description"),
                "createdDate": item.get("createdDate"),
                "lastUpdated": item.get("lastUpdated"),
                # Note: Detailed settings not available via API
                "_apiLimitation": "Detailed configuration (IP, domain, headers/footers) not exposed via API",
            }
            transformed.append(output)

        return transformed
