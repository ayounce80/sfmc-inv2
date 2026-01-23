"""Account/Business Unit extractor for SFMC.

Extracts Business Unit information via SOAP API.
"""

import logging
from typing import Any

from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class AccountExtractor(BaseExtractor):
    """Extractor for SFMC Business Unit information.

    Uses the BusinessUnit SOAP object type with QueryAllAccounts option
    to retrieve all accessible Business Units in the enterprise.
    """

    name = "account"
    description = "SFMC Business Units (SOAP)"
    object_type = "BusinessUnit"

    SOAP_OBJECT_TYPE = "BusinessUnit"

    SOAP_PROPERTIES = [
        "ID",
        "Name",
        "ParentID",
        "ParentName",
        "IsActive",
        "CustomerKey",
        "Description",
        "Email",
        "FromName",
        "BusinessName",
        "Phone",
        "Address",
        "City",
        "State",
        "Zip",
        "Country",
        "Fax",
        "CreatedDate",
        "ModifiedDate",
        "EditionID",
        "IsTestAccount",
        "DBID",
        "SubscriberFilter",
        "Locale.LocaleCode",
        "TimeZone.ID",
        "TimeZone.Name",
    ]

    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch Business Unit information via SOAP API.

        Uses QueryAllAccounts=true to retrieve all accessible BUs.
        Requires credentials from a parent BU for full access.
        """
        self._pages_fetched = 0

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=self.SOAP_PROPERTIES,
            max_pages=options.max_pages,
            query_all_accounts=True,
        )

        if not result.get("ok"):
            error = result.get("overall_status", result.get("error", "Unknown error"))
            logger.error(f"Failed to fetch business units: {error}")
            # Check for common permission issue
            if "Permission" in str(error):
                logger.warning(
                    "BusinessUnit retrieval requires credentials created on the parent BU. "
                    "Child BU credentials may not have access to this data."
                )
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} business units")

        return objects

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform business unit data for output."""
        transformed = []

        for item in items:
            locale = item.get("Locale", {})
            timezone = item.get("TimeZone", {})

            # Determine if this is the parent BU (ParentID == 0 or missing)
            parent_id = item.get("ParentID")
            is_parent_bu = parent_id in (None, "0", 0, "")

            output = {
                "id": item.get("ID"),
                "name": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                "isParentBU": is_parent_bu,
                # Parent relationship
                "parentId": parent_id if not is_parent_bu else None,
                "parentName": item.get("ParentName") if not is_parent_bu else None,
                # Status
                "isActive": self._parse_bool(item.get("IsActive")),
                "isTestAccount": self._parse_bool(item.get("IsTestAccount")),
                # Contact info
                "email": item.get("Email"),
                "fromName": item.get("FromName"),
                "businessName": item.get("BusinessName"),
                "phone": item.get("Phone"),
                "fax": item.get("Fax"),
                # Address
                "address": item.get("Address"),
                "city": item.get("City"),
                "state": item.get("State"),
                "zip": item.get("Zip"),
                "country": item.get("Country"),
                # IDs
                "editionId": item.get("EditionID"),
                "dbid": item.get("DBID"),
                # Locale/Timezone
                "localeCode": locale.get("LocaleCode") if isinstance(locale, dict) else None,
                "timeZoneId": timezone.get("ID") if isinstance(timezone, dict) else None,
                "timeZoneName": timezone.get("Name") if isinstance(timezone, dict) else None,
                # Subscriber filter (if any)
                "subscriberFilter": item.get("SubscriberFilter"),
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed

    def _parse_bool(self, value: Any) -> bool:
        """Parse boolean from various formats."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value) if value is not None else False
