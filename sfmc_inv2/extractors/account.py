"""Account extractor for SFMC.

Extracts Account (Business Unit) information via SOAP API.
"""

import logging
from typing import Any

from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class AccountExtractor(BaseExtractor):
    """Extractor for SFMC Account (Business Unit) information."""

    name = "account"
    description = "SFMC Account/Business Unit Information (SOAP)"
    object_type = "Account"

    SOAP_OBJECT_TYPE = "Account"

    SOAP_PROPERTIES = [
        "ID",
        "AccountType",
        "Name",
        "Email",
        "FromName",
        "BusinessName",
        "Phone",
        "Address",
        "Fax",
        "City",
        "State",
        "Zip",
        "Country",
        "IsActive",
        "EditionID",
        "IsTestAccount",
        "DBID",
        "CustomerID",
        "DeletedDate",
        "ParentID",
        "ParentName",
        "CustomerKey",
        "Description",
        "DefaultSendClassification.CustomerKey",
        "DefaultHomePage.ID",
        "CreatedDate",
        "ModifiedDate",
        "InheritAddress",
        "Locale.LocaleCode",
        "TimeZone.ID",
        "TimeZone.Name",
    ]

    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch account information via SOAP API.

        Note: Usually returns only the current BU and accessible child BUs.
        """
        self._pages_fetched = 0

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=self.SOAP_PROPERTIES,
            max_pages=options.max_pages,
        )

        if not result.get("ok"):
            logger.error(f"Failed to fetch accounts: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} accounts")

        return objects

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform account data for output."""
        transformed = []

        for item in items:
            send_class = item.get("DefaultSendClassification", {})
            home_page = item.get("DefaultHomePage", {})
            locale = item.get("Locale", {})
            timezone = item.get("TimeZone", {})

            output = {
                "id": item.get("ID"),
                "accountName": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                "accountType": item.get("AccountType"),
                "businessName": item.get("BusinessName"),
                # Contact info
                "email": item.get("Email"),
                "fromName": item.get("FromName"),
                "phone": item.get("Phone"),
                "fax": item.get("Fax"),
                # Address
                "address": item.get("Address"),
                "city": item.get("City"),
                "state": item.get("State"),
                "zip": item.get("Zip"),
                "country": item.get("Country"),
                "inheritAddress": item.get("InheritAddress") == "true" if isinstance(item.get("InheritAddress"), str) else item.get("InheritAddress"),
                # Status
                "isActive": item.get("IsActive") == "true" if isinstance(item.get("IsActive"), str) else item.get("IsActive"),
                "isTestAccount": item.get("IsTestAccount") == "true" if isinstance(item.get("IsTestAccount"), str) else item.get("IsTestAccount"),
                "deletedDate": item.get("DeletedDate"),
                # IDs
                "editionId": item.get("EditionID"),
                "dbid": item.get("DBID"),
                "customerId": item.get("CustomerID"),
                # Parent account
                "parentId": item.get("ParentID"),
                "parentName": item.get("ParentName"),
                # Defaults
                "defaultSendClassificationKey": send_class.get("CustomerKey") if isinstance(send_class, dict) else None,
                "defaultHomePageId": home_page.get("ID") if isinstance(home_page, dict) else None,
                # Locale/Timezone
                "localeCode": locale.get("LocaleCode") if isinstance(locale, dict) else None,
                "timeZoneId": timezone.get("ID") if isinstance(timezone, dict) else None,
                "timeZoneName": timezone.get("Name") if isinstance(timezone, dict) else None,
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed
