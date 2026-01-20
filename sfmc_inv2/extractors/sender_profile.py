"""Sender Profile extractor for SFMC.

Extracts Sender Profile definitions via SOAP API.
"""

import logging
from typing import Any

from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)


class SenderProfileExtractor(BaseExtractor):
    """Extractor for SFMC Sender Profiles."""

    name = "sender_profiles"
    description = "SFMC Sender Profiles (SOAP)"
    object_type = "SenderProfile"

    SOAP_OBJECT_TYPE = "SenderProfile"

    SOAP_PROPERTIES = [
        "ObjectID",
        "CustomerKey",
        "Name",
        "Description",
        "FromName",
        "FromAddress",
        "UseDefaultRMMRules",
        "AutoForwardToEmailAddress",
        "AutoForwardToName",
        "DirectForward",
        # "AutoForwardTriggeredSend.CustomerKey",  # Fails at enterprise level
        "AutoReply",
        # "AutoReplyTriggeredSend.CustomerKey",  # Fails at enterprise level
        "SenderHeaderEmailAddress",
        "SenderHeaderName",
        "DataRetentionPeriodLength",
        "DataRetentionPeriodUnitOfMeasure",
        # "ReplyManagementRuleSet.CustomerKey",  # Fails at enterprise level
        "CreatedDate",
        "ModifiedDate",
    ]

    required_caches = []

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch sender profiles via SOAP API."""
        self._pages_fetched = 0

        result = self._soap.retrieve_all_pages(
            object_type=self.SOAP_OBJECT_TYPE,
            properties=self.SOAP_PROPERTIES,
            max_pages=options.max_pages,
        )

        if not result.get("ok"):
            logger.error(f"Failed to fetch sender profiles: {result.get('error')}")
            return []

        self._pages_fetched = result.get("pages_retrieved", 1)
        objects = result.get("objects", [])
        logger.info(f"Retrieved {len(objects)} sender profiles")

        return objects

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform sender profile data for output."""
        transformed = []

        for item in items:
            auto_forward_ts = item.get("AutoForwardTriggeredSend", {})
            auto_reply_ts = item.get("AutoReplyTriggeredSend", {})
            rmm_ruleset = item.get("ReplyManagementRuleSet", {})

            output = {
                "objectId": item.get("ObjectID"),
                "name": item.get("Name"),
                "customerKey": item.get("CustomerKey"),
                "description": item.get("Description"),
                # From settings
                "fromName": item.get("FromName"),
                "fromAddress": item.get("FromAddress"),
                # Reply management
                "useDefaultRMMRules": item.get("UseDefaultRMMRules") == "true" if isinstance(item.get("UseDefaultRMMRules"), str) else item.get("UseDefaultRMMRules"),
                "autoForwardToEmailAddress": item.get("AutoForwardToEmailAddress"),
                "autoForwardToName": item.get("AutoForwardToName"),
                "directForward": item.get("DirectForward") == "true" if isinstance(item.get("DirectForward"), str) else item.get("DirectForward"),
                "autoForwardTriggeredSendKey": auto_forward_ts.get("CustomerKey") if isinstance(auto_forward_ts, dict) else None,
                "autoReply": item.get("AutoReply") == "true" if isinstance(item.get("AutoReply"), str) else item.get("AutoReply"),
                "autoReplyTriggeredSendKey": auto_reply_ts.get("CustomerKey") if isinstance(auto_reply_ts, dict) else None,
                # Sender header
                "senderHeaderEmailAddress": item.get("SenderHeaderEmailAddress"),
                "senderHeaderName": item.get("SenderHeaderName"),
                # Data retention
                "dataRetentionPeriodLength": item.get("DataRetentionPeriodLength"),
                "dataRetentionPeriodUnitOfMeasure": item.get("DataRetentionPeriodUnitOfMeasure"),
                # RMM
                "replyManagementRuleSetKey": rmm_ruleset.get("CustomerKey") if isinstance(rmm_ruleset, dict) else None,
                # Audit
                "createdDate": item.get("CreatedDate"),
                "modifiedDate": item.get("ModifiedDate"),
            }
            transformed.append(output)

        return transformed
