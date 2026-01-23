"""Query Activity extractor for SFMC.

Extracts SQL Query Activities with their SQL text and targets.
Analyzes SQL to identify Data Extension dependencies.
"""

import asyncio
import logging
import re
from typing import Any, Optional

from ..cache.cache_manager import CacheType
from ..types.relationships import RelationshipType
from .base_extractor import BaseExtractor, ExtractorOptions, ExtractorResult

logger = logging.getLogger(__name__)

# Regex patterns for SQL parsing
# Comprehensive pattern matching FROM and all JOIN types with optional schema prefix
# Captures: group 1 = schema prefix (e.g., 'ENT'), group 2 = table name
# Handles: FROM, LEFT JOIN, RIGHT JOIN, INNER JOIN, OUTER JOIN, CROSS JOIN, FULL OUTER JOIN
DE_TABLE_PATTERN = re.compile(
    r'\b(?:FROM|(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL\s+OUTER)?\s*JOIN)\s+'
    r'\[?(?:(\w+)\.)?\[?([A-Za-z_][A-Za-z0-9_]*)\]?',
    re.IGNORECASE
)


class QueryExtractor(BaseExtractor):
    """Extractor for SFMC Query Activities."""

    name = "queries"
    description = "SFMC SQL Query Activities"
    object_type = "QueryDefinition"

    required_caches = [CacheType.QUERY_FOLDERS]

    async def fetch_data(self, options: ExtractorOptions) -> list[dict[str, Any]]:
        """Fetch queries via REST API with pagination."""
        queries = []
        page = 1
        self._pages_fetched = 0

        while page <= options.max_pages:
            result = self._rest.get(
                f"/automation/v1/queries?$page={page}&$pageSize={options.page_size}"
            )

            if not result.get("ok"):
                logger.error(f"Failed to fetch queries page {page}: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            queries.extend(items)
            self._pages_fetched = page

            self._report_progress(options, "Fetching", len(queries), 0)

            if len(items) < options.page_size:
                break
            page += 1

        return queries

    async def enrich_item(
        self,
        item: dict[str, Any],
        options: ExtractorOptions,
    ) -> dict[str, Any]:
        """Enrich query with breadcrumb path and SQL analysis."""
        # Add breadcrumb path
        category_id = item.get("categoryId")
        if category_id:
            item["folderPath"] = self.get_breadcrumb(
                str(category_id), CacheType.QUERY_FOLDERS
            )

        # Analyze SQL if present
        query_text = item.get("queryText", "")
        if query_text:
            item["referencedDataExtensions"] = self._extract_de_references(query_text)

        return item

    def _extract_de_references(self, sql: str) -> list[dict[str, Any]]:
        """Extract Data Extension names from SQL query.

        Parses FROM and JOIN clauses to find referenced DEs, including
        cross-BU references with ENT. or _ENT. schema prefixes.

        Args:
            sql: SQL query text.

        Returns:
            List of DE reference dicts with name and isShared flag.
        """
        # Track references by name to avoid duplicates while preserving metadata
        references: dict[str, dict[str, Any]] = {}

        # Find all FROM and JOIN clause references with optional schema prefix
        for match in DE_TABLE_PATTERN.finditer(sql):
            schema_prefix = match.group(1)  # e.g., 'ENT', '_ENT', or None
            de_name = match.group(2)

            if not de_name or self._is_system_table(de_name):
                continue

            de_name = de_name.strip()

            # Determine if this is a shared/enterprise DE (cross-BU reference)
            is_shared = False
            if schema_prefix:
                schema_upper = schema_prefix.upper()
                if schema_upper in ("ENT", "_ENT"):
                    is_shared = True

            # Store or update reference (prefer isShared=True if seen both ways)
            if de_name in references:
                if is_shared:
                    references[de_name]["isShared"] = True
            else:
                references[de_name] = {
                    "name": de_name,
                    "isShared": is_shared,
                }

        # Return sorted by name
        return sorted(references.values(), key=lambda x: x["name"])

    def _is_system_table(self, name: str) -> bool:
        """Check if a table name is a system table."""
        system_prefixes = (
            "_",
            "sys",
            "information_schema",
        )
        system_names = {
            "dual",
            "subscribers",
            "subscriberattributes",
        }

        name_lower = name.lower()
        return (
            name_lower.startswith(system_prefixes)
            or name_lower in system_names
        )

    def transform_data(
        self,
        items: list[dict[str, Any]],
        options: ExtractorOptions,
    ) -> list[dict[str, Any]]:
        """Transform query data for output."""
        transformed = []

        for item in items:
            output = {
                "id": item.get("queryDefinitionId"),
                "name": item.get("name"),
                "customerKey": item.get("key"),
                "description": item.get("description"),
                "categoryId": item.get("categoryId"),
                "folderPath": item.get("folderPath"),
                # Always include queryText for relationship analysis (SQL parsing)
                "queryText": item.get("queryText"),
                "targetName": item.get("targetName"),
                "targetKey": item.get("targetKey"),
                "targetId": item.get("targetId"),
                "targetDescription": item.get("targetDescription"),
                "targetUpdateTypeName": item.get("targetUpdateTypeName"),
                "status": item.get("status"),
                "createdDate": item.get("createdDate"),
                "modifiedDate": item.get("modifiedDate"),
                "createdBy": item.get("createdBy"),
                "modifiedBy": item.get("modifiedBy"),
                "referencedDataExtensions": item.get("referencedDataExtensions", []),
                # Convenience field: list of DE names for simpler queries
                "referencedDataExtensionNames": [
                    ref["name"] for ref in item.get("referencedDataExtensions", [])
                ],
            }
            transformed.append(output)

        return transformed

    async def extract_relationships(
        self,
        items: list[dict[str, Any]],
        result: ExtractorResult,
    ) -> None:
        """Extract relationships from queries to Data Extensions."""
        for item in items:
            query_id = item.get("queryDefinitionId")
            query_name = item.get("name")

            if not query_id:
                continue

            # Target DE relationship (writes)
            target_id = item.get("targetId")
            target_name = item.get("targetName")

            if target_id:
                result.add_relationship(
                    source_id=str(query_id),
                    source_type="query",
                    source_name=query_name,
                    target_id=str(target_id),
                    target_type="data_extension",
                    target_name=target_name,
                    relationship_type=RelationshipType.QUERY_WRITES_DE,
                )

            # Source DE relationships (reads)
            for de_ref in item.get("referencedDataExtensions", []):
                de_name = de_ref.get("name") if isinstance(de_ref, dict) else de_ref
                is_shared = de_ref.get("isShared", False) if isinstance(de_ref, dict) else False

                result.add_relationship(
                    source_id=str(query_id),
                    source_type="query",
                    source_name=query_name,
                    target_id=de_name,  # Use name as we don't have ID
                    target_type="data_extension",
                    target_name=de_name,
                    relationship_type=RelationshipType.QUERY_READS_DE,
                    metadata={
                        "resolved_by_name": True,
                        "isShared": is_shared,
                    },
                )
