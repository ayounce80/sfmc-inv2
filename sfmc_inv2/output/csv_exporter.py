"""CSV exporter for inventory data.

Exports inventory data to CSV format with configurable columns.
Supports flattening nested structures for tabular output.
"""

import csv
import logging
from io import StringIO
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Column configurations for each object type
COLUMN_CONFIGS = {
    "automations": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("status", "Status"),
        ("statusName", "Status Name"),
        ("isActive", "Is Active"),
        ("type", "Type"),
        ("stepCount", "Step Count"),
        ("activityCount", "Activity Count"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
        ("lastRunTime", "Last Run Time"),
        ("lastRunStatus", "Last Run Status"),
    ],
    "data_extensions": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("isSendable", "Is Sendable"),
        ("rowCount", "Row Count"),
        ("fieldCount", "Field Count"),
        ("primaryKeyFields", "Primary Key Fields"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
        ("retentionPeriodLength", "Retention Period"),
        ("retentionPeriodUnitOfMeasure", "Retention Unit"),
    ],
    "queries": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("targetName", "Target DE Name"),
        ("targetKey", "Target DE Key"),
        ("targetUpdateTypeName", "Update Type"),
        ("status", "Status"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
        ("createdBy", "Created By"),
        ("modifiedBy", "Modified By"),
    ],
    "journeys": [
        ("id", "ID"),
        ("name", "Name"),
        ("key", "Key"),
        ("version", "Version"),
        ("status", "Status"),
        ("statusName", "Status Name"),
        ("entryMode", "Entry Mode"),
        ("channel", "Channel"),
        ("triggerCount", "Trigger Count"),
        ("activityCount", "Activity Count"),
        ("goalCount", "Goal Count"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
        ("lastPublishedDate", "Last Published"),
    ],
    # Phase 1 - Automation Activities (REST)
    "scripts": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("description", "Description"),
        ("status", "Status"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "imports": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("description", "Description"),
        ("destinationName", "Destination DE"),
        ("fileTransferLocationName", "File Location"),
        ("fileNamingPattern", "File Pattern"),
        ("updateTypeName", "Update Type"),
        ("status", "Status"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "data_extracts": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("description", "Description"),
        ("dataExtractTypeName", "Extract Type"),
        ("fileNamingPattern", "File Pattern"),
        ("status", "Status"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "filters": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("description", "Description"),
        ("sourceDataExtensionName", "Source DE"),
        ("destinationDataExtensionName", "Destination DE"),
        ("status", "Status"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "file_transfers": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("description", "Description"),
        ("fileTransferLocationName", "File Location"),
        ("fileNamingPattern", "File Pattern"),
        ("fileAction", "Action"),
        ("status", "Status"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    # Phase 2 - Content & Structure (REST)
    "assets": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("assetTypeName", "Asset Type"),
        ("status", "Status"),
        ("version", "Version"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
        ("createdBy", "Created By"),
        ("modifiedBy", "Modified By"),
    ],
    "folders": [
        ("id", "ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("parentId", "Parent ID"),
        ("parentName", "Parent Name"),
        ("contentType", "Content Type"),
        ("description", "Description"),
        ("isActive", "Is Active"),
        ("isEditable", "Is Editable"),
        ("allowChildren", "Allow Children"),
    ],
    "event_definitions": [
        ("id", "ID"),
        ("name", "Name"),
        ("eventDefinitionKey", "Event Key"),
        ("description", "Description"),
        ("dataExtensionName", "Data Extension"),
        ("mode", "Mode"),
        ("status", "Status"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    # Phase 3 - Messaging Objects (SOAP)
    "classic_emails": [
        ("id", "ID"),
        ("objectId", "Object ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("subject", "Subject"),
        ("status", "Status"),
        ("isHTMLPaste", "Is HTML Paste"),
        ("hasPreheader", "Has Preheader"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "triggered_sends": [
        ("id", "ID"),
        ("objectId", "Object ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("description", "Description"),
        ("status", "Status"),
        ("emailName", "Email Name"),
        ("listName", "List Name"),
        ("senderProfileName", "Sender Profile"),
        ("deliveryProfileName", "Delivery Profile"),
        ("sendClassificationName", "Send Classification"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "lists": [
        ("id", "ID"),
        ("objectId", "Object ID"),
        ("listName", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("description", "Description"),
        ("type", "Type"),
        ("listClassification", "Classification"),
        ("subscriberCount", "Subscriber Count"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "sender_profiles": [
        ("objectId", "Object ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("description", "Description"),
        ("fromName", "From Name"),
        ("fromAddress", "From Address"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "delivery_profiles": [
        ("objectId", "Object ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("description", "Description"),
        ("domainType", "Domain Type"),
        ("privateDomain", "Private Domain"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "send_classifications": [
        ("objectId", "Object ID"),
        ("name", "Name"),
        ("customerKey", "Customer Key"),
        ("description", "Description"),
        ("senderProfileKey", "Sender Profile Key"),
        ("deliveryProfileKey", "Delivery Profile Key"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "templates": [
        ("id", "ID"),
        ("objectId", "Object ID"),
        ("templateName", "Name"),
        ("customerKey", "Customer Key"),
        ("folderPath", "Folder Path"),
        ("templateSubject", "Subject"),
        ("isActive", "Is Active"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
    "account": [
        ("id", "ID"),
        ("accountName", "Account Name"),
        ("customerKey", "Customer Key"),
        ("accountType", "Account Type"),
        ("businessName", "Business Name"),
        ("isActive", "Is Active"),
        ("createdDate", "Created Date"),
        ("modifiedDate", "Modified Date"),
    ],
}


class CSVExporter:
    """Exports inventory data to CSV format."""

    def __init__(
        self,
        output_dir: Optional[Path] = None,
        include_all_fields: bool = False,
    ):
        """Initialize the CSV exporter.

        Args:
            output_dir: Directory for output files.
            include_all_fields: If True, include all fields not just configured ones.
        """
        self._output_dir = output_dir
        self._include_all_fields = include_all_fields

    def export(
        self,
        items: list[dict[str, Any]],
        object_type: str,
        filename: Optional[str] = None,
    ) -> str:
        """Export items to CSV.

        Args:
            items: List of items to export.
            object_type: Type of objects for column configuration.
            filename: Output filename. If None, returns CSV string.

        Returns:
            CSV content as string, or path if filename provided.
        """
        if not items:
            return ""

        # Get columns
        columns = self._get_columns(object_type, items[0])

        # Build CSV
        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow([col[1] for col in columns])

        # Write rows
        for item in items:
            row = [self._get_value(item, col[0]) for col in columns]
            writer.writerow(row)

        csv_content = output.getvalue()

        # Write to file if filename provided
        if filename:
            if self._output_dir:
                filepath = self._output_dir / filename
            else:
                filepath = Path(filename)

            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                f.write(csv_content)

            return str(filepath)

        return csv_content

    def _get_columns(
        self, object_type: str, sample_item: dict[str, Any]
    ) -> list[tuple[str, str]]:
        """Get column configuration for object type.

        Args:
            object_type: Type of objects.
            sample_item: Sample item for discovering fields.

        Returns:
            List of (field_name, header_name) tuples.
        """
        columns = COLUMN_CONFIGS.get(object_type, [])

        if self._include_all_fields:
            # Add any fields not in configuration
            existing_fields = {col[0] for col in columns}
            for field in sample_item.keys():
                if field not in existing_fields and not self._should_skip_field(field):
                    columns.append((field, self._field_to_header(field)))

        return columns

    def _should_skip_field(self, field: str) -> bool:
        """Check if a field should be skipped in CSV export."""
        skip_fields = {
            "steps",
            "activities",
            "fields",
            "schedule",
            "notifications",
            "triggers",
            "goals",
            "queryText",
            "stats",
        }
        return field in skip_fields

    def _field_to_header(self, field: str) -> str:
        """Convert a field name to a header name."""
        # Convert camelCase to Title Case
        import re

        words = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\W|$)|\d+", field)
        return " ".join(word.capitalize() for word in words)

    def _get_value(self, item: dict[str, Any], field: str) -> str:
        """Get a field value formatted for CSV.

        Args:
            item: Item dictionary.
            field: Field name.

        Returns:
            String value for CSV cell.
        """
        value = item.get(field)

        if value is None:
            return ""
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if isinstance(value, list):
            return ", ".join(str(v) for v in value)
        if isinstance(value, dict):
            return str(value)
        return str(value)

    def export_all(
        self,
        results: dict[str, list[dict[str, Any]]],
        prefix: str = "",
    ) -> dict[str, str]:
        """Export all object types to separate CSV files.

        Args:
            results: Dictionary of object_type -> list of items.
            prefix: Prefix for filenames.

        Returns:
            Dictionary of object_type -> file path.
        """
        exported = {}

        for object_type, items in results.items():
            if items:
                filename = f"{prefix}{object_type}.csv" if prefix else f"{object_type}.csv"
                path = self.export(items, object_type, filename)
                exported[object_type] = path

        return exported


def export_to_csv(
    items: list[dict[str, Any]],
    object_type: str,
    output_path: Optional[Path] = None,
) -> str:
    """Convenience function to export items to CSV.

    Args:
        items: List of items to export.
        object_type: Type of objects.
        output_path: Output file path.

    Returns:
        CSV content or file path.
    """
    exporter = CSVExporter(output_path.parent if output_path else None)
    return exporter.export(
        items,
        object_type,
        output_path.name if output_path else None,
    )
