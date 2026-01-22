#!/usr/bin/env python3
"""Generate comprehensive dependency reports from SFMC inventory data.

This script reads inventory NDJSON files and the relationship graph
to generate CSV reports for cleanup and analysis purposes.
"""

import argparse
import csv
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class InventoryLoader:
    """Loads inventory data from NDJSON files."""

    def __init__(self, inventory_path: Path):
        """Initialize the inventory loader.

        Args:
            inventory_path: Path to the inventory directory.
        """
        self.inventory_path = inventory_path
        self.objects_path = inventory_path / "objects"
        self.relationships_path = inventory_path / "relationships"
        self._cache: dict[str, list[dict[str, Any]]] = {}

    def load_objects(self, object_type: str) -> list[dict[str, Any]]:
        """Load objects of a given type from NDJSON file.

        Args:
            object_type: Type of objects to load (e.g., "queries", "automations").

        Returns:
            List of objects.
        """
        if object_type in self._cache:
            return self._cache[object_type]

        filepath = self.objects_path / f"{object_type}.ndjson"
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return []

        objects = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        objects.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse line in {filepath}: {e}")

        self._cache[object_type] = objects
        logger.info(f"Loaded {len(objects)} {object_type}")
        return objects

    def load_graph(self) -> dict[str, Any]:
        """Load the relationship graph.

        Returns:
            Graph dictionary with edges and orphans.
        """
        filepath = self.relationships_path / "graph.json"
        if not filepath.exists():
            logger.warning(f"Graph file not found: {filepath}")
            return {"edges": [], "orphans": [], "stats": {}}

        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_orphans(self) -> list[dict[str, Any]]:
        """Load orphaned objects.

        Returns:
            List of orphaned objects.
        """
        filepath = self.relationships_path / "orphans.json"
        if not filepath.exists():
            logger.warning(f"Orphans file not found: {filepath}")
            return []

        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)


class RelationshipAnalyzer:
    """Analyzes relationships between objects."""

    def __init__(self, graph: dict[str, Any]):
        """Initialize the relationship analyzer.

        Args:
            graph: Relationship graph dictionary.
        """
        self.edges = graph.get("edges", [])
        self.orphans = graph.get("orphans", [])

        # Build lookup indices
        self._by_target: dict[str, dict[str, list[dict]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._by_source: dict[str, dict[str, list[dict]]] = defaultdict(
            lambda: defaultdict(list)
        )

        for edge in self.edges:
            target_key = f"{edge['target_type']}:{edge['target_id']}"
            source_key = f"{edge['source_type']}:{edge['source_id']}"
            self._by_target[edge["target_type"]][edge["target_id"]].append(edge)
            self._by_source[edge["source_type"]][edge["source_id"]].append(edge)

    def get_automation_usage(self, object_id: str, object_type: str) -> list[dict]:
        """Get automations that use an object.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            List of automation edges.
        """
        edges = self._by_target[object_type].get(object_id, [])
        return [e for e in edges if e["source_type"] == "automation"]

    def get_journey_usage(self, object_id: str, object_type: str) -> list[dict]:
        """Get journeys that use an object.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            List of journey edges.
        """
        edges = self._by_target[object_type].get(object_id, [])
        return [e for e in edges if e["source_type"] == "journey"]

    def get_all_usage(self, object_id: str, object_type: str) -> list[dict]:
        """Get all objects that reference this object.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            List of edges where this object is the target.
        """
        return self._by_target[object_type].get(object_id, [])

    def get_dependencies(self, object_id: str, object_type: str) -> list[dict]:
        """Get all objects this object depends on.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            List of edges where this object is the source.
        """
        return self._by_source[object_type].get(object_id, [])

    def is_orphan(self, object_id: str, object_type: str) -> tuple[bool, str]:
        """Check if an object is orphaned.

        Args:
            object_id: ID of the object.
            object_type: Type of the object.

        Returns:
            Tuple of (is_orphan, reason).
        """
        for orphan in self.orphans:
            if orphan["id"] == object_id and orphan["object_type"] == object_type:
                return True, orphan.get("reason", "Not referenced")
        return False, ""


class ReportGenerator:
    """Generates CSV dependency reports."""

    def __init__(
        self,
        loader: InventoryLoader,
        analyzer: RelationshipAnalyzer,
        output_dir: Path,
    ):
        """Initialize the report generator.

        Args:
            loader: Inventory loader instance.
            analyzer: Relationship analyzer instance.
            output_dir: Directory for output CSV files.
        """
        self.loader = loader
        self.analyzer = analyzer
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _write_csv(
        self,
        filename: str,
        rows: list[dict[str, Any]],
        columns: list[tuple[str, str]],
    ) -> Path:
        """Write rows to a CSV file.

        Args:
            filename: Output filename.
            rows: List of row dictionaries.
            columns: List of (field_name, header_name) tuples.

        Returns:
            Path to the created file.
        """
        filepath = self.output_dir / filename
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow([col[1] for col in columns])
            # Write data rows
            for row in rows:
                writer.writerow([self._format_value(row.get(col[0])) for col in columns])

        logger.info(f"Wrote {len(rows)} rows to {filepath}")
        return filepath

    def _format_value(self, value: Any) -> str:
        """Format a value for CSV output."""
        if value is None:
            return ""
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if isinstance(value, list):
            return "; ".join(str(v) for v in value)
        if isinstance(value, dict):
            return json.dumps(value)
        return str(value)

    def _format_automation_list(self, edges: list[dict]) -> str:
        """Format automation names from edges."""
        names = []
        for edge in edges:
            name = edge.get("source_name") or edge.get("source_id")
            if name and name not in names:
                names.append(name)
        return "; ".join(names)

    # =========================================================================
    # Phase 1: Automation Activity Reports
    # =========================================================================

    def generate_query_report(self) -> Path:
        """Generate queries_dependency_report.csv."""
        queries = self.loader.load_objects("queries")
        rows = []

        for query in queries:
            query_id = query.get("id", "")
            auto_edges = self.analyzer.get_automation_usage(query_id, "query")
            is_orphan, reason = self.analyzer.is_orphan(query_id, "query")

            rows.append({
                "id": query_id,
                "name": query.get("name"),
                "customerKey": query.get("customerKey"),
                "folderPath": query.get("folderPath"),
                "targetDEId": query.get("targetId"),
                "targetDEName": query.get("targetName"),
                "targetDEKey": query.get("targetKey"),
                "updateType": query.get("targetUpdateTypeName"),
                "automationCount": len(auto_edges),
                "automations": self._format_automation_list(auto_edges),
                "status": query.get("status"),
                "createdDate": query.get("createdDate"),
                "modifiedDate": query.get("modifiedDate"),
                "isOrphan": is_orphan,
                "orphanReason": reason if is_orphan else "",
            })

        columns = [
            ("id", "ID"),
            ("name", "Name"),
            ("customerKey", "Customer Key"),
            ("folderPath", "Folder Path"),
            ("targetDEId", "Target DE ID"),
            ("targetDEName", "Target DE Name"),
            ("targetDEKey", "Target DE Key"),
            ("updateType", "Update Type"),
            ("automationCount", "Automation Count"),
            ("automations", "Automations"),
            ("status", "Status"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
            ("isOrphan", "Is Orphan"),
            ("orphanReason", "Orphan Reason"),
        ]

        return self._write_csv("queries_dependency_report.csv", rows, columns)

    def generate_script_report(self) -> Path:
        """Generate scripts_dependency_report.csv."""
        scripts = self.loader.load_objects("scripts")
        rows = []

        for script in scripts:
            script_id = script.get("id", "")
            auto_edges = self.analyzer.get_automation_usage(script_id, "script")
            is_orphan, reason = self.analyzer.is_orphan(script_id, "script")

            rows.append({
                "id": script_id,
                "name": script.get("name"),
                "customerKey": script.get("customerKey"),
                "folderPath": script.get("folderPath"),
                "description": script.get("description"),
                "automationCount": len(auto_edges),
                "automations": self._format_automation_list(auto_edges),
                "status": script.get("status"),
                "createdDate": script.get("createdDate"),
                "modifiedDate": script.get("modifiedDate"),
                "isOrphan": is_orphan,
                "orphanReason": reason if is_orphan else "",
            })

        columns = [
            ("id", "ID"),
            ("name", "Name"),
            ("customerKey", "Customer Key"),
            ("folderPath", "Folder Path"),
            ("description", "Description"),
            ("automationCount", "Automation Count"),
            ("automations", "Automations"),
            ("status", "Status"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
            ("isOrphan", "Is Orphan"),
            ("orphanReason", "Orphan Reason"),
        ]

        return self._write_csv("scripts_dependency_report.csv", rows, columns)

    def generate_import_report(self) -> Path:
        """Generate imports_dependency_report.csv."""
        imports = self.loader.load_objects("imports")
        rows = []

        for imp in imports:
            imp_id = imp.get("id", "")
            auto_edges = self.analyzer.get_automation_usage(imp_id, "import")
            is_orphan, reason = self.analyzer.is_orphan(imp_id, "import")

            rows.append({
                "id": imp_id,
                "name": imp.get("name"),
                "customerKey": imp.get("customerKey"),
                "folderPath": imp.get("folderPath"),
                "description": imp.get("description"),
                "destinationDEId": imp.get("destinationId"),
                "destinationDEName": imp.get("destinationName"),
                "destinationDEKey": imp.get("destinationKey"),
                "fileLocation": imp.get("fileTransferLocationName"),
                "filePattern": imp.get("fileNamingPattern"),
                "updateType": imp.get("updateTypeName"),
                "automationCount": len(auto_edges),
                "automations": self._format_automation_list(auto_edges),
                "status": imp.get("status"),
                "createdDate": imp.get("createdDate"),
                "modifiedDate": imp.get("modifiedDate"),
                "isOrphan": is_orphan,
                "orphanReason": reason if is_orphan else "",
            })

        columns = [
            ("id", "ID"),
            ("name", "Name"),
            ("customerKey", "Customer Key"),
            ("folderPath", "Folder Path"),
            ("description", "Description"),
            ("destinationDEId", "Destination DE ID"),
            ("destinationDEName", "Destination DE Name"),
            ("destinationDEKey", "Destination DE Key"),
            ("fileLocation", "File Location"),
            ("filePattern", "File Pattern"),
            ("updateType", "Update Type"),
            ("automationCount", "Automation Count"),
            ("automations", "Automations"),
            ("status", "Status"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
            ("isOrphan", "Is Orphan"),
            ("orphanReason", "Orphan Reason"),
        ]

        return self._write_csv("imports_dependency_report.csv", rows, columns)

    def generate_data_extract_report(self) -> Path:
        """Generate data_extracts_dependency_report.csv."""
        extracts = self.loader.load_objects("data_extracts")
        rows = []

        for extract in extracts:
            extract_id = extract.get("id", "")
            auto_edges = self.analyzer.get_automation_usage(extract_id, "data_extract")
            is_orphan, reason = self.analyzer.is_orphan(extract_id, "data_extract")

            rows.append({
                "id": extract_id,
                "name": extract.get("name"),
                "customerKey": extract.get("customerKey"),
                "folderPath": extract.get("folderPath"),
                "description": extract.get("description"),
                "extractType": extract.get("dataExtractTypeName"),
                "fileSpec": extract.get("fileSpec"),
                "filePattern": extract.get("fileNamingPattern"),
                "automationCount": len(auto_edges),
                "automations": self._format_automation_list(auto_edges),
                "status": extract.get("status"),
                "createdDate": extract.get("createdDate"),
                "modifiedDate": extract.get("modifiedDate"),
                "isOrphan": is_orphan,
                "orphanReason": reason if is_orphan else "",
            })

        columns = [
            ("id", "ID"),
            ("name", "Name"),
            ("customerKey", "Customer Key"),
            ("folderPath", "Folder Path"),
            ("description", "Description"),
            ("extractType", "Extract Type"),
            ("fileSpec", "File Spec"),
            ("filePattern", "File Pattern"),
            ("automationCount", "Automation Count"),
            ("automations", "Automations"),
            ("status", "Status"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
            ("isOrphan", "Is Orphan"),
            ("orphanReason", "Orphan Reason"),
        ]

        return self._write_csv("data_extracts_dependency_report.csv", rows, columns)

    def generate_file_transfer_report(self) -> Path:
        """Generate file_transfers_dependency_report.csv."""
        transfers = self.loader.load_objects("file_transfers")
        rows = []

        for transfer in transfers:
            transfer_id = transfer.get("id", "")
            # Note: file_transfer might use 'name' as ID if 'id' is null
            if not transfer_id:
                transfer_id = transfer.get("name", "")
            auto_edges = self.analyzer.get_automation_usage(transfer_id, "file_transfer")
            is_orphan, reason = self.analyzer.is_orphan(transfer_id, "file_transfer")

            rows.append({
                "id": transfer_id,
                "name": transfer.get("name"),
                "customerKey": transfer.get("customerKey"),
                "folderPath": transfer.get("folderPath"),
                "description": transfer.get("description"),
                "fileLocation": transfer.get("fileTransferLocationName"),
                "filePattern": transfer.get("fileNamingPattern"),
                "fileAction": transfer.get("fileAction"),
                "isCompressed": transfer.get("isCompressed"),
                "isEncrypted": transfer.get("isEncrypted"),
                "automationCount": len(auto_edges),
                "automations": self._format_automation_list(auto_edges),
                "status": transfer.get("status"),
                "createdDate": transfer.get("createdDate"),
                "modifiedDate": transfer.get("modifiedDate"),
                "isOrphan": is_orphan,
                "orphanReason": reason if is_orphan else "",
            })

        columns = [
            ("id", "ID"),
            ("name", "Name"),
            ("customerKey", "Customer Key"),
            ("folderPath", "Folder Path"),
            ("description", "Description"),
            ("fileLocation", "File Location"),
            ("filePattern", "File Pattern"),
            ("fileAction", "File Action"),
            ("isCompressed", "Is Compressed"),
            ("isEncrypted", "Is Encrypted"),
            ("automationCount", "Automation Count"),
            ("automations", "Automations"),
            ("status", "Status"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
            ("isOrphan", "Is Orphan"),
            ("orphanReason", "Orphan Reason"),
        ]

        return self._write_csv("file_transfers_dependency_report.csv", rows, columns)

    def generate_send_email_activities_report(self) -> Path:
        """Generate send_email_activities_report.csv from automation activities."""
        automations = self.loader.load_objects("automations")
        rows = []

        for automation in automations:
            auto_id = automation.get("id", "")
            auto_name = automation.get("name", "")
            auto_status = automation.get("status", "")

            # Look through steps for send email activities (type 42 is Refresh Group,
            # type 733 is Send Email, type 73 is User-Initiated Send)
            for step in automation.get("steps", []):
                for activity in step.get("activities", []):
                    obj_type_id = activity.get("objectTypeId")
                    type_name = activity.get("activityTypeName", "")

                    # Check for email-related activities
                    if obj_type_id in [733, 73] or "email" in type_name.lower():
                        rows.append({
                            "automationId": auto_id,
                            "automationName": auto_name,
                            "automationStatus": auto_status,
                            "stepNumber": step.get("step"),
                            "activityId": activity.get("id"),
                            "activityObjectId": activity.get("activityObjectId"),
                            "activityName": activity.get("name"),
                            "activityType": type_name,
                            "objectTypeId": obj_type_id,
                            "displayOrder": activity.get("displayOrder"),
                        })

        columns = [
            ("automationId", "Automation ID"),
            ("automationName", "Automation Name"),
            ("automationStatus", "Automation Status"),
            ("stepNumber", "Step Number"),
            ("activityId", "Activity ID"),
            ("activityObjectId", "Activity Object ID"),
            ("activityName", "Activity Name"),
            ("activityType", "Activity Type"),
            ("objectTypeId", "Object Type ID"),
            ("displayOrder", "Display Order"),
        ]

        return self._write_csv("send_email_activities_report.csv", rows, columns)

    # =========================================================================
    # Phase 2: Filter Reports
    # =========================================================================

    def generate_filter_report(self) -> Path:
        """Generate filter_activities_report.csv."""
        filters = self.loader.load_objects("filters")
        rows = []

        for flt in filters:
            flt_id = flt.get("id", "")
            auto_edges = self.analyzer.get_automation_usage(flt_id, "filter")
            is_orphan, reason = self.analyzer.is_orphan(flt_id, "filter")

            rows.append({
                "id": flt_id,
                "name": flt.get("name"),
                "customerKey": flt.get("customerKey"),
                "folderPath": flt.get("folderPath"),
                "description": flt.get("description"),
                "sourceDEId": flt.get("sourceDataExtensionId"),
                "sourceDEName": flt.get("sourceDataExtensionName"),
                "sourceDEKey": flt.get("sourceDataExtensionKey"),
                "destinationDEId": flt.get("destinationDataExtensionId"),
                "destinationDEName": flt.get("destinationDataExtensionName"),
                "destinationDEKey": flt.get("destinationDataExtensionKey"),
                "filterDefinitionId": flt.get("filterDefinitionId"),
                "automationCount": len(auto_edges),
                "automations": self._format_automation_list(auto_edges),
                "status": flt.get("status"),
                "createdDate": flt.get("createdDate"),
                "modifiedDate": flt.get("modifiedDate"),
                "isOrphan": is_orphan,
                "orphanReason": reason if is_orphan else "",
            })

        columns = [
            ("id", "ID"),
            ("name", "Name"),
            ("customerKey", "Customer Key"),
            ("folderPath", "Folder Path"),
            ("description", "Description"),
            ("sourceDEId", "Source DE ID"),
            ("sourceDEName", "Source DE Name"),
            ("sourceDEKey", "Source DE Key"),
            ("destinationDEId", "Destination DE ID"),
            ("destinationDEName", "Destination DE Name"),
            ("destinationDEKey", "Destination DE Key"),
            ("filterDefinitionId", "Filter Definition ID"),
            ("automationCount", "Automation Count"),
            ("automations", "Automations"),
            ("status", "Status"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
            ("isOrphan", "Is Orphan"),
            ("orphanReason", "Orphan Reason"),
        ]

        return self._write_csv("filter_activities_report.csv", rows, columns)

    # =========================================================================
    # Phase 3: Triggered Send Reports
    # =========================================================================

    def generate_triggered_send_reports(self) -> list[Path]:
        """Generate all triggered send reports.

        Returns:
            List of created file paths.
        """
        triggered_sends = self.loader.load_objects("triggered_sends")
        files = []

        # Report 1: All triggered sends by status
        rows_by_status = []
        for ts in triggered_sends:
            ts_id = ts.get("id", ts.get("objectId", ""))
            folder_path = ts.get("folderPath", "") or ""
            is_jb = "journeybuilder" in folder_path.lower()

            rows_by_status.append({
                "id": ts_id,
                "objectId": ts.get("objectId"),
                "name": ts.get("name"),
                "customerKey": ts.get("customerKey"),
                "status": ts.get("status"),
                "folderPath": folder_path,
                "emailId": ts.get("emailId"),
                "emailName": ts.get("emailName"),
                "listId": ts.get("listId"),
                "listName": ts.get("listName"),
                "sendClassificationKey": ts.get("sendClassificationKey"),
                "senderProfileKey": ts.get("senderProfileKey"),
                "deliveryProfileKey": ts.get("deliveryProfileKey"),
                "fromName": ts.get("fromName"),
                "fromAddress": ts.get("fromAddress"),
                "emailSubject": ts.get("emailSubject"),
                "isJourneyBuilder": is_jb,
                "createdDate": ts.get("createdDate"),
                "modifiedDate": ts.get("modifiedDate"),
            })

        columns_by_status = [
            ("id", "ID"),
            ("objectId", "Object ID"),
            ("name", "Name"),
            ("customerKey", "Customer Key"),
            ("status", "Status"),
            ("folderPath", "Folder Path"),
            ("emailId", "Email ID"),
            ("emailName", "Email Name"),
            ("listId", "List ID"),
            ("listName", "List Name"),
            ("sendClassificationKey", "Send Classification Key"),
            ("senderProfileKey", "Sender Profile Key"),
            ("deliveryProfileKey", "Delivery Profile Key"),
            ("fromName", "From Name"),
            ("fromAddress", "From Address"),
            ("emailSubject", "Email Subject"),
            ("isJourneyBuilder", "Is Journey Builder"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
        ]

        files.append(
            self._write_csv(
                "triggered_sends_by_status.csv", rows_by_status, columns_by_status
            )
        )

        # Report 2: Disabled triggered sends (Inactive, Deleted, Canceled)
        disabled_statuses = {"Inactive", "Deleted", "Canceled", "New"}
        rows_disabled = [r for r in rows_by_status if r["status"] in disabled_statuses]

        # Add reason column
        for row in rows_disabled:
            status = row.get("status", "")
            if status == "Deleted":
                row["reason"] = "Deleted - likely orphaned from deleted journey"
            elif status == "Inactive":
                row["reason"] = "Inactive - cleanup candidate"
            elif status == "Canceled":
                row["reason"] = "Canceled - review for deletion"
            elif status == "New":
                row["reason"] = "New - never activated, review"
            else:
                row["reason"] = "Unknown status"

        columns_disabled = columns_by_status + [("reason", "Reason")]
        files.append(
            self._write_csv(
                "triggered_sends_disabled.csv", rows_disabled, columns_disabled
            )
        )

        # Report 3: JB Orphans - JB-created sends that may be orphaned
        # These are triggered sends in JB folders OR with Deleted status
        rows_jb_orphans = []
        for r in rows_by_status:
            is_jb = r.get("isJourneyBuilder", False)
            status = r.get("status", "")

            # JB orphan criteria:
            # 1. In JB folder with Deleted/Inactive status
            # 2. Has "Deleted" status (likely from deleted journey)
            if is_jb and status in {"Deleted", "Inactive"}:
                r["orphanReason"] = f"JB send with {status} status"
                rows_jb_orphans.append(r)
            elif status == "Deleted":
                r["orphanReason"] = "Deleted status - likely JB orphan"
                rows_jb_orphans.append(r)

        columns_jb_orphans = columns_by_status + [("orphanReason", "Orphan Reason")]
        files.append(
            self._write_csv(
                "triggered_sends_jb_orphans.csv", rows_jb_orphans, columns_jb_orphans
            )
        )

        return files

    # =========================================================================
    # Phase 4: Journey Reports
    # =========================================================================

    def generate_journey_reports(self) -> list[Path]:
        """Generate all journey-related reports.

        Returns:
            List of created file paths.
        """
        journeys = self.loader.load_objects("journeys")
        event_definitions = self.loader.load_objects("event_definitions")
        files = []

        # Build event definition lookup
        event_def_by_id: dict[str, dict] = {}
        event_def_by_key: dict[str, dict] = {}
        for ed in event_definitions:
            ed_id = ed.get("id", "")
            ed_key = ed.get("eventDefinitionKey", "")
            if ed_id:
                event_def_by_id[ed_id] = ed
            if ed_key:
                event_def_by_key[ed_key] = ed

        # Track which event definitions are used by active journeys
        used_event_def_ids: set[str] = set()

        # Report 1: Journey Event Definitions mapping
        rows_journey_events = []
        for journey in journeys:
            journey_id = journey.get("id", "")
            journey_name = journey.get("name", "")
            journey_status = journey.get("status", "")

            for trigger in journey.get("triggers", []):
                event_def_id = trigger.get("eventDefinitionId", "")
                event_def_key = trigger.get("eventDefinitionKey", "")

                # Look up event definition details
                event_def = event_def_by_id.get(event_def_id) or event_def_by_key.get(
                    event_def_key, {}
                )

                # Track usage for active journeys
                if journey_status in {"Running", "Published"}:
                    if event_def_id:
                        used_event_def_ids.add(event_def_id)

                rows_journey_events.append({
                    "journeyId": journey_id,
                    "journeyName": journey_name,
                    "journeyKey": journey.get("key"),
                    "journeyStatus": journey_status,
                    "triggerId": trigger.get("id"),
                    "triggerKey": trigger.get("key"),
                    "triggerName": trigger.get("name"),
                    "triggerType": trigger.get("type"),
                    "eventDefinitionId": event_def_id,
                    "eventDefinitionKey": event_def_key,
                    "eventDefinitionName": event_def.get("name", ""),
                    "eventType": event_def.get("type", ""),
                    "dataExtensionId": event_def.get("dataExtensionId", ""),
                    "dataExtensionName": event_def.get("dataExtensionName", ""),
                })

        columns_journey_events = [
            ("journeyId", "Journey ID"),
            ("journeyName", "Journey Name"),
            ("journeyKey", "Journey Key"),
            ("journeyStatus", "Journey Status"),
            ("triggerId", "Trigger ID"),
            ("triggerKey", "Trigger Key"),
            ("triggerName", "Trigger Name"),
            ("triggerType", "Trigger Type"),
            ("eventDefinitionId", "Event Definition ID"),
            ("eventDefinitionKey", "Event Definition Key"),
            ("eventDefinitionName", "Event Definition Name"),
            ("eventType", "Event Type"),
            ("dataExtensionId", "Data Extension ID"),
            ("dataExtensionName", "Data Extension Name"),
        ]

        files.append(
            self._write_csv(
                "journey_event_definitions.csv",
                rows_journey_events,
                columns_journey_events,
            )
        )

        # Report 2: Journey Email Activities
        rows_journey_emails = []
        for journey in journeys:
            journey_id = journey.get("id", "")
            journey_name = journey.get("name", "")
            journey_status = journey.get("status", "")

            for activity in journey.get("activities", []):
                activity_type = activity.get("type", "")
                if activity_type in {"EMAILV2", "EMAIL", "EMAILSEND"}:
                    rows_journey_emails.append({
                        "journeyId": journey_id,
                        "journeyName": journey_name,
                        "journeyStatus": journey_status,
                        "activityId": activity.get("id"),
                        "activityKey": activity.get("key"),
                        "activityName": activity.get("name"),
                        "activityType": activity_type,
                        "outcomeCount": activity.get("outcomeCount"),
                    })

        columns_journey_emails = [
            ("journeyId", "Journey ID"),
            ("journeyName", "Journey Name"),
            ("journeyStatus", "Journey Status"),
            ("activityId", "Activity ID"),
            ("activityKey", "Activity Key"),
            ("activityName", "Activity Name"),
            ("activityType", "Activity Type"),
            ("outcomeCount", "Outcome Count"),
        ]

        files.append(
            self._write_csv(
                "journey_email_activities.csv",
                rows_journey_emails,
                columns_journey_emails,
            )
        )

        # Report 3: Event Definition Orphans
        rows_orphan_events = []
        for ed in event_definitions:
            ed_id = ed.get("id", "")
            is_orphan, reason = self.analyzer.is_orphan(ed_id, "event_definition")

            # Also check if not used by any active journey
            if ed_id not in used_event_def_ids:
                is_orphan = True
                reason = "Not used by any active journey"

            if is_orphan:
                rows_orphan_events.append({
                    "id": ed_id,
                    "name": ed.get("name"),
                    "eventDefinitionKey": ed.get("eventDefinitionKey"),
                    "description": ed.get("description"),
                    "type": ed.get("type"),
                    "mode": ed.get("mode"),
                    "status": ed.get("status"),
                    "dataExtensionId": ed.get("dataExtensionId"),
                    "dataExtensionName": ed.get("dataExtensionName"),
                    "createdDate": ed.get("createdDate"),
                    "modifiedDate": ed.get("modifiedDate"),
                    "orphanReason": reason,
                })

        columns_orphan_events = [
            ("id", "ID"),
            ("name", "Name"),
            ("eventDefinitionKey", "Event Definition Key"),
            ("description", "Description"),
            ("type", "Type"),
            ("mode", "Mode"),
            ("status", "Status"),
            ("dataExtensionId", "Data Extension ID"),
            ("dataExtensionName", "Data Extension Name"),
            ("createdDate", "Created Date"),
            ("modifiedDate", "Modified Date"),
            ("orphanReason", "Orphan Reason"),
        ]

        files.append(
            self._write_csv(
                "event_definition_orphans.csv",
                rows_orphan_events,
                columns_orphan_events,
            )
        )

        return files

    # =========================================================================
    # Master Report Generator
    # =========================================================================

    def generate_all_reports(self) -> dict[str, list[Path]]:
        """Generate all dependency reports.

        Returns:
            Dictionary of report category -> list of file paths.
        """
        results: dict[str, list[Path]] = {
            "automation_activities": [],
            "filters": [],
            "triggered_sends": [],
            "journeys": [],
        }

        logger.info("=" * 60)
        logger.info("Generating Dependency Reports")
        logger.info("=" * 60)

        # Phase 1: Automation Activity Reports
        logger.info("\n--- Phase 1: Automation Activity Reports ---")
        results["automation_activities"].append(self.generate_query_report())
        results["automation_activities"].append(self.generate_script_report())
        results["automation_activities"].append(self.generate_import_report())
        results["automation_activities"].append(self.generate_data_extract_report())
        results["automation_activities"].append(self.generate_file_transfer_report())
        results["automation_activities"].append(
            self.generate_send_email_activities_report()
        )

        # Phase 2: Filter Reports
        logger.info("\n--- Phase 2: Filter Reports ---")
        results["filters"].append(self.generate_filter_report())

        # Phase 3: Triggered Send Reports
        logger.info("\n--- Phase 3: Triggered Send Reports ---")
        results["triggered_sends"].extend(self.generate_triggered_send_reports())

        # Phase 4: Journey Reports
        logger.info("\n--- Phase 4: Journey Reports ---")
        results["journeys"].extend(self.generate_journey_reports())

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Report Generation Complete")
        logger.info("=" * 60)
        total_files = sum(len(files) for files in results.values())
        logger.info(f"Total reports generated: {total_files}")
        for category, files in results.items():
            logger.info(f"  {category}: {len(files)} reports")

        return results


def find_latest_inventory(base_path: Path) -> Optional[Path]:
    """Find the most recent inventory directory.

    Args:
        base_path: Base inventory directory.

    Returns:
        Path to the latest inventory or None.
    """
    if not base_path.exists():
        return None

    # Look for inventory_* directories
    inventory_dirs = list(base_path.glob("inventory_*"))
    if not inventory_dirs:
        # Check if base_path itself is an inventory
        if (base_path / "objects").exists():
            return base_path
        return None

    # Sort by modification time (newest first)
    inventory_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return inventory_dirs[0]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate SFMC dependency reports from inventory data"
    )
    parser.add_argument(
        "--inventory",
        "-i",
        type=Path,
        help="Path to inventory directory (auto-detects if not specified)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("reports"),
        help="Output directory for reports (default: reports)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Find inventory path
    if args.inventory:
        inventory_path = args.inventory
    else:
        # Try to find inventory in common locations
        search_paths = [
            Path("inventory"),
            Path("../inventory"),
            Path.cwd() / "inventory",
        ]
        inventory_path = None
        for search_path in search_paths:
            found = find_latest_inventory(search_path)
            if found:
                inventory_path = found
                break

    if not inventory_path or not inventory_path.exists():
        logger.error("Could not find inventory directory. Use --inventory to specify.")
        sys.exit(1)

    logger.info(f"Using inventory: {inventory_path}")
    logger.info(f"Output directory: {args.output}")

    # Load inventory
    loader = InventoryLoader(inventory_path)
    graph = loader.load_graph()
    analyzer = RelationshipAnalyzer(graph)

    # Generate reports
    generator = ReportGenerator(loader, analyzer, args.output)
    results = generator.generate_all_reports()

    # Print summary
    print("\n" + "=" * 60)
    print("Generated Reports:")
    print("=" * 60)
    for category, files in results.items():
        print(f"\n{category.replace('_', ' ').title()}:")
        for filepath in files:
            print(f"  - {filepath}")


if __name__ == "__main__":
    main()
