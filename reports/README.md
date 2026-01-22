# SFMC Dependency Reports

This directory contains tools and generated reports for analyzing SFMC object dependencies to support cleanup initiatives.

## Purpose

These reports help identify:

- **Orphaned automation activities** - Queries, scripts, imports, filters, data extracts, and file transfers not used by any automation
- **Disabled triggered sends** - Inactive, deleted, or canceled triggered sends
- **Journey Builder orphans** - Triggered sends created by deleted journeys
- **Unused event definitions** - Event definitions not linked to active journeys
- **Stale objects** - Objects that haven't been modified recently

## Quick Start

Generate all reports from your inventory data:

```bash
# From the project root
python reports/generate_dependency_reports.py --inventory inventory/inventory_ACCOUNTID_TIMESTAMP

# Or auto-detect the latest inventory
python reports/generate_dependency_reports.py

# Specify custom output directory
python reports/generate_dependency_reports.py --output my_reports/
```

## Generated Reports

### Phase 1: Automation Activity Reports

| File | Description |
|------|-------------|
| `queries_dependency_report.csv` | All query activities with automation usage, target DE info, orphan status |
| `scripts_dependency_report.csv` | All SSJS scripts with automation usage, orphan status |
| `imports_dependency_report.csv` | All import activities with destination DE, file source info |
| `data_extracts_dependency_report.csv` | All data extract activities with extract type, file specs |
| `file_transfers_dependency_report.csv` | All file transfer activities with location and action info |
| `send_email_activities_report.csv` | Email send activities found within automations |

### Phase 2: Filter Reports

| File | Description |
|------|-------------|
| `filter_activities_report.csv` | All filter activities with source/destination DEs, automation usage |

### Phase 3: Triggered Send Reports

| File | Description |
|------|-------------|
| `triggered_sends_by_status.csv` | Complete list of all triggered sends with status breakdown |
| `triggered_sends_disabled.csv` | Triggered sends with Inactive/Deleted/Canceled status |
| `triggered_sends_jb_orphans.csv` | JB-created triggered sends likely orphaned from deleted journeys |

### Phase 4: Journey Reports

| File | Description |
|------|-------------|
| `journey_event_definitions.csv` | Mapping of journeys to their entry event definitions |
| `journey_email_activities.csv` | Email activities within journeys (EMAILV2 type) |
| `event_definition_orphans.csv` | Event definitions not used by any active journey |

## Report Columns

### Common Fields

Most reports include these standard fields:

- **ID** - Unique object identifier
- **Name** - Display name
- **Customer Key** - External key for API access
- **Folder Path** - Location in SFMC folder hierarchy
- **Created Date** / **Modified Date** - Timestamps
- **Is Orphan** - Whether the object appears unused
- **Orphan Reason** - Why the object is considered orphaned

### Automation Usage Fields

- **Automation Count** - Number of automations using this object
- **Automations** - Semi-colon separated list of automation names

### Triggered Send Status Values

| Status | Meaning | Recommended Action |
|--------|---------|-------------------|
| Active | Currently in use | Keep |
| Inactive | Manually disabled | Review for deletion |
| Deleted | Soft-deleted, likely JB orphan | Safe to permanently delete |
| Canceled | Send was canceled | Review for deletion |
| New | Created but never activated | Review or delete |

## Usage Tips

### Importing to Excel

1. Open Excel
2. Go to **Data** > **From Text/CSV**
3. Select the CSV file
4. Excel will auto-detect delimiters
5. Each report becomes a separate sheet for easy cross-referencing

### Filtering Orphans

To find cleanup candidates, filter on:

```
Is Orphan = Yes
```

### Finding JB Orphans

Journey Builder creates implicit triggered send definitions. When journeys are deleted, these remain as orphans. To find them:

1. Open `triggered_sends_jb_orphans.csv`
2. All entries are cleanup candidates
3. Cross-reference with `triggered_sends_by_status.csv` for full details

### Verifying Before Deletion

Before deleting objects marked as orphans:

1. Check **Modified Date** - recent activity might indicate the object is still needed
2. Search for **Customer Key** in SFMC - might be referenced by external systems
3. Check **Folder Path** - objects in shared folders might be used by other teams

## Technical Details

### Data Sources

Reports are generated from:

- `inventory/objects/*.ndjson` - Raw inventory data
- `inventory/relationships/graph.json` - Relationship graph with edges between objects
- `inventory/relationships/orphans.json` - Pre-computed orphan list

### Orphan Detection Rules

Objects are considered orphaned based on these rules:

| Object Type | Must Be Referenced By |
|-------------|----------------------|
| query | automation |
| script | automation |
| import | automation |
| data_extract | automation |
| file_transfer | automation |
| filter | automation, journey |
| event_definition | journey |

### Relationship Types

The relationship graph tracks these connection types:

- `automation_contains_*` - Automation → Activity relationships
- `query_reads_de` / `query_writes_de` - Query → Data Extension
- `journey_uses_*` - Journey → Various objects
- `triggered_send_uses_*` - Triggered Send → Email, List, Profiles
- `event_definition_uses_de` - Event Definition → Data Extension

## Extending Reports

To add new reports, edit `generate_dependency_reports.py`:

1. Add a new method to `ReportGenerator` class
2. Define columns as list of `(field_name, header_name)` tuples
3. Build rows as list of dictionaries
4. Call `self._write_csv(filename, rows, columns)`
5. Add the method call to `generate_all_reports()`

Example:

```python
def generate_custom_report(self) -> Path:
    """Generate my_custom_report.csv."""
    objects = self.loader.load_objects("my_object_type")
    rows = []

    for obj in objects:
        rows.append({
            "id": obj.get("id"),
            "name": obj.get("name"),
            # ... more fields
        })

    columns = [
        ("id", "ID"),
        ("name", "Name"),
        # ... more columns
    ]

    return self._write_csv("my_custom_report.csv", rows, columns)
```

## See Also

- [SFMC Relationship Model](./SFMC_RELATIONSHIP_MODEL.md) - Technical documentation of object relationships
- Main project [README](../README.md) - Inventory extraction tool documentation
