# SFMC Object Relationship Model

This document describes the relationships between Salesforce Marketing Cloud objects as captured by the inventory tool.

## Architecture Overview

SFMC objects form a directed graph of dependencies. Understanding these relationships is critical for:

- Identifying orphaned objects safe for deletion
- Understanding the impact of deleting an object
- Mapping data flows through the platform

```
                    ┌─────────────────┐
                    │   Automation    │
                    └────────┬────────┘
                             │ contains
        ┌────────┬───────┬───┴───┬────────┬─────────┐
        ▼        ▼       ▼       ▼        ▼         ▼
     Query   Script   Import  Filter  Extract  Transfer
        │                │       │        │
        │ writes         │writes │writes  │reads
        ▼                ▼       ▼        ▼
    ┌───────────────────────────────────────────┐
    │            Data Extensions                 │
    └───────────────────────────────────────────┘
                         ▲
                         │ uses
              ┌──────────┴──────────┐
              │  Event Definition   │
              └──────────┬──────────┘
                         │ triggers
                         ▼
                    ┌─────────┐
                    │ Journey │
                    └────┬────┘
                         │ uses (implicit)
                         ▼
                  ┌──────────────┐
                  │Triggered Send│
                  └──────┬───────┘
                         │ uses
        ┌────────┬───────┴───────┬────────┐
        ▼        ▼               ▼        ▼
     Email     List       Sender Prof  Delivery Prof
```

## Core Object Types

### Automations

Automations are the primary orchestration mechanism in SFMC. They contain steps, and each step contains one or more activities.

**Contains:**
- Query Activities (SQL queries)
- Script Activities (SSJS)
- Import Activities
- Data Extract Activities
- File Transfer Activities
- Filter Activities
- Send Email Activities
- Refresh Group Activities

**Relationships:**
- `automation_contains_query`
- `automation_contains_script`
- `automation_contains_import`
- `automation_contains_extract`
- `automation_contains_transfer`
- `automation_contains_filter`
- `automation_contains_email`

### Queries

SQL Query Activities that read from and write to Data Extensions.

**Relationships:**
- `query_reads_de` - Data Extensions referenced in FROM/JOIN clauses
- `query_writes_de` - Target Data Extension for query output

**Key Fields:**
- `targetId` / `targetName` / `targetKey` - Target DE info
- `queryText` - SQL query text (for SQL analysis)
- `referencedDataExtensions` - Parsed DE references

### Scripts (SSJS Activities)

Server-Side JavaScript activities that can interact with platform objects.

**Relationships:**
- `script_uses_de` - Data Extensions referenced in script (requires script parsing)

**Note:** Script dependencies are harder to track automatically as they may reference objects dynamically.

### Imports

Import Activities that load data from files into Data Extensions.

**Relationships:**
- `import_writes_de` - Destination Data Extension
- `import_reads_file` - Source file location

**Key Fields:**
- `destinationId` / `destinationName` - Target DE
- `fileTransferLocationName` - FTP/SFTP location
- `fileNamingPattern` - File pattern to match

### Data Extracts

Extract activities that export data from Data Extensions to files.

**Relationships:**
- `extract_reads_de` - Source Data Extension (requires configuration parsing)
- `extract_writes_file` - Output file location

**Key Fields:**
- `dataExtractTypeName` - Type of extract
- `fileSpec` - Output file specification

### File Transfers

Activities that move files between locations.

**Relationships:**
- `transfer_reads_file` - Source location
- `transfer_writes_file` - Destination location

**Key Fields:**
- `fileTransferLocationName` - FTP/SFTP location
- `fileAction` - Move, copy, etc.

### Filters (Filter Activities)

Filter Activities that segment data from one DE to another.

**Relationships:**
- `filter_reads_de` - Source Data Extension
- `filter_writes_de` - Destination Data Extension

**Key Fields:**
- `sourceDataExtensionId` / `sourceDataExtensionName`
- `destinationDataExtensionId` / `destinationDataExtensionName`
- `filterDefinitionId` - Underlying filter logic

**Note on Filter Types:**
- **Filter Activities** - Schedulable activities in Automation Studio (what we capture)
- **Filter Definitions** - Underlying filter logic, also used for ad-hoc click-refresh filters
- For cleanup purposes, Filter Activities are sufficient as ad-hoc filters are rarely used

## Journey Builder

### Journeys

Journey Builder workflows that orchestrate customer experiences.

**Contains:**
- EMAILV2 Activities (email sends)
- WAIT Activities
- MULTICRITERIADECISION Activities (decision splits)
- Various other activity types

**Relationships:**
- `journey_uses_event` - Entry event definition
- `journey_uses_de` - Data Extensions used in journey
- `journey_uses_email` - Emails sent by journey
- `journey_uses_filter` - Filters used in journey
- `journey_uses_automation` - Automations triggered by journey

**Key Fields:**
- `status` - Draft, Running, Published, Stopped, etc.
- `triggers` - Array of entry triggers with event definition references
- `activities` - Array of journey activities

### Event Definitions

Entry events that trigger journey entry.

**Relationships:**
- `event_definition_uses_de` - Data Extension for event data

**Types:**
- `EmailAudience` - Email-based entry
- `AutomationAudience` - Automation-triggered entry
- `APIEvent` - API-triggered entry
- `SalesforceObjectTrigger` - Salesforce object change trigger

**Key Fields:**
- `eventDefinitionKey` - Unique key for API reference
- `dataExtensionId` / `dataExtensionName` - Associated DE

## Triggered Sends

### Triggered Send Definitions

Email send configurations that can be triggered via API or Journey Builder.

**Relationships:**
- `triggered_send_uses_email` - Email content
- `triggered_send_uses_list` - Subscriber list
- `triggered_send_uses_sender_profile` - Sender profile
- `triggered_send_uses_delivery_profile` - Delivery profile
- `triggered_send_uses_send_classification` - Send classification

**Key Fields:**
- `status` - Active, Inactive, Deleted, Canceled, New
- `emailId` / `emailName` - Email content reference
- `listId` / `listName` - Subscriber list
- `sendClassificationKey` / `senderProfileKey` / `deliveryProfileKey`

### Journey Builder and Triggered Sends

**Critical Insight:** Journey Builder creates implicit Triggered Send objects for EMAILV2 activities.

When you create an email activity in a journey:
1. JB auto-creates a Triggered Send Definition
2. The TS is placed in a special `triggered_send_journeybuilder` folder
3. The TS name often contains a UUID suffix

**Orphan Problem:** When journeys are deleted:
1. The journey is removed
2. The auto-created Triggered Send definitions **remain**
3. These TS definitions have status "Deleted" but aren't actually removed
4. Result: Large numbers of orphaned TS definitions

**Identifying JB Orphans:**
- Folder path contains "journeybuilder"
- Status is "Deleted" (most common)
- Name contains UUID pattern

### Send Classifications, Sender Profiles, Delivery Profiles

These form the messaging infrastructure layer.

```
Send Classification
    │
    ├── uses → Sender Profile (from name, from address)
    │
    └── uses → Delivery Profile (domain settings)
```

**Relationships:**
- `send_classification_uses_sender_profile`
- `send_classification_uses_delivery_profile`

## Orphan Detection Rules

Objects are considered orphaned if not referenced by expected source types:

| Object Type | Must Be Referenced By | Notes |
|-------------|----------------------|-------|
| `query` | automation | Standalone queries are orphans |
| `script` | automation | Standalone scripts are orphans |
| `import` | automation | Standalone imports are orphans |
| `data_extract` | automation | Standalone extracts are orphans |
| `file_transfer` | automation | Standalone transfers are orphans |
| `filter` | automation, journey | Used by either |
| `data_extension` | query, journey, import, filter, etc. | Many possible users |
| `email` | automation, journey, triggered_send | |
| `triggered_send` | *(special rules)* | See JB orphan notes |
| `event_definition` | journey | Must be used by active journey |
| `list` | triggered_send, journey | |
| `sender_profile` | send_classification, triggered_send | |
| `delivery_profile` | send_classification, triggered_send | |
| `send_classification` | triggered_send | |

## Data Flow Patterns

### Automation Data Pipeline

```
Source File → Import → Data Extension → Query → Target DE → Extract → Output File
```

### Journey Email Flow

```
Event Definition → Journey Entry → EMAILV2 Activity → (implicit) Triggered Send → Email
                                                                    │
                                                                    └── uses → List, Sender Profile, etc.
```

### Triggered Send Direct Flow

```
API Call → Triggered Send → Email
                │
                └── uses → List, Sender Profile, Delivery Profile
```

## Status Values

### Automation Status

| Status | Description |
|--------|-------------|
| Ready | Configured but not scheduled |
| Scheduled | Has future runs scheduled |
| Running | Currently executing |
| PausedSchedule | Schedule paused |
| Building | Being modified |
| Error | Failed execution |

### Journey Status

| Status | Description |
|--------|-------------|
| Draft | Not yet published |
| Published | Live and accepting entries |
| Running | Same as Published |
| Stopped | Manually stopped |
| ScheduledToStop | Will stop after current entries complete |

### Triggered Send Status

| Status | Description | Cleanup Action |
|--------|-------------|----------------|
| Active | In use | Keep |
| Inactive | Manually disabled | Review |
| Deleted | Soft-deleted | Safe to delete |
| Canceled | Send canceled | Review |
| New | Never activated | Review |

## API Endpoints Used

### REST API

- `/automation/v1/automations` - Automations
- `/automation/v1/queries` - Query activities
- `/automation/v1/scripts` - Script activities
- `/automation/v1/imports` - Import activities
- `/automation/v1/dataextracts` - Data extract activities
- `/automation/v1/filetransfers` - File transfer activities
- `/automation/v1/filters` - Filter activities
- `/interaction/v1/interactions` - Journeys
- `/interaction/v1/eventDefinitions` - Event definitions
- `/asset/v1/content/assets` - Content assets

### SOAP API

- `TriggeredSendDefinition` - Triggered sends
- `List` - Subscriber lists
- `Email` - Classic emails
- `SenderProfile` - Sender profiles
- `DeliveryProfile` - Delivery profiles
- `SendClassification` - Send classifications
- `DataExtension` - Data extensions
- `DataFolder` - Folders

## Limitations and Known Issues

1. **Script Dependencies** - SSJS script dependencies require parsing the script code, which may not catch all dynamic references.

2. **Journey Email Details** - Journey EMAILV2 activities reference emails by internal ID, but the full triggered send configuration is implicitly created.

3. **Cross-BU References** - Objects in shared data extensions or content areas may have cross-BU dependencies not fully captured.

4. **Historical Data** - The relationship graph represents the current state; historical relationships (before modifications) are not tracked.

5. **Dynamic References** - AMPscript and SSJS can reference objects dynamically by name/key, which may not be detected by static analysis.
