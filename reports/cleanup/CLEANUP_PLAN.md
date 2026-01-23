# SFMC Cleanup Plan - Discount Tire (14 Year Old Account)

**Generated:** 2026-01-22
**Total Objects:** 1,600
**Cleanup Candidates:** 1,125+ objects

---

## Executive Summary

This 14-year-old SFMC account has accumulated significant technical debt. Analysis identified:

- **220 HIGH confidence** cleanup candidates (safe to delete)
- **406 MEDIUM confidence** candidates (review recommended)
- **499 LOW confidence** candidates (business owner approval needed)
- **46 Journey** cleanup candidates (28 Draft, 18 Stopped)

Estimated data reduction: **1.5+ billion rows** of unused data

---

## Phase 1: High Confidence (Quick Wins)

These objects are safe to delete with minimal review.

### TEST Data Extensions (109 objects)

Pattern: Names containing "test", "_test", "testing"

| Example Names | Last Modified |
|---------------|---------------|
| DTC_Subscriber_Vehicles_Test | 2012-11-28 |
| email_lookup_test | 2013-10-08 |
| 0000ettest | 2013-11-14 |
| TEST-07-24-2015 | 2015-07-24 |
| ATC_DTC_General_Thank_You_Send_RaiseError_TEST | 2023-04-20 |

**Data Volume:** ~53 million rows

### BACKUP/ARCHIVE Data Extensions (17 objects)

Pattern: Names containing "backup", "_bak", "archive", "copy of", "_old"

| Example Names | Last Modified | Rows |
|---------------|---------------|------|
| DTC_Subscriber_Vehicles_Backup | 2012-11-30 | 0 |
| SendLog_Backup | 2013-04-05 | 3.5M |
| DTC Tire Product List 3-27-2018 Backup | 2018-03-27 | 18K |
| DTC_General_Thank_You_Send_v2 ARCHIVE | 2025-07-08 | 6.1M |

**Data Volume:** ~33 million rows

### Stale Automations (33 objects)

**Never Run or Paused Since 2023 (9):**
- ATC Thank_You_v3_SurveyURL_Import_dev
- DTC Thank_You_v3_SurveyURL_Import_dev
- Omniture Export Metrics and Campaigns 1045947
- Omniture Import Segments 1045947
- Tread Wear Journey
- daily_tracking_process - 2
- daily_tracking_process_makeup
- daily_tracking_process_migrated_from_folder_myprograms
- memorial_STO_reporting

**Very Old (Last Run Before 2022, 24):**
- Co Subscribers (last run: 2014)
- Dynamic Branding Demo (last run: 2015)
- SubscriberStatusExtract (last run: 2015)
- 18 Month Engaged Subscribers (last run: 2017)
- Bad Email Addresses (last run: 2018)
- ...and 19 more

### Other High Confidence Objects (61)

| Type | Count |
|------|-------|
| Queries | 20 |
| Classic Emails | 15 |
| Imports | 9 |
| Assets | 7 |
| Lists | 5 |
| Data Extracts | 4 |
| Filters | 1 |

---

## Phase 2: Medium Confidence (Review Recommended)

### Very Old Data Extensions (383 objects)

DEs not modified since January 2022. Sample:

| Name | Last Modified | Rows |
|------|---------------|------|
| Cart Abandonment Segment | 2011-10-03 | ? |
| DTC_subscriber_status | 2012-08-30 | ? |
| DTC_Subscriber_Stores | 2012-11-28 | ? |
| DTC_Tire_Rotation_Reminder_Send | 2013-03-21 | ? |

**Data Volume:** ~1.48 billion rows

### Draft Journeys Never Published (28)

These journeys were created but never activated:

**Discount Tire (DT) - 24 drafts:**
- DT_Journey_Final-COPY (2017)
- TEST_DT_Journey_SFMC (2018)
- Discount_Tire_CERT_Journey (2021)
- Einstein STO Demo (2023)
- DTC Welcome (2024)
- MLS_Journey (2025)
- ...and 18 more

**America's Tire (AT) - 4 drafts:**
- AT_Journey (2020)
- America_Tire_CERT_Journey (2021)
- Tire Tread Depth Reminder SMS - Email (TEST Copy) (2023)
- ATC Welcome (2024)

### Stopped Journeys (18)

Journeys that were stopped and may no longer be needed:

**Discount Tire:**
- Onereach_Prod_Test (2022)
- 20240219_DTC_PD_Dynamic_SL_Journey
- OrderShippedHome, OrderDelayedHome, OrderOutForDeliveryHome, OrderDeliveredHome (2024)
- BOPIS_Order_Arrival
- DTC_Promo_Message_Frequency_Journey_Template

**America's Tire:**
- Order tracking journeys (OrderShipped, Delayed, OutForDelivery, Delivered)
- BOPIS_Order_Arrival

---

## Phase 3: Careful Review Required

### Sendable Data Extensions (272)

These are subscriber-related DEs that may contain customer data. Review with business stakeholders before any action.

### Other Orphaned Objects (166+ DEs, 61 other)

Objects not referenced by any automation, journey, or other process. May be standalone reporting DEs or legitimately unused.

---

## Cleanup Reports Generated

CSV files for detailed review:

1. **`reports/cleanup/orphaned_data_extensions.csv`** - All 947 orphaned DEs with confidence ratings
2. **`reports/cleanup/stale_automations.csv`** - 50 automation cleanup candidates
3. **`reports/cleanup/other_orphans.csv`** - 128 other orphaned objects

---

## Recommended Approach

### Week 1: Phase 1 Quick Wins
1. Export TEST DE list, get stakeholder acknowledgment
2. Delete TEST DEs via API (109 objects)
3. Delete BACKUP/ARCHIVE DEs (17 objects)
4. Pause → Delete stale automations (33 objects)
5. Delete orphaned queries, imports, classic emails

### Week 2: Journey Cleanup
1. Review DRAFT journeys with Journey Builder team
2. Delete confirmed unused drafts
3. Review STOPPED journeys for potential reactivation or deletion

### Week 3-4: Phase 2 Review
1. Share "Very Old DEs" report with data governance team
2. Identify DEs supporting active processes
3. Archive or delete confirmed unused DEs

### Ongoing: Phase 3
1. Monthly review of orphaned objects
2. Implement naming conventions to prevent future accumulation
3. Document active DEs and their purposes

---

## API Deletion Notes

### Data Extension Deletion
```bash
# REST API - Delete DE by key
DELETE /data/v1/customobjectdata/key/{key}/rowset

# Or SOAP - Delete entire DE
<DeleteRequest>
  <Objects xsi:type="DataExtension">
    <CustomerKey>{customerKey}</CustomerKey>
  </Objects>
</DeleteRequest>
```

### Automation Deletion
```bash
# First pause if running
PATCH /automation/v1/automations/{id}
{"status": "PausedByUser"}

# Then delete
DELETE /automation/v1/automations/{id}
```

### Journey Deletion
Journeys cannot be deleted via API if they have any version history. Options:
- Stop the journey
- Mark as "Archived" in name
- Leave stopped (SFMC doesn't support journey deletion via API in most cases)

---

## Risk Mitigation

1. **Before deleting any DE**: Check for SQL queries that reference it
2. **Before deleting automations**: Verify no scheduled sends depend on them
3. **Export data**: Keep CSV exports of deleted object metadata for audit trail
4. **Staged approach**: Delete in batches, verify no errors before continuing
