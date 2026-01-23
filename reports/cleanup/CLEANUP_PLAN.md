# SFMC Cleanup Plan - Discount Tire (14 Year Old Account)

**Generated:** 2026-01-23
**Total Objects:** 4,130 (across Parent + DT + AT Business Units)
**Cleanup Candidates:** 1,398 objects (737 DEs + 277 automations + 384 other)

---

## Executive Summary

This 14-year-old SFMC account has accumulated significant technical debt. Analysis identified:

- **410 HIGH confidence** cleanup candidates (safe to delete)
- **503 MEDIUM confidence** candidates (review recommended)
- **139 LOW confidence** candidates (1-3 years old)
- **146 REVIEW required** candidates (active/high-volume DEs, recent objects)
- **46 Journey** cleanup candidates (28 Draft, 18 Stopped)

### Safety Checks Applied

This analysis includes safety checks to prevent accidental deletion of active data:

| Category | Criteria | Count | Total Rows | Confidence |
|----------|----------|-------|------------|------------|
| ACTIVE_ORPHAN | Modified <90 days, >1K rows | 12 DEs | N/A | REVIEW |
| SENDLOG_ARCHIVE | >100K rows, sendlog pattern, >1yr old | 8 DEs | 1.4B | MEDIUM |
| TRACKING_ARCHIVE | >100K rows, tracking pattern, >3yr old | 31 DEs | 857M | MEDIUM |
| ACTIVE_TRACKING | >100K rows, tracking pattern, <3yr old | 6 DEs | 14M | REVIEW |
| HIGH_VOL_OLD | >100K rows, no pattern, >3yr old | 50 DEs | 281M | MEDIUM |
| HIGH_VOL_ACTIVE | >100K rows, no pattern, <3yr old | 50 DEs | 279M | REVIEW |

### Extraction Methodology

- **Multi-BU extraction** across Parent (1045947), DT (1054904), and AT (1067662)
- **10,694 relationship mappings** (import→DE, query→DE, filter→DE, etc.)
- **90-day staleness threshold** for automations
- **210 fewer false-positive orphans** vs single-BU analysis

---

## Phase 1: High Confidence (Quick Wins) - 410 Objects

These objects are safe to delete with minimal review.

### TEST Data Extensions (77 objects)

Pattern: Names containing "test", "_test", "testing"

| Example Names | Last Modified |
|---------------|---------------|
| DTC_Subscriber_Vehicles_Test | 2012-11-28 |
| email_lookup_test | 2013-10-08 |
| 0000ettest | 2013-11-14 |
| TEST-07-24-2015 | 2015-07-24 |

### BACKUP/ARCHIVE Data Extensions (10 objects)

Pattern: Names containing "backup", "_bak", "archive", "copy of", "_old"

| Example Names | Last Modified |
|---------------|---------------|
| DTC_Subscriber_Vehicles_Backup | 2012-11-30 |
| SendLog_Backup | 2013-04-05 |
| DTC Tire Product List 3-27-2018 Backup | 2018-03-27 |

### Stale Automations (235 HIGH confidence)

Using 90-day staleness threshold:

| Category | Count | Description |
|----------|-------|-------------|
| PAUSED_STALE | 48 | Paused and not run in 90+ days |
| READY_VERY_OLD | 187 | Status "Ready" but last run over 1 year ago |

### Other High Confidence Objects (223)

| Type | Count |
|------|-------|
| Queries | 120 |
| Event Definitions | 86 |
| Data Extracts | 62 |
| Assets | 53 |
| Imports | 35 |
| Classic Emails | 15 |
| Lists | 6 |
| Filters | 3 |
| Scripts | 2 |
| Delivery Profiles | 2 |

---

## Phase 2: Medium Confidence (Review Recommended) - 503 Objects

### Very Old Data Extensions (290 objects)

DEs not modified in 3+ years. Sample:

| Name | Last Modified |
|------|---------------|
| Cart Abandonment Segment | 2011-10-03 |
| DTC_subscriber_status | 2012-08-30 |
| DTC_Subscriber_Stores | 2012-11-28 |
| DTC_Tire_Rotation_Reminder_Send | 2013-03-21 |

### Stale Automations (42 MEDIUM confidence)

| Category | Count | Description |
|----------|-------|-------------|
| READY_STALE | 21 | Status "Ready" but not run in 90+ days |
| INCOMPLETE | 19 | Status "Building" or "Inactive" |
| PAUSED_RECENT | 2 | Recently paused |

### Other Medium Confidence Objects (82)

Objects modified 1-3 years ago without detected relationships.

### High Volume Archive DEs (89 objects)

DEs with >100K rows that can be archived based on age and pattern:

| Category | Count | Total Rows | Description |
|----------|-------|------------|-------------|
| SENDLOG_ARCHIVE | 8 | 1.4B | SendLog DEs >1 year old |
| TRACKING_ARCHIVE | 31 | 857M | Tracking DEs (click/open/bounce) >3 years old |
| HIGH_VOL_OLD | 50 | 281M | Other high-volume DEs >3 years old |

**Examples:**
- ENT_SendLog (1.4B rows, 2020) - Enterprise send log archive
- Opens (208M rows, 2020) - Historical open tracking
- Clicks (187M rows, 2019) - Historical click tracking
- Bounces (45M rows, 2021) - Historical bounce data

### Draft Journeys Never Published (28)

**Discount Tire (DT) - 24 drafts:**
- DT_Journey_Final-COPY (2017)
- TEST_DT_Journey_SFMC (2018)
- Discount_Tire_CERT_Journey (2021)
- Einstein STO Demo (2023)
- DTC Welcome (2024)
- MLS_Journey (2025)

**America's Tire (AT) - 4 drafts:**
- AT_Journey (2020)
- America_Tire_CERT_Journey (2021)
- Tire Tread Depth Reminder SMS - Email (TEST Copy) (2023)
- ATC Welcome (2024)

### Stopped Journeys (18)

**Discount Tire:**
- Onereach_Prod_Test (2022)
- 20240219_DTC_PD_Dynamic_SL_Journey
- Order tracking journeys (OrderShipped, Delayed, OutForDelivery, Delivered)
- BOPIS_Order_Arrival

**America's Tire:**
- Order tracking journeys
- BOPIS_Order_Arrival

---

## Phase 3: Low Confidence - 139 Objects

### Old Data Extensions (139 objects)

DEs not modified in 1-3 years but not matching TEST/BACKUP patterns. Business owner approval needed.

---

## Phase 4: Review Required - 146 Objects

### Active Orphan DEs (12 objects)

Recently modified (<90 days) with significant data (>1K rows). Likely populated via:
- Journey SMS activities
- AMPscript/SSJS in emails
- External API integrations

**Examples:**
- AllContacts AW Enteprise (87M rows, modified Jan 2026)
- All Mobile Contacts AW (85M rows, modified Nov 2025)
- Black Friday 2024 Purchasers (18M rows, modified Nov 2025)
- ENT_SMS_SendLog (1.4M rows, modified Nov 2025)

### Active High Volume DEs (56 objects)

DEs with >100K rows that are recently active and need business owner review:

| Category | Count | Total Rows | Description |
|----------|-------|------------|-------------|
| ACTIVE_TRACKING | 6 | 14M | Active tracking DEs (<3 years old) |
| HIGH_VOL_ACTIVE | 50 | 279M | Other active high-volume DEs |

**Examples:**
- AT_Thank_You_SMS_Sends (267K rows, Jul 2025) - Active SMS tracking
- AT_ThankYou_ErrorLogging (252K rows, Jul 2025) - Active error logging
- AT_Thank_You_SMS_Click_Tracking (252K rows, Jul 2025) - Active click tracking

### Other Review Objects (78)

Recently modified objects without detected relationships.

---

## Cleanup Reports Generated

| File | Contents |
|------|----------|
| `orphaned_data_extensions.csv` | 737 DEs with confidence ratings |
| `stale_automations.csv` | 277 automation candidates |
| `other_orphans.csv` | 384 other orphaned objects |
| `SFMC_Cleanup_Analysis.xlsx` | Excel workbook with all reports |

---

## Recommended Approach

### Week 1: Phase 1 Quick Wins
1. Export TEST DE list, get stakeholder acknowledgment
2. Delete TEST DEs via API (77 objects)
3. Delete BACKUP/ARCHIVE DEs (10 objects)
4. Pause → Delete stale automations (235 objects)
5. Delete orphaned queries, imports, classic emails

### Week 2: Journey Cleanup
1. Review DRAFT journeys with Journey Builder team
2. Delete confirmed unused drafts
3. Review STOPPED journeys for potential reactivation or deletion

### Week 3-4: Phase 2 Review
1. Share "Very Old DEs" report with data governance team
2. Identify DEs supporting active processes
3. Archive or delete confirmed unused DEs
4. **Archive high-volume tracking data** (89 DEs, 2.5B+ rows):
   - Export historical SendLog data before deletion
   - Archive tracking DEs (Opens, Clicks, Bounces) to data warehouse
   - Coordinate with analytics team on data retention requirements

### Ongoing: Phase 3-4
1. Review LOW confidence DEs with business owners
2. Investigate ACTIVE_ORPHAN and ACTIVE HIGH_VOLUME DEs (68 total)
3. Document active DEs and their purposes
4. Implement naming conventions to prevent future accumulation

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
5. **NEVER delete REVIEW items** without explicit business owner approval
