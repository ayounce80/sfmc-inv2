#!/usr/bin/env python3
"""Build Excel workbook for SFMC cleanup review."""

import csv
import json
from datetime import datetime
from pathlib import Path

import xlsxwriter


def load_csv(path: Path) -> list[dict]:
    """Load CSV file."""
    with open(path) as f:
        return list(csv.DictReader(f))


def load_ndjson(path: Path) -> list[dict]:
    """Load NDJSON file."""
    items = []
    with open(path) as f:
        for line in f:
            if line.strip():
                items.append(json.loads(line))
    return items


def build_workbook(inventory_path: Path, cleanup_path: Path, output_path: Path):
    """Build Excel workbook with cleanup analysis."""

    # Load data
    des = load_csv(cleanup_path / "orphaned_data_extensions.csv")
    autos = load_csv(cleanup_path / "stale_automations.csv")
    others = load_csv(cleanup_path / "other_orphans.csv")
    triggered_sends = load_csv(cleanup_path / "inactive_triggered_sends.csv")
    event_defs = load_csv(cleanup_path / "orphaned_event_definitions.csv")

    # Load journeys for journey sheet
    journeys = load_ndjson(inventory_path / "objects/journeys.ndjson")

    # Create workbook
    wb = xlsxwriter.Workbook(str(output_path))

    # Define formats
    fmt_title = wb.add_format({
        'bold': True, 'font_size': 16, 'font_color': '#1F4E79'
    })
    fmt_header = wb.add_format({
        'bold': True, 'bg_color': '#1F4E79', 'font_color': 'white',
        'border': 1, 'text_wrap': True, 'valign': 'vcenter'
    })
    fmt_high = wb.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
    fmt_medium = wb.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C5700'})
    fmt_low = wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
    fmt_number = wb.add_format({'num_format': '#,##0'})
    fmt_date = wb.add_format({'num_format': 'yyyy-mm-dd'})
    fmt_percent = wb.add_format({'num_format': '0.0%'})
    fmt_bold = wb.add_format({'bold': True})
    fmt_wrap = wb.add_format({'text_wrap': True, 'valign': 'top'})

    # ========== SUMMARY SHEET ==========
    ws = wb.add_worksheet("Summary")
    ws.set_column('A:A', 40)
    ws.set_column('B:B', 15)
    ws.set_column('C:C', 20)
    ws.set_column('D:D', 50)

    ws.write('A1', 'SFMC Cleanup Analysis - Discount Tire', fmt_title)
    ws.write('A2', f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    ws.write('A3', f'Account: 14+ year old enterprise')

    row = 5
    ws.write(row, 0, 'CLEANUP SUMMARY', fmt_bold)
    row += 2

    # Calculate summaries
    high_des = [d for d in des if d['confidence'] == 'HIGH']
    med_des = [d for d in des if d['confidence'] == 'MEDIUM']
    low_des = [d for d in des if d['confidence'] in ('LOW', 'REVIEW')]

    high_autos = [a for a in autos if a['confidence'] == 'HIGH']
    med_autos = [a for a in autos if a['confidence'] == 'MEDIUM']

    high_ts = [t for t in triggered_sends if t['confidence'] == 'HIGH']
    med_ts = [t for t in triggered_sends if t['confidence'] == 'MEDIUM']

    high_events = [e for e in event_defs if e['confidence'] == 'HIGH']
    med_events = [e for e in event_defs if e['confidence'] == 'MEDIUM']

    high_other = [o for o in others if o['confidence'] == 'HIGH']
    med_other = [o for o in others if o['confidence'] == 'MEDIUM']

    draft_journeys = [j for j in journeys if j.get('status') == 'Draft']
    stopped_journeys = [j for j in journeys if j.get('status') == 'Stopped']

    # Summary table
    headers = ['Category', 'Count', 'Confidence', 'Notes']
    for col, h in enumerate(headers):
        ws.write(row, col, h, fmt_header)
    row += 1

    summary_data = [
        ('TEST Data Extensions', len([d for d in high_des if d['category'] == 'TEST']), 'HIGH', 'Names contain "test"'),
        ('BACKUP/ARCHIVE DEs', len([d for d in high_des if d['category'] == 'BACKUP']), 'HIGH', 'Names contain "backup", "archive"'),
        ('Very Old DEs (pre-2022)', len([d for d in med_des if d['category'] == 'VERY_OLD']), 'MEDIUM', 'Not modified since 2022'),
        ('Old DEs (pre-2024)', len([d for d in des if d['confidence'] == 'LOW']), 'LOW', 'Not modified since 2024'),
        ('', '', '', ''),
        ('Inactive Triggered Sends', len(triggered_sends), 'MIXED', f'{len(high_ts)} HIGH, {len(med_ts)} MEDIUM'),
        ('Orphaned Event Definitions', len(event_defs), 'MIXED', f'{len(high_events)} HIGH, {len(med_events)} MEDIUM'),
        ('', '', '', ''),
        ('Stale Automations (never/old)', len(high_autos), 'HIGH', 'Never run or last run before 2022'),
        ('Stale Automations (paused)', len(med_autos), 'MEDIUM', 'Recently paused'),
        ('', '', '', ''),
        ('Orphaned Queries', len([o for o in others if o['type'] == 'query']), 'MIXED', 'Not used in any automation'),
        ('Orphaned Classic Emails', len([o for o in others if o['type'] == 'classic_email']), 'MIXED', 'Not used in any send'),
        ('Orphaned Imports', len([o for o in others if o['type'] == 'import']), 'MIXED', 'Not used in any automation'),
        ('Orphaned Assets', len([o for o in others if o['type'] == 'asset']), 'MIXED', 'Not referenced'),
        ('Other Orphans', len([o for o in others if o['type'] not in ('query', 'classic_email', 'import', 'asset')]), 'MIXED', 'Lists, extracts, filters'),
        ('', '', '', ''),
        ('Draft Journeys', len(draft_journeys), 'MEDIUM', 'Never published'),
        ('Stopped Journeys', len(stopped_journeys), 'MEDIUM', 'May need review'),
    ]

    for item in summary_data:
        if item[0]:  # Skip empty separator rows
            ws.write(row, 0, item[0])
            ws.write(row, 1, item[1], fmt_number)
            conf = item[2]
            if conf == 'HIGH':
                ws.write(row, 2, conf, fmt_high)
            elif conf == 'MEDIUM':
                ws.write(row, 2, conf, fmt_medium)
            elif conf in ('LOW', 'REVIEW'):
                ws.write(row, 2, conf, fmt_low)
            else:
                ws.write(row, 2, conf)
            ws.write(row, 3, item[3])
        row += 1

    row += 2
    ws.write(row, 0, 'PHASE TOTALS', fmt_bold)
    row += 1

    phase1_total = len(high_des) + len(high_autos) + len(high_ts) + len(high_events) + len(high_other)
    phase2_total = len(med_des) + len(med_autos) + len(med_ts) + len(med_events) + len(draft_journeys) + len(stopped_journeys)
    phase3_total = len(low_des) + len([o for o in others if o['confidence'] == 'REVIEW'])

    ws.write(row, 0, 'Phase 1 (High Confidence)')
    ws.write(row, 1, phase1_total, fmt_number)
    ws.write(row, 2, 'Safe to delete', fmt_high)
    row += 1
    ws.write(row, 0, 'Phase 2 (Review Recommended)')
    ws.write(row, 1, phase2_total, fmt_number)
    ws.write(row, 2, 'Stakeholder review', fmt_medium)
    row += 1
    ws.write(row, 0, 'Phase 3 (Business Approval)')
    ws.write(row, 1, phase3_total, fmt_number)
    ws.write(row, 2, 'Careful review', fmt_low)
    row += 2
    ws.write(row, 0, 'GRAND TOTAL')
    ws.write(row, 1, phase1_total + phase2_total + phase3_total, fmt_number)
    ws.write(row, 2, '', fmt_bold)

    # ========== DATA EXTENSIONS SHEET ==========
    ws = wb.add_worksheet("Data Extensions")
    ws.set_column('A:A', 12)  # ID (truncated)
    ws.set_column('B:B', 50)  # Name
    ws.set_column('C:C', 12)  # Category
    ws.set_column('D:D', 12)  # Confidence
    ws.set_column('E:E', 10)  # Sendable
    ws.set_column('F:F', 15)  # Row Count
    ws.set_column('G:G', 12)  # Last Modified
    ws.set_column('H:H', 40)  # Folder Path

    headers = ['ID', 'Name', 'Category', 'Confidence', 'Sendable', 'Row Count', 'Last Modified', 'Folder Path']
    for col, h in enumerate(headers):
        ws.write(0, col, h, fmt_header)

    # Add autofilter
    ws.autofilter(0, 0, len(des), len(headers) - 1)

    for row, d in enumerate(des, 1):
        ws.write(row, 0, d['id'][:12] + '...' if len(d['id']) > 12 else d['id'])
        ws.write(row, 1, d['name'])
        ws.write(row, 2, d['category'])

        conf = d['confidence']
        if conf == 'HIGH':
            ws.write(row, 3, conf, fmt_high)
        elif conf == 'MEDIUM':
            ws.write(row, 3, conf, fmt_medium)
        else:
            ws.write(row, 3, conf, fmt_low)

        ws.write(row, 4, 'Yes' if d.get('is_sendable') == 'True' else 'No')
        ws.write(row, 5, int(d.get('row_count') or 0), fmt_number)
        ws.write(row, 6, d.get('last_modified', ''))
        ws.write(row, 7, d.get('folder_path', ''))

    ws.freeze_panes(1, 0)

    # ========== AUTOMATIONS SHEET ==========
    ws = wb.add_worksheet("Automations")
    ws.set_column('A:A', 12)
    ws.set_column('B:B', 50)
    ws.set_column('C:C', 15)
    ws.set_column('D:D', 18)
    ws.set_column('E:E', 12)
    ws.set_column('F:F', 12)
    ws.set_column('G:G', 15)
    ws.set_column('H:H', 12)

    headers = ['ID', 'Name', 'Status', 'Category', 'Confidence', 'Last Run', 'Schedule Type', 'Activities']
    for col, h in enumerate(headers):
        ws.write(0, col, h, fmt_header)

    ws.autofilter(0, 0, len(autos), len(headers) - 1)

    for row, a in enumerate(autos, 1):
        ws.write(row, 0, a['id'][:12] + '...' if len(a['id']) > 12 else a['id'])
        ws.write(row, 1, a['name'])
        ws.write(row, 2, a['status'])
        ws.write(row, 3, a['category'])

        conf = a['confidence']
        if conf == 'HIGH':
            ws.write(row, 4, conf, fmt_high)
        elif conf == 'MEDIUM':
            ws.write(row, 4, conf, fmt_medium)
        else:
            ws.write(row, 4, conf, fmt_low)

        ws.write(row, 5, a.get('last_run', ''))
        ws.write(row, 6, a.get('schedule_type', ''))
        ws.write(row, 7, int(a.get('activity_count') or 0))

    ws.freeze_panes(1, 0)

    # ========== TRIGGERED SENDS SHEET ==========
    ws = wb.add_worksheet("Triggered Sends")
    ws.set_column('A:A', 12)  # ID
    ws.set_column('B:B', 50)  # Name
    ws.set_column('C:C', 15)  # Category
    ws.set_column('D:D', 12)  # Confidence
    ws.set_column('E:E', 12)  # Last Sent
    ws.set_column('F:F', 12)  # Created
    ws.set_column('G:G', 40)  # Email Name

    headers = ['ID', 'Name', 'Category', 'Confidence', 'Last Sent', 'Created', 'Email Name']
    for col, h in enumerate(headers):
        ws.write(0, col, h, fmt_header)

    ws.autofilter(0, 0, len(triggered_sends), len(headers) - 1)

    for row, t in enumerate(triggered_sends, 1):
        ws.write(row, 0, t['id'][:12] + '...' if len(t['id']) > 12 else t['id'])
        ws.write(row, 1, t['name'])
        ws.write(row, 2, t['category'])

        conf = t['confidence']
        if conf == 'HIGH':
            ws.write(row, 3, conf, fmt_high)
        elif conf == 'MEDIUM':
            ws.write(row, 3, conf, fmt_medium)
        else:
            ws.write(row, 3, conf, fmt_low)

        ws.write(row, 4, t.get('last_sent', ''))
        ws.write(row, 5, t.get('created', ''))
        ws.write(row, 6, t.get('email_name', ''))

    ws.freeze_panes(1, 0)

    # ========== EVENT DEFINITIONS SHEET ==========
    ws = wb.add_worksheet("Event Definitions")
    ws.set_column('A:A', 12)  # ID
    ws.set_column('B:B', 50)  # Name
    ws.set_column('C:C', 12)  # Confidence
    ws.set_column('D:D', 12)  # Last Modified
    ws.set_column('E:E', 40)  # Reason

    headers = ['ID', 'Name', 'Confidence', 'Last Modified', 'Reason']
    for col, h in enumerate(headers):
        ws.write(0, col, h, fmt_header)

    ws.autofilter(0, 0, len(event_defs), len(headers) - 1)

    for row, e in enumerate(event_defs, 1):
        ws.write(row, 0, e['id'][:12] + '...' if len(e['id']) > 12 else e['id'])
        ws.write(row, 1, e['name'])

        conf = e['confidence']
        if conf == 'HIGH':
            ws.write(row, 2, conf, fmt_high)
        elif conf == 'MEDIUM':
            ws.write(row, 2, conf, fmt_medium)
        else:
            ws.write(row, 2, conf, fmt_low)

        ws.write(row, 3, e.get('last_modified', ''))
        ws.write(row, 4, e.get('reason', ''))

    ws.freeze_panes(1, 0)

    # ========== OTHER ORPHANS SHEET ==========
    ws = wb.add_worksheet("Other Orphans")
    ws.set_column('A:A', 12)
    ws.set_column('B:B', 15)
    ws.set_column('C:C', 50)
    ws.set_column('D:D', 12)
    ws.set_column('E:E', 12)
    ws.set_column('F:F', 40)

    headers = ['ID', 'Type', 'Name', 'Confidence', 'Last Modified', 'Reason']
    for col, h in enumerate(headers):
        ws.write(0, col, h, fmt_header)

    ws.autofilter(0, 0, len(others), len(headers) - 1)

    for row, o in enumerate(others, 1):
        ws.write(row, 0, o['id'][:12] + '...' if len(o['id']) > 12 else o['id'])
        ws.write(row, 1, o['type'])
        ws.write(row, 2, o['name'])

        conf = o['confidence']
        if conf == 'HIGH':
            ws.write(row, 3, conf, fmt_high)
        elif conf == 'MEDIUM':
            ws.write(row, 3, conf, fmt_medium)
        else:
            ws.write(row, 3, conf, fmt_low)

        ws.write(row, 4, o.get('last_modified', ''))
        ws.write(row, 5, o.get('reason', ''))

    ws.freeze_panes(1, 0)

    # ========== JOURNEYS SHEET ==========
    ws = wb.add_worksheet("Journeys")
    ws.set_column('A:A', 12)
    ws.set_column('B:B', 50)
    ws.set_column('C:C', 12)
    ws.set_column('D:D', 12)
    ws.set_column('E:E', 12)
    ws.set_column('F:F', 12)
    ws.set_column('G:G', 10)
    ws.set_column('H:H', 10)

    headers = ['ID', 'Name', 'Status', 'BU MID', 'Created', 'Modified', 'Triggers', 'Activities']
    for col, h in enumerate(headers):
        ws.write(0, col, h, fmt_header)

    # Filter to cleanup candidates (Draft and Stopped)
    cleanup_journeys = [j for j in journeys if j.get('status') in ('Draft', 'Stopped')]
    cleanup_journeys.sort(key=lambda x: (x.get('status', ''), x.get('modifiedDate', '')))

    ws.autofilter(0, 0, len(cleanup_journeys), len(headers) - 1)

    for row, j in enumerate(cleanup_journeys, 1):
        ws.write(row, 0, j.get('id', '')[:12] + '...')
        ws.write(row, 1, j.get('name', ''))

        status = j.get('status', '')
        if status == 'Draft':
            ws.write(row, 2, status, fmt_medium)
        elif status == 'Stopped':
            ws.write(row, 2, status, fmt_low)
        else:
            ws.write(row, 2, status)

        ws.write(row, 3, j.get('_sourceBuMid', ''))
        ws.write(row, 4, (j.get('createdDate') or '')[:10])
        ws.write(row, 5, (j.get('modifiedDate') or '')[:10])
        ws.write(row, 6, j.get('triggerCount', 0))
        ws.write(row, 7, j.get('activityCount', 0))

    ws.freeze_panes(1, 0)

    # ========== INSTRUCTIONS SHEET ==========
    ws = wb.add_worksheet("Instructions")
    ws.set_column('A:A', 100)

    instructions = """
SFMC CLEANUP WORKBOOK - INSTRUCTIONS

This workbook contains analysis of orphaned and stale objects in your SFMC account.

SHEETS:
1. Summary - Overview of cleanup candidates by category and confidence level
2. Data Extensions - Orphaned DEs with cleanup confidence ratings
3. Automations - Stale automation cleanup candidates
4. Triggered Sends - Inactive triggered sends (not Deleted status)
5. Event Definitions - Orphaned event definitions not used by journeys
6. Other Orphans - Other orphaned objects (queries, emails, imports, etc.)
7. Journeys - Draft/Stopped journeys for review

CONFIDENCE LEVELS:
• HIGH (Green) - Safe to delete with minimal review
• MEDIUM (Yellow) - Review recommended before deletion
• LOW/REVIEW (Red) - Requires business owner approval

RECOMMENDED PROCESS:

Phase 1 - Quick Wins:
1. Filter "Data Extensions" sheet by Confidence = HIGH
2. Review TEST and BACKUP categories
3. Review "Triggered Sends" - NEVER_SENT and OLD_SEND are high confidence
4. Get stakeholder sign-off
5. Delete via SFMC UI or API

Phase 2 - Automations & Events:
1. Review "Automations" sheet
2. Verify no dependencies on PAUSED_STALE items
3. Review "Event Definitions" for orphaned journey entry points
4. Pause any running automations first
5. Delete confirmed unused objects

Phase 3 - Detailed Review:
1. Review MEDIUM confidence items with data governance team
2. Check if "Very Old" DEs support any active processes
3. Review Draft/Stopped journeys with Journey Builder team

NOTES:
• Row counts may be 0 for DEs that couldn't be queried
• "Sendable" column is informational only - no confidence penalty applied
• Triggered Sends with "Deleted" status are excluded (already soft-deleted)
• Some Classic Emails may be templates - verify before deletion
• Journeys cannot be deleted via API in most cases - mark as archived
• Event Definitions are journey entry points - verify no active journey uses them

BEFORE DELETING:
• Export metadata CSV for audit trail
• Verify no SQL queries reference the DE
• Check for any scheduled sends using the object
• Test in sandbox first if available
"""

    for row, line in enumerate(instructions.strip().split('\n')):
        ws.write(row, 0, line, fmt_wrap if len(line) > 80 else None)

    wb.close()
    print(f"Workbook created: {output_path}")
    return output_path


if __name__ == "__main__":
    inventory_dir = Path("inventory")
    inventories = sorted(inventory_dir.iterdir(), key=lambda p: p.name, reverse=True)
    latest = inventories[0] if inventories else None

    if not latest:
        print("No inventory found!")
        exit(1)

    output = build_workbook(
        latest,
        Path("reports/cleanup"),
        Path("reports/cleanup/SFMC_Cleanup_Analysis.xlsx")
    )
