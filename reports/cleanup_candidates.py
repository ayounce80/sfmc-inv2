#!/usr/bin/env python3
"""Generate cleanup candidate reports from SFMC inventory."""

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path


def load_ndjson(path: Path) -> list[dict]:
    """Load NDJSON file."""
    items = []
    with open(path) as f:
        for line in f:
            if line.strip():
                items.append(json.loads(line))
    return items


def analyze_inventory(inventory_path: Path, output_dir: Path):
    """Analyze inventory and generate cleanup reports."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    with open(inventory_path / "relationships/orphans.json") as f:
        orphans = json.load(f)

    des = {d["id"]: d for d in load_ndjson(inventory_path / "objects/data_extensions.ndjson")}
    automations = load_ndjson(inventory_path / "objects/automations.ndjson")
    queries = {q["id"]: q for q in load_ndjson(inventory_path / "objects/queries.ndjson")}
    imports = {i["id"]: i for i in load_ndjson(inventory_path / "objects/imports.ndjson")}
    triggered_sends = load_ndjson(inventory_path / "objects/triggered_sends.ndjson")

    # Dynamic cutoffs based on current date
    today = datetime.now()
    cutoff_90_days = today - timedelta(days=90)   # Stale automation threshold
    cutoff_1_year = today - timedelta(days=365)   # Old objects threshold
    cutoff_3_years = today - timedelta(days=1095)  # Very old objects threshold

    # === REPORT 1: Orphaned Data Extensions ===
    de_report = []
    for o in orphans:
        if o["object_type"] != "data_extension":
            continue

        de_id = o["id"]
        de = des.get(de_id, {})
        name = o.get("name", "Unknown")
        name_lower = name.lower()

        mod_str = o.get("last_modified", "")
        try:
            mod_date = datetime.fromisoformat(mod_str.replace("Z", "+00:00")).replace(tzinfo=None)
        except:
            mod_date = None

        is_sendable = de.get("isSendable", False)
        row_count = de.get("rowCount", 0)

        # Safety checks for DEs that are likely active even without detected relationships
        # (e.g., populated via API, Journey SMS, AMPscript, external integrations)
        is_recently_active = (
            mod_date and mod_date > cutoff_90_days and row_count and row_count > 1000
        )
        is_high_volume = row_count and row_count > 100000  # >100K rows = review required

        # Categorize
        if is_recently_active or is_high_volume:
            # Override: Active or high-volume DEs should never be HIGH confidence deletes
            category = "ACTIVE_ORPHAN" if is_recently_active else "HIGH_VOLUME"
            confidence = "REVIEW"
        elif "test" in name_lower or "_test" in name_lower or "testing" in name_lower:
            category = "TEST"
            confidence = "HIGH"
        elif "backup" in name_lower or "_bak" in name_lower or "copy of" in name_lower or "archive" in name_lower or "_old" in name_lower:
            category = "BACKUP"
            confidence = "HIGH"
        elif mod_date and mod_date < cutoff_3_years:
            category = "VERY_OLD"
            confidence = "MEDIUM"
        elif mod_date and mod_date < cutoff_1_year:
            category = "OLD"
            confidence = "LOW"
        else:
            category = "OTHER"
            confidence = "REVIEW"

        de_report.append({
            "id": de_id,
            "name": name,
            "category": category,
            "confidence": confidence,
            "is_sendable": is_sendable,
            "row_count": row_count,
            "last_modified": mod_str[:10] if mod_str else "",
            "folder_path": o.get("folder_path", ""),
        })

    # Sort by confidence then category
    conf_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "REVIEW": 3}
    de_report.sort(key=lambda x: (conf_order.get(x["confidence"], 99), x["category"], x["name"]))

    with open(output_dir / "orphaned_data_extensions.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "category", "confidence", "is_sendable", "row_count", "last_modified", "folder_path"])
        writer.writeheader()
        writer.writerows(de_report)

    print(f"Orphaned DEs: {len(de_report)}")
    print(f"  HIGH confidence: {sum(1 for r in de_report if r['confidence'] == 'HIGH')}")
    print(f"  MEDIUM confidence: {sum(1 for r in de_report if r['confidence'] == 'MEDIUM')}")
    print(f"  LOW confidence: {sum(1 for r in de_report if r['confidence'] == 'LOW')}")

    # === REPORT 2: Stale Automations ===
    auto_report = []
    for a in automations:
        last_run_str = a.get("lastRunTime", "")
        status = a.get("status", "Unknown")

        try:
            last_run = datetime.fromisoformat(last_run_str.replace("Z", "+00:00")).replace(tzinfo=None) if last_run_str else None
        except:
            last_run = None

        # Categorize using 90-day staleness threshold per user request
        if status == "PausedSchedule" and (not last_run or last_run < cutoff_90_days):
            category = "PAUSED_STALE"
            confidence = "HIGH"
        elif status == "PausedSchedule":
            category = "PAUSED_RECENT"
            confidence = "MEDIUM"
        elif status == "Ready" and last_run and last_run < cutoff_1_year:
            category = "READY_VERY_OLD"
            confidence = "HIGH"
        elif status == "Ready" and last_run and last_run < cutoff_90_days:
            category = "READY_STALE"
            confidence = "MEDIUM"
        elif status in ("Building", "Inactive"):
            category = "INCOMPLETE"
            confidence = "MEDIUM"
        else:
            category = "ACTIVE"
            confidence = "KEEP"

        auto_report.append({
            "id": a.get("id", ""),
            "name": a.get("name", "Unknown"),
            "status": status,
            "category": category,
            "confidence": confidence,
            "last_run": last_run_str[:10] if last_run_str else "Never",
            "schedule_type": a.get("scheduleType", ""),
            "activity_count": len(a.get("steps", [])),
        })

    # Filter to cleanup candidates only
    cleanup_autos = [r for r in auto_report if r["confidence"] != "KEEP"]
    cleanup_autos.sort(key=lambda x: (conf_order.get(x["confidence"], 99), x["category"], x["name"]))

    with open(output_dir / "stale_automations.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "status", "category", "confidence", "last_run", "schedule_type", "activity_count"])
        writer.writeheader()
        writer.writerows(cleanup_autos)

    print(f"\nStale Automations: {len(cleanup_autos)}")
    print(f"  HIGH confidence: {sum(1 for r in cleanup_autos if r['confidence'] == 'HIGH')}")
    print(f"  MEDIUM confidence: {sum(1 for r in cleanup_autos if r['confidence'] == 'MEDIUM')}")

    # === REPORT 3: Other Orphans (Queries, Imports, etc.) ===
    other_report = []
    for o in orphans:
        if o["object_type"] == "data_extension":
            continue

        mod_str = o.get("last_modified", "")
        try:
            mod_date = datetime.fromisoformat(mod_str.replace("Z", "+00:00")).replace(tzinfo=None) if mod_str else None
        except:
            mod_date = None

        name = o.get("name", "Unknown")
        name_lower = name.lower()

        if "test" in name_lower:
            confidence = "HIGH"
        elif mod_date and mod_date < cutoff_3_years:
            confidence = "HIGH"
        elif mod_date and mod_date < cutoff_1_year:
            confidence = "MEDIUM"
        else:
            confidence = "REVIEW"

        other_report.append({
            "id": o["id"],
            "type": o["object_type"],
            "name": name,
            "confidence": confidence,
            "last_modified": mod_str[:10] if mod_str else "",
            "reason": o.get("reason", ""),
        })

    other_report.sort(key=lambda x: (x["type"], conf_order.get(x["confidence"], 99), x["name"]))

    with open(output_dir / "other_orphans.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "type", "name", "confidence", "last_modified", "reason"])
        writer.writeheader()
        writer.writerows(other_report)

    print(f"\nOther Orphans: {len(other_report)}")
    by_type = {}
    for r in other_report:
        by_type[r["type"]] = by_type.get(r["type"], 0) + 1
    for t, c in sorted(by_type.items()):
        print(f"  {t}: {c}")

    # === SUMMARY ===
    print("\n" + "=" * 60)
    print("CLEANUP SUMMARY")
    print("=" * 60)

    high_conf_des = sum(1 for r in de_report if r["confidence"] == "HIGH")
    high_conf_autos = sum(1 for r in cleanup_autos if r["confidence"] == "HIGH")
    high_conf_other = sum(1 for r in other_report if r["confidence"] == "HIGH")

    print(f"\nHIGH CONFIDENCE cleanup candidates:")
    print(f"  Data Extensions: {high_conf_des}")
    print(f"  Automations: {high_conf_autos}")
    print(f"  Other objects: {high_conf_other}")
    print(f"  TOTAL: {high_conf_des + high_conf_autos + high_conf_other}")

    med_conf_des = sum(1 for r in de_report if r["confidence"] == "MEDIUM")
    med_conf_autos = sum(1 for r in cleanup_autos if r["confidence"] == "MEDIUM")
    med_conf_other = sum(1 for r in other_report if r["confidence"] == "MEDIUM")

    print(f"\nMEDIUM CONFIDENCE candidates (review recommended):")
    print(f"  Data Extensions: {med_conf_des}")
    print(f"  Automations: {med_conf_autos}")
    print(f"  Other objects: {med_conf_other}")
    print(f"  TOTAL: {med_conf_des + med_conf_autos + med_conf_other}")

    print(f"\nReports written to: {output_dir}")


if __name__ == "__main__":
    # Find latest inventory
    inventory_dir = Path("inventory")
    inventories = sorted(inventory_dir.iterdir(), key=lambda p: p.name, reverse=True)
    latest = inventories[0] if inventories else None

    if not latest:
        print("No inventory found!")
        exit(1)

    print(f"Analyzing inventory: {latest.name}\n")
    analyze_inventory(latest, Path("reports/cleanup"))
