#!/usr/bin/env python3
"""Test retrieving journeys from child BU."""

import sys
sys.path.insert(0, str(__file__).rsplit("/", 2)[0])

import os
import httpx
from sfmc_inv2.core.config import get_config_with_account


def get_journeys(account_id: str, bu_name: str):
    """Retrieve journeys from a specific BU."""
    config = get_config_with_account(account_id)

    # Get token for this BU
    payload = {
        "grant_type": "client_credentials",
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "account_id": int(account_id),
    }

    with httpx.Client(timeout=30) as client:
        token_resp = client.post(config.auth_url, json=payload)
        if not token_resp.is_success:
            print(f"Failed to get token for {bu_name}: {token_resp.text}")
            return

        token = token_resp.json()["access_token"]

        # Get journeys
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Journey listing endpoint
        journey_url = f"{config.rest_url}/interaction/v1/interactions"
        resp = client.get(journey_url, headers=headers, params={"$pageSize": 50})

        if not resp.is_success:
            print(f"Failed to get journeys from {bu_name}: {resp.status_code} {resp.text[:200]}")
            return

        data = resp.json()
        items = data.get("items", [])
        count = data.get("count", len(items))

        print(f"\n{bu_name} (MID {account_id}): {count} journey(s)")
        print("-" * 50)

        for j in items[:10]:  # Show first 10
            name = j.get("name", "Unnamed")
            status = j.get("status", "Unknown")
            jid = j.get("id", "")[:8]
            print(f"  [{status}] {name} ({jid}...)")

        if count > 10:
            print(f"  ... and {count - 10} more")


def main():
    print("=" * 60)
    print("Testing Journey Retrieval from Multiple BUs")
    print("=" * 60)

    bus_to_test = [
        (os.environ.get("SFMC_ACCOUNT_ID", ""), "Parent (Admin)"),
        (os.environ.get("SFMC_AT_ID", ""), "America's Tire"),
    ]

    for account_id, name in bus_to_test:
        if account_id:
            get_journeys(account_id, name)


if __name__ == "__main__":
    main()
