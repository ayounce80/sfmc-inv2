#!/usr/bin/env python3
"""Diagnostic script to verify multi-BU access configuration."""

import sys
sys.path.insert(0, str(__file__).rsplit("/", 2)[0])

import httpx
from sfmc_inv2.core.config import get_config, get_config_with_account
from sfmc_inv2.clients.soap_client import SOAPClient


def test_token_auth(account_id: str, label: str) -> tuple[bool, str]:
    """Test if we can get a token for a specific account_id."""
    config = get_config_with_account(account_id)

    payload = {
        "grant_type": "client_credentials",
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "account_id": int(account_id),
    }

    try:
        with httpx.Client(timeout=30) as client:
            response = client.post(config.auth_url, json=payload)
            if response.is_success:
                data = response.json()
                return True, f"Token obtained (expires_in: {data.get('expires_in')}s)"
            else:
                return False, f"HTTP {response.status_code}: {response.text[:200]}"
    except Exception as e:
        return False, f"Exception: {e}"


def get_business_units():
    """Retrieve all accessible business units via SOAP."""
    config = get_config()
    soap = SOAPClient(config)

    properties = ["ID", "Name", "ParentID", "ParentName", "IsActive", "CustomerKey"]

    result = soap.retrieve_all_pages(
        object_type="BusinessUnit",
        properties=properties,
        max_pages=10,
        query_all_accounts=True,
    )

    return result


def main():
    config = get_config()

    print("=" * 60)
    print("SFMC Multi-BU Diagnostic")
    print("=" * 60)
    print(f"Subdomain: {config.subdomain}")
    print(f"Client ID: {config.client_id[:8]}...")
    print(f"Default Account ID: {config.account_id}")
    print()

    # Test accounts from .env
    import os
    accounts = {
        "Parent (SFMC_ACCOUNT_ID)": os.environ.get("SFMC_ACCOUNT_ID", ""),
        "DT Child (SFMC_DT_ID)": os.environ.get("SFMC_DT_ID", ""),
        "AT Child (SFMC_AT_ID)": os.environ.get("SFMC_AT_ID", ""),
    }

    print("-" * 60)
    print("Testing Token Authentication for Each BU")
    print("-" * 60)

    for label, account_id in accounts.items():
        if not account_id:
            print(f"{label}: NOT CONFIGURED")
            continue

        success, message = test_token_auth(account_id, label)
        status = "✓" if success else "✗"
        print(f"{status} {label} (MID {account_id}): {message}")

    print()
    print("-" * 60)
    print("Retrieving All Business Units via SOAP QueryAllAccounts")
    print("-" * 60)

    result = get_business_units()

    if not result.get("ok"):
        print(f"Failed: {result.get('overall_status', result.get('error', 'Unknown'))}")
        return 1

    objects = result.get("objects", [])
    print(f"Found {len(objects)} Business Unit(s):\n")

    # Sort by ParentID to show hierarchy
    def get_parent_id(bu):
        pid = bu.get("ParentID", "0")
        return "0" if pid in (None, "", "0", 0) else str(pid)

    sorted_bus = sorted(objects, key=lambda x: (get_parent_id(x), x.get("Name", "")))

    for bu in sorted_bus:
        bu_id = bu.get("ID")
        name = bu.get("Name")
        parent_id = bu.get("ParentID")
        parent_name = bu.get("ParentName")
        is_active = bu.get("IsActive")

        # Check if this is a parent BU
        is_parent = parent_id in (None, "0", 0, "")
        indent = "  " if not is_parent else ""
        parent_info = f" (parent: {parent_name})" if parent_name and not is_parent else ""

        print(f"{indent}MID {bu_id}: {name}{parent_info}")
        print(f"{indent}  Active: {is_active}, CustomerKey: {bu.get('CustomerKey')}")

    print()
    print("-" * 60)
    print("Summary")
    print("-" * 60)

    # Check if env MIDs match actual BUs
    bu_ids = {str(bu.get("ID")) for bu in objects}

    for label, account_id in accounts.items():
        if not account_id:
            continue
        in_bu_list = account_id in bu_ids
        status = "✓ Found in BU list" if in_bu_list else "✗ NOT in BU list"
        print(f"{label} (MID {account_id}): {status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
