"""Configuration management for SFMC Inventory Tool.

Loads configuration from environment variables with .env file support.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load .env file from project root
_env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(_env_path)


@dataclass
class SFMCConfig:
    """SFMC API configuration."""

    subdomain: str
    client_id: str
    client_secret: str
    account_id: Optional[str] = None
    soap_debug: bool = False
    rest_debug: bool = False
    soap_max_pages: int = 100  # Maximum pages for SOAP pagination
    child_account_ids: list[str] = None  # Child BU MIDs for multi-BU extraction

    def __post_init__(self):
        """Initialize mutable defaults."""
        if self.child_account_ids is None:
            self.child_account_ids = []

    @property
    def all_account_ids(self) -> list[str]:
        """Get all configured account IDs (parent + children)."""
        accounts = []
        if self.account_id:
            accounts.append(self.account_id)
        accounts.extend(self.child_account_ids)
        return accounts

    @property
    def auth_url(self) -> str:
        """OAuth2 token endpoint URL."""
        return f"https://{self.subdomain}.auth.marketingcloudapis.com/v2/token"

    @property
    def rest_url(self) -> str:
        """REST API base URL."""
        return f"https://{self.subdomain}.rest.marketingcloudapis.com"

    @property
    def soap_url(self) -> str:
        """SOAP API endpoint URL."""
        return f"https://{self.subdomain}.soap.marketingcloudapis.com/Service.asmx"

    def validate(self) -> list[str]:
        """Validate required configuration fields.

        Returns:
            List of validation error messages, empty if valid.
        """
        errors = []
        if not self.subdomain:
            errors.append("SFMC_SUBDOMAIN is required")
        if not self.client_id:
            errors.append("SFMC_CLIENT_ID is required")
        if not self.client_secret:
            errors.append("SFMC_CLIENT_SECRET is required")
        return errors


def get_config() -> SFMCConfig:
    """Load configuration from environment variables.

    Returns:
        SFMCConfig instance populated from environment.
    """
    # Collect child BU MIDs from SFMC_CHILD_BU_* or legacy SFMC_*_ID vars
    child_ids = []

    # Check for comma-separated list in SFMC_CHILD_BUS
    child_bus_env = os.environ.get("SFMC_CHILD_BUS", "")
    if child_bus_env:
        child_ids.extend([mid.strip() for mid in child_bus_env.split(",") if mid.strip()])

    # Also check legacy individual vars (SFMC_DT_ID, SFMC_AT_ID, etc.)
    for key, value in os.environ.items():
        if key.startswith("SFMC_") and key.endswith("_ID") and key not in (
            "SFMC_ACCOUNT_ID", "SFMC_CLIENT_ID"
        ):
            if value and value not in child_ids:
                child_ids.append(value)

    return SFMCConfig(
        subdomain=os.environ.get("SFMC_SUBDOMAIN", ""),
        client_id=os.environ.get("SFMC_CLIENT_ID", ""),
        client_secret=os.environ.get("SFMC_CLIENT_SECRET", ""),
        account_id=os.environ.get("SFMC_ACCOUNT_ID"),
        soap_debug=os.environ.get("SFMC_SOAP_DEBUG", "").lower() == "true",
        rest_debug=os.environ.get("SFMC_REST_DEBUG", "").lower() == "true",
        soap_max_pages=int(os.environ.get("SFMC_SOAP_MAX_PAGES", "100")),
        child_account_ids=child_ids,
    )


def get_config_with_account(account_id: str) -> SFMCConfig:
    """Load config from environment but override account_id.

    Args:
        account_id: The SFMC Account/MID to use.

    Returns:
        SFMCConfig instance with overridden account_id.
    """
    base = get_config()
    return SFMCConfig(
        subdomain=base.subdomain,
        client_id=base.client_id,
        client_secret=base.client_secret,
        account_id=account_id,
        soap_debug=base.soap_debug,
        rest_debug=base.rest_debug,
        soap_max_pages=base.soap_max_pages,
    )
