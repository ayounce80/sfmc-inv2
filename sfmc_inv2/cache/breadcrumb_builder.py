"""Breadcrumb path builder for SFMC folder hierarchies.

Builds folder paths like "Marketing > Campaigns > 2025 Q1" from folder data.
Uses memoization to cache results for performance.
"""

from functools import lru_cache
from typing import Any, Optional


class BreadcrumbBuilder:
    """Builds breadcrumb paths from folder hierarchy data.

    Uses memoization to avoid redundant path calculations.
    """

    def __init__(
        self,
        folders: dict[str, dict[str, Any]],
        separator: str = " > ",
        name_key: str = "name",
        parent_key: str = "parentId",
    ):
        """Initialize the breadcrumb builder.

        Args:
            folders: Dictionary of folder ID -> folder data.
            separator: String to join path segments.
            name_key: Key for folder name in folder dict.
            parent_key: Key for parent folder ID in folder dict.
        """
        self._folders = folders
        self._separator = separator
        self._name_key = name_key
        self._parent_key = parent_key
        self._cache: dict[str, str] = {}
        self._missing: set[str] = set()

    def build(self, folder_id: Optional[str]) -> str:
        """Build the breadcrumb path for a folder.

        Args:
            folder_id: ID of the folder to build path for.

        Returns:
            Breadcrumb path string, or empty string if folder not found.
        """
        if not folder_id:
            return ""

        folder_id = str(folder_id)

        # Check cache
        if folder_id in self._cache:
            return self._cache[folder_id]

        # Build path recursively
        path = self._build_recursive(folder_id)
        self._cache[folder_id] = path
        return path

    def _build_recursive(self, folder_id: str) -> str:
        """Recursively build path by traversing parent references."""
        # Base cases
        if not folder_id or folder_id == "0":
            return ""

        # Already cached?
        if folder_id in self._cache:
            return self._cache[folder_id]

        # Get folder data
        folder = self._folders.get(folder_id)
        if not folder:
            self._missing.add(folder_id)
            return ""

        folder_name = folder.get(self._name_key, "")
        parent_id = folder.get(self._parent_key)

        # Convert parent_id to string if needed
        if parent_id is not None:
            parent_id = str(parent_id)

        # Build parent path first
        if parent_id and parent_id != "0":
            parent_path = self._build_recursive(parent_id)
            if parent_path:
                path = f"{parent_path}{self._separator}{folder_name}"
            else:
                path = folder_name
        else:
            path = folder_name

        # Cache and return
        self._cache[folder_id] = path
        return path

    def get_missing_folders(self) -> set[str]:
        """Get IDs of folders referenced but not in data."""
        return self._missing.copy()

    def clear_cache(self) -> None:
        """Clear the path cache."""
        self._cache.clear()

    def update_folders(self, folders: dict[str, dict[str, Any]]) -> None:
        """Update folder data and clear cache.

        Args:
            folders: New folder data dictionary.
        """
        self._folders = folders
        self._cache.clear()
        self._missing.clear()


def build_breadcrumb(
    folder_id: Optional[str],
    folders: dict[str, dict[str, Any]],
    separator: str = " > ",
    name_key: str = "name",
    parent_key: str = "parentId",
) -> str:
    """Convenience function to build a single breadcrumb path.

    For multiple lookups, use BreadcrumbBuilder directly for memoization.

    Args:
        folder_id: ID of the folder.
        folders: Dictionary of folder ID -> folder data.
        separator: String to join path segments.
        name_key: Key for folder name in folder dict.
        parent_key: Key for parent folder ID in folder dict.

    Returns:
        Breadcrumb path string.
    """
    builder = BreadcrumbBuilder(folders, separator, name_key, parent_key)
    return builder.build(folder_id)
