"""Unified cache manager for SFMC folder and definition data.

Provides lazy-loaded caching for:
- Folder hierarchies (automation, DE, query, content, etc.)
- Object definitions (queries, scripts, emails)

All caches load on first access and support pre-warming.

Multi-BU Support:
- MID-keyed caching for current and parent BU
- Cross-BU lookup with parent BU fallback
- Shared resource tracking via `_fromParentBU` flag
"""

import logging
import threading
import time
from collections import defaultdict
from enum import Enum
from typing import Any, Callable, Optional

from ..clients.rest_client import RESTClient, get_rest_client
from ..clients.soap_client import SOAPClient, get_soap_client, build_simple_filter
from .breadcrumb_builder import BreadcrumbBuilder

logger = logging.getLogger(__name__)


class CacheType(str, Enum):
    """Types of cacheable data."""

    # Folders (SOAP)
    AUTOMATION_FOLDERS = "automation_folders"
    EMAIL_FOLDERS = "email_folders"
    TEMPLATE_FOLDERS = "template_folders"
    TRIGGERED_SEND_FOLDERS = "triggered_send_folders"
    LIST_FOLDERS = "list_folders"
    JOURNEY_FOLDERS = "journey_folders"

    # Folders (REST)
    DE_FOLDERS = "de_folders"
    QUERY_FOLDERS = "query_folders"
    SCRIPT_FOLDERS = "script_folders"
    IMPORT_FOLDERS = "import_folders"
    DATAEXTRACT_FOLDERS = "dataextract_folders"
    FILETRANSFER_FOLDERS = "filetransfer_folders"
    FILTER_FOLDERS = "filter_folders"

    # Content categories (REST)
    CONTENT_CATEGORIES = "content_categories"

    # Definitions
    QUERIES = "queries"
    SCRIPTS = "scripts"
    EMAILS = "emails"
    TRIGGERED_SENDS = "triggered_sends"


# Folder content type to CacheType mapping
FOLDER_CONTENT_TYPES: dict[str, CacheType] = {
    "automations": CacheType.AUTOMATION_FOLDERS,
    "dataextension": CacheType.DE_FOLDERS,
    "queryactivity": CacheType.QUERY_FOLDERS,
    "ssjsactivity": CacheType.SCRIPT_FOLDERS,
    "importactivity": CacheType.IMPORT_FOLDERS,
    "dataextractactivity": CacheType.DATAEXTRACT_FOLDERS,
    "filetransferactivity": CacheType.FILETRANSFER_FOLDERS,
    "filteractivity": CacheType.FILTER_FOLDERS,
    "email": CacheType.EMAIL_FOLDERS,
    "template": CacheType.TEMPLATE_FOLDERS,
    "triggered_send": CacheType.TRIGGERED_SEND_FOLDERS,
    "list": CacheType.LIST_FOLDERS,
    "journey": CacheType.JOURNEY_FOLDERS,
    "asset": CacheType.CONTENT_CATEGORIES,
}


class CacheManager:
    """Thread-safe lazy-loading cache manager for SFMC data.

    Caches folder hierarchies and object definitions on first access.
    Provides breadcrumb path building with memoization.

    Multi-BU Features:
    - MID-keyed caching for current and parent business units
    - Cross-BU lookup with automatic parent BU fallback
    - Shared resource tracking via `_fromParentBU` metadata
    """

    def __init__(
        self,
        rest_client: Optional[RESTClient] = None,
        soap_client: Optional[SOAPClient] = None,
        account_id: Optional[str] = None,
        parent_account_id: Optional[str] = None,
    ):
        """Initialize the cache manager.

        Args:
            rest_client: REST client instance.
            soap_client: SOAP client instance.
            account_id: Current business unit MID (Member ID).
            parent_account_id: Parent business unit MID for Enterprise 2.0 accounts.
        """
        self._rest = rest_client or get_rest_client()
        self._soap = soap_client or get_soap_client()

        # Business Unit IDs
        self._account_id = account_id
        self._parent_account_id = parent_account_id

        # Cache storage - single BU mode (backward compatible)
        self._caches: dict[CacheType, dict[str, Any]] = {}
        self._loaded: set[CacheType] = set()
        self._lock = threading.RLock()

        # MID-keyed cache storage for multi-BU mode
        # Structure: {MID: {CacheType: {id: object}}}
        self._bu_caches: dict[str, dict[CacheType, dict[str, Any]]] = {}
        self._bu_loaded: dict[str, set[CacheType]] = {}

        # Initialize BU cache storage
        if account_id:
            self._bu_caches[account_id] = {}
            self._bu_loaded[account_id] = set()
        if parent_account_id and parent_account_id != account_id:
            self._bu_caches[parent_account_id] = {}
            self._bu_loaded[parent_account_id] = set()

        # Breadcrumb builders (created on demand)
        self._breadcrumb_builders: dict[CacheType, BreadcrumbBuilder] = {}

        # Track missing folders for reporting
        self._missing_folders: dict[CacheType, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        # Load timing stats
        self._load_times: dict[CacheType, float] = {}

    def get_folders(self, cache_type: CacheType) -> dict[str, dict[str, Any]]:
        """Get folder data for a cache type, loading if needed.

        Args:
            cache_type: Type of folder cache to retrieve.

        Returns:
            Dictionary of folder ID -> folder data.
        """
        self._ensure_loaded(cache_type)
        return self._caches.get(cache_type, {})

    def get_breadcrumb(
        self,
        folder_id: Optional[str],
        cache_type: CacheType,
        separator: str = " > ",
    ) -> str:
        """Get the breadcrumb path for a folder.

        Args:
            folder_id: ID of the folder.
            cache_type: Type of folder cache.
            separator: Path segment separator.

        Returns:
            Breadcrumb path string.
        """
        if not folder_id:
            return ""

        self._ensure_loaded(cache_type)

        # Get or create breadcrumb builder
        with self._lock:
            if cache_type not in self._breadcrumb_builders:
                folders = self._caches.get(cache_type, {})
                self._breadcrumb_builders[cache_type] = BreadcrumbBuilder(
                    folders, separator
                )

            builder = self._breadcrumb_builders[cache_type]

        path = builder.build(str(folder_id))

        # Track missing folders
        for missing_id in builder.get_missing_folders():
            self._missing_folders[cache_type][missing_id] += 1

        return path

    def get_queries(self) -> dict[str, dict[str, Any]]:
        """Get query definitions cache."""
        self._ensure_loaded(CacheType.QUERIES)
        return self._caches.get(CacheType.QUERIES, {})

    def get_scripts(self) -> dict[str, dict[str, Any]]:
        """Get script definitions cache."""
        self._ensure_loaded(CacheType.SCRIPTS)
        return self._caches.get(CacheType.SCRIPTS, {})

    def get_emails(self) -> dict[str, dict[str, Any]]:
        """Get email definitions cache."""
        self._ensure_loaded(CacheType.EMAILS)
        return self._caches.get(CacheType.EMAILS, {})

    def get_content_categories(self) -> dict[str, dict[str, Any]]:
        """Get Content Builder categories cache."""
        self._ensure_loaded(CacheType.CONTENT_CATEGORIES)
        return self._caches.get(CacheType.CONTENT_CATEGORIES, {})

    def warm(self, cache_types: list[CacheType]) -> dict[CacheType, bool]:
        """Pre-load specified caches.

        Args:
            cache_types: List of cache types to warm.

        Returns:
            Dictionary of cache type -> success status.
        """
        results = {}
        for cache_type in cache_types:
            try:
                self._ensure_loaded(cache_type)
                results[cache_type] = True
            except Exception as e:
                logger.error(f"Failed to warm cache {cache_type.value}: {e}")
                results[cache_type] = False
        return results

    def clear(self, cache_type: Optional[CacheType] = None) -> None:
        """Clear cache(s).

        Args:
            cache_type: Specific cache to clear, or None for all.
        """
        with self._lock:
            if cache_type:
                self._caches.pop(cache_type, None)
                self._loaded.discard(cache_type)
                self._breadcrumb_builders.pop(cache_type, None)
            else:
                self._caches.clear()
                self._loaded.clear()
                self._breadcrumb_builders.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats.
        """
        with self._lock:
            stats = {
                "loaded_caches": list(self._loaded),
                "cache_sizes": {
                    ct.value: len(self._caches.get(ct, {}))
                    for ct in self._loaded
                },
                "load_times": {
                    ct.value: t for ct, t in self._load_times.items()
                },
                "missing_folders": {
                    ct.value: dict(folders)
                    for ct, folders in self._missing_folders.items()
                    if folders
                },
                "account_id": self._account_id,
                "parent_account_id": self._parent_account_id,
                "bu_cache_sizes": {
                    mid: {ct.value: len(caches.get(ct, {})) for ct in self._bu_loaded.get(mid, set())}
                    for mid, caches in self._bu_caches.items()
                },
            }
        return stats

    # -------------------------------------------------------------------------
    # Multi-BU Support Methods
    # -------------------------------------------------------------------------

    @property
    def account_id(self) -> Optional[str]:
        """Get the current business unit MID."""
        return self._account_id

    @property
    def parent_account_id(self) -> Optional[str]:
        """Get the parent business unit MID."""
        return self._parent_account_id

    @property
    def has_parent_bu(self) -> bool:
        """Check if a parent BU is configured."""
        return (
            self._parent_account_id is not None
            and self._parent_account_id != self._account_id
        )

    def set_account_ids(
        self,
        account_id: str,
        parent_account_id: Optional[str] = None,
    ) -> None:
        """Set or update the business unit IDs.

        Args:
            account_id: Current business unit MID.
            parent_account_id: Parent business unit MID (optional).
        """
        with self._lock:
            self._account_id = account_id
            self._parent_account_id = parent_account_id

            # Initialize BU cache storage
            if account_id and account_id not in self._bu_caches:
                self._bu_caches[account_id] = {}
                self._bu_loaded[account_id] = set()

            if parent_account_id and parent_account_id not in self._bu_caches:
                self._bu_caches[parent_account_id] = {}
                self._bu_loaded[parent_account_id] = set()

    def lookup(
        self,
        cache_type: CacheType,
        key: str,
        allow_parent: bool = True,
    ) -> Optional[dict[str, Any]]:
        """Lookup an item with optional parent BU fallback.

        This method supports cross-BU lookup for shared resources.
        When an item is not found in the current BU cache, it will
        optionally search the parent BU cache.

        Note: Parent BU fallback requires `load_shared_resources()` to be
        called first to populate the parent BU cache. Currently,
        `load_shared_resources()` is a placeholder that requires API
        context switching to the parent BU, which is not yet implemented.
        The parent BU fallback will only work if the parent cache has been
        manually populated via `store_in_bu_cache()`.

        Args:
            cache_type: Type of cache to search.
            key: Item key/ID to lookup.
            allow_parent: If True, fall back to parent BU cache.

        Returns:
            Item dict with `_fromParentBU` flag if found in parent,
            or None if not found.
        """
        self._ensure_loaded(cache_type)

        # Try current BU first
        result = self._caches.get(cache_type, {}).get(key)
        if result is not None:
            return result

        # Try parent BU if allowed and configured
        if allow_parent and self.has_parent_bu:
            parent_cache = self._bu_caches.get(self._parent_account_id, {})
            parent_items = parent_cache.get(cache_type, {})
            result = parent_items.get(key)

            if result is not None:
                # Return a copy with parent BU flag
                result_copy = dict(result)
                result_copy["_fromParentBU"] = True
                result_copy["_parentAccountId"] = self._parent_account_id
                return result_copy

        return None

    def lookup_by_name(
        self,
        cache_type: CacheType,
        name: str,
        name_field: str = "name",
        allow_parent: bool = True,
    ) -> Optional[dict[str, Any]]:
        """Lookup an item by name with optional parent BU fallback.

        Args:
            cache_type: Type of cache to search.
            name: Item name to lookup.
            name_field: Field name containing the name (default "name").
            allow_parent: If True, fall back to parent BU cache.

        Returns:
            Item dict with `_fromParentBU` flag if found in parent,
            or None if not found.
        """
        self._ensure_loaded(cache_type)

        # Search current BU
        for item in self._caches.get(cache_type, {}).values():
            if item.get(name_field) == name:
                return item

        # Search parent BU if allowed
        if allow_parent and self.has_parent_bu:
            parent_cache = self._bu_caches.get(self._parent_account_id, {})
            for item in parent_cache.get(cache_type, {}).values():
                if item.get(name_field) == name:
                    result_copy = dict(item)
                    result_copy["_fromParentBU"] = True
                    result_copy["_parentAccountId"] = self._parent_account_id
                    return result_copy

        return None

    def load_shared_resources(
        self,
        cache_type: CacheType,
        shared_folder_names: Optional[list[str]] = None,
    ) -> int:
        """Load shared resources from parent BU into separate cache.

        PLACEHOLDER: This method is scaffolding for future implementation.
        Currently returns 0 without loading any data because it requires
        API context switching to the parent BU, which is not yet implemented.

        To implement this properly, the method would need to:
        1. Switch API context to parent BU (requires separate auth or BU switching)
        2. Fetch resources from parent BU using the appropriate loader
        3. Store results in `_bu_caches[parent_account_id]`

        For now, parent BU caches can be manually populated using
        `store_in_bu_cache()` if the data is obtained through other means.

        Args:
            cache_type: Type of resources to load.
            shared_folder_names: Optional list of folder names to filter by.
                If None, loads all resources from parent BU.

        Returns:
            Number of resources loaded (currently always 0).
        """
        if not self.has_parent_bu:
            logger.debug("No parent BU configured, skipping shared resource load")
            return 0

        parent_mid = self._parent_account_id

        with self._lock:
            if cache_type in self._bu_loaded.get(parent_mid, set()):
                # Already loaded
                return len(self._bu_caches.get(parent_mid, {}).get(cache_type, {}))

        # TODO: This would require switching the API context to the parent BU
        # For now, we rely on the current BU's cache and mark shared resources
        # when they are identified by naming convention (e.g., ENT. prefix)
        logger.debug(
            f"Shared resource loading for {cache_type.value} from parent BU "
            f"{parent_mid} is not yet implemented"
        )
        return 0

    def get_bu_cache(
        self,
        account_id: str,
        cache_type: CacheType,
    ) -> dict[str, Any]:
        """Get cache for a specific business unit.

        Args:
            account_id: Business unit MID.
            cache_type: Type of cache to retrieve.

        Returns:
            Dictionary of cached items for that BU and type.
        """
        with self._lock:
            return self._bu_caches.get(account_id, {}).get(cache_type, {})

    def store_in_bu_cache(
        self,
        account_id: str,
        cache_type: CacheType,
        items: dict[str, Any],
    ) -> None:
        """Store items in a specific BU's cache.

        Args:
            account_id: Business unit MID.
            cache_type: Type of cache.
            items: Dictionary of items to store (id -> item).
        """
        with self._lock:
            if account_id not in self._bu_caches:
                self._bu_caches[account_id] = {}
                self._bu_loaded[account_id] = set()

            self._bu_caches[account_id][cache_type] = items
            self._bu_loaded[account_id].add(cache_type)

    def is_shared_resource(self, item: dict[str, Any]) -> bool:
        """Check if an item is a shared resource from parent BU.

        Detects shared resources by:
        1. `_fromParentBU` flag (set by lookup methods)
        2. ENT. prefix in name (SFMC convention for shared DEs)
        3. Enterprise-level folder path

        Args:
            item: Item dictionary to check.

        Returns:
            True if the item appears to be a shared resource.
        """
        # Check explicit flag
        if item.get("_fromParentBU"):
            return True

        # Check for ENT. prefix (common SFMC convention)
        name = item.get("name", "")
        if name.startswith("ENT.") or name.startswith("_ENT."):
            return True

        # Check folder path for shared/enterprise indicators
        folder_path = item.get("folderPath", "")
        if folder_path:
            path_lower = folder_path.lower()
            if "shared" in path_lower or "enterprise" in path_lower:
                return True

        return False

    def _ensure_loaded(self, cache_type: CacheType) -> None:
        """Ensure a cache is loaded, loading if needed."""
        if cache_type in self._loaded:
            return

        with self._lock:
            # Double-check after lock
            if cache_type in self._loaded:
                return

            start_time = time.time()
            self._load_cache(cache_type)
            self._load_times[cache_type] = time.time() - start_time
            self._loaded.add(cache_type)

            logger.debug(
                f"Loaded {cache_type.value}: {len(self._caches.get(cache_type, {}))} items "
                f"in {self._load_times[cache_type]:.2f}s"
            )

    def _load_cache(self, cache_type: CacheType) -> None:
        """Load a specific cache type."""
        loaders: dict[CacheType, Callable[[], dict[str, Any]]] = {
            # SOAP folder caches
            CacheType.AUTOMATION_FOLDERS: lambda: self._load_soap_folders("automations"),
            CacheType.EMAIL_FOLDERS: lambda: self._load_soap_folders("email"),
            CacheType.TEMPLATE_FOLDERS: lambda: self._load_soap_folders("template"),
            CacheType.TRIGGERED_SEND_FOLDERS: lambda: self._load_soap_folders(
                "triggered_send_definition"
            ),
            CacheType.LIST_FOLDERS: lambda: self._load_soap_folders("list"),
            CacheType.JOURNEY_FOLDERS: lambda: self._load_soap_folders("journey"),
            # REST folder caches
            CacheType.DE_FOLDERS: lambda: self._load_rest_folders("dataextension"),
            CacheType.QUERY_FOLDERS: lambda: self._load_rest_folders("queryactivity"),
            CacheType.SCRIPT_FOLDERS: lambda: self._load_rest_folders("ssjsactivity"),
            CacheType.IMPORT_FOLDERS: lambda: self._load_rest_folders("importactivity"),
            CacheType.DATAEXTRACT_FOLDERS: lambda: self._load_rest_folders(
                "dataextractactivity"
            ),
            CacheType.FILETRANSFER_FOLDERS: lambda: self._load_rest_folders(
                "filetransferactivity"
            ),
            CacheType.FILTER_FOLDERS: lambda: self._load_rest_folders("filteractivity"),
            # Content categories
            CacheType.CONTENT_CATEGORIES: self._load_content_categories,
            # Definitions
            CacheType.QUERIES: self._load_queries,
            CacheType.SCRIPTS: self._load_scripts,
            CacheType.EMAILS: self._load_emails,
            CacheType.TRIGGERED_SENDS: self._load_triggered_sends,
        }

        loader = loaders.get(cache_type)
        if loader:
            self._caches[cache_type] = loader()
        else:
            logger.warning(f"No loader for cache type: {cache_type.value}")
            self._caches[cache_type] = {}

    def _load_soap_folders(self, content_type: str) -> dict[str, dict[str, Any]]:
        """Load folders via SOAP API.

        Args:
            content_type: SFMC content type for folder filtering.

        Returns:
            Dictionary of folder ID -> folder data.
        """
        filter_xml = build_simple_filter("ContentType", "equals", content_type)

        result = self._soap.retrieve_all_pages(
            object_type="DataFolder",
            properties=[
                "ID",
                "Name",
                "ParentFolder.ID",
                "ParentFolder.Name",
                "ContentType",
                "Description",
                "IsActive",
                "IsEditable",
                "AllowChildren",
            ],
            filter_xml=filter_xml,
        )

        folders = {}
        if result.get("ok"):
            for obj in result.get("objects", []):
                folder_id = str(obj.get("ID", ""))
                if folder_id:
                    parent_folder = obj.get("ParentFolder", {})
                    parent_id = parent_folder.get("ID") if isinstance(parent_folder, dict) else None

                    folders[folder_id] = {
                        "id": folder_id,
                        "name": obj.get("Name", ""),
                        "parentId": str(parent_id) if parent_id else None,
                        "parentName": parent_folder.get("Name") if isinstance(parent_folder, dict) else None,
                        "contentType": obj.get("ContentType", ""),
                        "description": obj.get("Description", ""),
                        "isActive": obj.get("IsActive", "true") == "true",
                        "isEditable": obj.get("IsEditable", "true") == "true",
                        "allowChildren": obj.get("AllowChildren", "true") == "true",
                    }

        return folders

    def _load_rest_folders(self, content_type: str) -> dict[str, dict[str, Any]]:
        """Load folders via REST API.

        Args:
            content_type: SFMC content type for folder filtering.

        Returns:
            Dictionary of folder ID -> folder data.
        """
        folders = {}
        page = 1
        page_size = 500

        while True:
            result = self._rest.get(
                f"/email/v1/category?$filter=categoryType eq '{content_type}'&$pageSize={page_size}&$page={page}"
            )

            if not result.get("ok"):
                logger.warning(f"Failed to load {content_type} folders: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", data.get("categories", []))

            if not items:
                break

            for item in items:
                folder_id = str(item.get("id", item.get("categoryId", "")))
                if folder_id:
                    folders[folder_id] = {
                        "id": folder_id,
                        "name": item.get("name", item.get("categoryName", "")),
                        "parentId": str(item.get("parentId", "")) or None,
                        "contentType": content_type,
                        "description": item.get("description", ""),
                    }

            # Check for more pages
            if len(items) < page_size:
                break
            page += 1

        return folders

    def _load_content_categories(self) -> dict[str, dict[str, Any]]:
        """Load Content Builder asset categories."""
        categories = {}
        page = 1
        page_size = 500

        while True:
            result = self._rest.get(
                f"/asset/v1/content/categories?$page={page}&$pageSize={page_size}"
            )

            if not result.get("ok"):
                logger.warning(f"Failed to load content categories: {result.get('error')}")
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            for item in items:
                cat_id = str(item.get("id", ""))
                if cat_id:
                    categories[cat_id] = {
                        "id": cat_id,
                        "name": item.get("name", ""),
                        "parentId": str(item.get("parentId", "")) or None,
                        "description": item.get("description", ""),
                        "categoryType": item.get("categoryType", ""),
                    }

            if len(items) < page_size:
                break
            page += 1

        return categories

    def _load_queries(self) -> dict[str, dict[str, Any]]:
        """Load query activity definitions."""
        queries = {}
        page = 1
        page_size = 500

        while True:
            result = self._rest.get(
                f"/automation/v1/queries?$page={page}&$pageSize={page_size}"
            )

            if not result.get("ok"):
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            for item in items:
                query_id = str(item.get("queryDefinitionId", ""))
                if query_id:
                    queries[query_id] = item

            if len(items) < page_size:
                break
            page += 1

        return queries

    def _load_scripts(self) -> dict[str, dict[str, Any]]:
        """Load SSJS script activity definitions."""
        scripts = {}
        page = 1
        page_size = 500

        while True:
            result = self._rest.get(
                f"/automation/v1/scripts?$page={page}&$pageSize={page_size}"
            )

            if not result.get("ok"):
                break

            data = result.get("data", {})
            items = data.get("items", [])

            if not items:
                break

            for item in items:
                script_id = str(item.get("ssjsActivityId", ""))
                if script_id:
                    scripts[script_id] = item

            if len(items) < page_size:
                break
            page += 1

        return scripts

    def _load_emails(self) -> dict[str, dict[str, Any]]:
        """Load email definitions via SOAP."""
        result = self._soap.retrieve_all_pages(
            object_type="Email",
            properties=[
                "ID",
                "Name",
                "CustomerKey",
                "Subject",
                "CategoryID",
                "CreatedDate",
                "ModifiedDate",
                "Status",
            ],
        )

        emails = {}
        if result.get("ok"):
            for obj in result.get("objects", []):
                email_id = str(obj.get("ID", ""))
                if email_id:
                    emails[email_id] = {
                        "id": email_id,
                        "name": obj.get("Name", ""),
                        "customerKey": obj.get("CustomerKey", ""),
                        "subject": obj.get("Subject", ""),
                        "categoryId": obj.get("CategoryID"),
                        "createdDate": obj.get("CreatedDate"),
                        "modifiedDate": obj.get("ModifiedDate"),
                        "status": obj.get("Status"),
                    }

        return emails

    def _load_triggered_sends(self) -> dict[str, dict[str, Any]]:
        """Load triggered send definitions via SOAP."""
        result = self._soap.retrieve_all_pages(
            object_type="TriggeredSendDefinition",
            properties=[
                "ObjectID",
                "Name",
                "CustomerKey",
                "Description",
                "CategoryID",
                "TriggeredSendStatus",
                "Email.ID",
                "Email.Name",
                "CreatedDate",
                "ModifiedDate",
            ],
        )

        triggered_sends = {}
        if result.get("ok"):
            for obj in result.get("objects", []):
                ts_id = str(obj.get("ObjectID", ""))
                if ts_id:
                    email = obj.get("Email", {})
                    triggered_sends[ts_id] = {
                        "id": ts_id,
                        "name": obj.get("Name", ""),
                        "customerKey": obj.get("CustomerKey", ""),
                        "description": obj.get("Description", ""),
                        "categoryId": obj.get("CategoryID"),
                        "status": obj.get("TriggeredSendStatus"),
                        "emailId": email.get("ID") if isinstance(email, dict) else None,
                        "emailName": email.get("Name") if isinstance(email, dict) else None,
                        "createdDate": obj.get("CreatedDate"),
                        "modifiedDate": obj.get("ModifiedDate"),
                    }

        return triggered_sends


# Module-level singleton
_default_manager: Optional[CacheManager] = None
_manager_lock = threading.Lock()


def get_cache_manager(
    rest_client: Optional[RESTClient] = None,
    soap_client: Optional[SOAPClient] = None,
    account_id: Optional[str] = None,
    parent_account_id: Optional[str] = None,
) -> CacheManager:
    """Get or create the default cache manager.

    Args:
        rest_client: REST client instance.
        soap_client: SOAP client instance.
        account_id: Current business unit MID.
        parent_account_id: Parent business unit MID (for Enterprise 2.0).

    Returns:
        CacheManager singleton instance.
    """
    global _default_manager
    if _default_manager is None:
        with _manager_lock:
            if _default_manager is None:
                _default_manager = CacheManager(
                    rest_client, soap_client, account_id, parent_account_id
                )
    elif account_id:
        # Update account IDs if provided
        _default_manager.set_account_ids(account_id, parent_account_id)
    return _default_manager


def reset_cache_manager() -> None:
    """Reset the default cache manager."""
    global _default_manager
    with _manager_lock:
        _default_manager = None
