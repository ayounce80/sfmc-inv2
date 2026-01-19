"""Persistent configuration storage for TUI settings.

Stores user preferences like last selection, custom presets,
and output directory preferences.
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

import platformdirs

logger = logging.getLogger(__name__)

APP_NAME = "sfmc-inv2"
APP_AUTHOR = "sfmc"


def get_config_dir() -> Path:
    """Get the platform-specific config directory."""
    return Path(platformdirs.user_config_dir(APP_NAME, APP_AUTHOR))


def get_config_path() -> Path:
    """Get the path to the config file."""
    return get_config_dir() / "config.json"


class ConfigStore:
    """Persistent configuration storage using JSON file."""

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the config store.

        Args:
            config_path: Path to config file. Uses default if None.
        """
        self._path = config_path or get_config_path()
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load configuration from disk."""
        try:
            if self._path.exists():
                with open(self._path, "r") as f:
                    self._data = json.load(f)
                logger.debug(f"Loaded config from {self._path}")
        except Exception as e:
            logger.warning(f"Failed to load config: {e}")
            self._data = {}

    def _save(self) -> None:
        """Save configuration to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w") as f:
                json.dump(self._data, f, indent=2)
            logger.debug(f"Saved config to {self._path}")
        except Exception as e:
            logger.warning(f"Failed to save config: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value.

        Args:
            key: Configuration key (supports dot notation for nested).
            default: Default value if not found.

        Returns:
            Configuration value or default.
        """
        keys = key.split(".")
        value = self._data

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default

            if value is None:
                return default

        return value

    def set(self, key: str, value: Any) -> None:
        """Set a configuration value.

        Args:
            key: Configuration key (supports dot notation for nested).
            value: Value to set.
        """
        keys = key.split(".")
        data = self._data

        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]

        data[keys[-1]] = value
        self._save()

    def delete(self, key: str) -> None:
        """Delete a configuration value.

        Args:
            key: Configuration key to delete.
        """
        keys = key.split(".")
        data = self._data

        for k in keys[:-1]:
            if k not in data:
                return
            data = data[k]

        if keys[-1] in data:
            del data[keys[-1]]
            self._save()

    # Convenience methods for common settings

    def get_last_selection(self) -> list[str]:
        """Get the last selected extractors."""
        return self.get("last_selection", [])

    def set_last_selection(self, extractors: list[str]) -> None:
        """Save the last selected extractors."""
        self.set("last_selection", extractors)

    def get_output_dir(self) -> str:
        """Get the preferred output directory."""
        return self.get("output_dir", "./inventory")

    def set_output_dir(self, path: str) -> None:
        """Save the preferred output directory."""
        self.set("output_dir", path)

    def get_output_format(self) -> str:
        """Get the preferred output format."""
        return self.get("output_format", "json")

    def set_output_format(self, fmt: str) -> None:
        """Save the preferred output format."""
        self.set("output_format", fmt)

    def get_custom_presets(self) -> dict[str, list[str]]:
        """Get user-defined custom presets."""
        return self.get("custom_presets", {})

    def set_custom_preset(self, name: str, extractors: list[str]) -> None:
        """Save a custom preset."""
        presets = self.get_custom_presets()
        presets[name] = extractors
        self.set("custom_presets", presets)

    def delete_custom_preset(self, name: str) -> None:
        """Delete a custom preset."""
        presets = self.get_custom_presets()
        if name in presets:
            del presets[name]
            self.set("custom_presets", presets)

    def get_include_details(self) -> bool:
        """Get whether to include object details."""
        return self.get("include_details", True)

    def set_include_details(self, value: bool) -> None:
        """Save whether to include object details."""
        self.set("include_details", value)

    def get_include_content(self) -> bool:
        """Get whether to include content (SQL, scripts)."""
        return self.get("include_content", False)

    def set_include_content(self, value: bool) -> None:
        """Save whether to include content."""
        self.set("include_content", value)


# Module-level default instance
_default_store: Optional[ConfigStore] = None


def get_config_store() -> ConfigStore:
    """Get or create the default config store."""
    global _default_store
    if _default_store is None:
        _default_store = ConfigStore()
    return _default_store
