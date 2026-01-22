"""Tests for the type registry module."""

import pytest

from sfmc_inv2.types.type_registry import (
    TYPE_REGISTRY,
    TypeDefinition,
    get_type_definition,
    get_type_by_extractor,
    get_all_types,
    get_shared_types,
    get_dependencies,
    get_dependency_paths,
    get_extractor_to_type_map,
    get_type_to_extractor_map,
)


class TestTypeRegistry:
    """Test type registry functionality."""

    def test_registry_has_all_types(self):
        """Registry should contain all expected types."""
        expected_types = {
            "folder",
            "data_extension",
            "query",
            "script",
            "import",
            "data_extract",
            "file_transfer",
            "filter",
            "event_definition",
            "automation",
            "journey",
            "classic_email",
            "triggered_send",
            "list",
            "sender_profile",
            "delivery_profile",
            "send_classification",
            "asset",
            "template",
            "account",
        }
        assert set(TYPE_REGISTRY.keys()) == expected_types

    def test_get_type_definition(self):
        """Should return correct type definition."""
        type_def = get_type_definition("automation")
        assert type_def is not None
        assert type_def.name == "automation"
        assert type_def.extractor_name == "automations"
        assert "query" in type_def.dependencies

    def test_get_type_definition_not_found(self):
        """Should return None for unknown type."""
        assert get_type_definition("unknown_type") is None

    def test_get_type_by_extractor(self):
        """Should find type by extractor name."""
        type_def = get_type_by_extractor("automations")
        assert type_def is not None
        assert type_def.name == "automation"

    def test_get_all_types(self):
        """Should return all type names."""
        all_types = get_all_types()
        assert len(all_types) == 20
        assert "automation" in all_types
        assert "journey" in all_types

    def test_get_shared_types(self):
        """Should return types that can be shared from parent BU."""
        shared = get_shared_types()
        assert "data_extension" in shared
        assert "folder" in shared
        assert "automation" not in shared
        assert "journey" not in shared

    def test_get_dependencies(self):
        """Should return dependencies for a type."""
        deps = get_dependencies("automation")
        assert "query" in deps
        assert "script" in deps
        assert "folder" in deps

    def test_get_dependencies_empty(self):
        """Types with no dependencies should return empty list."""
        deps = get_dependencies("sender_profile")
        assert deps == []

    def test_get_dependency_paths(self):
        """Should return paths for dependency type."""
        paths = get_dependency_paths("automation", "query")
        assert len(paths) > 0
        assert "steps[].activities[].objectTypeId=300" in paths

    def test_extractor_type_maps(self):
        """Maps should be inverse of each other."""
        ext_to_type = get_extractor_to_type_map()
        type_to_ext = get_type_to_extractor_map()

        assert ext_to_type["automations"] == "automation"
        assert type_to_ext["automation"] == "automations"

        # Verify bidirectional
        for ext_name, type_name in ext_to_type.items():
            assert type_to_ext[type_name] == ext_name


class TestTypeDefinition:
    """Test TypeDefinition dataclass."""

    def test_automation_type_definition(self):
        """Automation type should have correct metadata."""
        type_def = TYPE_REGISTRY["automation"]

        assert type_def.id_field == "id"
        assert type_def.key_field == "key"
        assert type_def.name_field == "name"
        assert type_def.shared_from_parent is False

    def test_data_extension_is_shared(self):
        """Data extension should be shareable from parent."""
        type_def = TYPE_REGISTRY["data_extension"]
        assert type_def.shared_from_parent is True

    def test_soap_types_marked_correctly(self):
        """SOAP types should have api_type=SOAP."""
        soap_types = ["classic_email", "triggered_send", "list", "sender_profile"]
        for type_name in soap_types:
            type_def = TYPE_REGISTRY[type_name]
            assert type_def.api_type == "SOAP", f"{type_name} should be SOAP"
