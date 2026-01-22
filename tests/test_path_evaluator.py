"""Tests for the JSON path evaluator module."""

import pytest

from sfmc_inv2.core.path_evaluator import (
    PathEvaluator,
    evaluate_path,
    evaluate_paths,
    evaluate_path_with_context,
    find_activities_by_type,
    extract_dependency_refs,
)


class TestPathEvaluator:
    """Test PathEvaluator class."""

    @pytest.fixture
    def evaluator(self):
        return PathEvaluator()

    @pytest.fixture
    def automation_obj(self):
        """Sample automation-like object."""
        return {
            "id": "auto-123",
            "name": "Test Automation",
            "steps": [
                {
                    "name": "Step 1",
                    "activities": [
                        {"id": "act-1", "objectTypeId": 300, "name": "Query 1"},
                        {"id": "act-2", "objectTypeId": 423, "name": "Script 1"},
                    ],
                },
                {
                    "name": "Step 2",
                    "activities": [
                        {"id": "act-3", "objectTypeId": 300, "name": "Query 2"},
                        {"id": "act-4", "objectTypeId": 43, "name": "Import 1"},
                    ],
                },
            ],
        }

    def test_simple_field_access(self, evaluator):
        """Should access simple fields."""
        obj = {"name": "test", "id": "123"}
        assert evaluator.evaluate(obj, "name") == ["test"]
        assert evaluator.evaluate(obj, "id") == ["123"]

    def test_nested_field_access(self, evaluator):
        """Should access nested fields."""
        obj = {"user": {"name": "test", "email": "test@example.com"}}
        assert evaluator.evaluate(obj, "user.name") == ["test"]
        assert evaluator.evaluate(obj, "user.email") == ["test@example.com"]

    def test_array_iteration(self, evaluator):
        """Should iterate over arrays."""
        obj = {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}
        result = evaluator.evaluate(obj, "items[].id")
        assert result == [1, 2, 3]

    def test_nested_array_iteration(self, evaluator, automation_obj):
        """Should iterate over nested arrays."""
        result = evaluator.evaluate(automation_obj, "steps[].activities[].id")
        assert result == ["act-1", "act-2", "act-3", "act-4"]

    def test_filter_by_value(self, evaluator, automation_obj):
        """Should filter by exact value."""
        result = evaluator.evaluate(
            automation_obj, "steps[].activities[].objectTypeId=300"
        )
        # Returns the parent objects that match
        assert len(result) == 2

    def test_filter_numeric_value(self, evaluator):
        """Should handle numeric filter values."""
        obj = {"items": [{"type": 300}, {"type": 423}, {"type": 300}]}
        result = evaluator.evaluate(obj, "items[].type=300")
        assert len(result) == 2

    def test_missing_field_returns_empty(self, evaluator):
        """Should return empty list for missing fields."""
        obj = {"name": "test"}
        assert evaluator.evaluate(obj, "missing") == []
        assert evaluator.evaluate(obj, "deep.missing.field") == []

    def test_none_object_returns_empty(self, evaluator):
        """Should return empty list for None object."""
        assert evaluator.evaluate(None, "field") == []

    def test_empty_path_returns_empty(self, evaluator):
        """Should return empty list for empty path."""
        obj = {"name": "test"}
        assert evaluator.evaluate(obj, "") == []

    def test_evaluate_all_paths(self, evaluator, automation_obj):
        """Should combine results from multiple paths."""
        paths = [
            "steps[].activities[].objectTypeId=300",
            "steps[].activities[].objectTypeId=423",
        ]
        result = evaluator.evaluate_all(automation_obj, paths)
        assert len(result) == 3  # 2 queries + 1 script

    def test_evaluate_all_handles_list_values(self, evaluator):
        """Should handle list values without TypeError during dedup."""
        obj = {
            "items": [
                {"tags": ["a", "b"]},
                {"tags": ["c", "d"]},
                {"tags": ["a", "b"]},  # duplicate
            ]
        }
        # This should not raise TypeError when deduping lists
        result = evaluator.evaluate_all(obj, ["items[].tags"])
        # Should have 2 unique lists (dedup by string representation)
        assert len(result) == 2
        assert ["a", "b"] in result
        assert ["c", "d"] in result

    def test_evaluate_all_handles_mixed_types(self, evaluator):
        """Should handle mixed value types (dict, list, primitives)."""
        obj = {
            "config": {"key": "value"},
            "tags": ["tag1", "tag2"],
            "name": "test",
            "count": 42,
        }
        result = evaluator.evaluate_all(obj, ["config", "tags", "name", "count"])
        assert len(result) == 4
        assert {"key": "value"} in result
        assert ["tag1", "tag2"] in result
        assert "test" in result
        assert 42 in result

    def test_evaluate_with_context(self, evaluator, automation_obj):
        """Should return values with parent context."""
        results = evaluator.evaluate_with_context(
            automation_obj, "steps[].activities[].objectTypeId=300"
        )
        assert len(results) == 2

        for obj, context in results:
            assert "id" in context
            assert "name" in context


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_evaluate_path(self):
        """evaluate_path should work correctly."""
        obj = {"name": "test"}
        assert evaluate_path(obj, "name") == ["test"]

    def test_evaluate_paths(self):
        """evaluate_paths should combine results."""
        obj = {"a": 1, "b": 2, "c": 3}
        result = evaluate_paths(obj, ["a", "b"])
        assert 1 in result
        assert 2 in result

    def test_find_activities_by_type(self):
        """Should find all activities of specified type."""
        automation = {
            "steps": [
                {
                    "activities": [
                        {"id": "1", "objectTypeId": 300, "name": "Q1"},
                        {"id": "2", "objectTypeId": 423, "name": "S1"},
                    ]
                }
            ]
        }

        queries = find_activities_by_type(automation, 300)
        assert len(queries) == 1
        assert queries[0]["name"] == "Q1"

    def test_extract_dependency_refs(self):
        """Should extract references using registry-style paths."""
        obj = {
            "targetKey": "DE_001",
            "steps": [{"activities": [{"objectTypeId": 300}]}],
        }

        paths = {
            "data_extension": ["targetKey"],
            "query": ["steps[].activities[].objectTypeId=300"],
        }

        refs = extract_dependency_refs(obj, paths)
        assert "data_extension" in refs
        assert refs["data_extension"] == ["DE_001"]
        assert "query" in refs
