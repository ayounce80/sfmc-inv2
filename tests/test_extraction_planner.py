"""Tests for the extraction planner module."""

import pytest

from sfmc_inv2.orchestration.extraction_planner import (
    ExtractionPlanner,
    ExtractionPlan,
    ExtractionStep,
    plan_extraction,
    get_extraction_order,
)


class TestExtractionPlanner:
    """Test extraction planner functionality."""

    def test_plan_single_extractor(self):
        """Should plan single extractor with dependencies."""
        planner = ExtractionPlanner(include_dependencies=True)
        plan = planner.plan(["queries"])

        # Should include queries and its dependencies
        extractors = plan.all_extractor_names
        assert "queries" in extractors
        assert "data_extensions" in extractors  # dependency
        assert "folders" in extractors  # dependency

    def test_plan_without_dependencies(self):
        """Should only include requested extractors."""
        planner = ExtractionPlanner(include_dependencies=False)
        plan = planner.plan(["queries"])

        extractors = plan.all_extractor_names
        assert extractors == ["queries"]

    def test_plan_marks_cache_only(self):
        """Dependencies should be marked cache_only."""
        planner = ExtractionPlanner(include_dependencies=True)
        plan = planner.plan(["automations"])

        # automations should NOT be cache_only
        assert "automations" in plan.output_extractor_names
        assert "automations" not in plan.cache_only_extractor_names

        # dependencies should be cache_only
        assert "queries" in plan.cache_only_extractor_names
        assert "folders" in plan.cache_only_extractor_names

    def test_topological_order(self):
        """Dependencies should come before dependents."""
        planner = ExtractionPlanner(include_dependencies=True)
        plan = planner.plan(["automations"])

        extractors = plan.all_extractor_names

        # folder should come before query (query depends on folder)
        folder_idx = extractors.index("folders")
        query_idx = extractors.index("queries")
        assert folder_idx < query_idx

        # query should come before automation (automation depends on query)
        automation_idx = extractors.index("automations")
        assert query_idx < automation_idx

    def test_plan_multiple_extractors(self):
        """Should handle multiple requested extractors."""
        planner = ExtractionPlanner(include_dependencies=True)
        plan = planner.plan(["automations", "journeys"])

        output = plan.output_extractor_names
        assert "automations" in output
        assert "journeys" in output

    def test_validate_dependencies(self):
        """Should identify missing dependencies."""
        planner = ExtractionPlanner()

        # Queries need data_extensions
        missing = planner.validate_dependencies(["queries"])
        assert "queries" in missing
        assert "data_extensions" in missing["queries"]

        # Full set should have no missing deps
        missing = planner.validate_dependencies(
            ["queries", "data_extensions", "folders"]
        )
        assert missing == {}


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_plan_extraction(self):
        """plan_extraction should work correctly."""
        plan = plan_extraction(["automations"])
        assert isinstance(plan, ExtractionPlan)
        assert len(plan.steps) > 0

    def test_get_extraction_order(self):
        """get_extraction_order should return ordered list."""
        order = get_extraction_order(["automations"])
        assert isinstance(order, list)
        assert "automations" in order
        assert "queries" in order


class TestExtractionPlan:
    """Test ExtractionPlan dataclass."""

    def test_all_extractor_names(self):
        """Should return all extractor names in order."""
        plan = ExtractionPlan(
            steps=[
                ExtractionStep("folder", "folders", cache_only=True),
                ExtractionStep("query", "queries", cache_only=True),
                ExtractionStep("automation", "automations", cache_only=False),
            ]
        )

        assert plan.all_extractor_names == ["folders", "queries", "automations"]

    def test_output_extractor_names(self):
        """Should return only non-cache-only extractors."""
        plan = ExtractionPlan(
            steps=[
                ExtractionStep("folder", "folders", cache_only=True),
                ExtractionStep("automation", "automations", cache_only=False),
            ]
        )

        assert plan.output_extractor_names == ["automations"]

    def test_cache_only_extractor_names(self):
        """Should return only cache-only extractors."""
        plan = ExtractionPlan(
            steps=[
                ExtractionStep("folder", "folders", cache_only=True),
                ExtractionStep("automation", "automations", cache_only=False),
            ]
        )

        assert plan.cache_only_extractor_names == ["folders"]
