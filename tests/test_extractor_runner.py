"""Tests for the extractor runner module.

Includes integration tests for layer-based execution ordering.
"""

import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from sfmc_inv2.orchestration.extractor_runner import (
    ExtractorRunner,
    RunnerConfig,
    RunnerResult,
)
from sfmc_inv2.extractors import ExtractorResult


class TestLayerBasedExecution:
    """Integration tests for dependency-aware layer execution."""

    @pytest.fixture
    def execution_log(self):
        """Shared log to track extractor execution order."""
        return []

    @pytest.fixture
    def mock_extractor_factory(self, execution_log):
        """Factory that creates mock extractors tracking execution."""

        def create_mock_extractor(name: str, delay: float = 0.01):
            """Create a mock extractor that logs start/end times."""

            class MockExtractor:
                async def extract(self, options):
                    start_time = datetime.now()
                    execution_log.append({
                        "name": name,
                        "event": "start",
                        "time": start_time,
                    })

                    # Simulate some work
                    await asyncio.sleep(delay)

                    end_time = datetime.now()
                    execution_log.append({
                        "name": name,
                        "event": "end",
                        "time": end_time,
                    })

                    result = ExtractorResult(extractor_name=name, success=True)
                    result.completed_at = end_time
                    return result

            return MockExtractor

        return create_mock_extractor

    @pytest.mark.asyncio
    async def test_layer_execution_order(self, execution_log, mock_extractor_factory):
        """Extractors in layer N should complete before layer N+1 starts."""
        # Create mock extractors for a known dependency chain:
        # folder (layer 0) -> data_extension (layer 1) -> query (layer 2)
        mock_extractors = {
            "folders": mock_extractor_factory("folders"),
            "data_extensions": mock_extractor_factory("data_extensions"),
            "queries": mock_extractor_factory("queries"),
        }

        with patch(
            "sfmc_inv2.orchestration.extractor_runner.get_extractor",
            side_effect=lambda name: mock_extractors.get(name),
        ):
            config = RunnerConfig(
                use_extraction_planner=True,
                include_dependencies=False,  # Use exactly what we request
                max_concurrent_extractors=10,  # Allow all to run if they could
            )
            runner = ExtractorRunner(config)

            # Request extractors that have dependencies
            await runner.run(["folders", "data_extensions", "queries"])

        # Verify execution order from the log
        # Extract start/end times for each extractor
        times = {}
        for entry in execution_log:
            name = entry["name"]
            if name not in times:
                times[name] = {}
            times[name][entry["event"]] = entry["time"]

        # folders (layer 0) should end before data_extensions (layer 1) starts
        assert times["folders"]["end"] <= times["data_extensions"]["start"], (
            "folders should complete before data_extensions starts"
        )

        # data_extensions (layer 1) should end before queries (layer 2) starts
        assert times["data_extensions"]["end"] <= times["queries"]["start"], (
            "data_extensions should complete before queries starts"
        )

    @pytest.mark.asyncio
    async def test_parallel_within_layer(self, execution_log, mock_extractor_factory):
        """Extractors in the same layer should run in parallel."""
        # query and script are both in the same layer (both depend on DE + folder)
        mock_extractors = {
            "folders": mock_extractor_factory("folders", delay=0.01),
            "data_extensions": mock_extractor_factory("data_extensions", delay=0.01),
            "queries": mock_extractor_factory("queries", delay=0.05),
            "scripts": mock_extractor_factory("scripts", delay=0.05),
        }

        with patch(
            "sfmc_inv2.orchestration.extractor_runner.get_extractor",
            side_effect=lambda name: mock_extractors.get(name),
        ):
            config = RunnerConfig(
                use_extraction_planner=True,
                include_dependencies=False,
                max_concurrent_extractors=10,
            )
            runner = ExtractorRunner(config)

            await runner.run(["folders", "data_extensions", "queries", "scripts"])

        # Extract times
        times = {}
        for entry in execution_log:
            name = entry["name"]
            if name not in times:
                times[name] = {}
            times[name][entry["event"]] = entry["time"]

        # queries and scripts should start at approximately the same time
        # (both are in the same layer after data_extensions)
        query_start = times["queries"]["start"]
        script_start = times["scripts"]["start"]

        # They should start within a small window (allowing for async scheduling)
        time_diff = abs((query_start - script_start).total_seconds())
        assert time_diff < 0.02, (
            f"queries and scripts should start nearly simultaneously, "
            f"but diff was {time_diff:.3f}s"
        )

        # Both should start after data_extensions ends
        assert times["data_extensions"]["end"] <= times["queries"]["start"]
        assert times["data_extensions"]["end"] <= times["scripts"]["start"]

    @pytest.mark.asyncio
    async def test_non_planner_mode_runs_concurrently(
        self, execution_log, mock_extractor_factory
    ):
        """Without planner, all extractors should run concurrently."""
        mock_extractors = {
            "folders": mock_extractor_factory("folders", delay=0.05),
            "data_extensions": mock_extractor_factory("data_extensions", delay=0.05),
            "queries": mock_extractor_factory("queries", delay=0.05),
        }

        with patch(
            "sfmc_inv2.orchestration.extractor_runner.get_extractor",
            side_effect=lambda name: mock_extractors.get(name),
        ):
            config = RunnerConfig(
                use_extraction_planner=False,  # Disable planner
                max_concurrent_extractors=10,
            )
            runner = ExtractorRunner(config)

            await runner.run(["folders", "data_extensions", "queries"])

        # Extract start times
        start_times = [
            entry["time"]
            for entry in execution_log
            if entry["event"] == "start"
        ]

        # All should start within a small window (concurrent execution)
        time_range = (max(start_times) - min(start_times)).total_seconds()
        assert time_range < 0.02, (
            f"All extractors should start nearly simultaneously without planner, "
            f"but time range was {time_range:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_layer_execution_with_dependencies_included(
        self, execution_log, mock_extractor_factory
    ):
        """When include_dependencies=True, deps are added and ordered correctly."""
        # Only request automations, but it should pull in its dependencies
        mock_extractors = {
            "folders": mock_extractor_factory("folders"),
            "data_extensions": mock_extractor_factory("data_extensions"),
            "queries": mock_extractor_factory("queries"),
            "scripts": mock_extractor_factory("scripts"),
            "imports": mock_extractor_factory("imports"),
            "data_extracts": mock_extractor_factory("data_extracts"),
            "filters": mock_extractor_factory("filters"),
            "file_transfers": mock_extractor_factory("file_transfers"),
            "event_definitions": mock_extractor_factory("event_definitions"),
            "automations": mock_extractor_factory("automations"),
        }

        with patch(
            "sfmc_inv2.orchestration.extractor_runner.get_extractor",
            side_effect=lambda name: mock_extractors.get(name),
        ):
            config = RunnerConfig(
                use_extraction_planner=True,
                include_dependencies=True,  # Should add all deps
                max_concurrent_extractors=10,
            )
            runner = ExtractorRunner(config)

            # Only request automations
            result = await runner.run(["automations"])

        # Verify dependencies were executed
        executed = {entry["name"] for entry in execution_log if entry["event"] == "end"}
        assert "folders" in executed, "folders dependency should be executed"
        assert "queries" in executed, "queries dependency should be executed"
        assert "automations" in executed, "automations should be executed"

        # Verify automations ran last (after all its dependencies)
        times = {}
        for entry in execution_log:
            name = entry["name"]
            if name not in times:
                times[name] = {}
            times[name][entry["event"]] = entry["time"]

        auto_start = times["automations"]["start"]
        for dep in ["folders", "queries", "scripts", "imports"]:
            if dep in times:
                assert times[dep]["end"] <= auto_start, (
                    f"{dep} should complete before automations starts"
                )


class TestRunnerResult:
    """Test RunnerResult statistics and properties."""

    def test_success_all_succeeded(self):
        """success should be True when all extractors succeed."""
        result = RunnerResult()
        result.results["a"] = ExtractorResult(extractor_name="a", success=True)
        result.results["b"] = ExtractorResult(extractor_name="b", success=True)

        assert result.success is True
        assert result.partial_success is True

    def test_success_some_failed(self):
        """success should be False when any extractor fails."""
        result = RunnerResult()
        result.results["a"] = ExtractorResult(extractor_name="a", success=True)
        result.results["b"] = ExtractorResult(extractor_name="b", success=False)

        assert result.success is False
        assert result.partial_success is True

    def test_success_all_failed(self):
        """Both success and partial_success False when all fail."""
        result = RunnerResult()
        result.results["a"] = ExtractorResult(extractor_name="a", success=False)
        result.results["b"] = ExtractorResult(extractor_name="b", success=False)

        assert result.success is False
        assert result.partial_success is False
