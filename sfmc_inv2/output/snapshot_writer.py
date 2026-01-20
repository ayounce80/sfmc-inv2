"""Snapshot writer for inventory output.

Writes extraction results to disk in organized file structure:
- manifest.json: Entry point with metadata
- objects/*.ndjson: Object data in newline-delimited JSON
- relationships/graph.json: Relationship data
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import orjson

from ..orchestration import RunnerResult
from ..types.inventory import InventoryManifest, InventoryMetadata
from .. import __version__

logger = logging.getLogger(__name__)


def json_dumps(obj: Any) -> bytes:
    """Serialize object to JSON bytes using orjson."""
    return orjson.dumps(
        obj,
        option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS,
        default=_json_default,
    )


def ndjson_dumps(obj: Any) -> bytes:
    """Serialize object to NDJSON bytes (no indent)."""
    return orjson.dumps(obj, default=_json_default)


def _json_default(obj: Any) -> Any:
    """Default serializer for non-standard types."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    raise TypeError(f"Cannot serialize {type(obj)}")


class SnapshotWriter:
    """Writes inventory snapshots to disk."""

    def __init__(
        self,
        output_dir: Path,
        subdomain: str = "",
        account_id: Optional[str] = None,
    ):
        """Initialize the snapshot writer.

        Args:
            output_dir: Base output directory.
            subdomain: SFMC subdomain for metadata.
            account_id: SFMC account/MID for metadata.
        """
        self._base_dir = output_dir
        self._subdomain = subdomain
        self._account_id = account_id

        # Create timestamped directory with MID if available
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if account_id:
            self._output_dir = output_dir / f"inventory_{account_id}_{timestamp}"
        else:
            self._output_dir = output_dir / f"inventory_{timestamp}"

    @property
    def output_dir(self) -> Path:
        """Get the output directory path."""
        return self._output_dir

    async def write(self, result: RunnerResult) -> Path:
        """Write extraction results to disk.

        Args:
            result: Runner result with all extraction data.

        Returns:
            Path to the output directory.
        """
        # Create directories
        self._output_dir.mkdir(parents=True, exist_ok=True)
        (self._output_dir / "objects").mkdir(exist_ok=True)
        (self._output_dir / "relationships").mkdir(exist_ok=True)

        # Build metadata
        metadata = InventoryMetadata(
            tool_version=__version__,
            extraction_started=result.started_at,
            extraction_completed=result.completed_at,
            sfmc_subdomain=self._subdomain,
            sfmc_account_id=self._account_id,
            selected_extractors=result.extractors_run,
        )

        # Build file list
        files: dict[str, str] = {}

        # Write object files
        for extractor_name, extractor_result in result.results.items():
            if extractor_result.items:
                filename = f"objects/{extractor_name}.ndjson"
                await self._write_ndjson(
                    self._output_dir / filename,
                    extractor_result.items,
                )
                files[extractor_name] = filename

        # Write relationships
        if result.relationship_graph.edges:
            rel_filename = "relationships/graph.json"
            await self._write_json(
                self._output_dir / rel_filename,
                result.relationship_graph.model_dump(),
            )
            files["relationships"] = rel_filename

        # Write orphans if any
        if result.relationship_graph.orphans:
            orphan_filename = "relationships/orphans.json"
            await self._write_json(
                self._output_dir / orphan_filename,
                [o.model_dump() for o in result.relationship_graph.orphans],
            )
            files["orphans"] = orphan_filename

        # Build and write manifest
        stats = result.get_statistics()
        manifest = InventoryManifest(
            metadata=metadata,
            statistics=stats,
            files=files,
            errors=[
                error
                for extractor_result in result.results.values()
                for error in extractor_result.errors
            ],
        )

        await self._write_json(
            self._output_dir / "manifest.json",
            manifest.model_dump(),
        )

        # Write separate statistics file
        await self._write_json(
            self._output_dir / "statistics.json",
            stats.model_dump(),
        )

        logger.info(f"Wrote inventory to {self._output_dir}")
        return self._output_dir

    async def _write_json(self, path: Path, data: Any) -> None:
        """Write data as formatted JSON."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._write_json_sync,
            path,
            data,
        )

    def _write_json_sync(self, path: Path, data: Any) -> None:
        """Synchronous JSON write."""
        with open(path, "wb") as f:
            f.write(json_dumps(data))

    async def _write_ndjson(self, path: Path, items: list[Any]) -> None:
        """Write items as newline-delimited JSON (one object per line)."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._write_ndjson_sync,
            path,
            items,
        )

    def _write_ndjson_sync(self, path: Path, items: list[Any]) -> None:
        """Synchronous NDJSON write."""
        with open(path, "wb") as f:
            for item in items:
                f.write(ndjson_dumps(item))
                f.write(b"\n")


def write_snapshot_sync(
    result: RunnerResult,
    output_dir: Path,
    subdomain: str = "",
    account_id: Optional[str] = None,
) -> Path:
    """Synchronous wrapper for writing snapshots."""
    writer = SnapshotWriter(output_dir, subdomain, account_id)
    return asyncio.run(writer.write(result))
