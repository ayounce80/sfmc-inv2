"""Inventory snapshot and metadata models."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class ExtractionError(BaseModel):
    """Error that occurred during extraction."""

    extractor: str = Field(description="Name of the extractor that failed")
    error_type: str = Field(description="Type of error (e.g., APIError, ParseError)")
    message: str = Field(description="Error message")
    timestamp: datetime = Field(default_factory=datetime.now)
    details: Optional[dict[str, Any]] = Field(default=None, description="Additional error context")


class ExtractorStats(BaseModel):
    """Statistics for a single extractor run."""

    name: str = Field(description="Extractor name")
    status: str = Field(description="completed, failed, skipped")
    items_extracted: int = Field(default=0)
    items_enriched: int = Field(default=0)
    duration_seconds: float = Field(default=0.0)
    errors: list[ExtractionError] = Field(default_factory=list)


class InventoryStatistics(BaseModel):
    """Statistics for the entire inventory extraction."""

    total_objects: int = Field(default=0)
    total_relationships: int = Field(default=0)
    extractors_run: int = Field(default=0)
    extractors_succeeded: int = Field(default=0)
    extractors_failed: int = Field(default=0)
    total_duration_seconds: float = Field(default=0.0)
    by_extractor: dict[str, ExtractorStats] = Field(default_factory=dict)
    by_object_type: dict[str, int] = Field(default_factory=dict)


class InventoryMetadata(BaseModel):
    """Metadata about the inventory extraction."""

    version: str = Field(default="1.0.0", description="Inventory format version")
    tool_version: str = Field(description="sfmc-inv2 version")
    extraction_started: datetime = Field(default_factory=datetime.now)
    extraction_completed: Optional[datetime] = Field(default=None)
    sfmc_subdomain: str = Field(description="SFMC subdomain used")
    sfmc_account_id: Optional[str] = Field(default=None, description="Business Unit MID")
    selected_extractors: list[str] = Field(default_factory=list)
    preset_used: Optional[str] = Field(default=None, description="Preset name if used")
    output_format: str = Field(default="json")


class InventoryManifest(BaseModel):
    """Main manifest file pointing to all inventory data."""

    metadata: InventoryMetadata
    statistics: InventoryStatistics
    files: dict[str, str] = Field(
        default_factory=dict,
        description="Map of content type to file path (e.g., 'automations': 'objects/automations.ndjson')",
    )
    errors: list[ExtractionError] = Field(default_factory=list)


class InventorySnapshot(BaseModel):
    """Complete inventory snapshot with all extracted data.

    This is the in-memory representation; on disk, data is split across files.
    """

    manifest: InventoryManifest
    objects: dict[str, list[dict[str, Any]]] = Field(
        default_factory=dict,
        description="Objects by type (e.g., 'automations': [...])",
    )
    relationships: dict[str, Any] = Field(
        default_factory=dict,
        description="Relationship graph data",
    )
