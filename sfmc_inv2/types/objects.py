"""SFMC object type models."""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class ObjectStatus(str, Enum):
    """Status values for SFMC objects."""

    ACTIVE = "Active"
    INACTIVE = "Inactive"
    BUILDING = "Building"
    ERROR = "Error"
    PAUSED = "Paused"
    STOPPED = "Stopped"
    SCHEDULED = "Scheduled"
    RUNNING = "Running"
    READY = "Ready"
    DRAFT = "Draft"
    PUBLISHED = "Published"


class SFMCObject(BaseModel):
    """Base model for all SFMC objects."""

    id: str = Field(description="Unique identifier")
    name: str = Field(description="Object name")
    description: Optional[str] = Field(default=None)
    created_date: Optional[datetime] = Field(default=None)
    modified_date: Optional[datetime] = Field(default=None)
    folder_id: Optional[str] = Field(default=None, alias="categoryId")
    folder_path: Optional[str] = Field(default=None, description="Breadcrumb path")
    customer_key: Optional[str] = Field(default=None, alias="customerKey")

    model_config = {"populate_by_name": True, "extra": "allow"}


class Folder(BaseModel):
    """SFMC folder/category."""

    id: str = Field(description="Folder ID")
    name: str = Field(description="Folder name")
    parent_id: Optional[str] = Field(default=None, alias="parentId")
    content_type: Optional[str] = Field(default=None, description="Type of content stored")
    description: Optional[str] = Field(default=None)
    is_active: bool = Field(default=True)
    is_editable: bool = Field(default=True)
    allow_children: bool = Field(default=True)

    model_config = {"populate_by_name": True, "extra": "allow"}


# --- Automations ---


class AutomationActivity(BaseModel):
    """Activity within an automation step."""

    id: str = Field(description="Activity ID")
    name: str = Field(description="Activity name")
    activity_type_id: int = Field(alias="activityTypeId")
    activity_type_name: Optional[str] = Field(default=None, description="Resolved type name")
    object_id: Optional[str] = Field(default=None, alias="objectId", description="Referenced object ID")
    display_order: int = Field(default=0, alias="displayOrder")
    target_data_extension_id: Optional[str] = Field(default=None, description="Target DE ID")
    target_data_extension_name: Optional[str] = Field(default=None, description="Target DE name")

    model_config = {"populate_by_name": True, "extra": "allow"}


class AutomationStep(BaseModel):
    """Step within an automation."""

    id: str = Field(description="Step ID")
    name: Optional[str] = Field(default=None)
    step_number: int = Field(alias="stepNumber")
    activities: list[AutomationActivity] = Field(default_factory=list)

    model_config = {"populate_by_name": True, "extra": "allow"}


class AutomationSchedule(BaseModel):
    """Schedule configuration for an automation."""

    schedule_type: Optional[str] = Field(default=None, alias="typeId")
    schedule_type_name: Optional[str] = Field(default=None, description="Resolved type name")
    start_date: Optional[datetime] = Field(default=None, alias="startDate")
    end_date: Optional[datetime] = Field(default=None, alias="endDate")
    recurrence_type: Optional[str] = Field(default=None)
    time_zone: Optional[str] = Field(default=None, alias="timezoneId")

    model_config = {"populate_by_name": True, "extra": "allow"}


class Automation(SFMCObject):
    """SFMC Automation."""

    status: Optional[str] = Field(default=None)
    status_name: Optional[str] = Field(default=None, description="Resolved status name")
    automation_type: Optional[str] = Field(default=None, alias="type")
    is_active: bool = Field(default=True, alias="isActive")
    steps: list[AutomationStep] = Field(default_factory=list)
    schedule: Optional[AutomationSchedule] = Field(default=None)
    last_run_time: Optional[datetime] = Field(default=None, alias="lastRunTime")
    last_run_status: Optional[str] = Field(default=None)
    notifications: Optional[dict[str, Any]] = Field(default=None)

    model_config = {"populate_by_name": True, "extra": "allow"}


# --- Data Extensions ---


class DataExtensionField(BaseModel):
    """Field within a Data Extension."""

    name: str = Field(description="Field name")
    field_type: str = Field(alias="fieldType", description="Data type")
    max_length: Optional[int] = Field(default=None, alias="maxLength")
    is_primary_key: bool = Field(default=False, alias="isPrimaryKey")
    is_required: bool = Field(default=False, alias="isRequired")
    default_value: Optional[str] = Field(default=None, alias="defaultValue")
    ordinal: int = Field(default=0)
    description: Optional[str] = Field(default=None)
    scale: Optional[int] = Field(default=None, description="Decimal scale")

    model_config = {"populate_by_name": True, "extra": "allow"}


class DataExtension(SFMCObject):
    """SFMC Data Extension."""

    is_sendable: bool = Field(default=False, alias="isSendable")
    is_testable: bool = Field(default=False, alias="isTestable")
    sendable_data_extension_field: Optional[str] = Field(default=None, alias="sendableDataExtensionField")
    sendable_subscriber_field: Optional[str] = Field(default=None, alias="sendableSubscriberField")
    row_count: Optional[int] = Field(default=None, alias="rowCount")
    retention_period_length: Optional[int] = Field(default=None, alias="retentionPeriodLength")
    retention_period_unit: Optional[str] = Field(default=None, alias="retentionPeriodUnitOfMeasure")
    delete_at_end_of_retention: bool = Field(default=False, alias="deleteAtEndOfRetentionPeriod")
    reset_retention_on_import: bool = Field(default=False, alias="resetRetentionPeriodOnImport")
    data_retention_period: Optional[str] = Field(default=None)
    template_id: Optional[str] = Field(default=None, alias="templateId")
    fields: list[DataExtensionField] = Field(default_factory=list)

    model_config = {"populate_by_name": True, "extra": "allow"}


# --- Queries ---


class Query(SFMCObject):
    """SFMC SQL Query Activity."""

    query_text: Optional[str] = Field(default=None, alias="queryText")
    target_name: Optional[str] = Field(default=None, alias="targetName")
    target_key: Optional[str] = Field(default=None, alias="targetKey")
    target_id: Optional[str] = Field(default=None, alias="targetId")
    target_description: Optional[str] = Field(default=None, alias="targetDescription")
    target_update_type: Optional[str] = Field(default=None, alias="targetUpdateTypeName")
    status: Optional[str] = Field(default=None)
    created_by: Optional[str] = Field(default=None, alias="createdBy")
    modified_by: Optional[str] = Field(default=None, alias="modifiedBy")

    model_config = {"populate_by_name": True, "extra": "allow"}


# --- Journeys ---


class JourneyActivity(BaseModel):
    """Activity within a Journey."""

    id: str = Field(description="Activity key")
    name: str = Field(description="Activity name")
    activity_type: str = Field(alias="type")
    config_url: Optional[str] = Field(default=None, alias="configurationUrl")
    outcomes: list[dict[str, Any]] = Field(default_factory=list)
    arguments: Optional[dict[str, Any]] = Field(default=None)

    model_config = {"populate_by_name": True, "extra": "allow"}


class JourneyGoal(BaseModel):
    """Goal configuration for a Journey."""

    name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    metric: Optional[str] = Field(default=None)
    target: Optional[float] = Field(default=None)

    model_config = {"populate_by_name": True, "extra": "allow"}


class Journey(SFMCObject):
    """SFMC Journey Builder Journey."""

    version: Optional[int] = Field(default=None)
    status: Optional[str] = Field(default=None)
    definition_id: Optional[str] = Field(default=None, alias="definitionId")
    workflow_api_version: Optional[float] = Field(default=None, alias="workflowApiVersion")
    entry_mode: Optional[str] = Field(default=None, alias="entryMode")
    channel: Optional[str] = Field(default=None)
    triggers: list[dict[str, Any]] = Field(default_factory=list)
    goals: list[JourneyGoal] = Field(default_factory=list)
    activities: list[JourneyActivity] = Field(default_factory=list)
    stats: Optional[dict[str, Any]] = Field(default=None)

    model_config = {"populate_by_name": True, "extra": "allow"}


# --- Assets ---


class AssetType(BaseModel):
    """Asset type definition."""

    id: int
    name: str
    display_name: Optional[str] = Field(default=None, alias="displayName")

    model_config = {"populate_by_name": True, "extra": "allow"}


class Asset(SFMCObject):
    """SFMC Content Builder Asset."""

    asset_type: Optional[AssetType] = Field(default=None, alias="assetType")
    content: Optional[str] = Field(default=None, description="Asset content/HTML")
    content_type: Optional[str] = Field(default=None, alias="contentType")
    category: Optional[dict[str, Any]] = Field(default=None)
    owner: Optional[dict[str, Any]] = Field(default=None)
    enterprise_id: Optional[int] = Field(default=None, alias="enterpriseId")
    member_id: Optional[int] = Field(default=None, alias="memberId")
    status: Optional[dict[str, Any]] = Field(default=None)
    thumbnail_url: Optional[str] = Field(default=None, alias="thumbnail")
    file_properties: Optional[dict[str, Any]] = Field(default=None, alias="fileProperties")
    views: Optional[dict[str, Any]] = Field(default=None)
    channels: Optional[dict[str, Any]] = Field(default=None)

    model_config = {"populate_by_name": True, "extra": "allow"}
