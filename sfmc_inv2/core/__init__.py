"""Core infrastructure for SFMC Inventory Tool."""

from .config import get_config, SFMCConfig
from .path_evaluator import (
    PathEvaluator,
    evaluate_path,
    evaluate_paths,
    evaluate_path_with_context,
    find_activities_by_type,
    extract_dependency_refs,
)

__all__ = [
    # Config
    "get_config",
    "SFMCConfig",
    # Path Evaluator
    "PathEvaluator",
    "evaluate_path",
    "evaluate_paths",
    "evaluate_path_with_context",
    "find_activities_by_type",
    "extract_dependency_refs",
]
