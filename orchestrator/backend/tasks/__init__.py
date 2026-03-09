"""Tasks module."""

from .workflow_tasks import (
    TASK_REGISTRY,
    deploy_search,
    deploy_semantic,
    deploy_spcs,
    feature_engineering,
    generate_app,
    process_documents,
    process_structured,
    profile_schema,
    register_all_tasks,
    register_models,
    scan_sources,
    train_models,
)

__all__ = [
    "TASK_REGISTRY",
    "register_all_tasks",
    "scan_sources",
    "profile_schema",
    "process_structured",
    "process_documents",
    "deploy_search",
    "deploy_semantic",
    "feature_engineering",
    "train_models",
    "register_models",
    "generate_app",
    "deploy_spcs",
]
