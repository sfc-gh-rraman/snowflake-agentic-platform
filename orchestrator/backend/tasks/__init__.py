"""Tasks module."""

from .workflow_tasks import (
    TASK_REGISTRY,
    approve_plan,
    check_cke,
    check_fhir_tables,
    check_observability,
    create_analytics,
    detect_domain,
    final_report,
    generate_plan,
    log_results,
    post_governance_check,
    register_all_tasks,
    validate_quality,
    verify_fhir,
    verify_governance,
)

from .scenario_tasks import (
    SCENARIO_DEFINITIONS,
    SCENARIO_TASK_REGISTRY,
)

__all__ = [
    "TASK_REGISTRY",
    "SCENARIO_TASK_REGISTRY",
    "SCENARIO_DEFINITIONS",
    "register_all_tasks",
    "check_fhir_tables",
    "check_observability",
    "check_cke",
    "detect_domain",
    "generate_plan",
    "approve_plan",
    "verify_fhir",
    "validate_quality",
    "verify_governance",
    "post_governance_check",
    "create_analytics",
    "log_results",
    "final_report",
]
