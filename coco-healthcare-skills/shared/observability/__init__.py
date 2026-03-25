"""Observability module for Health Sciences orchestrator.

Provides execution logging for orchestrator plans, skill executions, and
governance actions. Logs to Snowflake tables for auditability and compliance.

Usage in skills:
    from shared.observability.logger import ExecutionLogger

    logger = ExecutionLogger(conn, schema="OBSERVABILITY")
    plan_id = logger.log_plan_start(session_id, user_request, domain, steps)
    logger.log_skill_start(session_id, plan_id, step_num, skill_name)
    logger.log_skill_complete(session_id, plan_id, step_num, artifacts)
    logger.log_governance_action(session_id, skill_name, action, target, policy_type)
    logger.log_plan_complete(session_id, plan_id)
"""
