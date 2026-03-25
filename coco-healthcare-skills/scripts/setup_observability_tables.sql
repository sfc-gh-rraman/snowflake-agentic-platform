-- =============================================================================
-- Observability Tables for Health Sciences Orchestrator
-- =============================================================================
-- Creates three logging tables for auditability and regulatory compliance.
-- Run this script in the database/schema where you want execution logs stored.
--
-- Usage:
--   snowsql -c <connection> -f scripts/setup_observability_tables.sql
--   OR run in CoCo / Snowsight with USE DATABASE / USE SCHEMA set.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS OBSERVABILITY;
USE SCHEMA OBSERVABILITY;

CREATE TABLE IF NOT EXISTS ORCHESTRATOR_EXECUTION_LOG (
    session_id        STRING        NOT NULL,
    plan_id           STRING        NOT NULL,
    user_request      TEXT,
    detected_domain   STRING,
    plan_steps        VARIANT,
    plan_approved     BOOLEAN,
    started_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at      TIMESTAMP_NTZ,
    status            STRING        DEFAULT 'PENDING',
    CONSTRAINT pk_orch_log PRIMARY KEY (session_id, plan_id)
);

COMMENT ON TABLE ORCHESTRATOR_EXECUTION_LOG IS
    'Tracks each orchestrator plan lifecycle: request -> plan -> approval -> execution -> completion.';

CREATE TABLE IF NOT EXISTS SKILL_EXECUTION_LOG (
    session_id          STRING        NOT NULL,
    plan_id             STRING        NOT NULL,
    step_number         NUMBER        NOT NULL,
    skill_name          STRING        NOT NULL,
    skill_type          STRING,
    input_context       VARIANT,
    artifacts_produced  VARIANT,
    governance_applied  VARIANT,
    preflight_status    STRING,
    started_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at        TIMESTAMP_NTZ,
    status              STRING        DEFAULT 'PENDING',
    error_message       TEXT,
    CONSTRAINT pk_skill_log PRIMARY KEY (session_id, plan_id, step_number)
);

COMMENT ON TABLE SKILL_EXECUTION_LOG IS
    'Tracks individual skill executions within an orchestrator plan.';

CREATE TABLE IF NOT EXISTS GOVERNANCE_AUDIT_LOG (
    session_id          STRING        NOT NULL,
    skill_name          STRING        NOT NULL,
    governance_action   STRING        NOT NULL,
    target_object       STRING,
    policy_type         STRING,
    policy_definition   VARIANT,
    applied_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE GOVERNANCE_AUDIT_LOG IS
    'Immutable audit trail of governance actions (masking, row-access, classification) applied during skill execution.';

-- =============================================================================
-- Views for common queries
-- =============================================================================

CREATE OR REPLACE VIEW ACTIVE_SESSIONS AS
SELECT
    o.session_id,
    o.plan_id,
    o.user_request,
    o.detected_domain,
    o.status AS plan_status,
    o.started_at,
    COUNT(s.step_number) AS total_steps,
    SUM(CASE WHEN s.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_steps,
    SUM(CASE WHEN s.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_steps
FROM ORCHESTRATOR_EXECUTION_LOG o
LEFT JOIN SKILL_EXECUTION_LOG s
    ON o.session_id = s.session_id AND o.plan_id = s.plan_id
WHERE o.status IN ('PENDING', 'IN_PROGRESS')
GROUP BY o.session_id, o.plan_id, o.user_request, o.detected_domain, o.status, o.started_at;

CREATE OR REPLACE VIEW GOVERNANCE_SUMMARY AS
SELECT
    DATE_TRUNC('day', applied_at) AS day,
    governance_action,
    policy_type,
    COUNT(*) AS action_count,
    COUNT(DISTINCT session_id) AS session_count,
    COUNT(DISTINCT target_object) AS object_count
FROM GOVERNANCE_AUDIT_LOG
GROUP BY day, governance_action, policy_type
ORDER BY day DESC, action_count DESC;

CREATE OR REPLACE VIEW SKILL_PERFORMANCE AS
SELECT
    skill_name,
    skill_type,
    COUNT(*) AS execution_count,
    AVG(TIMESTAMPDIFF('second', started_at, completed_at)) AS avg_duration_seconds,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failure_count,
    ROUND(success_count / NULLIF(execution_count, 0) * 100, 1) AS success_rate_pct
FROM SKILL_EXECUTION_LOG
WHERE completed_at IS NOT NULL
GROUP BY skill_name, skill_type
ORDER BY execution_count DESC;
