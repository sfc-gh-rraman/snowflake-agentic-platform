-- Observability Dashboards for Snowflake Agentic Platform
-- Reference: docs/DEVOPS.md

-- ============================================================================
-- EXECUTION MONITORING
-- ============================================================================

-- Active executions with duration
-- Use: Monitor currently running pipelines
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_ACTIVE_EXECUTIONS AS
SELECT 
    plan_id,
    use_case_description,
    status,
    created_at,
    DATEDIFF('minute', created_at, CURRENT_TIMESTAMP()) AS duration_minutes,
    CASE 
        WHEN DATEDIFF('minute', created_at, CURRENT_TIMESTAMP()) > 30 THEN 'ALERT'
        WHEN DATEDIFF('minute', created_at, CURRENT_TIMESTAMP()) > 15 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS health_status
FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
WHERE status = 'running';


-- Execution history (last 24 hours)
-- Use: Review recent pipeline runs
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_EXECUTION_HISTORY AS
SELECT 
    plan_id,
    use_case_description,
    status,
    created_at,
    updated_at,
    DATEDIFF('second', created_at, COALESCE(updated_at, CURRENT_TIMESTAMP())) AS duration_seconds,
    ROUND(DATEDIFF('second', created_at, COALESCE(updated_at, CURRENT_TIMESTAMP())) / 60.0, 2) AS duration_minutes
FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY created_at DESC;


-- ============================================================================
-- PHASE & AGENT MONITORING
-- ============================================================================

-- Failed phases (last 24 hours)
-- Use: Identify problematic pipeline phases
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_FAILED_PHASES AS
SELECT 
    phase_name,
    COUNT(*) AS failure_count,
    AVG(retry_count) AS avg_retries,
    MAX(retry_count) AS max_retries,
    COUNT(DISTINCT plan_id) AS affected_plans
FROM AGENTIC_PLATFORM.STATE.PHASE_STATE
WHERE status = 'failed' 
  AND started_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY phase_name
ORDER BY failure_count DESC;


-- Phase performance metrics
-- Use: Identify slow phases for optimization
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_PHASE_PERFORMANCE AS
SELECT 
    phase_name,
    COUNT(*) AS execution_count,
    AVG(DATEDIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP()))) AS avg_duration_seconds,
    MIN(DATEDIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP()))) AS min_duration_seconds,
    MAX(DATEDIFF('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP()))) AS max_duration_seconds,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failure_count,
    ROUND(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate
FROM AGENTIC_PLATFORM.STATE.PHASE_STATE
WHERE started_at > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY phase_name
ORDER BY execution_count DESC;


-- ============================================================================
-- CORTEX CALL MONITORING
-- ============================================================================

-- Cortex call costs (last 24 hours)
-- Use: Track token usage and identify expensive operations
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_CORTEX_COSTS AS
SELECT 
    call_type,
    model,
    COUNT(*) AS call_count,
    SUM(input_tokens) AS total_input_tokens,
    SUM(output_tokens) AS total_output_tokens,
    SUM(input_tokens + output_tokens) AS total_tokens,
    AVG(latency_ms) AS avg_latency_ms,
    MAX(latency_ms) AS max_latency_ms
FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS
WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY call_type, model
ORDER BY total_tokens DESC;


-- Cortex error rate
-- Use: Monitor LLM reliability
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_CORTEX_ERROR_RATE AS
SELECT 
    DATE_TRUNC('hour', created_at) AS hour,
    model,
    COUNT(*) AS total_calls,
    SUM(CASE WHEN response_payload:error IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    ROUND(SUM(CASE WHEN response_payload:error IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS error_rate
FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS
WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('hour', created_at), model
HAVING error_rate > 0
ORDER BY hour DESC;


-- ============================================================================
-- ARTIFACT TRACKING
-- ============================================================================

-- Created artifacts summary
-- Use: Track what resources have been created
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_ARTIFACTS_SUMMARY AS
SELECT 
    artifact_type,
    COUNT(*) AS artifact_count,
    COUNT(DISTINCT plan_id) AS unique_plans,
    MIN(created_at) AS first_created,
    MAX(created_at) AS last_created
FROM AGENTIC_PLATFORM.STATE.ARTIFACTS
GROUP BY artifact_type
ORDER BY artifact_count DESC;


-- Recent artifacts (last 24 hours)
-- Use: See latest created resources
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_RECENT_ARTIFACTS AS
SELECT 
    artifact_id,
    plan_id,
    artifact_type,
    artifact_name,
    artifact_location,
    created_at
FROM AGENTIC_PLATFORM.STATE.ARTIFACTS
WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY created_at DESC;


-- ============================================================================
-- CHECKPOINT MONITORING
-- ============================================================================

-- LangGraph checkpoint status
-- Use: Verify state persistence is working
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_CHECKPOINT_STATUS AS
SELECT 
    DATE_TRUNC('hour', created_at) AS hour,
    COUNT(*) AS checkpoint_count,
    COUNT(DISTINCT thread_id) AS unique_threads,
    COUNT(DISTINCT plan_id) AS unique_plans
FROM AGENTIC_PLATFORM.STATE.LANGGRAPH_CHECKPOINTS
WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('hour', created_at)
ORDER BY hour DESC;


-- ============================================================================
-- ALERTING QUERIES
-- ============================================================================

-- Alert: Executions exceeding 30 minutes
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_ALERT_LONG_EXECUTIONS AS
SELECT 
    plan_id,
    use_case_description,
    created_at,
    DATEDIFF('minute', created_at, CURRENT_TIMESTAMP()) AS duration_minutes,
    'LONG_RUNNING' AS alert_type
FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
WHERE status = 'running'
  AND DATEDIFF('minute', created_at, CURRENT_TIMESTAMP()) > 30;


-- Alert: High LLM error rate (>5%)
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_ALERT_LLM_ERRORS AS
SELECT 
    model,
    COUNT(*) AS total_calls,
    SUM(CASE WHEN response_payload:error IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    ROUND(SUM(CASE WHEN response_payload:error IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS error_rate,
    'HIGH_ERROR_RATE' AS alert_type
FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS
WHERE created_at > DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY model
HAVING error_rate > 5;


-- Alert: Phases with >3 retries
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_ALERT_EXCESSIVE_RETRIES AS
SELECT 
    plan_id,
    phase_id,
    phase_name,
    retry_count,
    error_message,
    'EXCESSIVE_RETRIES' AS alert_type
FROM AGENTIC_PLATFORM.STATE.PHASE_STATE
WHERE retry_count >= 3
  AND started_at > DATEADD('day', -1, CURRENT_TIMESTAMP());


-- ============================================================================
-- COMBINED DASHBOARD QUERY
-- ============================================================================

-- Executive dashboard: Single query for key metrics
CREATE OR REPLACE VIEW AGENTIC_PLATFORM.ORCHESTRATOR.V_EXECUTIVE_DASHBOARD AS
SELECT 
    (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS WHERE status = 'running') AS active_executions,
    (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS WHERE status = 'completed' AND created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS completed_24h,
    (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS WHERE status = 'failed' AND created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS failed_24h,
    (SELECT SUM(input_tokens + output_tokens) FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS total_tokens_24h,
    (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.ARTIFACTS WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS artifacts_created_24h,
    (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.PHASE_STATE WHERE status = 'failed' AND started_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS phase_failures_24h;


-- ============================================================================
-- QUICK DIAGNOSTIC PROCEDURES
-- ============================================================================

-- Get full execution details for a plan
-- Usage: CALL AGENTIC_PLATFORM.ORCHESTRATOR.SP_GET_EXECUTION_DETAILS('plan_id_here');
CREATE OR REPLACE PROCEDURE AGENTIC_PLATFORM.ORCHESTRATOR.SP_GET_EXECUTION_DETAILS(PLAN_ID_PARAM VARCHAR)
RETURNS TABLE (
    section VARCHAR,
    key VARCHAR,
    value VARIANT
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        SELECT 'PLAN' AS section, 'status' AS key, TO_VARIANT(status) AS value
        FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS WHERE plan_id = :PLAN_ID_PARAM
        UNION ALL
        SELECT 'PHASES' AS section, phase_name AS key, OBJECT_CONSTRUCT('status', status, 'retries', retry_count, 'error', error_message) AS value
        FROM AGENTIC_PLATFORM.STATE.PHASE_STATE WHERE plan_id = :PLAN_ID_PARAM
        UNION ALL
        SELECT 'ARTIFACTS' AS section, artifact_type AS key, TO_VARIANT(artifact_name) AS value
        FROM AGENTIC_PLATFORM.STATE.ARTIFACTS WHERE plan_id = :PLAN_ID_PARAM
        UNION ALL
        SELECT 'CORTEX_CALLS' AS section, model AS key, OBJECT_CONSTRUCT('count', COUNT(*), 'tokens', SUM(input_tokens + output_tokens)) AS value
        FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS WHERE plan_id = :PLAN_ID_PARAM
        GROUP BY model
    );
    RETURN TABLE(res);
END;
$$;
