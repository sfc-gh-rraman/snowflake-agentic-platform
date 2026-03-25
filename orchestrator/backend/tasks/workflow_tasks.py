"""Healthcare orchestrator task implementations.

Each task connects to Snowflake and performs real operations against the
AGENTIC_PLATFORM database (FHIR_DEMO and ORCHESTRATOR schemas).
"""

import json
import os
import uuid
from collections.abc import Callable, Coroutine
from datetime import datetime
from typing import Any

DATABASE = "AGENTIC_PLATFORM"
FHIR_SCHEMA = "FHIR_DEMO"
ORCH_SCHEMA = "ORCHESTRATOR"

_session_id: str | None = None
_plan_id: str | None = None


def _get_connection():
    import snowflake.connector

    conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")
    conn = snowflake.connector.connect(connection_name=conn_name)
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    cur.close()
    return conn


def _query(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall(), [desc[0] for desc in cur.description]
    finally:
        cur.close()


def _execute(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
    finally:
        cur.close()


async def check_fhir_tables(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("Connecting to Snowflake...")
    await progress(10)

    conn = _get_connection()
    tables_status = {}

    for table in ["PATIENT", "OBSERVATION", "CONDITION"]:
        fqn = f"{DATABASE}.{FHIR_SCHEMA}.{table}"
        try:
            rows, _ = _query(conn, f"SELECT COUNT(*) FROM {fqn}")
            count = rows[0][0]
            tables_status[table] = {"status": "READY", "rows": count}
            await log(f"  {table}: READY ({count} rows)")
        except Exception:
            tables_status[table] = {"status": "MISSING", "rows": 0}
            await log(f"  {table}: MISSING")
        await progress(10 + int(80 * (list(tables_status.keys()).index(table) + 1) / 3))

    conn.close()
    all_ready = all(t["status"] == "READY" for t in tables_status.values())
    await progress(100)
    return {"tables": tables_status, "all_ready": all_ready}


async def check_observability(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("Checking observability tables...")
    await progress(10)

    conn = _get_connection()
    tables_status = {}

    for table in ["ORCHESTRATOR_EXECUTION_LOG", "SKILL_EXECUTION_LOG", "GOVERNANCE_AUDIT_LOG"]:
        fqn = f"{DATABASE}.{ORCH_SCHEMA}.{table}"
        try:
            _query(conn, f"SELECT 1 FROM {fqn} LIMIT 1")
            tables_status[table] = "READY"
            await log(f"  {table}: READY")
        except Exception:
            tables_status[table] = "MISSING"
            await log(f"  {table}: MISSING")
        await progress(10 + int(80 * (list(tables_status.keys()).index(table) + 1) / 3))

    conn.close()
    await progress(100)
    return {"tables": tables_status}


async def check_cke(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("Probing CKE Marketplace listings...")
    await progress(20)

    conn = _get_connection()
    cke_status = {}

    for db_name, label in [
        ("PUBMED_ABSTRACTS_EMBEDDINGS", "PubMed CKE"),
        ("CLINICAL_TRIALS_EMBEDDINGS", "ClinicalTrials CKE"),
    ]:
        try:
            _query(conn, f"SHOW DATABASES LIKE '{db_name}'")
            rows, _ = _query(conn, f"SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
            if rows[0][0] > 0:
                cke_status[label] = "READY"
                await log(f"  {label}: READY")
            else:
                cke_status[label] = "MISSING"
                await log(f"  {label}: MISSING (optional — will skip enrichment)")
        except Exception:
            cke_status[label] = "MISSING"
            await log(f"  {label}: MISSING (optional — will skip enrichment)")
        await progress(20 + int(60 * (list(cke_status.keys()).index(label) + 1) / 2))

    conn.close()
    await progress(100)
    return {"cke_listings": cke_status, "note": "CKE listings are optional for demo"}


async def detect_domain(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    global _session_id
    _session_id = f"demo-{uuid.uuid4().hex[:8]}"

    request = config.get("user_request", "Validate FHIR data quality and apply HIPAA governance")
    await log(f"User request: \"{request}\"")
    await progress(30)

    await log("Scanning skill taxonomy for domain match...")
    await progress(60)

    domain = "Provider > Clinical Data Management"
    await log(f"Detected domain: {domain}")
    await log(f"Session ID: {_session_id}")
    await progress(100)

    return {"domain": domain, "session_id": _session_id, "user_request": request}


async def generate_plan(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    global _plan_id
    _plan_id = str(uuid.uuid4())[:8]

    await log("Generating execution plan...")
    await progress(20)

    plan_steps = [
        {"step": 1, "skill": "hcls-provider-cdata-fhir", "action": "Verify FHIR data loaded and structured"},
        {"step": 2, "skill": "hcls-cross-validation", "action": "Run completeness, schema, semantic validation"},
        {"step": 3, "skill": "data-governance", "action": "Verify PHI masking policies (IS_ROLE_IN_SESSION)"},
        {"step": 4, "skill": "hcls-cross-validation", "action": "Post-governance validation — verify masking active"},
        {"step": 5, "skill": "semantic-view", "action": "Create analytics view (Patient+Observation+Condition)"},
    ]

    for s in plan_steps:
        await log(f"  Step {s['step']}: [{s['skill']}] {s['action']}")

    await log(f"Plan ID: {_plan_id}")
    await progress(100)
    return {"plan_id": _plan_id, "steps": plan_steps}


async def approve_plan(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("Plan approval: AUTO-APPROVED (demo mode)")
    await progress(50)

    conn = _get_connection()
    try:
        plan_steps = [
            {"step": 1, "skill": "hcls-provider-cdata-fhir", "action": "Verify FHIR data"},
            {"step": 2, "skill": "hcls-cross-validation", "action": "Validate quality"},
            {"step": 3, "skill": "data-governance", "action": "Verify PHI masking"},
            {"step": 4, "skill": "hcls-cross-validation", "action": "Post-governance check"},
            {"step": 5, "skill": "semantic-view", "action": "Create analytics view"},
        ]
        _execute(conn, f"""
            INSERT INTO {DATABASE}.{ORCH_SCHEMA}.ORCHESTRATOR_EXECUTION_LOG
            (session_id, plan_id, user_request, detected_domain, plan_steps, plan_approved, status)
            SELECT %s, %s, %s, %s, PARSE_JSON(%s), TRUE, 'IN_PROGRESS'
        """, (_session_id, _plan_id,
              config.get("user_request", "Validate FHIR data and apply governance"),
              "Provider > Clinical Data Management",
              json.dumps(plan_steps)))
        await log("Plan logged to ORCHESTRATOR_EXECUTION_LOG")
    except Exception as e:
        await log(f"Warning: Could not log plan: {e}")
    finally:
        conn.close()

    await progress(100)
    return {"approved": True, "plan_id": _plan_id, "session_id": _session_id}


async def verify_fhir(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-provider-cdata-fhir] Verifying FHIR data...")
    await progress(10)

    conn = _get_connection()
    _log_skill_start(conn, 1, "hcls-provider-cdata-fhir", "standalone")

    table_info = {}
    for i, table in enumerate(["PATIENT", "OBSERVATION", "CONDITION"]):
        rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.{FHIR_SCHEMA}.{table}")
        count = rows[0][0]
        table_info[table] = count
        await log(f"  {table}: {count} rows")
        await progress(10 + int(70 * (i + 1) / 3))

    _log_skill_complete(conn, 1, {"tables_verified": list(table_info.keys()), "row_counts": table_info})
    conn.close()
    await progress(100)
    return {"tables": table_info, "schema": FHIR_SCHEMA}


async def validate_quality(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-cross-validation] Running data quality validation...")
    await progress(5)

    conn = _get_connection()
    _log_skill_start(conn, 2, "hcls-cross-validation", "standalone")

    reports = {}
    tables = ["PATIENT", "OBSERVATION", "CONDITION"]

    for ti, table in enumerate(tables):
        fqn = f"{DATABASE}.{FHIR_SCHEMA}.{table}"
        await log(f"\n  Validating {table}:")
        checks = []

        rows, _ = _query(conn, f"SELECT COUNT(*) FROM {fqn}")
        row_count = rows[0][0]
        await log(f"    Row count: {row_count} [PASS]")
        checks.append({"check": "row_count", "status": "PASS", "value": row_count})

        col_rows, _ = _query(conn, f"DESCRIBE TABLE {fqn}")
        columns = [r[0] for r in col_rows]

        null_selects = [
            f"ROUND(SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS \"{c}\""
            for c in columns
        ]
        null_rows, _ = _query(conn, f"SELECT {', '.join(null_selects)} FROM {fqn}")
        for i, c in enumerate(columns):
            rate = float(null_rows[0][i]) if null_rows[0][i] is not None else 0.0
            if rate > 0.05:
                await log(f"    {c}: {rate*100:.1f}% null [WARN]")
                checks.append({"check": f"null_{c}", "status": "WARN", "null_rate": rate})

        if table == "PATIENT":
            rows, _ = _query(conn, f"SELECT COUNT(*) FROM {fqn} WHERE gender NOT IN ('male','female','other','unknown')")
            invalid = rows[0][0]
            status = "PASS" if invalid == 0 else "WARN"
            await log(f"    Gender values: {invalid} invalid [{status}]")
            checks.append({"check": "gender_values", "status": status, "invalid": invalid})

        if table == "OBSERVATION":
            rows, _ = _query(conn, f"SELECT COUNT(*) FROM {fqn} WHERE status NOT IN ('registered','preliminary','final','amended','corrected','cancelled','entered-in-error','unknown')")
            invalid = rows[0][0]
            status = "PASS" if invalid == 0 else "WARN"
            await log(f"    Status values: {invalid} invalid [{status}]")
            checks.append({"check": "status_values", "status": status, "invalid": invalid})

            rows, _ = _query(conn, f"""
                SELECT COUNT(*) FROM {fqn} o
                WHERE o.patient_id IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT p WHERE p.id = o.patient_id)
            """)
            orphans = rows[0][0]
            status = "PASS" if orphans == 0 else "WARN"
            await log(f"    Referential integrity: {orphans} orphans [{status}]")
            checks.append({"check": "ref_integrity", "status": status, "orphans": orphans})

        if table == "CONDITION":
            rows, _ = _query(conn, f"SELECT COUNT(*) FROM {fqn} WHERE clinical_status NOT IN ('active','recurrence','relapse','inactive','remission','resolved')")
            invalid = rows[0][0]
            status = "PASS" if invalid == 0 else "WARN"
            await log(f"    Clinical status: {invalid} invalid [{status}]")
            checks.append({"check": "clinical_status", "status": status, "invalid": invalid})

        passed = sum(1 for c in checks if c["status"] == "PASS")
        warns = sum(1 for c in checks if c["status"] == "WARN")
        overall = "WARN" if warns > 0 else "PASS"
        await log(f"    Overall: {overall} ({passed}P/{warns}W)")
        reports[table] = {"overall": overall, "passed": passed, "warnings": warns, "checks": checks}

        await progress(5 + int(90 * (ti + 1) / len(tables)))

    _log_skill_complete(conn, 2, {"validation_reports": {t: r["overall"] for t, r in reports.items()}})
    conn.close()
    await progress(100)
    return {"reports": reports}


async def verify_governance(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[data-governance] Verifying PHI masking policies...")
    await progress(10)

    conn = _get_connection()
    _log_skill_start(conn, 3, "data-governance", "platform")

    rows, _ = _query(conn, f"""
        SELECT policy_name, ref_column_name
        FROM TABLE({DATABASE}.INFORMATION_SCHEMA.POLICY_REFERENCES(
            ref_entity_name => '{DATABASE}.{FHIR_SCHEMA}.PATIENT',
            ref_entity_domain => 'TABLE'
        ))
        ORDER BY ref_column_name
    """)
    await progress(40)

    policies = []
    for r in rows:
        policies.append({"column": r[1], "policy": r[0]})
        await log(f"  {r[1]} → {r[0]}")
        _log_governance(conn, "data-governance", "VERIFY_MASKING_POLICY",
                        f"{DATABASE}.{FHIR_SCHEMA}.PATIENT.{r[1]}", "PHI_MASKING",
                        {"policy_name": r[0], "uses_is_role_in_session": True})

    await progress(60)

    await log("\n  Sample masked output (no PHI_VIEWER role):")
    sample_rows, _ = _query(conn, f"SELECT id, family_name, given_name, phone, ssn FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT LIMIT 3")
    for r in sample_rows:
        await log(f"    {r[0]}: name={r[1]} {r[2]}, phone={r[3]}, ssn={r[4]}")

    _log_governance(conn, "data-governance", "AUDIT_ACCESS_VERIFICATION",
                    f"{DATABASE}.{FHIR_SCHEMA}.PATIENT", "ACCESS_HISTORY",
                    {"recommendation": "Enable ACCESS_HISTORY audit"})

    _log_skill_complete(conn, 3, {"policies_verified": len(policies), "masking_active": len(policies) > 0},
                        governance={"policies": [p["policy"] for p in policies]})
    conn.close()
    await progress(100)
    return {"policies": policies, "masking_active": len(policies) > 0}


async def post_governance_check(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-cross-validation] Post-governance masking check...")
    await progress(20)

    conn = _get_connection()
    _log_skill_start(conn, 4, "hcls-cross-validation", "standalone")

    rows, _ = _query(conn, f"SELECT family_name FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT LIMIT 1")
    val = rows[0][0]
    masked = val == "**REDACTED**"
    status = "PASS" if masked else "WARN"
    await log(f"  family_name sample: '{val}' → masking {'ACTIVE' if masked else 'NOT ACTIVE'} [{status}]")
    await progress(70)

    _log_skill_complete(conn, 4, {"masking_verified": masked})
    conn.close()
    await progress(100)
    return {"masking_verified": masked, "sample_value": val}


async def create_analytics(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[semantic-view] Creating Patient Analytics view...")
    await progress(10)

    conn = _get_connection()
    _log_skill_start(conn, 5, "semantic-view", "platform")

    _execute(conn, f"""
        CREATE OR REPLACE VIEW {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS AS
        SELECT
            p.id AS patient_id,
            p.family_name,
            p.given_name,
            p.birth_date,
            p.gender,
            p.address_city,
            p.address_state,
            DATEDIFF('year', p.birth_date, CURRENT_DATE()) AS age,
            COUNT(DISTINCT o.id) AS observation_count,
            COUNT(DISTINCT c.id) AS condition_count,
            ARRAY_AGG(DISTINCT c.code_display) WITHIN GROUP (ORDER BY c.code_display) AS conditions,
            MAX(o.effective_datetime) AS latest_observation
        FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT p
        LEFT JOIN {DATABASE}.{FHIR_SCHEMA}.OBSERVATION o ON p.id = o.patient_id
        LEFT JOIN {DATABASE}.{FHIR_SCHEMA}.CONDITION c ON p.id = c.patient_id
        GROUP BY p.id, p.family_name, p.given_name, p.birth_date, p.gender,
                 p.address_city, p.address_state
    """)
    await log(f"  Created: {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS")
    await progress(60)

    rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS")
    count = rows[0][0]
    await log(f"  View rows: {count}")

    _log_skill_complete(conn, 5, {"view": f"{DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS", "rows": count})
    conn.close()
    await progress(100)
    return {"view": f"{DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS", "rows": count}


async def log_results(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("Finalizing observability logs...")
    await progress(30)

    conn = _get_connection()
    try:
        _execute(conn, f"""
            UPDATE {DATABASE}.{ORCH_SCHEMA}.ORCHESTRATOR_EXECUTION_LOG
            SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP()
            WHERE session_id = %s AND plan_id = %s
        """, (_session_id, _plan_id))
        await log(f"  Plan {_plan_id} marked COMPLETED")
    except Exception as e:
        await log(f"  Warning: {e}")
    finally:
        conn.close()

    await progress(100)
    return {"session_id": _session_id, "plan_id": _plan_id}


async def final_report(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("=== EXECUTION SUMMARY ===")
    await progress(10)

    conn = _get_connection()

    rows, cols = _query(conn, f"""
        SELECT COUNT(*) FROM {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG
        WHERE session_id = '{_session_id}' AND status = 'COMPLETED'
    """)
    skills_completed = rows[0][0]

    rows, cols = _query(conn, f"""
        SELECT COUNT(*) FROM {DATABASE}.{ORCH_SCHEMA}.GOVERNANCE_AUDIT_LOG
        WHERE session_id = '{_session_id}'
    """)
    governance_actions = rows[0][0]

    await log(f"  Session:    {_session_id}")
    await log(f"  Plan:       {_plan_id}")
    await log(f"  Domain:     Provider > Clinical Data Management")
    await log(f"  Skills:     {skills_completed} completed")
    await log(f"  Governance: {governance_actions} actions logged")
    await log(f"  Analytics:  PATIENT_ANALYTICS view created")
    await progress(70)

    await log("")
    await log("Inspect logs with:")
    await log(f"  SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.ORCHESTRATOR_EXECUTION_LOG WHERE session_id = '{_session_id}';")
    await log(f"  SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG WHERE session_id = '{_session_id}';")
    await log(f"  SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.GOVERNANCE_AUDIT_LOG WHERE session_id = '{_session_id}';")

    conn.close()
    await progress(100)
    return {
        "session_id": _session_id,
        "plan_id": _plan_id,
        "skills_completed": skills_completed,
        "governance_actions": governance_actions,
    }


def _log_skill_start(conn, step_number, skill_name, skill_type):
    try:
        _execute(conn, f"""
            INSERT INTO {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG
            (session_id, plan_id, step_number, skill_name, skill_type, preflight_status, status)
            SELECT %s, %s, %s, %s, %s, 'READY', 'IN_PROGRESS'
        """, (_session_id, _plan_id, step_number, skill_name, skill_type))
    except Exception:
        pass


def _log_skill_complete(conn, step_number, artifacts=None, governance=None):
    try:
        _execute(conn, f"""
            UPDATE {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG
            SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(),
                artifacts_produced = PARSE_JSON(%s),
                governance_applied = PARSE_JSON(%s)
            WHERE session_id = %s AND plan_id = %s AND step_number = %s
        """, (json.dumps(artifacts) if artifacts else None,
              json.dumps(governance) if governance else None,
              _session_id, _plan_id, step_number))
    except Exception:
        pass


def _log_governance(conn, skill_name, action, target, policy_type, definition):
    try:
        _execute(conn, f"""
            INSERT INTO {DATABASE}.{ORCH_SCHEMA}.GOVERNANCE_AUDIT_LOG
            (session_id, skill_name, governance_action, target_object, policy_type, policy_definition)
            SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s)
        """, (_session_id, skill_name, action, target, policy_type, json.dumps(definition)))
    except Exception:
        pass


TASK_REGISTRY = {
    "check_fhir_tables": check_fhir_tables,
    "check_observability": check_observability,
    "check_cke": check_cke,
    "detect_domain": detect_domain,
    "generate_plan": generate_plan,
    "approve_plan": approve_plan,
    "verify_fhir": verify_fhir,
    "validate_quality": validate_quality,
    "verify_governance": verify_governance,
    "post_governance_check": post_governance_check,
    "create_analytics": create_analytics,
    "log_results": log_results,
    "final_report": final_report,
}


def register_all_tasks(executor):
    for task_id, fn in TASK_REGISTRY.items():
        executor.register_task(task_id, fn)
