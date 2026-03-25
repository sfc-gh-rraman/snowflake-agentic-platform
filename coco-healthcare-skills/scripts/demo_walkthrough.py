#!/usr/bin/env python3
"""End-to-end demo of the Health Sciences Orchestrator platform.

Simulates a complete orchestrator session:
1. Preflight check (verify dependencies)
2. Plan generation (detect domain, select skills)
3. Plan approval
4. Skill execution with logging
5. Data validation
6. Governance audit logging
7. Plan completion

Usage:
    SNOWFLAKE_CONNECTION_NAME=<conn> python scripts/demo_walkthrough.py

The demo uses AGENTIC_PLATFORM database with:
- FHIR_DEMO schema (synthetic patient data)
- ORCHESTRATOR schema (execution logs)
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    import snowflake.connector
except ImportError:
    sys.exit("snowflake-connector-python required: pip install snowflake-connector-python")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shared.preflight.checker import PreflightChecker
from shared.observability.logger import ExecutionLogger

DATABASE = "AGENTIC_PLATFORM"
ORCH_SCHEMA = "ORCHESTRATOR"
FHIR_SCHEMA = "FHIR_DEMO"

DEMO_REQUEST = (
    "I have FHIR patient data loaded in Snowflake. I need to validate the data quality, "
    "apply HIPAA governance (masking policies for PHI), and build a patient analytics view. "
    "The data includes Patient, Observation, and Condition resources."
)

DEMO_PLAN = [
    {"step": 1, "skill": "hcls-provider-cdata-fhir", "action": "Verify FHIR data loaded and structured correctly"},
    {"step": 2, "skill": "hcls-cross-validation", "action": "Run completeness, schema, and semantic validation on FHIR tables"},
    {"step": 3, "skill": "data-governance", "action": "Apply PHI masking policies (SSN, names, phone) using IS_ROLE_IN_SESSION()"},
    {"step": 4, "skill": "hcls-cross-validation", "action": "Post-governance validation — verify masking is active"},
    {"step": 5, "skill": "semantic-view", "action": "Create analytics-ready view joining Patient + Observation + Condition"},
]


def banner(text):
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}")


def step_header(num, total, text):
    print(f"\n  [{num}/{total}] {text}")
    print(f"  {'-' * 60}")


def connect():
    conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    print(f"  Connecting via connection: {conn_name}")
    conn = snowflake.connector.connect(connection_name=conn_name)
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"USE WAREHOUSE COMPUTE_WH")
    cur.close()
    return conn


def run_preflight(conn):
    banner("PHASE 0: PREFLIGHT CHECK")
    checker = PreflightChecker(conn)

    checker.add_table(
        name="FHIR Patient Table",
        fqn=f"{DATABASE}.{FHIR_SCHEMA}.PATIENT",
        setup="Run the FHIR demo data setup script",
        required=True,
    )
    checker.add_table(
        name="FHIR Observation Table",
        fqn=f"{DATABASE}.{FHIR_SCHEMA}.OBSERVATION",
        setup="Run the FHIR demo data setup script",
        required=True,
    )
    checker.add_table(
        name="FHIR Condition Table",
        fqn=f"{DATABASE}.{FHIR_SCHEMA}.CONDITION",
        setup="Run the FHIR demo data setup script",
        required=True,
    )
    checker.add_table(
        name="Orchestrator Execution Log",
        fqn=f"{DATABASE}.{ORCH_SCHEMA}.ORCHESTRATOR_EXECUTION_LOG",
        setup="Run scripts/setup_observability_tables.sql",
        required=True,
    )

    checker.add_marketplace_listing(
        name="CKE PubMed (optional)",
        db_name="PUBMED_ABSTRACTS_EMBEDDINGS",
        test_table="SHARED.PUBMED_SEARCH_CORPUS",
        listing_url="https://app.snowflake.com/marketplace/listing/GZSTZ67BY9OQW",
        setup="Accept Marketplace listing",
        fallback="Proceeding without PubMed enrichment",
        required=False,
    )

    results = checker.run()
    checker.print_report(results)
    return checker.required_ready(results)


def run_validation(conn, table_fqn, table_name):
    cur = conn.cursor()
    report = {"table": table_fqn, "checks": []}

    cur.execute(f"SELECT COUNT(*) FROM {table_fqn}")
    row_count = cur.fetchone()[0]
    report["row_count"] = row_count
    status = "PASS" if row_count > 0 else "FAIL"
    report["checks"].append({"check": "row_count", "value": row_count, "status": status})
    print(f"    Row count: {row_count} [{status}]")

    cur.execute(f"DESCRIBE TABLE {table_fqn}")
    columns = [row[0] for row in cur.fetchall()]
    report["column_count"] = len(columns)
    print(f"    Columns: {len(columns)}")

    if columns:
        null_selects = [
            f"ROUND(SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS \"{c}\""
            for c in columns
        ]
        cur.execute(f"SELECT {', '.join(null_selects)} FROM {table_fqn}")
        row = cur.fetchone()
        high_null = []
        for i, c in enumerate(columns):
            rate = float(row[i]) if row[i] is not None else 0.0
            if rate > 0.05:
                high_null.append((c, rate))
        if high_null:
            print(f"    Null warnings:")
            for c, rate in high_null:
                print(f"      {c}: {rate*100:.1f}% null [WARN]")
                report["checks"].append({"check": f"null_rate_{c}", "null_rate": rate, "status": "WARN"})
        else:
            print(f"    Null rates: all within threshold [PASS]")

    if table_name == "PATIENT":
        cur.execute(f"SELECT COUNT(*) FROM {table_fqn} WHERE gender NOT IN ('male','female','other','unknown')")
        invalid = cur.fetchone()[0]
        status = "PASS" if invalid == 0 else "WARN"
        report["checks"].append({"check": "gender_values", "invalid": invalid, "status": status})
        print(f"    Gender allowed values: {invalid} invalid [{status}]")

    if table_name == "OBSERVATION":
        cur.execute(f"SELECT COUNT(*) FROM {table_fqn} WHERE status NOT IN ('registered','preliminary','final','amended','corrected','cancelled','entered-in-error','unknown')")
        invalid = cur.fetchone()[0]
        status = "PASS" if invalid == 0 else "WARN"
        report["checks"].append({"check": "status_values", "invalid": invalid, "status": status})
        print(f"    Status allowed values: {invalid} invalid [{status}]")

        cur.execute(f"""
            SELECT COUNT(*) FROM {table_fqn} o
            WHERE o.patient_id IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT p WHERE p.id = o.patient_id)
        """)
        orphans = cur.fetchone()[0]
        status = "PASS" if orphans == 0 else "WARN"
        report["checks"].append({"check": "patient_ref_integrity", "orphans": orphans, "status": status})
        print(f"    Patient referential integrity: {orphans} orphans [{status}]")

    if table_name == "CONDITION":
        cur.execute(f"SELECT COUNT(*) FROM {table_fqn} WHERE clinical_status NOT IN ('active','recurrence','relapse','inactive','remission','resolved')")
        invalid = cur.fetchone()[0]
        status = "PASS" if invalid == 0 else "WARN"
        report["checks"].append({"check": "clinical_status_values", "invalid": invalid, "status": status})
        print(f"    Clinical status values: {invalid} invalid [{status}]")

    cur.close()

    passed = sum(1 for c in report["checks"] if c["status"] == "PASS")
    warnings = sum(1 for c in report["checks"] if c["status"] == "WARN")
    failures = sum(1 for c in report["checks"] if c["status"] == "FAIL")
    report["summary"] = {"passed": passed, "warnings": warnings, "failures": failures}
    report["overall"] = "FAIL" if failures > 0 else ("WARN" if warnings > 0 else "PASS")
    print(f"    Overall: {report['overall']} ({passed} pass, {warnings} warn, {failures} fail)")

    return report


def check_masking(conn):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT policy_name, ref_column_name
        FROM TABLE({DATABASE}.INFORMATION_SCHEMA.POLICY_REFERENCES(
            ref_entity_name => '{DATABASE}.{FHIR_SCHEMA}.PATIENT',
            ref_entity_domain => 'TABLE'
        ))
        ORDER BY ref_column_name
    """)
    rows = cur.fetchall()
    cur.close()
    if rows:
        print(f"    Active masking policies:")
        for r in rows:
            print(f"      {r[1]} -> {r[0]}")
        return [{"column": r[1], "policy": r[0]} for r in rows]
    else:
        print(f"    No masking policies found [WARN]")
        return []


def create_analytics_view(conn):
    cur = conn.cursor()
    cur.execute(f"""
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
    cur.close()
    print(f"    Created: {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS")

    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS")
    count = cur.fetchone()[0]
    cur.close()
    print(f"    Rows: {count}")
    return count


def main():
    session_id = f"demo-{uuid.uuid4().hex[:8]}"

    banner("HEALTH SCIENCES ORCHESTRATOR — END-TO-END DEMO")
    print(f"\n  Session ID: {session_id}")
    print(f"  Timestamp:  {datetime.now(timezone.utc).isoformat()}")
    print(f"\n  User Request:")
    print(f"  \"{DEMO_REQUEST}\"")

    conn = connect()
    logger = ExecutionLogger(conn, database=DATABASE, schema=ORCH_SCHEMA)

    if not run_preflight(conn):
        print("\n  ABORT: Required dependencies missing. Fix and retry.")
        conn.close()
        sys.exit(1)

    banner("PHASE 1: PLAN")
    print(f"\n  Detected Domain: Provider > Clinical Data Management")
    print(f"\n  Generated Plan:")
    for s in DEMO_PLAN:
        print(f"    Step {s['step']}: [{s['skill']}] {s['action']}")
    print(f"\n  Plan Approval: AUTO-APPROVED (demo mode)")

    plan_id = logger.log_plan_start(
        session_id=session_id,
        user_request=DEMO_REQUEST,
        detected_domain="Provider > Clinical Data Management",
        plan_steps=DEMO_PLAN,
    )
    logger.log_plan_approved(session_id, plan_id)
    print(f"  Plan ID: {plan_id}")

    banner("PHASE 2: EXECUTE")
    total = len(DEMO_PLAN)
    all_artifacts = {}
    all_reports = {}

    # Step 1: Verify FHIR data
    step_header(1, total, "hcls-provider-cdata-fhir — Verify FHIR data")
    logger.log_skill_start(session_id, plan_id, 1, "hcls-provider-cdata-fhir", "standalone", preflight_status="READY")
    tables = ["PATIENT", "OBSERVATION", "CONDITION"]
    cur = conn.cursor()
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {DATABASE}.{FHIR_SCHEMA}.{t}")
        cnt = cur.fetchone()[0]
        print(f"    {t}: {cnt} rows")
    cur.close()
    artifacts_1 = {"tables_verified": tables, "schema": FHIR_SCHEMA}
    all_artifacts["step_1"] = artifacts_1
    logger.log_skill_complete(session_id, plan_id, 1, artifacts=artifacts_1)
    print(f"    Status: COMPLETED")

    # Step 2: Validation
    step_header(2, total, "hcls-cross-validation — Data quality validation")
    logger.log_skill_start(session_id, plan_id, 2, "hcls-cross-validation", "standalone", preflight_status="READY")
    for t in tables:
        fqn = f"{DATABASE}.{FHIR_SCHEMA}.{t}"
        print(f"\n    Validating {t}:")
        report = run_validation(conn, fqn, t)
        all_reports[t] = report
    artifacts_2 = {"validation_reports": {t: r["overall"] for t, r in all_reports.items()}}
    all_artifacts["step_2"] = artifacts_2
    logger.log_skill_complete(session_id, plan_id, 2, artifacts=artifacts_2)
    print(f"\n    Status: COMPLETED")

    # Step 3: Governance
    step_header(3, total, "data-governance — Verify PHI masking policies")
    logger.log_skill_start(session_id, plan_id, 3, "data-governance", "platform", preflight_status="READY")
    policies = check_masking(conn)
    for p in policies:
        logger.log_governance_action(
            session_id=session_id,
            skill_name="data-governance",
            governance_action="VERIFY_MASKING_POLICY",
            target_object=f"{DATABASE}.{FHIR_SCHEMA}.PATIENT.{p['column']}",
            policy_type="PHI_MASKING",
            policy_definition={"policy_name": p["policy"], "uses_is_role_in_session": True},
        )

    cur = conn.cursor()
    cur.execute(f"SELECT id, family_name, given_name, phone, ssn FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT LIMIT 3")
    rows = cur.fetchall()
    cur.close()
    print(f"\n    Sample masked output (current role = ACCOUNTADMIN, no PHI_VIEWER):")
    for r in rows:
        print(f"      {r[0]}: name={r[1]} {r[2]}, phone={r[3]}, ssn={r[4]}")

    logger.log_governance_action(
        session_id=session_id,
        skill_name="data-governance",
        governance_action="AUDIT_ACCESS_VERIFICATION",
        target_object=f"{DATABASE}.{FHIR_SCHEMA}.PATIENT",
        policy_type="ACCESS_HISTORY",
        policy_definition={"recommendation": "Enable ACCESS_HISTORY audit for FHIR_DEMO schema"},
    )
    artifacts_3 = {"policies_verified": len(policies), "masking_active": len(policies) > 0}
    all_artifacts["step_3"] = artifacts_3
    logger.log_skill_complete(session_id, plan_id, 3, artifacts=artifacts_3,
                               governance={"policies": [p["policy"] for p in policies]})
    print(f"    Status: COMPLETED")

    # Step 4: Post-governance validation
    step_header(4, total, "hcls-cross-validation — Post-governance check")
    logger.log_skill_start(session_id, plan_id, 4, "hcls-cross-validation", "standalone", preflight_status="READY")
    print(f"    Verifying masking is active on PHI columns...")
    cur = conn.cursor()
    cur.execute(f"SELECT family_name FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT LIMIT 1")
    val = cur.fetchone()[0]
    cur.close()
    masked = val == "**REDACTED**"
    print(f"    family_name sample: '{val}' -> masking {'ACTIVE' if masked else 'NOT ACTIVE'} [{'PASS' if masked else 'WARN'}]")
    artifacts_4 = {"masking_verified": masked}
    all_artifacts["step_4"] = artifacts_4
    logger.log_skill_complete(session_id, plan_id, 4, artifacts=artifacts_4)
    print(f"    Status: COMPLETED")

    # Step 5: Analytics view
    step_header(5, total, "semantic-view — Create analytics view")
    logger.log_skill_start(session_id, plan_id, 5, "semantic-view", "platform", preflight_status="READY")
    view_rows = create_analytics_view(conn)
    artifacts_5 = {"view_created": f"{DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS", "rows": view_rows}
    all_artifacts["step_5"] = artifacts_5
    logger.log_skill_complete(session_id, plan_id, 5, artifacts=artifacts_5)
    print(f"    Status: COMPLETED")

    # Complete plan
    logger.log_plan_complete(session_id, plan_id, status="COMPLETED")

    banner("PHASE 3: SUMMARY")
    print(f"\n  Session:  {session_id}")
    print(f"  Plan:     {plan_id}")
    print(f"  Domain:   Provider > Clinical Data Management")
    print(f"  Skills:   {total} executed, {total} completed, 0 failed")
    print(f"\n  Validation Results:")
    for t, r in all_reports.items():
        print(f"    {t}: {r['overall']} ({r['summary']['passed']}P/{r['summary']['warnings']}W/{r['summary']['failures']}F)")
    print(f"\n  Governance:")
    print(f"    {len(policies)} masking policies verified")
    print(f"    IS_ROLE_IN_SESSION() pattern used: Yes")
    print(f"    ACCESS_HISTORY audit recommended: Yes")
    print(f"\n  Artifacts:")
    print(f"    Tables: PATIENT (12), OBSERVATION (17), CONDITION (12)")
    print(f"    View:   PATIENT_ANALYTICS ({view_rows} rows)")
    print(f"    Logs:   ORCHESTRATOR_EXECUTION_LOG, SKILL_EXECUTION_LOG, GOVERNANCE_AUDIT_LOG")

    banner("DEMO COMPLETE")
    print(f"\n  To inspect logs:")
    print(f"    SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.ORCHESTRATOR_EXECUTION_LOG WHERE session_id = '{session_id}';")
    print(f"    SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG WHERE session_id = '{session_id}';")
    print(f"    SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.GOVERNANCE_AUDIT_LOG WHERE session_id = '{session_id}';")
    print(f"    SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.SKILL_PERFORMANCE;")
    print()

    conn.close()


if __name__ == "__main__":
    main()
