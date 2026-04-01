"""Healthcare orchestrator task implementations with Cortex AI integration.

Each task connects to Snowflake and performs real operations against the
AGENTIC_PLATFORM database (FHIR_DEMO and ORCHESTRATOR schemas), using
SNOWFLAKE.CORTEX.COMPLETE() for AI-powered analysis and report generation.
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
CORTEX_MODEL = "mistral-large2"

_session_id: str | None = None
_plan_id: str | None = None


def _get_connection():
    import snowflake.connector

    if os.path.exists("/snowflake/session/token"):
        with open("/snowflake/session/token") as f:
            token = f.read().strip()
        host = os.environ.get("SNOWFLAKE_HOST", "")
        account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
        conn = snowflake.connector.connect(
            host=host,
            account=account,
            authenticator="oauth",
            token=token,
            database=DATABASE,
            warehouse="COMPUTE_WH",
            schema=FHIR_SCHEMA,
        )
    else:
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


def _cortex_complete(conn, prompt, model=None):
    from ..engine.langfuse_integration import track_cortex_call
    return track_cortex_call(conn, prompt, model=model or CORTEX_MODEL)


SKILL_TAXONOMY = """Health Sciences Skill Taxonomy:
- Provider > Clinical Research > Imaging (DICOM parse, ingest, analytics, viewer, governance, ML)
- Provider > Clinical Data Management > FHIR (transform FHIR R4 to relational tables)
- Provider > Clinical Data Management > Clinical NLP (extract entities from clinical text)
- Provider > Clinical Data Management > OMOP (transform EHR/claims to OMOP CDM v5.4)
- Provider > Clinical Data Management > Clinical Docs (extract intelligence from clinical PDFs)
- Provider > Revenue Cycle > Claims Analysis (cohort building, utilization, HEDIS measures)
- Pharma > Drug Safety > Pharmacovigilance (FAERS adverse event analysis, PRR/ROR signals)
- Pharma > Drug Safety > Clinical Trial Protocol (generate trial protocols for FDA submissions)
- Pharma > Genomics > Nextflow (nf-core pipelines for sequencing)
- Pharma > Genomics > Variant Annotation (ClinVar, gnomAD, ACMG classification)
- Pharma > Genomics > Single-Cell QC (scRNA-seq automated QC)
- Pharma > Genomics > Survival Analysis (Kaplan-Meier, Cox proportional hazards)
- Pharma > Lab > Allotrope (convert lab instrument files to Allotrope format)
- Cross-Industry > Research Problem Selection (systematic problem selection)
- Cross-Industry > PubMed CKE (RAG search over biomedical literature)
- Cross-Industry > ClinicalTrials CKE (RAG search over trial registry)
- Cross-Industry > Data Validation (completeness, schema, semantic checks)"""


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
    await log(f'User request: "{request}"')
    await progress(20)

    conn = _get_connection()
    await log("Classifying request against skill taxonomy via Cortex AI...")
    await progress(40)

    classification = _cortex_complete(conn, f"""You are a healthcare data platform router. Given the user request and skill taxonomy below, classify the request.

User Request: {request}

{SKILL_TAXONOMY}

Respond in JSON format with these fields:
- domain: the most specific matching taxonomy path (e.g. "Provider > Clinical Data Management > FHIR")
- skills: array of skill names that should be invoked (e.g. ["hcls-provider-cdata-fhir", "hcls-cross-validation"])
- confidence: HIGH, MEDIUM, or LOW
- reasoning: 1-sentence explanation

JSON only, no markdown fences.""")

    await progress(70)

    try:
        result = json.loads(classification)
        domain = result.get("domain", "Provider > Clinical Data Management")
        skills = result.get("skills", [])
        confidence = result.get("confidence", "HIGH")
        reasoning = result.get("reasoning", "")
    except (json.JSONDecodeError, KeyError):
        domain = "Provider > Clinical Data Management"
        skills = ["hcls-provider-cdata-fhir", "hcls-cross-validation"]
        confidence = "HIGH"
        reasoning = classification[:200]

    await log(f"  Domain: {domain}")
    await log(f"  Skills: {', '.join(skills)}")
    await log(f"  Confidence: {confidence}")
    if reasoning:
        await log(f"  Reasoning: {reasoning}")
    await log(f"  Session ID: {_session_id}")

    conn.close()
    await progress(100)
    return {
        "domain": domain,
        "skills": skills,
        "confidence": confidence,
        "reasoning": reasoning,
        "session_id": _session_id,
        "user_request": request,
    }


async def generate_plan(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    global _plan_id
    _plan_id = str(uuid.uuid4())[:8]

    request = config.get("user_request", "Validate FHIR data quality and apply HIPAA governance")
    await log("Generating AI-powered execution plan via Cortex...")
    await progress(20)

    conn = _get_connection()

    plan_json = _cortex_complete(conn, f"""You are a healthcare data orchestrator. Generate an execution plan for:

Request: {request}

Available data: FHIR tables (PATIENT, OBSERVATION, CONDITION) in {DATABASE}.{FHIR_SCHEMA}
Available capabilities: FHIR validation, data quality checks, HIPAA governance verification, semantic analytics, Cortex AI enrichment

Generate a JSON array of steps. Each step has:
- step: integer
- skill: skill identifier (e.g. "hcls-provider-cdata-fhir", "hcls-cross-validation", "data-governance", "semantic-view", "cortex-ai-enrichment")
- action: description of what this step does
- produces: what artifact or outcome this step creates

Return 5-7 steps. JSON array only, no markdown fences.""")

    await progress(60)

    try:
        plan_steps = json.loads(plan_json)
        if not isinstance(plan_steps, list):
            raise ValueError("not a list")
    except (json.JSONDecodeError, ValueError):
        plan_steps = [
            {"step": 1, "skill": "hcls-provider-cdata-fhir", "action": "Validate FHIR R4 schema conformance", "produces": "schema_report"},
            {"step": 2, "skill": "hcls-cross-validation", "action": "Run completeness, referential integrity, and semantic validation", "produces": "validation_report"},
            {"step": 3, "skill": "data-governance", "action": "Verify HIPAA PHI masking policies and generate compliance assessment", "produces": "governance_audit"},
            {"step": 4, "skill": "hcls-cross-validation", "action": "Post-governance validation — confirm masking active", "produces": "masking_verification"},
            {"step": 5, "skill": "semantic-view", "action": "Create Patient Analytics view with Cortex AI risk summaries", "produces": "analytics_view"},
            {"step": 6, "skill": "cortex-ai-enrichment", "action": "Generate AI clinical summaries for each patient", "produces": "patient_summaries"},
        ]

    for s in plan_steps:
        produces = s.get("produces", "")
        await log(f"  Step {s['step']}: [{s['skill']}] {s['action']}")
        if produces:
            await log(f"           Produces: {produces}")

    await log(f"\nPlan ID: {_plan_id}")
    conn.close()
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
            {"step": 1, "skill": "hcls-provider-cdata-fhir", "action": "Validate FHIR R4 schema conformance"},
            {"step": 2, "skill": "hcls-cross-validation", "action": "Run data quality validation"},
            {"step": 3, "skill": "data-governance", "action": "Verify HIPAA PHI masking policies"},
            {"step": 4, "skill": "hcls-cross-validation", "action": "Post-governance masking check"},
            {"step": 5, "skill": "semantic-view", "action": "Create analytics view with AI enrichment"},
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


FHIR_REQUIRED_FIELDS = {
    "PATIENT": ["id", "gender", "birth_date"],
    "OBSERVATION": ["id", "status", "patient_id", "code_display"],
    "CONDITION": ["id", "patient_id", "clinical_status", "code_display"],
}

FHIR_VALUE_SETS = {
    "PATIENT.gender": ["male", "female", "other", "unknown"],
    "OBSERVATION.status": ["registered", "preliminary", "final", "amended", "corrected", "cancelled", "entered-in-error", "unknown"],
    "CONDITION.clinical_status": ["active", "recurrence", "relapse", "inactive", "remission", "resolved"],
}


async def verify_fhir(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-provider-cdata-fhir] FHIR R4 Schema Conformance Check...")
    await progress(5)

    conn = _get_connection()
    _log_skill_start(conn, 1, "hcls-provider-cdata-fhir", "standalone")

    fhir_report = {}

    for i, table in enumerate(["PATIENT", "OBSERVATION", "CONDITION"]):
        fqn = f"{DATABASE}.{FHIR_SCHEMA}.{table}"
        await log(f"\n  === {table} ===")
        issues = []

        rows, _ = _query(conn, f"SELECT COUNT(*) FROM {fqn}")
        count = rows[0][0]
        await log(f"  Row count: {count}")

        col_rows, _ = _query(conn, f"DESCRIBE TABLE {fqn}")
        actual_cols = {r[0].lower() for r in col_rows}
        required = FHIR_REQUIRED_FIELDS.get(table, [])
        missing_req = [f for f in required if f.lower() not in actual_cols]
        if missing_req:
            await log(f"  FAIL: Missing required FHIR fields: {missing_req}")
            issues.append({"type": "missing_required_field", "fields": missing_req})
        else:
            await log(f"  Required fields: ALL PRESENT ({', '.join(required)})")

        for vs_key, valid_values in FHIR_VALUE_SETS.items():
            tbl, col = vs_key.split(".")
            if tbl != table:
                continue
            try:
                rows, _ = _query(conn, f"""
                    SELECT {col}, COUNT(*) AS cnt
                    FROM {fqn}
                    WHERE {col} NOT IN ({','.join(f"'{v}'" for v in valid_values)})
                    AND {col} IS NOT NULL
                    GROUP BY {col}
                """)
                if rows:
                    invalid_vals = {r[0]: r[1] for r in rows}
                    await log(f"  WARN: {col} has {sum(invalid_vals.values())} rows with non-standard values: {list(invalid_vals.keys())[:5]}")
                    issues.append({"type": "invalid_value_set", "column": col, "invalid_values": invalid_vals})
                else:
                    await log(f"  Value set {col}: CONFORMANT")
            except Exception:
                pass

        if table in ["OBSERVATION", "CONDITION"]:
            rows, _ = _query(conn, f"""
                SELECT COUNT(*) FROM {fqn} t
                WHERE t.patient_id IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT p WHERE p.id = t.patient_id)
            """)
            orphans = rows[0][0]
            if orphans > 0:
                await log(f"  WARN: {orphans} orphan references (patient_id not in PATIENT)")
                issues.append({"type": "referential_integrity", "orphans": orphans})
            else:
                await log(f"  Referential integrity: PASS")

        status = "PASS" if not issues else "WARN"
        await log(f"  Result: {status} ({len(issues)} issues)")
        fhir_report[table] = {"status": status, "rows": count, "issues": issues}
        await progress(5 + int(85 * (i + 1) / 3))

    _log_skill_complete(conn, 1, {
        "fhir_conformance": {t: r["status"] for t, r in fhir_report.items()},
        "total_issues": sum(len(r["issues"]) for r in fhir_report.values()),
    })
    conn.close()
    await progress(100)
    return {"fhir_report": fhir_report}


async def validate_quality(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-cross-validation] Running data quality validation with AI analysis...")
    await progress(5)

    conn = _get_connection()
    _log_skill_start(conn, 2, "hcls-cross-validation", "standalone")

    reports = {}
    tables = ["PATIENT", "OBSERVATION", "CONDITION"]
    all_findings = []

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
            f'ROUND(SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS "{c}"'
            for c in columns
        ]
        null_rows, _ = _query(conn, f"SELECT {', '.join(null_selects)} FROM {fqn}")
        null_summary = {}
        for idx, c in enumerate(columns):
            rate = float(null_rows[0][idx]) if null_rows[0][idx] is not None else 0.0
            null_summary[c] = rate
            if rate > 0.05:
                await log(f"    {c}: {rate*100:.1f}% null [WARN]")
                checks.append({"check": f"null_{c}", "status": "WARN", "null_rate": rate})
                all_findings.append(f"{table}.{c} is {rate*100:.1f}% null")

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
            if orphans > 0:
                all_findings.append(f"OBSERVATION has {orphans} orphan patient references")

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
        reports[table] = {"overall": overall, "passed": passed, "warnings": warns, "checks": checks, "null_summary": null_summary}

        await progress(5 + int(70 * (ti + 1) / len(tables)))

    await log("\n  Generating AI quality assessment...")
    await progress(80)

    findings_text = "; ".join(all_findings) if all_findings else "No data quality issues found."
    ai_assessment = _cortex_complete(conn, f"""You are a healthcare data quality analyst. Summarize the data quality findings for a FHIR clinical dataset.

Dataset: {DATABASE}.{FHIR_SCHEMA} with tables PATIENT ({reports['PATIENT']['checks'][0]['value']} rows), OBSERVATION ({reports['OBSERVATION']['checks'][0]['value']} rows), CONDITION ({reports['CONDITION']['checks'][0]['value']} rows).

Findings: {findings_text}

Table results: PATIENT={reports['PATIENT']['overall']}, OBSERVATION={reports['OBSERVATION']['overall']}, CONDITION={reports['CONDITION']['overall']}

Provide a 3-5 sentence clinical data quality assessment. Include: overall quality score (A-F grade), key risks for downstream analytics, and 1-2 specific recommendations. Be concise and professional.""")

    await log(f"\n  AI Quality Assessment:")
    for line in ai_assessment.split("\n"):
        if line.strip():
            await log(f"    {line.strip()}")

    _log_skill_complete(conn, 2, {
        "validation_reports": {t: r["overall"] for t, r in reports.items()},
        "ai_assessment": ai_assessment[:500],
    })
    conn.close()
    await progress(100)
    return {"reports": reports, "ai_assessment": ai_assessment}


async def verify_governance(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[data-governance] HIPAA PHI Masking Verification & Compliance Assessment...")
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
    await progress(30)

    policies = []
    masked_columns = []
    for r in rows:
        policies.append({"column": r[1], "policy": r[0]})
        masked_columns.append(r[1])
        await log(f"  {r[1]} -> {r[0]}")
        _log_governance(conn, "data-governance", "VERIFY_MASKING_POLICY",
                        f"{DATABASE}.{FHIR_SCHEMA}.PATIENT.{r[1]}", "PHI_MASKING",
                        {"policy_name": r[0], "uses_is_role_in_session": True})

    await progress(40)

    await log("\n  Sample masked output (no PHI_VIEWER role):")
    sample_rows, _ = _query(conn, f"SELECT id, family_name, given_name, phone, ssn FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT LIMIT 3")
    for r in sample_rows:
        await log(f"    {r[0]}: name={r[1]} {r[2]}, phone={r[3]}, ssn={r[4]}")

    await progress(50)

    phi_columns = ["family_name", "given_name", "phone", "ssn", "address_line", "email"]
    unprotected = [c for c in phi_columns if c.upper() not in [m.upper() for m in masked_columns]]
    protected = [c for c in phi_columns if c.upper() in [m.upper() for m in masked_columns]]

    await log(f"\n  PHI Coverage Analysis:")
    await log(f"    Protected columns: {', '.join(protected) if protected else 'NONE'}")
    if unprotected:
        await log(f"    UNPROTECTED PHI columns: {', '.join(unprotected)}")
    else:
        await log(f"    All identified PHI columns are protected")

    await progress(60)

    await log("\n  Generating AI HIPAA Compliance Assessment...")
    compliance = _cortex_complete(conn, f"""You are a HIPAA compliance analyst. Assess the data governance posture of a clinical FHIR dataset.

Dataset: {DATABASE}.{FHIR_SCHEMA}.PATIENT
Masking policies found: {len(policies)} (columns: {', '.join(masked_columns)})
Policy mechanism: IS_ROLE_IN_SESSION('PHI_VIEWER') — dynamic masking
Protected PHI columns: {', '.join(protected)}
Unprotected PHI columns: {', '.join(unprotected) if unprotected else 'None'}
Sample masked values: family_name=**REDACTED**, ssn=**REDACTED**

Provide a HIPAA compliance assessment in 4-5 sentences covering:
1. Overall compliance rating (COMPLIANT / PARTIALLY COMPLIANT / NON-COMPLIANT)
2. Strength of the masking approach (IS_ROLE_IN_SESSION)
3. Any gaps in PHI protection
4. Recommendation for audit trail
Be direct and specific.""")

    await log(f"\n  AI HIPAA Compliance Assessment:")
    for line in compliance.split("\n"):
        if line.strip():
            await log(f"    {line.strip()}")

    _log_governance(conn, "data-governance", "HIPAA_COMPLIANCE_ASSESSMENT",
                    f"{DATABASE}.{FHIR_SCHEMA}.PATIENT", "HIPAA_ASSESSMENT",
                    {"rating": "assessed", "policies": len(policies), "ai_assessment": compliance[:300]})

    _log_skill_complete(conn, 3, {
        "policies_verified": len(policies),
        "masking_active": len(policies) > 0,
        "phi_coverage": f"{len(protected)}/{len(phi_columns)}",
    }, governance={"policies": [p["policy"] for p in policies], "ai_assessment": compliance[:300]})

    conn.close()
    await progress(100)
    return {"policies": policies, "masking_active": len(policies) > 0, "compliance_assessment": compliance}


async def post_governance_check(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-cross-validation] Post-governance masking verification...")
    await progress(20)

    conn = _get_connection()
    _log_skill_start(conn, 4, "hcls-cross-validation", "standalone")

    phi_cols = ["family_name", "given_name", "phone", "ssn"]
    masking_results = {}

    for col in phi_cols:
        try:
            rows, _ = _query(conn, f"SELECT {col} FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT LIMIT 1")
            val = rows[0][0]
            masked = val == "**REDACTED**"
            masking_results[col] = {"value": val, "masked": masked}
            status = "PASS" if masked else "WARN"
            await log(f"  {col}: '{val}' -> masking {'ACTIVE' if masked else 'NOT ACTIVE'} [{status}]")
        except Exception:
            masking_results[col] = {"value": "ERROR", "masked": False}
            await log(f"  {col}: ERROR reading column")

    all_masked = all(r["masked"] for r in masking_results.values())
    await log(f"\n  Overall masking status: {'ALL COLUMNS PROTECTED' if all_masked else 'GAPS DETECTED'}")

    _log_skill_complete(conn, 4, {"masking_verified": all_masked, "columns_checked": phi_cols})
    conn.close()
    await progress(100)
    return {"masking_verified": all_masked, "masking_results": masking_results}


async def create_analytics(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[semantic-view] Creating Patient Analytics with Cortex AI Enrichment...")
    await progress(5)

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
    await progress(20)

    rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS")
    view_count = rows[0][0]
    await log(f"  View rows: {view_count}")
    await progress(30)

    await log("\n  Generating AI Clinical Risk Summaries per patient...")

    _execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES (
            patient_id VARCHAR,
            session_id VARCHAR,
            risk_summary VARCHAR,
            generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    await progress(40)

    patient_rows, _ = _query(conn, f"""
        SELECT patient_id, gender, age, observation_count, condition_count, conditions
        FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS
        ORDER BY condition_count DESC
        LIMIT 5
    """)

    summaries = []
    for pi, pr in enumerate(patient_rows):
        pid, gender, age, obs_count, cond_count, conditions = pr[0], pr[1], pr[2], pr[3], pr[4], pr[5]
        cond_text = conditions if isinstance(conditions, str) else json.dumps(conditions) if conditions else "None"

        summary = _cortex_complete(conn, f"""Generate a brief clinical risk summary for this patient profile (2-3 sentences):
- Gender: {gender}, Age: {age}
- Observations recorded: {obs_count}
- Active conditions ({cond_count}): {cond_text}
Focus on: risk stratification level (LOW/MODERATE/HIGH), key clinical concerns, recommended follow-up. Be concise.""")

        summaries.append({"patient_id": pid, "summary": summary})
        await log(f"  Patient {pid}: {summary[:120]}...")

        try:
            _execute(conn, f"""
                INSERT INTO {DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES
                (patient_id, session_id, risk_summary)
                VALUES (%s, %s, %s)
            """, (pid, _session_id, summary))
        except Exception:
            pass

        await progress(40 + int(50 * (pi + 1) / len(patient_rows)))

    await log(f"\n  Generated {len(summaries)} AI clinical risk summaries")
    await log(f"  Stored in: {DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES")

    _log_skill_complete(conn, 5, {
        "view": f"{DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS",
        "rows": view_count,
        "risk_summaries": len(summaries),
        "risk_table": f"{DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES",
    })
    conn.close()
    await progress(100)
    return {
        "view": f"{DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS",
        "rows": view_count,
        "risk_summaries": len(summaries),
        "risk_table": f"{DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES",
    }


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
    await log("=== GENERATING AI EXECUTIVE SUMMARY ===")
    await progress(10)

    conn = _get_connection()

    rows, _ = _query(conn, f"""
        SELECT COUNT(*) FROM {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG
        WHERE session_id = '{_session_id}' AND status = 'COMPLETED'
    """)
    skills_completed = rows[0][0]

    rows, _ = _query(conn, f"""
        SELECT COUNT(*) FROM {DATABASE}.{ORCH_SCHEMA}.GOVERNANCE_AUDIT_LOG
        WHERE session_id = '{_session_id}'
    """)
    governance_actions = rows[0][0]

    risk_count = 0
    try:
        rows, _ = _query(conn, f"""
            SELECT COUNT(*) FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES
            WHERE session_id = '{_session_id}'
        """)
        risk_count = rows[0][0]
    except Exception:
        pass

    await log(f"  Session:         {_session_id}")
    await log(f"  Plan:            {_plan_id}")
    await log(f"  Domain:          Provider > Clinical Data Management")
    await log(f"  Skills executed:  {skills_completed}")
    await log(f"  Governance actions: {governance_actions}")
    await log(f"  Risk summaries:  {risk_count}")
    await log(f"  Analytics view:  PATIENT_ANALYTICS")
    await progress(40)

    skill_details = ""
    try:
        rows, _ = _query(conn, f"""
            SELECT skill_name, status, artifacts_produced
            FROM {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG
            WHERE session_id = '{_session_id}'
            ORDER BY step_number
        """)
        skill_details = "; ".join(f"{r[0]}={r[1]}" for r in rows)
    except Exception:
        pass

    await log("\n  Generating AI Executive Summary...")
    await progress(50)

    exec_summary = _cortex_complete(conn, f"""Generate a concise executive summary (6-8 sentences) for a healthcare data orchestration run:

Session: {_session_id}
Domain: Provider > Clinical Data Management (FHIR)
Skills executed: {skills_completed} ({skill_details})
Governance actions logged: {governance_actions}
AI risk summaries generated: {risk_count}
Artifacts: PATIENT_ANALYTICS view, PATIENT_RISK_SUMMARIES table, governance audit log

Cover:
1. What was accomplished (data validation, governance verification, AI enrichment)
2. Key findings (data quality, compliance status)
3. Artifacts produced and their value
4. Recommended next steps
Write for a healthcare IT executive audience. Be specific about Snowflake capabilities used.""")

    await log(f"\n  === EXECUTIVE SUMMARY ===")
    for line in exec_summary.split("\n"):
        if line.strip():
            await log(f"  {line.strip()}")

    await progress(80)

    await log(f"\n  === ARTIFACTS PRODUCED ===")
    await log(f"  1. {DATABASE}.{FHIR_SCHEMA}.PATIENT_ANALYTICS (analytics view)")
    await log(f"  2. {DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES ({risk_count} AI summaries)")
    await log(f"  3. {DATABASE}.{ORCH_SCHEMA}.ORCHESTRATOR_EXECUTION_LOG (session trace)")
    await log(f"  4. {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG ({skills_completed} skill records)")
    await log(f"  5. {DATABASE}.{ORCH_SCHEMA}.GOVERNANCE_AUDIT_LOG ({governance_actions} actions)")

    await log(f"\n  Inspect with:")
    await log(f"  SELECT * FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT_RISK_SUMMARIES WHERE session_id = '{_session_id}';")
    await log(f"  SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.SKILL_EXECUTION_LOG WHERE session_id = '{_session_id}';")
    await log(f"  SELECT * FROM {DATABASE}.{ORCH_SCHEMA}.GOVERNANCE_AUDIT_LOG WHERE session_id = '{_session_id}';")

    conn.close()
    await progress(100)
    return {
        "session_id": _session_id,
        "plan_id": _plan_id,
        "skills_completed": skills_completed,
        "governance_actions": governance_actions,
        "risk_summaries": risk_count,
        "executive_summary": exec_summary,
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
    from .scenario_tasks import SCENARIO_TASK_REGISTRY
    for task_id, fn in SCENARIO_TASK_REGISTRY.items():
        executor.register_task(task_id, fn)
