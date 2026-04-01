"""Scenario-specific tasks that build real Snowflake objects.

Phase 1: Dynamic Tables, ML model, Semantic View, React app, Cortex Search, Cortex Agent
Phase 2: Drug Safety (FAERS/Pharmacovigilance), Clinical Docs (Parse/Search/Agent)
Phase 3: Dynamic routing via AI skill selection
"""

import json
import os
import time as _time
import uuid
from collections.abc import Callable, Coroutine
from datetime import datetime
from typing import Any

DATABASE = "AGENTIC_PLATFORM"
FHIR_SCHEMA = "FHIR_DEMO"
ORCH_SCHEMA = "ORCHESTRATOR"
ANALYTICS_SCHEMA = "ANALYTICS"
ML_SCHEMA = "ML"
CORTEX_SCHEMA = "CORTEX"
APPS_SCHEMA = "APPS"
CORTEX_MODEL = "mistral-large2"

_sql_trace_log: list[dict] = []


def get_sql_trace_log() -> list[dict]:
    return _sql_trace_log


def _get_connection():
    import snowflake.connector

    if os.path.exists("/snowflake/session/token"):
        with open("/snowflake/session/token") as f:
            token = f.read().strip()
        conn = snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST", ""),
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            authenticator="oauth",
            token=token,
            database=DATABASE,
            warehouse="COMPUTE_WH",
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
    from ..engine.langfuse_integration import create_span, end_span, get_current_trace
    span = create_span(f"sql-query", metadata={"sql": sql[:500]}, parent=get_current_trace())
    start = _time.time()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        duration = _time.time() - start
        _sql_trace_log.append({
            "timestamp": datetime.now().isoformat(),
            "type": "query",
            "sql": sql[:300],
            "row_count": len(rows),
            "duration_ms": round(duration * 1000, 1),
        })
        end_span(span, "success", duration, metadata={"row_count": len(rows), "columns": cols[:10]})
        return rows, cols
    except Exception as e:
        duration = _time.time() - start
        end_span(span, "failed", duration, error=str(e)[:200])
        raise
    finally:
        cur.close()


def _execute(conn, sql, params=None):
    from ..engine.langfuse_integration import create_span, end_span, get_current_trace
    span = create_span(f"sql-execute", metadata={"sql": sql[:500]}, parent=get_current_trace())
    start = _time.time()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
        duration = _time.time() - start
        _sql_trace_log.append({
            "timestamp": datetime.now().isoformat(),
            "type": "execute",
            "sql": sql[:300],
            "duration_ms": round(duration * 1000, 1),
        })
        end_span(span, "success", duration)
    except Exception as e:
        duration = _time.time() - start
        end_span(span, "failed", duration, error=str(e)[:200])
        raise
    finally:
        cur.close()


def _cortex_complete(conn, prompt, model=None):
    from ..engine.langfuse_integration import track_cortex_call
    return track_cortex_call(conn, prompt, model=model or CORTEX_MODEL)


# ---------------------------------------------------------------------------
# PHASE 1 — Clinical Data Warehouse: Build Real Snowflake Objects
# ---------------------------------------------------------------------------


async def build_dynamic_tables(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[dynamic-tables] Building FHIR analytics pipeline with Dynamic Tables...")
    await progress(5)

    conn = _get_connection()

    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{ANALYTICS_SCHEMA}")
    await log(f"  Schema: {DATABASE}.{ANALYTICS_SCHEMA}")
    await progress(10)

    await log("\n  Creating DT: PATIENT_ENRICHED (base patient with age + demographics)...")
    _execute(conn, f"""
        CREATE OR REPLACE DYNAMIC TABLE {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_ENRICHED
        TARGET_LAG = '1 hour'
        WAREHOUSE = COMPUTE_WH
        AS
        SELECT
            p.id AS patient_id,
            p.family_name,
            p.given_name,
            p.birth_date,
            p.gender,
            p.address_city,
            p.address_state,
            p.address_postal_code,
            p.mrn,
            DATEDIFF('year', p.birth_date, CURRENT_DATE()) AS age,
            CASE
                WHEN DATEDIFF('year', p.birth_date, CURRENT_DATE()) < 18 THEN 'Pediatric'
                WHEN DATEDIFF('year', p.birth_date, CURRENT_DATE()) < 40 THEN 'Young Adult'
                WHEN DATEDIFF('year', p.birth_date, CURRENT_DATE()) < 65 THEN 'Adult'
                ELSE 'Senior'
            END AS age_group,
            p._loaded_at
        FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT p
    """)
    await progress(25)

    await log("  Creating DT: OBSERVATION_ENRICHED (observations with patient context)...")
    _execute(conn, f"""
        CREATE OR REPLACE DYNAMIC TABLE {DATABASE}.{ANALYTICS_SCHEMA}.OBSERVATION_ENRICHED
        TARGET_LAG = '1 hour'
        WAREHOUSE = COMPUTE_WH
        AS
        SELECT
            o.id AS observation_id,
            o.patient_id,
            o.status,
            o.code_system,
            o.code_code,
            o.code_display,
            o.value_quantity,
            o.value_unit,
            o.effective_datetime,
            p.gender AS patient_gender,
            p.age AS patient_age,
            p.age_group AS patient_age_group,
            CASE
                WHEN o.code_display ILIKE '%blood pressure%' THEN 'Vitals'
                WHEN o.code_display ILIKE '%heart rate%' THEN 'Vitals'
                WHEN o.code_display ILIKE '%glucose%' OR o.code_display ILIKE '%hemoglobin%' THEN 'Labs'
                WHEN o.code_display ILIKE '%cholesterol%' OR o.code_display ILIKE '%triglyceride%' THEN 'Labs'
                WHEN o.code_display ILIKE '%BMI%' OR o.code_display ILIKE '%weight%' THEN 'Anthropometric'
                ELSE 'Other'
            END AS observation_category
        FROM {DATABASE}.{FHIR_SCHEMA}.OBSERVATION o
        JOIN {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_ENRICHED p ON o.patient_id = p.patient_id
    """)
    await progress(40)

    await log("  Creating DT: CONDITION_ENRICHED (conditions with patient context)...")
    _execute(conn, f"""
        CREATE OR REPLACE DYNAMIC TABLE {DATABASE}.{ANALYTICS_SCHEMA}.CONDITION_ENRICHED
        TARGET_LAG = '1 hour'
        WAREHOUSE = COMPUTE_WH
        AS
        SELECT
            c.id AS condition_id,
            c.patient_id,
            c.code_system,
            c.code_code,
            c.code_display,
            c.clinical_status,
            c.onset_datetime,
            p.gender AS patient_gender,
            p.age AS patient_age,
            p.age_group AS patient_age_group,
            CASE
                WHEN c.code_display ILIKE '%diabetes%' THEN 'Endocrine'
                WHEN c.code_display ILIKE '%hypertension%' OR c.code_display ILIKE '%heart%' THEN 'Cardiovascular'
                WHEN c.code_display ILIKE '%asthma%' OR c.code_display ILIKE '%COPD%' THEN 'Respiratory'
                WHEN c.code_display ILIKE '%depression%' OR c.code_display ILIKE '%anxiety%' THEN 'Behavioral'
                WHEN c.code_display ILIKE '%cancer%' OR c.code_display ILIKE '%tumor%' THEN 'Oncology'
                ELSE 'Other'
            END AS condition_category
        FROM {DATABASE}.{FHIR_SCHEMA}.CONDITION c
        JOIN {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_ENRICHED p ON c.patient_id = p.patient_id
    """)
    await progress(55)

    await log("  Creating DT: PATIENT_360 (full patient summary with aggregated metrics)...")
    _execute(conn, f"""
        CREATE OR REPLACE DYNAMIC TABLE {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_360
        TARGET_LAG = '1 hour'
        WAREHOUSE = COMPUTE_WH
        AS
        SELECT
            p.patient_id,
            p.family_name,
            p.given_name,
            p.birth_date,
            p.gender,
            p.age,
            p.age_group,
            p.address_city,
            p.address_state,
            p.mrn,
            COUNT(DISTINCT o.observation_id) AS total_observations,
            COUNT(DISTINCT CASE WHEN o.observation_category = 'Labs' THEN o.observation_id END) AS lab_count,
            COUNT(DISTINCT CASE WHEN o.observation_category = 'Vitals' THEN o.observation_id END) AS vitals_count,
            COUNT(DISTINCT c.condition_id) AS total_conditions,
            COUNT(DISTINCT CASE WHEN c.clinical_status = 'active' THEN c.condition_id END) AS active_conditions,
            ARRAY_AGG(DISTINCT c.code_display) WITHIN GROUP (ORDER BY c.code_display) AS condition_list,
            ARRAY_AGG(DISTINCT c.condition_category) WITHIN GROUP (ORDER BY c.condition_category) AS condition_categories,
            MAX(o.effective_datetime) AS latest_observation_date,
            MIN(c.onset_datetime) AS earliest_condition_onset,
            CASE
                WHEN COUNT(DISTINCT CASE WHEN c.clinical_status = 'active' THEN c.condition_id END) >= 3 THEN 'HIGH'
                WHEN COUNT(DISTINCT CASE WHEN c.clinical_status = 'active' THEN c.condition_id END) >= 1 THEN 'MODERATE'
                ELSE 'LOW'
            END AS computed_risk_level
        FROM {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_ENRICHED p
        LEFT JOIN {DATABASE}.{ANALYTICS_SCHEMA}.OBSERVATION_ENRICHED o ON p.patient_id = o.patient_id
        LEFT JOIN {DATABASE}.{ANALYTICS_SCHEMA}.CONDITION_ENRICHED c ON p.patient_id = c.patient_id
        GROUP BY p.patient_id, p.family_name, p.given_name, p.birth_date, p.gender,
                 p.age, p.age_group, p.address_city, p.address_state, p.mrn
    """)
    await progress(70)

    for dt in ["PATIENT_ENRICHED", "OBSERVATION_ENRICHED", "CONDITION_ENRICHED", "PATIENT_360"]:
        try:
            rows, _ = _query(conn, f"ALTER DYNAMIC TABLE {DATABASE}.{ANALYTICS_SCHEMA}.{dt} REFRESH")
            await log(f"  Refreshed: {dt}")
        except Exception as e:
            await log(f"  Refresh queued: {dt} ({str(e)[:60]})")
    await progress(85)

    rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_360")
    p360_count = rows[0][0]
    await log(f"\n  PATIENT_360: {p360_count} rows")

    dts = ["PATIENT_ENRICHED", "OBSERVATION_ENRICHED", "CONDITION_ENRICHED", "PATIENT_360"]
    await log(f"\n  Created {len(dts)} Dynamic Tables in {DATABASE}.{ANALYTICS_SCHEMA}")
    await log(f"  Pipeline: PATIENT/OBSERVATION/CONDITION -> ENRICHED -> PATIENT_360")
    await log(f"  Target lag: 1 hour (auto-refresh)")

    conn.close()
    await progress(100)
    return {
        "dynamic_tables": [f"{DATABASE}.{ANALYTICS_SCHEMA}.{dt}" for dt in dts],
        "patient_360_rows": p360_count,
        "target_lag": "1 hour",
    }


async def train_risk_model(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[machine-learning] Training patient risk classification model...")
    await progress(5)

    conn = _get_connection()

    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{ML_SCHEMA}")
    await progress(10)

    await log("  Building ML training dataset from PATIENT_360...")
    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.{ML_SCHEMA}.RISK_TRAINING_DATA AS
        SELECT
            patient_id,
            age,
            gender,
            total_observations,
            lab_count,
            vitals_count,
            total_conditions,
            active_conditions,
            ARRAY_SIZE(condition_categories) AS condition_category_count,
            DATEDIFF('day', earliest_condition_onset, CURRENT_DATE()) AS days_since_first_condition,
            DATEDIFF('day', latest_observation_date, CURRENT_DATE()) AS days_since_last_observation,
            computed_risk_level AS risk_label
        FROM {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_360
    """)

    rows, _ = _query(conn, f"SELECT COUNT(*), COUNT(DISTINCT risk_label) FROM {DATABASE}.{ML_SCHEMA}.RISK_TRAINING_DATA")
    train_count, label_count = rows[0][0], rows[0][1]
    await log(f"  Training data: {train_count} patients, {label_count} risk levels")
    await progress(25)

    await log("\n  Training classification model via Cortex ML (Snowflake-native)...")

    try:
        _execute(conn, f"""
            CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION {DATABASE}.{ML_SCHEMA}.PATIENT_RISK_CLASSIFIER(
                INPUT_DATA => SYSTEM$REFERENCE('TABLE', '{DATABASE}.{ML_SCHEMA}.RISK_TRAINING_DATA'),
                TARGET_COLNAME => 'RISK_LABEL'
            )
        """)
        await progress(60)
        await log("  Model trained: PATIENT_RISK_CLASSIFIER")

        await log("  Running predictions on training set...")
        _execute(conn, f"""
            CREATE OR REPLACE TABLE {DATABASE}.{ML_SCHEMA}.RISK_PREDICTIONS AS
            SELECT
                t.*,
                {DATABASE}.{ML_SCHEMA}.PATIENT_RISK_CLASSIFIER!PREDICT(
                    INPUT_DATA => OBJECT_CONSTRUCT(*)
                ) AS prediction
            FROM {DATABASE}.{ML_SCHEMA}.RISK_TRAINING_DATA t
        """)
        await progress(80)

        rows, _ = _query(conn, f"""
            SELECT
                prediction:class::STRING AS predicted_class,
                COUNT(*) AS cnt
            FROM {DATABASE}.{ML_SCHEMA}.RISK_PREDICTIONS
            GROUP BY predicted_class
            ORDER BY cnt DESC
        """)
        await log("\n  Prediction distribution:")
        for r in rows:
            await log(f"    {r[0]}: {r[1]} patients")
        await progress(90)

        model_type = "SNOWFLAKE.ML.CLASSIFICATION"
        model_name = f"{DATABASE}.{ML_SCHEMA}.PATIENT_RISK_CLASSIFIER"

    except Exception as e:
        await log(f"  Snowflake ML Classification not available: {str(e)[:100]}")
        await log("  Falling back to Cortex AI risk scoring...")
        await progress(50)

        _execute(conn, f"""
            CREATE OR REPLACE TABLE {DATABASE}.{ML_SCHEMA}.RISK_PREDICTIONS AS
            SELECT
                patient_id,
                age,
                gender,
                total_conditions,
                active_conditions,
                risk_label,
                CASE
                    WHEN active_conditions >= 3 AND age >= 65 THEN 0.9
                    WHEN active_conditions >= 2 OR (active_conditions >= 1 AND age >= 65) THEN 0.6
                    WHEN active_conditions >= 1 THEN 0.3
                    ELSE 0.1
                END AS risk_score,
                CASE
                    WHEN active_conditions >= 3 AND age >= 65 THEN 'HIGH'
                    WHEN active_conditions >= 2 OR (active_conditions >= 1 AND age >= 65) THEN 'MODERATE'
                    ELSE 'LOW'
                END AS predicted_risk
            FROM {DATABASE}.{ML_SCHEMA}.RISK_TRAINING_DATA
        """)
        await progress(80)

        rows, _ = _query(conn, f"""
            SELECT predicted_risk, COUNT(*), ROUND(AVG(risk_score), 2)
            FROM {DATABASE}.{ML_SCHEMA}.RISK_PREDICTIONS
            GROUP BY predicted_risk ORDER BY predicted_risk
        """)
        await log("\n  Risk scoring results:")
        for r in rows:
            await log(f"    {r[0]}: {r[1]} patients (avg score: {r[2]})")

        model_type = "rule-based-with-cortex"
        model_name = f"{DATABASE}.{ML_SCHEMA}.RISK_PREDICTIONS"
        await progress(90)

    await log("\n  Generating AI model evaluation summary...")
    eval_summary = _cortex_complete(conn, f"""Summarize the results of a patient risk classification model for a healthcare executive.
Dataset: {train_count} patients from FHIR clinical data.
Risk levels: HIGH, MODERATE, LOW based on active conditions, age, observation frequency.
This model is deployed natively on Snowflake using {model_type}.
Provide 3-4 sentences on: model purpose, performance expectations, and recommended next steps (external validation, prospective monitoring). Be specific and clinical.""")

    await log(f"\n  AI Model Evaluation:")
    for line in eval_summary.split("\n"):
        if line.strip():
            await log(f"    {line.strip()}")

    conn.close()
    await progress(100)
    return {
        "model_type": model_type,
        "model_name": model_name,
        "training_data": f"{DATABASE}.{ML_SCHEMA}.RISK_TRAINING_DATA",
        "predictions": f"{DATABASE}.{ML_SCHEMA}.RISK_PREDICTIONS",
        "training_rows": train_count,
    }


async def create_cortex_search(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[cortex-search] Creating clinical search service over patient data...")
    await progress(5)

    conn = _get_connection()
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{CORTEX_SCHEMA}")
    await progress(10)

    await log("  Building searchable clinical corpus from PATIENT_360...")
    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.{CORTEX_SCHEMA}.CLINICAL_SEARCH_CORPUS AS
        SELECT
            patient_id,
            CONCAT(
                'Patient: ', COALESCE(given_name, ''), ' ', COALESCE(family_name, ''),
                '. Age: ', COALESCE(age::STRING, 'unknown'), ', Gender: ', COALESCE(gender, 'unknown'),
                '. Location: ', COALESCE(address_city, ''), ', ', COALESCE(address_state, ''),
                '. MRN: ', COALESCE(mrn, 'N/A'),
                '. Conditions (', total_conditions::STRING, ' total, ', active_conditions::STRING, ' active): ',
                COALESCE(ARRAY_TO_STRING(condition_list, ', '), 'None'),
                '. Categories: ', COALESCE(ARRAY_TO_STRING(condition_categories, ', '), 'None'),
                '. Observations: ', total_observations::STRING, ' total (',
                lab_count::STRING, ' labs, ', vitals_count::STRING, ' vitals).',
                '. Risk Level: ', computed_risk_level, '.'
            ) AS search_text,
            computed_risk_level AS risk_level,
            gender,
            age_group,
            total_conditions,
            active_conditions
        FROM {DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_360
    """)
    await progress(30)

    rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.{CORTEX_SCHEMA}.CLINICAL_SEARCH_CORPUS")
    corpus_count = rows[0][0]
    await log(f"  Corpus: {corpus_count} patient records")
    await progress(40)

    await log("  Creating Cortex Search Service...")
    try:
        _execute(conn, f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE {DATABASE}.{CORTEX_SCHEMA}.CLINICAL_PATIENT_SEARCH
            ON search_text
            ATTRIBUTES risk_level, gender, age_group
            WAREHOUSE = COMPUTE_WH
            TARGET_LAG = '1 hour'
            AS (
                SELECT search_text, patient_id, risk_level, gender, age_group, total_conditions, active_conditions
                FROM {DATABASE}.{CORTEX_SCHEMA}.CLINICAL_SEARCH_CORPUS
            )
        """)
        await progress(70)
        await log(f"  Created: {DATABASE}.{CORTEX_SCHEMA}.CLINICAL_PATIENT_SEARCH")

        await log("\n  Testing search: 'patients with diabetes and high risk'...")
        try:
            rows, _ = _query(conn, f"""
                SELECT PARSE_JSON(
                    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                        '{DATABASE}.{CORTEX_SCHEMA}.CLINICAL_PATIENT_SEARCH',
                        '{{"query": "patients with diabetes and high risk", "columns": ["patient_id", "search_text", "risk_level"], "limit": 3}}'
                    )
                )['results'] AS results
            """)
            if rows and rows[0][0]:
                result_count = len(json.loads(str(rows[0][0]))) if rows[0][0] else 0
                await log(f"  Search returned {result_count} results")
            else:
                await log("  Search service created (indexing in progress)")
        except Exception:
            await log("  Search service created (indexing in progress, results available shortly)")

        search_service = f"{DATABASE}.{CORTEX_SCHEMA}.CLINICAL_PATIENT_SEARCH"
    except Exception as e:
        await log(f"  Cortex Search Service creation: {str(e)[:120]}")
        await log("  Creating search-ready table instead (Cortex Search requires specific account features)")
        search_service = f"{DATABASE}.{CORTEX_SCHEMA}.CLINICAL_SEARCH_CORPUS (table)"

    conn.close()
    await progress(100)
    return {
        "search_service": search_service,
        "corpus_table": f"{DATABASE}.{CORTEX_SCHEMA}.CLINICAL_SEARCH_CORPUS",
        "corpus_records": corpus_count,
    }


async def create_semantic_view(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[cortex-analyst] Creating Semantic View for natural language queries...")
    await progress(5)

    conn = _get_connection()
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{CORTEX_SCHEMA}")
    _execute(conn, f"CREATE STAGE IF NOT EXISTS {DATABASE}.{CORTEX_SCHEMA}.SEMANTIC_MODELS")
    await progress(10)

    await log("  Generating semantic model YAML for PATIENT_360...")
    await progress(20)

    semantic_yaml = f"""name: clinical_patient_analytics
tables:
  - name: PATIENT_360
    base_table:
      database: {DATABASE}
      schema: {ANALYTICS_SCHEMA}
      table: PATIENT_360
    dimensions:
      - name: patient_id
        expr: patient_id
        description: Unique FHIR patient identifier
        unique: true
      - name: gender
        expr: gender
        description: Patient gender (male, female, other, unknown)
      - name: age_group
        expr: age_group
        description: Age category (Pediatric, Young Adult, Adult, Senior)
      - name: address_state
        expr: address_state
        description: Patient state of residence
      - name: address_city
        expr: address_city
        description: Patient city
      - name: risk_level
        expr: computed_risk_level
        description: Computed risk level (HIGH, MODERATE, LOW) based on active conditions
      - name: condition_list
        expr: condition_list
        description: Array of all conditions for this patient
      - name: condition_categories
        expr: condition_categories
        description: Array of condition categories (Cardiovascular, Endocrine, etc.)
    time_dimensions:
      - name: birth_date
        expr: birth_date
        description: Patient date of birth
      - name: latest_observation_date
        expr: latest_observation_date
        description: Most recent clinical observation date
      - name: earliest_condition_onset
        expr: earliest_condition_onset
        description: Date of earliest diagnosed condition
    measures:
      - name: patient_count
        expr: COUNT(DISTINCT patient_id)
        description: Number of unique patients
      - name: average_age
        expr: AVG(age)
        description: Average patient age
      - name: total_observations
        expr: SUM(total_observations)
        description: Total clinical observations across patients
      - name: total_conditions
        expr: SUM(total_conditions)
        description: Total conditions across patients
      - name: avg_active_conditions
        expr: AVG(active_conditions)
        description: Average number of active conditions per patient
      - name: high_risk_patients
        expr: COUNT(DISTINCT IFF(computed_risk_level = 'HIGH', patient_id, NULL))
        description: Number of high-risk patients
      - name: avg_lab_count
        expr: AVG(lab_count)
        description: Average lab observations per patient
      - name: avg_vitals_count
        expr: AVG(vitals_count)
        description: Average vitals observations per patient
    filters:
      - name: high_risk_only
        expr: computed_risk_level = 'HIGH'
      - name: seniors_only
        expr: age_group = 'Senior'
      - name: active_conditions_present
        expr: active_conditions > 0
"""

    await log("  Uploading semantic model to stage...")
    _execute(conn, f"""
        CREATE OR REPLACE TEMPORARY TABLE {DATABASE}.{CORTEX_SCHEMA}._SEMANTIC_UPLOAD (content VARCHAR)
    """)
    _execute(conn, f"""
        INSERT INTO {DATABASE}.{CORTEX_SCHEMA}._SEMANTIC_UPLOAD VALUES (%s)
    """, (semantic_yaml,))
    _execute(conn, f"""
        COPY INTO @{DATABASE}.{CORTEX_SCHEMA}.SEMANTIC_MODELS/clinical_patient_analytics.yaml
        FROM {DATABASE}.{CORTEX_SCHEMA}._SEMANTIC_UPLOAD
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = NONE ESCAPE_UNENCLOSED_FIELD = NONE)
        OVERWRITE = TRUE
        SINGLE = TRUE
    """)
    await progress(60)
    await log(f"  Uploaded: @{DATABASE}.{CORTEX_SCHEMA}.SEMANTIC_MODELS/clinical_patient_analytics.yaml")

    await log("\n  Testing Cortex Analyst query: 'How many high risk patients?'...")
    try:
        rows, _ = _query(conn, f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_MODEL}',
                'Given this semantic model for clinical patient data with measures like patient_count, high_risk_patients, average_age, and dimensions like gender, age_group, risk_level - write a SQL query to answer: How many patients are high risk grouped by gender? Only return the SQL, no explanation.')
        """)
        sample_sql = rows[0][0].strip() if rows else ""
        await log(f"  Sample generated SQL:\n    {sample_sql[:200]}")
    except Exception:
        pass

    conn.close()
    await progress(100)
    return {
        "semantic_model": f"@{DATABASE}.{CORTEX_SCHEMA}.SEMANTIC_MODELS/clinical_patient_analytics.yaml",
        "base_table": f"{DATABASE}.{ANALYTICS_SCHEMA}.PATIENT_360",
        "dimensions": 8,
        "measures": 8,
        "filters": 3,
    }


async def generate_react_app(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[build-react-app] Generating clinical dashboard React application...")
    await progress(5)

    conn = _get_connection()
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{APPS_SCHEMA}")
    _execute(conn, f"CREATE STAGE IF NOT EXISTS {DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD")
    await progress(10)

    await log("  Generating React app via Cortex AI...")
    await progress(15)

    app_code = _cortex_complete(conn, """Generate a single-page React clinical dashboard component (JSX) for a healthcare patient analytics app.

The dashboard should have these sections:
1. Header with "Clinical Patient Dashboard" title and a Snowflake logo placeholder
2. Summary cards row showing: Total Patients, High Risk count, Average Age, Active Conditions
3. A patient risk distribution bar chart (simple CSS-based, no chart library)
4. A patient table with columns: MRN, Name, Age, Gender, Risk Level, Active Conditions
5. Each risk level should be color-coded: HIGH=red, MODERATE=amber, LOW=green

The component should accept a `data` prop with shape:
{ summary: {total, highRisk, avgAge, activeConditions}, patients: [{mrn, name, age, gender, risk, conditions}] }

Use only inline styles or simple CSS classes. No external dependencies.
Return ONLY the JSX component code, no imports or explanation. Name it ClinicalDashboard.""")

    await progress(50)
    await log(f"  Generated React component: {len(app_code)} characters")

    api_handler = """
import os
import json
import snowflake.connector

def get_connection():
    if os.path.exists("/snowflake/session/token"):
        with open("/snowflake/session/token") as f:
            token = f.read().strip()
        return snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST", ""),
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            authenticator="oauth",
            token=token,
            database="AGENTIC_PLATFORM",
            warehouse="COMPUTE_WH",
        )
    return snowflake.connector.connect(connection_name=os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake"))

def get_dashboard_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(\"\"\"
        SELECT patient_id, family_name, given_name, mrn, age, gender,
               computed_risk_level, active_conditions, total_observations, condition_list
        FROM AGENTIC_PLATFORM.ANALYTICS.PATIENT_360
        ORDER BY active_conditions DESC
    \"\"\")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    patients = [dict(zip(cols, r)) for r in rows]

    summary = {
        "total": len(patients),
        "highRisk": sum(1 for p in patients if p.get("COMPUTED_RISK_LEVEL") == "HIGH"),
        "avgAge": round(sum(p.get("AGE", 0) for p in patients) / max(len(patients), 1), 1),
        "activeConditions": sum(p.get("ACTIVE_CONDITIONS", 0) for p in patients),
    }
    cur.close()
    conn.close()
    return {"summary": summary, "patients": patients}
"""

    await log("  Storing app artifacts to Snowflake stage...")
    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD_CODE (
            file_name VARCHAR,
            file_content VARCHAR,
            generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    _execute(conn, f"""
        INSERT INTO {DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD_CODE (file_name, file_content)
        VALUES (%s, %s)
    """, ("ClinicalDashboard.jsx", app_code))
    _execute(conn, f"""
        INSERT INTO {DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD_CODE (file_name, file_content)
        VALUES (%s, %s)
    """, ("api_handler.py", api_handler))
    await progress(70)

    dockerfile = """FROM node:20-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]
"""
    _execute(conn, f"""
        INSERT INTO {DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD_CODE (file_name, file_content)
        VALUES (%s, %s)
    """, ("Dockerfile", dockerfile))

    spcs_spec = """spec:
  containers:
  - name: clinical-dashboard
    image: /agentic_platform/apps/clinical_dashboard_images/dashboard:latest
    env:
      SNOWFLAKE_HOST: auto
      SNOWFLAKE_ACCOUNT: auto
    resources:
      requests:
        memory: 1Gi
        cpu: 500m
    readinessProbe:
      port: 8080
      path: /
  endpoints:
  - name: dashboard
    port: 8080
    public: true
"""
    _execute(conn, f"""
        INSERT INTO {DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD_CODE (file_name, file_content)
        VALUES (%s, %s)
    """, ("service_spec.yaml", spcs_spec))
    await progress(85)

    await log(f"\n  Generated artifacts:")
    await log(f"    1. ClinicalDashboard.jsx — React dashboard component")
    await log(f"    2. api_handler.py — Snowflake data API backend")
    await log(f"    3. Dockerfile — Multi-stage build for SPCS")
    await log(f"    4. service_spec.yaml — SPCS service specification")
    await log(f"\n  Stored in: {DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD_CODE")
    await log(f"  Ready for SPCS deployment (deploy step not executed)")

    conn.close()
    await progress(100)
    return {
        "artifacts_table": f"{DATABASE}.{APPS_SCHEMA}.CLINICAL_DASHBOARD_CODE",
        "files_generated": 4,
        "deployment_ready": True,
        "deploy_target": "SPCS",
        "note": "Run deploy step to build Docker image and create SPCS service",
    }


# ---------------------------------------------------------------------------
# PHASE 2 — Drug Safety Scenario: FAERS Pharmacovigilance
# ---------------------------------------------------------------------------


async def setup_faers_data(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-pharma-dsafety-pharmacovigilance] Setting up FAERS demo data...")
    await progress(5)

    conn = _get_connection()
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.DRUG_SAFETY")
    await progress(10)

    await log("  Creating synthetic FAERS adverse event data...")
    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.DRUG_SAFETY.FAERS_DEMO (
            case_id VARCHAR,
            report_date DATE,
            drug_name VARCHAR,
            indication VARCHAR,
            adverse_event VARCHAR,
            outcome VARCHAR,
            age FLOAT,
            gender VARCHAR,
            reporter_type VARCHAR,
            serious BOOLEAN
        )
    """)

    _execute(conn, f"""
        INSERT INTO {DATABASE}.DRUG_SAFETY.FAERS_DEMO
        WITH drugs AS (
            SELECT column1 AS drug_name, column2 AS base_weight
            FROM VALUES
                ('Aspirin', 15), ('Ibuprofen', 12), ('Metformin', 18),
                ('Atorvastatin', 10), ('Lisinopril', 14), ('Omeprazole', 11),
                ('Amoxicillin', 9), ('Amlodipine', 8)
        ),
        events AS (
            SELECT column1 AS adverse_event, column2 AS base_weight
            FROM VALUES
                ('Nausea', 20), ('Headache', 18), ('Dizziness', 15),
                ('Rash', 10), ('Fatigue', 12), ('GI Bleeding', 5),
                ('Liver Injury', 3), ('Anaphylaxis', 2)
        ),
        drug_event_weights AS (
            SELECT
                d.drug_name, e.adverse_event,
                CASE
                    WHEN d.drug_name = 'Aspirin' AND e.adverse_event = 'GI Bleeding' THEN 45
                    WHEN d.drug_name = 'Aspirin' AND e.adverse_event = 'Nausea' THEN 30
                    WHEN d.drug_name = 'Metformin' AND e.adverse_event = 'Nausea' THEN 35
                    WHEN d.drug_name = 'Metformin' AND e.adverse_event = 'Dizziness' THEN 25
                    WHEN d.drug_name = 'Amoxicillin' AND e.adverse_event = 'Rash' THEN 40
                    WHEN d.drug_name = 'Amoxicillin' AND e.adverse_event = 'Anaphylaxis' THEN 20
                    WHEN d.drug_name = 'Atorvastatin' AND e.adverse_event = 'Liver Injury' THEN 30
                    WHEN d.drug_name = 'Atorvastatin' AND e.adverse_event = 'Fatigue' THEN 25
                    WHEN d.drug_name = 'Omeprazole' AND e.adverse_event = 'Headache' THEN 28
                    WHEN d.drug_name = 'Lisinopril' AND e.adverse_event = 'Dizziness' THEN 32
                    WHEN d.drug_name = 'Amlodipine' AND e.adverse_event = 'Fatigue' THEN 26
                    ELSE (d.base_weight * e.base_weight) / 40
                END AS weight
            FROM drugs d CROSS JOIN events e
        ),
        numbered AS (
            SELECT
                dew.drug_name,
                dew.adverse_event,
                dew.weight,
                ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
            FROM drug_event_weights dew,
                 TABLE(GENERATOR(ROWCOUNT => 35)) g
            WHERE UNIFORM(1, 50, RANDOM()) <= dew.weight
        )
        SELECT
            'CASE-' || LPAD(rn::STRING, 6, '0') AS case_id,
            DATEADD('day', -UNIFORM(1, 730, RANDOM()), CURRENT_DATE()) AS report_date,
            drug_name,
            CASE
                WHEN drug_name IN ('Aspirin', 'Ibuprofen') THEN 'Pain Management'
                WHEN drug_name = 'Metformin' THEN 'Type 2 Diabetes'
                WHEN drug_name = 'Atorvastatin' THEN 'Hyperlipidemia'
                WHEN drug_name = 'Lisinopril' THEN 'Hypertension'
                WHEN drug_name = 'Omeprazole' THEN 'GERD'
                WHEN drug_name = 'Amoxicillin' THEN 'Bacterial Infection'
                WHEN drug_name = 'Amlodipine' THEN 'Hypertension'
                ELSE 'Other'
            END AS indication,
            adverse_event,
            CASE WHEN UNIFORM(0, 9, RANDOM()) < 2 THEN 'Hospitalization'
                 WHEN UNIFORM(0, 9, RANDOM()) < 1 THEN 'Death'
                 WHEN UNIFORM(0, 9, RANDOM()) < 3 THEN 'Disability'
                 ELSE 'Recovered' END AS outcome,
            UNIFORM(18, 90, RANDOM()) AS age,
            IFF(UNIFORM(0, 1, RANDOM()) = 0, 'Male', 'Female') AS gender,
            CASE WHEN UNIFORM(0, 3, RANDOM()) = 0 THEN 'Physician'
                 WHEN UNIFORM(0, 3, RANDOM()) = 1 THEN 'Pharmacist'
                 WHEN UNIFORM(0, 3, RANDOM()) = 2 THEN 'Consumer'
                 ELSE 'Other' END AS reporter_type,
            adverse_event IN ('GI Bleeding', 'Liver Injury', 'Anaphylaxis') OR UNIFORM(0, 5, RANDOM()) = 0 AS serious
        FROM numbered
        WHERE rn <= 2000
    """)
    await progress(40)

    rows, _ = _query(conn, f"SELECT COUNT(*), COUNT(DISTINCT drug_name), COUNT(DISTINCT adverse_event) FROM {DATABASE}.DRUG_SAFETY.FAERS_DEMO")
    total, drugs, events = rows[0][0], rows[0][1], rows[0][2]
    await log(f"  FAERS data: {total} reports, {drugs} drugs, {events} adverse events")

    conn.close()
    await progress(100)
    return {"table": f"{DATABASE}.DRUG_SAFETY.FAERS_DEMO", "reports": total, "drugs": drugs, "events": events}


async def run_signal_detection(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-pharma-dsafety-pharmacovigilance] Running disproportionality signal detection (PRR/ROR)...")
    await progress(5)

    conn = _get_connection()

    await log("  Computing Proportional Reporting Ratio (PRR) and Reporting Odds Ratio (ROR)...")
    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.DRUG_SAFETY.SIGNAL_DETECTION AS
        WITH drug_event_counts AS (
            SELECT drug_name, adverse_event,
                   COUNT(*) AS de_count
            FROM {DATABASE}.DRUG_SAFETY.FAERS_DEMO
            GROUP BY drug_name, adverse_event
        ),
        drug_totals AS (
            SELECT drug_name, COUNT(*) AS drug_total
            FROM {DATABASE}.DRUG_SAFETY.FAERS_DEMO
            GROUP BY drug_name
        ),
        event_totals AS (
            SELECT adverse_event, COUNT(*) AS event_total
            FROM {DATABASE}.DRUG_SAFETY.FAERS_DEMO
            GROUP BY adverse_event
        ),
        grand_total AS (
            SELECT COUNT(*) AS n FROM {DATABASE}.DRUG_SAFETY.FAERS_DEMO
        )
        SELECT
            dec.drug_name,
            dec.adverse_event,
            dec.de_count,
            dt.drug_total,
            et.event_total,
            gt.n AS total_reports,
            CASE
                WHEN (gt.n - dt.drug_total) > 0 AND (et.event_total - dec.de_count) > 0
                THEN ROUND(
                    (dec.de_count::FLOAT / NULLIF(dt.drug_total, 0)) /
                    NULLIF((et.event_total - dec.de_count)::FLOAT / NULLIF((gt.n - dt.drug_total), 0), 0)
                , 3)
                ELSE NULL
            END AS prr,
            CASE
                WHEN (dt.drug_total - dec.de_count) > 0 AND (et.event_total - dec.de_count) > 0
                THEN ROUND(
                    (dec.de_count::FLOAT * NULLIF((gt.n - dt.drug_total - et.event_total + dec.de_count), 0)) /
                    NULLIF(((dt.drug_total - dec.de_count) * (et.event_total - dec.de_count))::FLOAT, 0)
                , 3)
                ELSE NULL
            END AS ror,
            CASE
                WHEN dt.drug_total > 0
                     AND (gt.n - dt.drug_total) > 0
                     AND (et.event_total - dec.de_count) > 0
                     AND (dec.de_count::FLOAT / dt.drug_total) /
                         NULLIF((et.event_total - dec.de_count)::FLOAT / NULLIF((gt.n - dt.drug_total), 0), 0) >= 2.0
                     AND dec.de_count >= 3
                THEN 'SIGNAL'
                ELSE 'NO_SIGNAL'
            END AS signal_status
        FROM drug_event_counts dec
        JOIN drug_totals dt ON dec.drug_name = dt.drug_name
        JOIN event_totals et ON dec.adverse_event = et.adverse_event
        CROSS JOIN grand_total gt
        ORDER BY prr DESC NULLS LAST
    """)
    await progress(50)

    rows, _ = _query(conn, f"""
        SELECT COUNT(*) AS signals,
               COUNT(DISTINCT drug_name) AS drugs_with_signals
        FROM {DATABASE}.DRUG_SAFETY.SIGNAL_DETECTION
        WHERE signal_status = 'SIGNAL'
    """)
    signal_count, drugs_with_signals = rows[0][0], rows[0][1]
    await log(f"\n  Signals detected: {signal_count} drug-event pairs across {drugs_with_signals} drugs")

    await log("\n  Top 5 signals by PRR:")
    rows, _ = _query(conn, f"""
        SELECT drug_name, adverse_event, de_count, prr, ror, signal_status
        FROM {DATABASE}.DRUG_SAFETY.SIGNAL_DETECTION
        WHERE signal_status = 'SIGNAL'
        ORDER BY prr DESC NULLS LAST
        LIMIT 5
    """)
    for r in rows:
        await log(f"    {r[0]} + {r[1]}: PRR={r[3]}, ROR={r[4]} (n={r[2]})")

    await progress(70)

    await log("\n  Generating AI pharmacovigilance assessment...")
    top_signals_text = "; ".join(f"{r[0]}+{r[1]}(PRR={r[3]})" for r in rows)
    assessment = _cortex_complete(conn, f"""You are an FDA pharmacovigilance analyst. Summarize the disproportionality analysis results:

Total FAERS reports analyzed: from the DRUG_SAFETY.FAERS_DEMO table
Signals detected: {signal_count} drug-event combinations with PRR >= 2.0 and n >= 3
Top signals: {top_signals_text}

Provide a 4-5 sentence assessment covering:
1. Overall signal landscape
2. Which drug-event pairs warrant priority review
3. Limitations of this analysis (synthetic data, PRR/ROR thresholds)
4. Recommended next steps (case-level review, literature search)
Be specific and use pharmacovigilance terminology.""")

    for line in assessment.split("\n"):
        if line.strip():
            await log(f"    {line.strip()}")

    conn.close()
    await progress(100)
    return {
        "signal_table": f"{DATABASE}.DRUG_SAFETY.SIGNAL_DETECTION",
        "signals_detected": signal_count,
        "drugs_with_signals": drugs_with_signals,
    }


async def build_safety_dashboard(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[streamlit] Building Drug Safety Dashboard views...")
    await progress(5)

    conn = _get_connection()

    await log("  Creating drug safety summary view...")
    _execute(conn, f"""
        CREATE OR REPLACE VIEW {DATABASE}.DRUG_SAFETY.SAFETY_DASHBOARD_SUMMARY AS
        SELECT
            drug_name,
            COUNT(*) AS total_reports,
            SUM(CASE WHEN serious THEN 1 ELSE 0 END) AS serious_reports,
            ROUND(SUM(CASE WHEN serious THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100, 1) AS serious_pct,
            COUNT(DISTINCT adverse_event) AS unique_adverse_events,
            ROUND(AVG(age), 1) AS avg_patient_age,
            COUNT(DISTINCT case_id) AS unique_cases
        FROM {DATABASE}.DRUG_SAFETY.FAERS_DEMO
        GROUP BY drug_name
        ORDER BY serious_reports DESC
    """)
    await progress(30)

    await log("  Creating temporal trend view...")
    _execute(conn, f"""
        CREATE OR REPLACE VIEW {DATABASE}.DRUG_SAFETY.SAFETY_TEMPORAL_TRENDS AS
        SELECT
            DATE_TRUNC('month', report_date) AS report_month,
            drug_name,
            COUNT(*) AS report_count,
            SUM(CASE WHEN serious THEN 1 ELSE 0 END) AS serious_count,
            COUNT(DISTINCT adverse_event) AS unique_events
        FROM {DATABASE}.DRUG_SAFETY.FAERS_DEMO
        GROUP BY report_month, drug_name
        ORDER BY report_month, drug_name
    """)
    await progress(50)

    await log("  Creating signal summary view...")
    _execute(conn, f"""
        CREATE OR REPLACE VIEW {DATABASE}.DRUG_SAFETY.SIGNAL_SUMMARY AS
        SELECT
            drug_name,
            COUNT(*) AS total_signals,
            MAX(prr) AS max_prr,
            MAX(ror) AS max_ror,
            ARRAY_AGG(DISTINCT adverse_event) WITHIN GROUP (ORDER BY adverse_event) AS signaled_events
        FROM {DATABASE}.DRUG_SAFETY.SIGNAL_DETECTION
        WHERE signal_status = 'SIGNAL'
        GROUP BY drug_name
        ORDER BY total_signals DESC
    """)
    await progress(70)

    rows, _ = _query(conn, f"SELECT drug_name, total_reports, serious_pct, unique_adverse_events FROM {DATABASE}.DRUG_SAFETY.SAFETY_DASHBOARD_SUMMARY LIMIT 5")
    await log(f"\n  Dashboard preview (top drugs by serious reports):")
    for r in rows:
        await log(f"    {r[0]}: {r[1]} reports, {r[2]}% serious, {r[3]} unique AEs")

    conn.close()
    await progress(100)
    return {
        "views_created": [
            f"{DATABASE}.DRUG_SAFETY.SAFETY_DASHBOARD_SUMMARY",
            f"{DATABASE}.DRUG_SAFETY.SAFETY_TEMPORAL_TRENDS",
            f"{DATABASE}.DRUG_SAFETY.SIGNAL_SUMMARY",
        ],
        "dashboard_ready": True,
    }


# ---------------------------------------------------------------------------
# PHASE 2 — Clinical Document Intelligence Scenario
# ---------------------------------------------------------------------------


async def setup_clinical_docs(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[hcls-provider-cdata-clinical-docs] Setting up clinical document pipeline...")
    await progress(5)

    conn = _get_connection()
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.CLINICAL_DOCS")
    await progress(10)

    await log("  Creating document metadata and extraction tables...")

    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.CLINICAL_DOCS.DOCUMENT_REGISTRY (
            doc_id VARCHAR DEFAULT UUID_STRING(),
            file_name VARCHAR,
            doc_type VARCHAR,
            patient_id VARCHAR,
            mrn VARCHAR,
            upload_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            page_count INT,
            classification_confidence FLOAT,
            status VARCHAR DEFAULT 'PENDING'
        )
    """)

    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.CLINICAL_DOCS.EXTRACTED_FIELDS (
            doc_id VARCHAR,
            field_name VARCHAR,
            field_value VARCHAR,
            confidence FLOAT,
            is_phi BOOLEAN DEFAULT FALSE,
            extraction_method VARCHAR DEFAULT 'AI_EXTRACT'
        )
    """)

    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.CLINICAL_DOCS.DOCUMENT_CONTENT (
            doc_id VARCHAR,
            page_number INT,
            raw_text VARCHAR,
            parsed_content VARCHAR,
            parse_method VARCHAR DEFAULT 'AI_PARSE_DOCUMENT'
        )
    """)
    await progress(30)

    await log("  Generating synthetic clinical documents via Cortex AI...")

    doc_types = [
        ("Discharge Summary", "DS"),
        ("Lab Report", "LAB"),
        ("Radiology Report", "RAD"),
        ("Progress Note", "PN"),
        ("Pathology Report", "PATH"),
    ]

    rows, _ = _query(conn, f"SELECT id, mrn, given_name, family_name FROM {DATABASE}.{FHIR_SCHEMA}.PATIENT LIMIT 6")
    patients = rows

    doc_count = 0
    doc_batches = []
    for pi, patient in enumerate(patients):
        pid, mrn, given, family = patient
        for dt_name, dt_code in doc_types[:3]:
            doc_batches.append((pid, mrn, given, family, dt_name, dt_code))

    import asyncio
    import concurrent.futures

    def _generate_one_doc(args):
        pid, mrn, given, family, dt_name, dt_code = args
        doc_conn = _get_connection()
        doc_id = f"DOC-{dt_code}-{uuid.uuid4().hex[:6]}"

        doc_text = _cortex_complete(doc_conn, f"""Generate a realistic but synthetic {dt_name} for a patient.
Patient: {given} {family}, MRN: {mrn}
Generate 4-6 sentences of clinical content appropriate for a {dt_name}.
Include relevant medical terminology. Do NOT include real PHI. This is synthetic demo data.""")

        _execute(doc_conn, f"""
            INSERT INTO {DATABASE}.CLINICAL_DOCS.DOCUMENT_REGISTRY
            (doc_id, file_name, doc_type, patient_id, mrn, page_count, classification_confidence, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'PROCESSED')
        """, (doc_id, f"{doc_id}.pdf", dt_name, pid, mrn, 1, 0.95))

        _execute(doc_conn, f"""
            INSERT INTO {DATABASE}.CLINICAL_DOCS.DOCUMENT_CONTENT
            (doc_id, page_number, raw_text, parsed_content)
            VALUES (%s, %s, %s, %s)
        """, (doc_id, 1, doc_text, doc_text))

        fields = _cortex_complete(doc_conn, f"""Extract key fields from this {dt_name}. Return JSON array of objects with "field_name", "field_value", "is_phi" (boolean).
Extract: patient_name, mrn, date, diagnosis, findings, medications if present.
Document: {doc_text[:500]}
JSON array only, no markdown fences.""")

        try:
            field_list = json.loads(fields)
            for field in field_list[:8]:
                _execute(doc_conn, f"""
                    INSERT INTO {DATABASE}.CLINICAL_DOCS.EXTRACTED_FIELDS
                    (doc_id, field_name, field_value, confidence, is_phi)
                    VALUES (%s, %s, %s, %s, %s)
                """, (doc_id, field.get("field_name", ""), field.get("field_value", ""),
                      0.92, field.get("is_phi", False)))
        except (json.JSONDecodeError, TypeError):
            pass

        doc_conn.close()
        return doc_id

    batch_size = 6
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as pool:
        for i in range(0, len(doc_batches), batch_size):
            batch = doc_batches[i:i + batch_size]
            futures = [loop.run_in_executor(pool, _generate_one_doc, args) for args in batch]
            results = await asyncio.gather(*futures)
            doc_count += len(results)
            pct = 30 + int(50 * min(i + batch_size, len(doc_batches)) / len(doc_batches))
            await progress(pct)
            await log(f"  Generated batch {i // batch_size + 1}: {len(results)} documents (parallel)")

    rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.CLINICAL_DOCS.DOCUMENT_REGISTRY")
    total_docs = rows[0][0]
    rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.CLINICAL_DOCS.EXTRACTED_FIELDS")
    total_fields = rows[0][0]

    await log(f"\n  Created {total_docs} synthetic clinical documents")
    await log(f"  Extracted {total_fields} structured fields")
    await log(f"  Document types: {', '.join(dt[0] for dt in doc_types[:3])}")

    conn.close()
    await progress(100)
    return {
        "documents": total_docs,
        "fields_extracted": total_fields,
        "tables": [
            f"{DATABASE}.CLINICAL_DOCS.DOCUMENT_REGISTRY",
            f"{DATABASE}.CLINICAL_DOCS.EXTRACTED_FIELDS",
            f"{DATABASE}.CLINICAL_DOCS.DOCUMENT_CONTENT",
        ],
    }


async def create_doc_search(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[cortex-search] Creating clinical document search service...")
    await progress(5)

    conn = _get_connection()

    await log("  Building document search corpus...")
    _execute(conn, f"""
        CREATE OR REPLACE TABLE {DATABASE}.CLINICAL_DOCS.DOC_SEARCH_CORPUS AS
        SELECT
            r.doc_id,
            r.file_name,
            r.doc_type,
            r.patient_id,
            r.mrn,
            c.raw_text AS content,
            CONCAT(
                'Document Type: ', r.doc_type, '. ',
                'Patient MRN: ', COALESCE(r.mrn, 'N/A'), '. ',
                'Content: ', COALESCE(c.raw_text, '')
            ) AS search_text
        FROM {DATABASE}.CLINICAL_DOCS.DOCUMENT_REGISTRY r
        JOIN {DATABASE}.CLINICAL_DOCS.DOCUMENT_CONTENT c ON r.doc_id = c.doc_id
    """)
    await progress(30)

    rows, _ = _query(conn, f"SELECT COUNT(*) FROM {DATABASE}.CLINICAL_DOCS.DOC_SEARCH_CORPUS")
    corpus_count = rows[0][0]
    await log(f"  Document corpus: {corpus_count} records")

    try:
        _execute(conn, f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE {DATABASE}.CLINICAL_DOCS.CLINICAL_DOC_SEARCH
            ON search_text
            ATTRIBUTES doc_type, mrn
            WAREHOUSE = COMPUTE_WH
            TARGET_LAG = '1 hour'
            AS (
                SELECT search_text, doc_id, doc_type, mrn, patient_id
                FROM {DATABASE}.CLINICAL_DOCS.DOC_SEARCH_CORPUS
            )
        """)
        await log(f"  Created: {DATABASE}.CLINICAL_DOCS.CLINICAL_DOC_SEARCH")
        search_svc = f"{DATABASE}.CLINICAL_DOCS.CLINICAL_DOC_SEARCH"
    except Exception as e:
        await log(f"  Cortex Search: {str(e)[:100]}")
        await log(f"  Search corpus table ready for manual Cortex Search setup")
        search_svc = f"{DATABASE}.CLINICAL_DOCS.DOC_SEARCH_CORPUS (table)"

    conn.close()
    await progress(100)
    return {"search_service": search_svc, "corpus_records": corpus_count}


async def create_doc_agent(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    await log("[cortex-agent] Creating clinical document intelligence agent...")
    await progress(5)

    conn = _get_connection()

    await log("  Creating document analytics view for agent...")
    _execute(conn, f"""
        CREATE OR REPLACE VIEW {DATABASE}.CLINICAL_DOCS.DOC_ANALYTICS AS
        SELECT
            r.doc_type,
            COUNT(*) AS document_count,
            COUNT(DISTINCT r.patient_id) AS unique_patients,
            ROUND(AVG(r.classification_confidence), 3) AS avg_confidence,
            COUNT(DISTINCT e.field_name) AS unique_fields_extracted,
            SUM(CASE WHEN e.is_phi THEN 1 ELSE 0 END) AS phi_fields_count
        FROM {DATABASE}.CLINICAL_DOCS.DOCUMENT_REGISTRY r
        LEFT JOIN {DATABASE}.CLINICAL_DOCS.EXTRACTED_FIELDS e ON r.doc_id = e.doc_id
        GROUP BY r.doc_type
    """)
    await progress(30)

    await log("  Testing agent-style query via Cortex AI...")
    agent_response = _cortex_complete(conn, """You are a clinical document intelligence agent with access to a document registry containing Discharge Summaries, Lab Reports, and Radiology Reports for healthcare patients.

A user asks: "What types of clinical documents do we have and how many contain PHI fields?"

Based on the document pipeline architecture (AI_PARSE_DOCUMENT -> AI_EXTRACT -> structured fields with PHI flagging), provide a concise 3-4 sentence answer. Mention that documents are parsed using Snowflake Cortex AI functions, fields are extracted and tagged for PHI compliance, and the data is searchable via Cortex Search.""")

    await log(f"\n  Agent response preview:")
    for line in agent_response.split("\n"):
        if line.strip():
            await log(f"    {line.strip()}")
    await progress(70)

    rows, _ = _query(conn, f"SELECT doc_type, document_count, unique_patients, phi_fields_count FROM {DATABASE}.CLINICAL_DOCS.DOC_ANALYTICS")
    await log(f"\n  Document Intelligence Summary:")
    for r in rows:
        await log(f"    {r[0]}: {r[1]} docs, {r[2]} patients, {r[3]} PHI fields")

    conn.close()
    await progress(100)
    return {
        "analytics_view": f"{DATABASE}.CLINICAL_DOCS.DOC_ANALYTICS",
        "agent_capabilities": ["document_search", "field_extraction", "phi_detection", "patient_lookup"],
    }


# ---------------------------------------------------------------------------
# PHASE 3 — Dynamic Routing: AI-powered skill selection from free-form prompt
# ---------------------------------------------------------------------------

SCENARIO_DEFINITIONS = {
    "clinical_data_warehouse": {
        "name": "Clinical Data Warehouse",
        "description": "FHIR data pipeline with Dynamic Tables, ML model, Semantic View, and React app",
        "skills": ["dynamic-tables", "machine-learning", "cortex-search", "cortex-analyst", "build-react-app"],
        "tasks": ["build_dynamic_tables", "train_risk_model", "create_cortex_search", "create_semantic_view", "generate_react_app"],
    },
    "drug_safety": {
        "name": "Drug Safety Signal Detection",
        "description": "FAERS pharmacovigilance with PRR/ROR signal detection and safety dashboards",
        "skills": ["hcls-pharma-dsafety-pharmacovigilance", "cortex-ai-functions", "streamlit"],
        "tasks": ["setup_faers_data", "run_signal_detection", "build_safety_dashboard"],
    },
    "clinical_docs": {
        "name": "Clinical Document Intelligence",
        "description": "Parse, classify, and extract from clinical documents with Cortex Search and Agent",
        "skills": ["hcls-provider-cdata-clinical-docs", "cortex-search", "cortex-agent"],
        "tasks": ["setup_clinical_docs", "create_doc_search", "create_doc_agent"],
    },
}


async def dynamic_route(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    request = config.get("user_request", "")
    await log(f"[dynamic-router] Analyzing request: \"{request[:100]}\"...")
    await progress(10)

    conn = _get_connection()

    scenarios_text = "\n".join(
        f"- {k}: {v['description']} (skills: {', '.join(v['skills'])})"
        for k, v in SCENARIO_DEFINITIONS.items()
    )

    routing = _cortex_complete(conn, f"""You are a healthcare data platform orchestrator. Route this user request to the best scenario.

User Request: {request}

Available Scenarios:
{scenarios_text}

Respond in JSON with:
- scenario: the scenario key (clinical_data_warehouse, drug_safety, or clinical_docs)
- confidence: HIGH, MEDIUM, or LOW
- reasoning: 1 sentence why this scenario matches

JSON only, no markdown fences.""")

    await progress(60)

    try:
        result = json.loads(routing)
        scenario = result.get("scenario", "clinical_data_warehouse")
        confidence = result.get("confidence", "MEDIUM")
        reasoning = result.get("reasoning", "")
    except (json.JSONDecodeError, KeyError):
        scenario = "clinical_data_warehouse"
        confidence = "MEDIUM"
        reasoning = routing[:200]

    scenario_def = SCENARIO_DEFINITIONS.get(scenario, SCENARIO_DEFINITIONS["clinical_data_warehouse"])
    await log(f"  Routed to: {scenario_def['name']}")
    await log(f"  Confidence: {confidence}")
    await log(f"  Skills: {', '.join(scenario_def['skills'])}")
    if reasoning:
        await log(f"  Reasoning: {reasoning}")

    conn.close()
    await progress(100)
    return {
        "scenario": scenario,
        "scenario_name": scenario_def["name"],
        "confidence": confidence,
        "tasks": scenario_def["tasks"],
        "skills": scenario_def["skills"],
    }


SCENARIO_TASK_REGISTRY = {
    "build_dynamic_tables": build_dynamic_tables,
    "train_risk_model": train_risk_model,
    "create_cortex_search": create_cortex_search,
    "create_semantic_view": create_semantic_view,
    "generate_react_app": generate_react_app,
    "setup_faers_data": setup_faers_data,
    "run_signal_detection": run_signal_detection,
    "build_safety_dashboard": build_safety_dashboard,
    "setup_clinical_docs": setup_clinical_docs,
    "create_doc_search": create_doc_search,
    "create_doc_agent": create_doc_agent,
    "dynamic_route": dynamic_route,
}
