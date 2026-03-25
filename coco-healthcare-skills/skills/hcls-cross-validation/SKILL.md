---
name: hcls-cross-validation
description: >-
  Cross-cutting data validation skill for healthcare pipelines.
  Runs data completeness checks, schema consistency validation, and semantic
  quality assessment against FHIR, OMOP, DICOM, and clinical document tables.
  Invoke after any skill that creates or transforms tables. Triggers include
  validate, data quality, completeness check, schema validation, semantic check,
  QA, conformance, data integrity, verify pipeline output.
tools: ["*"]
platform_affinities:
  produces: [validation_report]
  benefits_from:
    - skill: data-quality
      when: "ongoing monitoring with Snowflake Data Metric Functions"
    - skill: data-governance
      when: "validation reveals PHI in unprotected columns"
---

# Cross-Cutting Data Validation

Validate healthcare data pipelines for completeness, schema consistency, and
semantic correctness. Available to all sub-industries as a cross-cutting concern.

## When to Use This Skill

- After loading FHIR bundles — verify resource counts and required fields
- After OMOP CDM transformation — validate vocabulary mappings and referential integrity
- After DICOM ingestion — verify tag completeness and study/series/instance hierarchy
- After clinical document extraction — validate extracted fields against doc type specs
- After any data pipeline — general completeness and null-rate checks
- When a user asks to "validate", "QA", or "check" pipeline outputs

## Validation Suites

### 1. Data Completeness

Checks that required columns have acceptable fill rates:

```python
python scripts/validate.py completeness \
    --table MY_DB.MY_SCHEMA.PATIENT \
    --domain fhir \
    --connection $SNOWFLAKE_CONNECTION_NAME
```

Checks:
- Null rate per column (flag if > threshold)
- Row count (flag if 0 or below expected)
- Required-field coverage per domain (FHIR requires id, resourceType; OMOP requires person_id, etc.)

### 2. Schema Consistency

Validates table schemas match expected data model definitions:

```python
python scripts/validate.py schema \
    --table MY_DB.MY_SCHEMA.OBSERVATION \
    --domain fhir \
    --connection $SNOWFLAKE_CONNECTION_NAME
```

Checks:
- All expected columns exist
- Data types match expected types (VARCHAR vs NUMBER, etc.)
- No unexpected extra columns (warning, not failure)
- Primary key columns are NOT NULL and unique

### 3. Semantic Validation

Domain-specific correctness checks:

```python
python scripts/validate.py semantic \
    --table MY_DB.MY_SCHEMA.CONDITION \
    --domain fhir \
    --connection $SNOWFLAKE_CONNECTION_NAME
```

Checks by domain:

| Domain | Semantic Checks |
|--------|-----------------|
| FHIR | Valid resource references (Patient/xxx format), known code systems (SNOMED, LOINC, ICD-10), valid status values |
| OMOP | Valid concept_ids (exist in vocabulary), valid domain assignments, referential integrity (person_id exists) |
| DICOM | Valid DICOM UIDs (format check), modality values in allowed set, study-series-instance hierarchy intact |
| Clinical Docs | Document types match extraction spec, required extracted fields present, confidence scores above threshold |

### 4. Full Validation (All Three)

```python
python scripts/validate.py all \
    --table MY_DB.MY_SCHEMA.OBSERVATION \
    --domain fhir \
    --connection $SNOWFLAKE_CONNECTION_NAME
```

## Output Format

The validator produces a structured JSON report:

```json
{
  "table": "MY_DB.MY_SCHEMA.OBSERVATION",
  "domain": "fhir",
  "timestamp": "2026-03-25T10:30:00Z",
  "suites": {
    "completeness": {
      "status": "PASS",
      "checks": 12,
      "passed": 11,
      "warnings": 1,
      "failures": 0,
      "details": [...]
    },
    "schema": {
      "status": "PASS",
      "checks": 8,
      "passed": 8,
      "details": [...]
    },
    "semantic": {
      "status": "WARN",
      "checks": 5,
      "passed": 4,
      "warnings": 1,
      "details": [...]
    }
  },
  "overall_status": "WARN"
}
```

## Domain Definitions

Domain definitions are in `scripts/domains/`. Each domain YAML defines:

```yaml
domain: fhir
tables:
  patient:
    required_columns: [id, resource_type, family_name, given_name, birth_date, gender]
    primary_key: [id]
    type_expectations:
      id: VARCHAR
      birth_date: DATE
      gender: VARCHAR
    semantic_checks:
      - type: allowed_values
        column: gender
        values: [male, female, other, unknown]
      - type: reference_format
        column: id
        pattern: "^[A-Za-z0-9\\-\\.]{1,64}$"
```

## Preflight Check

This skill requires only a Snowflake connection and the target table to exist.
No external dependencies (no Marketplace listings, no Cortex Search services).

```python
from shared.preflight.checker import PreflightChecker

checker = PreflightChecker(conn)
checker.add_table(
    name="Target table",
    fqn=target_table_fqn,
    setup="Ensure the target table exists before running validation.",
    required=True,
)
results = checker.run()
```

## Integration with Observability

Validation results can be logged to the governance audit trail:

```python
from shared.observability.logger import ExecutionLogger

logger = ExecutionLogger(conn, database="MY_DB", schema="OBSERVABILITY")
logger.log_governance_action(
    session_id=session_id,
    skill_name="hcls-cross-validation",
    governance_action="DATA_VALIDATION",
    target_object=table_fqn,
    policy_type="quality_check",
    policy_definition=validation_report,
)
```
