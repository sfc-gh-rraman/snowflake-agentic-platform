---
name: hcls-provider-cdata-fhir
description: Transform FHIR (Fast Healthcare Interoperability Resources) data into relational tables for analytics. Use when parsing FHIR bundles, extracting resources (Patient, Observation, Condition, MedicationRequest, etc.), flattening nested JSON, or loading healthcare data into Snowflake. Triggers include FHIR, HL7, healthcare interoperability, Patient resource, Observation, Condition, Bundle, ndjson, healthcare JSON.
---

# FHIR Data Transformation

Transform FHIR R4 resources into analytics-ready relational tables in Snowflake.

## When to Use This Skill

- Parsing FHIR Bundles (JSON or NDJSON)
- Extracting specific resource types (Patient, Observation, Condition, etc.)
- Flattening nested FHIR structures for SQL analytics
- Loading FHIR data into Snowflake tables
- Building healthcare data pipelines from EHR exports

## Quick Start

```python
# Parse a FHIR Bundle and load to Snowflake
python scripts/fhir_to_tables.py bundle.json --output-dir ./output

# Parse NDJSON (one resource per line)
python scripts/fhir_to_tables.py patients.ndjson --format ndjson

# Load directly to Snowflake
python scripts/fhir_to_tables.py bundle.json --snowflake --database HEALTHCARE --schema FHIR_RAW
```

## Supported Resource Types

| Resource | Table Name | Key Fields Extracted |
|----------|------------|---------------------|
| Patient | `patient` | id, name, birthDate, gender, address, telecom, identifier |
| Observation | `observation` | id, patient_id, code, value, effectiveDateTime, status |
| Condition | `condition` | id, patient_id, code, clinicalStatus, onsetDateTime |
| MedicationRequest | `medication_request` | id, patient_id, medication, authoredOn, status, dosage |
| Encounter | `encounter` | id, patient_id, type, period_start, period_end, status |
| Procedure | `procedure` | id, patient_id, code, performedDateTime, status |
| DiagnosticReport | `diagnostic_report` | id, patient_id, code, effectiveDateTime, conclusion |
| Immunization | `immunization` | id, patient_id, vaccineCode, occurrenceDateTime, status |
| AllergyIntolerance | `allergy_intolerance` | id, patient_id, code, clinicalStatus, type |
| CarePlan | `care_plan` | id, patient_id, status, intent, category, period |

## Workflow

### Step 1: Analyze FHIR Data

First, understand what's in your FHIR file:

```python
python scripts/analyze_fhir.py input.json
```

Output shows:
- Resource type counts
- Date ranges
- Patient count
- Code systems used (SNOMED, LOINC, ICD-10, etc.)

### Step 2: Transform to Tables

```python
python scripts/fhir_to_tables.py input.json \
    --resources Patient,Observation,Condition \
    --output-dir ./fhir_tables
```

Outputs CSV files per resource type, ready for Snowflake COPY INTO.

### Step 3: Load to Snowflake

```sql
-- Create stage
CREATE OR REPLACE STAGE fhir_stage;

-- Put files
PUT file://./fhir_tables/*.csv @fhir_stage;

-- Create and load Patient table
CREATE OR REPLACE TABLE patient (
    id VARCHAR,
    family_name VARCHAR,
    given_name VARCHAR,
    birth_date DATE,
    gender VARCHAR,
    address_city VARCHAR,
    address_state VARCHAR,
    address_postal_code VARCHAR,
    phone VARCHAR,
    email VARCHAR,
    mrn VARCHAR,
    ssn VARCHAR,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO patient FROM @fhir_stage/patient.csv
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
```

## Handling Nested Data

FHIR resources contain deeply nested structures. This skill provides two approaches:

### Approach 1: Flattened Tables (Recommended for Analytics)

Each resource becomes a wide, denormalized table:

```
Observation → observation table with columns:
  - id, patient_id, status
  - code_system, code_code, code_display (from CodeableConcept)
  - value_quantity, value_unit (from valueQuantity)
  - effective_datetime (from effectiveDateTime)
```

### Approach 2: Raw JSON + Views (Recommended for Flexibility)

Load raw FHIR JSON into a VARIANT column, then create views:

```sql
-- Raw table
CREATE TABLE fhir_raw (
    resource_type VARCHAR,
    resource_id VARCHAR,
    data VARIANT,
    _loaded_at TIMESTAMP_NTZ
);

-- View for observations
CREATE VIEW v_observation AS
SELECT
    data:id::VARCHAR AS id,
    data:subject:reference::VARCHAR AS patient_ref,
    data:code:coding[0]:system::VARCHAR AS code_system,
    data:code:coding[0]:code::VARCHAR AS code_code,
    data:code:coding[0]:display::VARCHAR AS code_display,
    data:valueQuantity:value::FLOAT AS value,
    data:valueQuantity:unit::VARCHAR AS unit,
    data:effectiveDateTime::TIMESTAMP AS effective_datetime
FROM fhir_raw
WHERE resource_type = 'Observation';
```

## Code System Mappings

Common code systems in FHIR data:

| System URI | Name | Used For |
|------------|------|----------|
| `http://snomed.info/sct` | SNOMED CT | Conditions, procedures |
| `http://loinc.org` | LOINC | Lab observations |
| `http://hl7.org/fhir/sid/icd-10-cm` | ICD-10-CM | Diagnoses |
| `http://www.nlm.nih.gov/research/umls/rxnorm` | RxNorm | Medications |
| `http://hl7.org/fhir/sid/cvx` | CVX | Vaccines |
| `http://hl7.org/fhir/sid/ndc` | NDC | Drug products |

## Synthea Test Data

For testing, use [Synthea](https://github.com/synthetichealth/synthea) synthetic patient data:

```bash
# Download sample Synthea data
curl -L -o synthea_sample.zip https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_fhir_r4_sep2019.zip
unzip synthea_sample.zip
```

## Best Practices

1. **Preserve raw data**: Always keep original FHIR JSON in a raw table
2. **Track lineage**: Include `_source_file` and `_loaded_at` columns
3. **Handle nulls**: FHIR fields are often optional; use COALESCE or NVL
4. **Parse references**: Patient references like `Patient/123` need splitting
5. **Normalize codes**: Create lookup tables for code systems

## Reference Files

- `references/fhir_r4_resources.md` - Resource structure reference
- `references/common_code_systems.md` - Code system details
- `references/snowflake_schema.md` - Recommended Snowflake schema design

## Requirements

```
fhir.resources>=7.0.0
pandas>=2.0.0
snowflake-connector-python>=3.0.0
```
