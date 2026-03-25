---
name: hcls-provider-cdata-omop
description: Transform clinical data to the OMOP Common Data Model for observational research. Use when converting EHR data, claims data, or other clinical sources to OMOP CDM v5.4 tables (PERSON, VISIT_OCCURRENCE, CONDITION_OCCURRENCE, DRUG_EXPOSURE, etc.). Triggers include OMOP, CDM, Common Data Model, OHDSI, observational research, cohort definition, claims transformation, vocabulary mapping.
---

# OMOP CDM Data Modeling

Transform clinical data sources into the OMOP Common Data Model (CDM) v5.4 for standardized observational research.

## When to Use This Skill

- Converting EHR extracts to OMOP CDM format
- Transforming healthcare claims data
- Building OHDSI-compliant research databases
- Mapping source codes to OMOP standard vocabularies
- Creating ETL pipelines for clinical data warehouses

## OMOP CDM Overview

The OMOP CDM is a standardized data model that enables:
- Consistent representation of clinical data across institutions
- Federated network studies (OHDSI)
- Reusable analytic tools (ATLAS, HADES)
- Standard vocabulary mapping (SNOMED, LOINC, RxNorm)

## Core Clinical Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| PERSON | Patient demographics | person_id, gender_concept_id, birth_datetime, race_concept_id |
| VISIT_OCCURRENCE | Encounters/visits | visit_occurrence_id, person_id, visit_concept_id, visit_start/end_date |
| CONDITION_OCCURRENCE | Diagnoses | condition_concept_id, person_id, condition_start_date |
| DRUG_EXPOSURE | Medications | drug_concept_id, person_id, drug_exposure_start_date, quantity |
| PROCEDURE_OCCURRENCE | Procedures | procedure_concept_id, person_id, procedure_date |
| MEASUREMENT | Labs/vitals | measurement_concept_id, person_id, value_as_number, unit_concept_id |
| OBSERVATION | Other clinical facts | observation_concept_id, person_id, value_as_string |
| DEATH | Mortality | person_id, death_date, cause_concept_id |

## Quick Start

### Step 1: Set Up OMOP Schema in Snowflake

```sql
-- Create OMOP database and schema
CREATE DATABASE IF NOT EXISTS OMOP_CDM;
CREATE SCHEMA IF NOT EXISTS OMOP_CDM.CDM_V54;

-- Create DDL from script
python scripts/create_omop_ddl.py --dialect snowflake > omop_ddl.sql

-- Execute DDL
-- Or use: scripts/snowflake_omop_ddl.sql
```

### Step 2: Load Vocabulary Tables

Download vocabularies from [Athena](https://athena.ohdsi.org/):
1. Register/login at athena.ohdsi.org
2. Select vocabularies (SNOMED, LOINC, RxNorm, ICD10CM, etc.)
3. Download and unzip

```sql
-- Create stage for vocabulary files
CREATE STAGE vocab_stage;
PUT file://./vocabulary/*.csv @vocab_stage;

-- Load vocabulary tables
COPY INTO CONCEPT FROM @vocab_stage/CONCEPT.csv
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\t' SKIP_HEADER = 1);
    
COPY INTO CONCEPT_RELATIONSHIP FROM @vocab_stage/CONCEPT_RELATIONSHIP.csv
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\t' SKIP_HEADER = 1);
    
-- Continue for other vocab tables...
```

### Step 3: Map Source Data

```python
# Example: Map ICD-10 codes to OMOP concept_ids
python scripts/map_to_omop.py \
    --source-codes icd10_codes.csv \
    --vocabulary ICD10CM \
    --output mapped_conditions.csv
```

### Step 4: Transform and Load

```python
# Transform source data to OMOP format
python scripts/transform_to_omop.py \
    --source-type claims \
    --input claims_data.csv \
    --output-dir ./omop_tables/
```

## Vocabulary Mapping

OMOP uses standard vocabularies. Map your source codes using CONCEPT_RELATIONSHIP:

```sql
-- Map ICD-10-CM to SNOMED
SELECT 
    c1.concept_code AS source_code,
    c1.concept_name AS source_name,
    c2.concept_id AS standard_concept_id,
    c2.concept_name AS standard_name
FROM CONCEPT c1
JOIN CONCEPT_RELATIONSHIP cr ON c1.concept_id = cr.concept_id_1
JOIN CONCEPT c2 ON cr.concept_id_2 = c2.concept_id
WHERE c1.vocabulary_id = 'ICD10CM'
  AND cr.relationship_id = 'Maps to'
  AND c2.standard_concept = 'S'
  AND c1.concept_code = 'E11.9';  -- Type 2 diabetes
```

### Common Vocabulary Mappings

| Source System | Source Vocabulary | Target Domain | Standard Vocabulary |
|---------------|-------------------|---------------|---------------------|
| ICD-10-CM | Diagnosis codes | Condition | SNOMED |
| ICD-10-PCS | Procedure codes | Procedure | SNOMED |
| CPT4/HCPCS | Procedure codes | Procedure | CPT4/HCPCS |
| NDC | Drug codes | Drug | RxNorm |
| LOINC | Lab codes | Measurement | LOINC |

## ETL Patterns

### Person Table

```sql
INSERT INTO PERSON (
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    person_source_value,
    gender_source_value,
    race_source_value,
    ethnicity_source_value
)
SELECT
    patient_id,
    CASE gender 
        WHEN 'M' THEN 8507  -- Male
        WHEN 'F' THEN 8532  -- Female
        ELSE 0
    END,
    YEAR(birth_date),
    MONTH(birth_date),
    DAY(birth_date),
    birth_date,
    COALESCE(race_concept_map.concept_id, 0),
    COALESCE(ethnicity_concept_map.concept_id, 0),
    NULL,
    patient_id,
    gender,
    race,
    ethnicity
FROM source_patients
LEFT JOIN concept race_concept_map 
    ON source_patients.race = race_concept_map.concept_code
    AND race_concept_map.vocabulary_id = 'Race'
LEFT JOIN concept ethnicity_concept_map
    ON source_patients.ethnicity = ethnicity_concept_map.concept_code
    AND ethnicity_concept_map.vocabulary_id = 'Ethnicity';
```

### Condition Occurrence

```sql
INSERT INTO CONDITION_OCCURRENCE (
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_type_concept_id,
    condition_source_value,
    condition_source_concept_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY patient_id, diagnosis_date),
    patient_id,
    COALESCE(standard_concept.concept_id, 0),
    diagnosis_date,
    diagnosis_date,
    NULL,
    32817,  -- EHR encounter diagnosis
    icd10_code,
    source_concept.concept_id
FROM source_diagnoses
LEFT JOIN concept source_concept 
    ON source_diagnoses.icd10_code = source_concept.concept_code
    AND source_concept.vocabulary_id = 'ICD10CM'
LEFT JOIN concept_relationship cr 
    ON source_concept.concept_id = cr.concept_id_1
    AND cr.relationship_id = 'Maps to'
LEFT JOIN concept standard_concept 
    ON cr.concept_id_2 = standard_concept.concept_id
    AND standard_concept.standard_concept = 'S';
```

## Data Quality Checks

Use OHDSI Data Quality Dashboard checks:

```sql
-- Check for unmapped conditions
SELECT 
    condition_source_value,
    COUNT(*) as count
FROM CONDITION_OCCURRENCE
WHERE condition_concept_id = 0
GROUP BY condition_source_value
ORDER BY count DESC
LIMIT 100;

-- Check for future dates
SELECT COUNT(*) 
FROM CONDITION_OCCURRENCE 
WHERE condition_start_date > CURRENT_DATE;

-- Check person coverage
SELECT 
    'CONDITION_OCCURRENCE' as table_name,
    COUNT(DISTINCT person_id) as persons_with_data,
    (SELECT COUNT(*) FROM PERSON) as total_persons,
    ROUND(COUNT(DISTINCT person_id) * 100.0 / (SELECT COUNT(*) FROM PERSON), 2) as coverage_pct
FROM CONDITION_OCCURRENCE;
```

## Reference Files

- `references/omop_cdm_v54_schema.md` - Full schema documentation
- `references/vocabulary_mapping.md` - Vocabulary mapping guide
- `references/etl_conventions.md` - ETL best practices
- `scripts/snowflake_omop_ddl.sql` - Snowflake DDL for OMOP CDM

## Resources

- [OHDSI OMOP CDM](https://ohdsi.github.io/CommonDataModel/)
- [Athena Vocabulary](https://athena.ohdsi.org/)
- [Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/)
- [OHDSI Forums](https://forums.ohdsi.org/)

## Requirements

```
pandas>=2.0.0
snowflake-connector-python>=3.0.0
```
