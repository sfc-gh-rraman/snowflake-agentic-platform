---
name: data-model-knowledge
description: "Query the DICOM data model knowledge repository (Cortex Search Service) to retrieve latest table definitions, column specifications, DICOM tags, PHI indicators, and relationship metadata at runtime. Use when building schemas, generating DDL, validating data models, or answering data model questions."
parent_skill: hcls-provider-imaging
---

# Data Model Knowledge Repository

## Preflight Check (REQUIRED -- Run Before Any Query)

Before executing any data model search, verify both the reference table and Cortex Search Service exist:

### Check 1: Reference Table

```sql
SELECT COUNT(*) FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_DOCS LIMIT 1;
```

### Check 2: Cortex Search Service

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "test", "columns": ["CONTENT"], "limit": 1}'
);
```

| Check 1 | Check 2 | Status | Action |
|---------|---------|--------|--------|
| OK | OK | READY | Proceed with dynamic data model queries |
| OK | FAIL | PARTIAL | Table exists but search service is missing -- guide user to recreate search service (see Setup) |
| FAIL | FAIL | MISSING | Guide user through full Setup below |
| ERROR | ERROR | ERROR | Show error, check permissions on `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE` |

### Fallback (When MISSING or PARTIAL)

If the Cortex Search Service is not available:
- **Use hardcoded DICOM schema definitions** from `dicom-parser/SKILL.md` references section
- **Inform the user**: "DICOM data model search service is not available -- using hardcoded schema definitions. Results may not reflect the latest data model updates."
- **Never block the parent skill** -- the imaging router and sub-skills must continue to work with hardcoded fallbacks

### Auto-Detection for Imaging Router

The imaging router runs this preflight as part of its Step 0 (Data Model Knowledge pre-step):
1. Run both probes above
2. If READY -- use dynamic search results to ground DDL/pipeline generation
3. If MISSING -- fall back to hardcoded schemas, note the fallback in output
4. Set `$DMK_AVAILABLE` context flag for sub-skills to check

## When to Load

Healthcare-imaging router or any skill that needs to reference the DICOM data model at runtime instead of relying on hardcoded schema definitions.

## Architecture

```
Excel Spreadsheet (dicom_data_model_reference.xlsx)
    |
    v  export_search_corpus_csv.py
CSV (dicom_model_search_corpus.csv)
    |
    v  COPY INTO
Snowflake Table (DICOM_MODEL_REFERENCE)
    |
    v  Cortex Search Service
DICOM_MODEL_SEARCH_SVC  <-- Skills query this at runtime
```

## Prerequisites

- Cortex Search Service `DICOM_MODEL_SEARCH_SVC` created in `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE`
- Run `scripts/setup_dicom_model_knowledge_repo.sql` for initial setup
- Warehouse with `SNOWFLAKE.CORTEX_USER` database role granted

## Setup (One-Time)

### 1. Generate or Update the Spreadsheet

If starting fresh, generate from the 18-table DICOM model:
```bash
python scripts/generate_dicom_model_spreadsheet.py
```

If updating an existing spreadsheet, edit `references/dicom_data_model_reference.xlsx` directly.

### 2. Export Search Corpus

```bash
python scripts/export_search_corpus_csv.py
```

### 3. Load to Snowflake and Create Search Service

```sql
-- Run the setup script
-- scripts/setup_dicom_model_knowledge_repo.sql
```

Or manually:
```sql
USE DATABASE UNSTRUCTURED_HEALTHDATA;
USE SCHEMA DATA_MODEL_KNOWLEDGE;

PUT file://references/dicom_model_search_corpus.csv @dicom_model_stage AUTO_COMPRESS=FALSE;

COPY INTO DICOM_MODEL_REFERENCE
FROM @dicom_model_stage/dicom_model_search_corpus.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE CORTEX SEARCH SERVICE DICOM_MODEL_SEARCH_SVC
    ON search_text
    ATTRIBUTES table_name, column_name, data_type, category, contains_phi
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
AS (
    SELECT search_text, table_name, column_name, data_type, dicom_tag,
           category, description, constraints, contains_phi, relationships
    FROM DICOM_MODEL_REFERENCE
);
```

## Runtime Query Patterns

### Pattern 1: Find Tables for a Use Case

When a user describes what they need, search the model to find relevant tables/columns:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "patient demographics and identifiers", "columns": ["table_name", "column_name", "data_type", "description", "contains_phi"]}'
);
```

### Pattern 2: Generate DDL from Search Results

Search for columns of a specific table, then generate CREATE TABLE:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "dicom_dose_summary radiation exposure", "columns": ["table_name", "column_name", "data_type", "constraints", "description"]}'
);
```

Use the results to construct DDL dynamically with Cortex AI:

```sql
WITH model_knowledge AS (
    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
        '{"query": "dose summary radiation CT exposure", "columns": ["table_name", "column_name", "data_type", "constraints", "description"]}'
    ) AS context
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-70b',
    'Based on this data model reference, generate a Snowflake CREATE TABLE DDL statement for the dose summary table. Use the exact column names, data types, and constraints from the reference. Reference: ' || context::STRING
) AS generated_ddl
FROM model_knowledge;
```

### Pattern 3: Identify PHI Columns

Find all columns containing Protected Health Information:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "PHI protected health information patient name birth date", "columns": ["table_name", "column_name", "description", "contains_phi"]}'
);
```

### Pattern 4: Explore Relationships

Find foreign key relationships and entity hierarchy:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "foreign key relationship patient study series instance", "columns": ["table_name", "column_name", "constraints", "description"]}'
);
```

### Pattern 5: Modality-Specific Schemas

Find tables/columns relevant to a specific imaging modality:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "segmentation SEG metadata segment label anatomic region", "columns": ["table_name", "column_name", "data_type", "description", "dicom_tag"]}'
);
```

## Integration with Other Skills

### dicom-parser
Before generating DDL, query this service to get the latest model definition:
```
1. User asks to create DICOM schema
2. Search DICOM_MODEL_SEARCH_SVC for relevant tables
3. Use search results (not hardcoded SKILL.md) to generate DDL
4. Apply governance (imaging-governance) for PHI columns found in results
```

### dicom-ingestion
When building ingestion pipelines, query the model to understand target schema:
```
1. User provides DICOM files to ingest
2. Search model for target table columns and data types
3. Generate COPY INTO with correct column mappings
4. Create Dynamic Tables referencing model-accurate column definitions
```

### imaging-governance
Query PHI indicators to auto-generate masking policies:
```
1. Search for contains_phi = "Y" columns
2. Generate masking policies for each PHI column found
3. Apply row-access policies based on table relationships
```

## Updating the Data Model

When the DICOM data model changes:

1. Edit `references/dicom_data_model_reference.xlsx`
2. Run `python scripts/export_search_corpus_csv.py`
3. Reload to Snowflake:
```sql
TRUNCATE TABLE DICOM_MODEL_REFERENCE;
PUT file://references/dicom_model_search_corpus.csv @dicom_model_stage OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
COPY INTO DICOM_MODEL_REFERENCE FROM @dicom_model_stage/dicom_model_search_corpus.csv
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```
4. Cortex Search Service auto-refreshes based on TARGET_LAG (1 day default)

## Extending to Other Data Models

This pattern is reusable for any reference data model:

| Domain | Spreadsheet | Search Service |
|--------|------------|----------------|
| DICOM Imaging | dicom_data_model_reference.xlsx | DICOM_MODEL_SEARCH_SVC |
| FHIR R4 | fhir_r4_resource_model.xlsx | FHIR_MODEL_SEARCH_SVC |
| OMOP CDM v5.4 | omop_cdm_v54_model.xlsx | OMOP_MODEL_SEARCH_SVC |
| FAERS | faers_data_model.xlsx | FAERS_MODEL_SEARCH_SVC |
| Claims (837/835) | claims_data_model.xlsx | CLAIMS_MODEL_SEARCH_SVC |

Follow the same pattern: Spreadsheet → CSV → Table → Cortex Search Service → Skill queries at runtime.
