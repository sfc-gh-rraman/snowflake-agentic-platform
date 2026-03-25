---
name: dicom-ingestion
description: "DICOM data ingestion pipelines on Snowflake. Ingest imaging metadata from PACS, stages, and external sources into Snowflake using streams, tasks, and dynamic tables."
parent_skill: hcls-provider-imaging
---

# DICOM Data Ingestion Pipeline

## When to Load

Healthcare-imaging router Step: After user intent matches INGEST.

## Prerequisites

- Snowflake database and schema for imaging data
- Source DICOM files accessible (local, S3, GCS, Azure Blob, or PACS export)
- Appropriate roles with CREATE STAGE, CREATE TABLE, CREATE DYNAMIC TABLE privileges
- For DICOM file parsing and schema creation, use `dicom-parser/SKILL.md` first — it provides the comprehensive 18-table data model and pydicom parser script (`scripts/parse_dicom.py`)

## Workflow

### Step 0: Query Data Model Knowledge (Auto — Injected by Router)

The healthcare-imaging router automatically runs this step before loading this skill. The search results from `DICOM_MODEL_SEARCH_SVC` provide the target table schema.

**Query target table definitions for the ingestion scope:**
```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "study series instance patient columns data types for ingestion pipeline", "columns": ["table_name", "column_name", "data_type", "constraints", "dicom_tag", "relationships"]}'
);
```

**Use the results to:**
- Build accurate COPY INTO column mappings (match VARIANT paths to exact column names/types)
- Generate Dynamic Table SELECT lists with correct column names, types, and DICOM tag paths
- Set up Stream/Task INSERT statements with proper target schema
- Validate that all required columns (from constraints) are populated

**If search service is unavailable**, fall back to the hardcoded schema in `dicom-parser/SKILL.md`.

### Step 1: Gather Source Information

**Goal:** Understand the imaging data source and volume.

**Ask** user:
```
1. Where are your DICOM files? (S3 bucket, Azure Blob, GCS, local files, PACS export)
2. What is the approximate volume? (number of studies/series/images)
3. Is this a one-time load or continuous ingestion?
4. Do you need to extract pixel data or metadata only?
```

**Output:** Source configuration parameters

### Step 2: Create Staging Infrastructure

**Goal:** Set up Snowflake stages and file formats for DICOM metadata.

**Actions:**

1. Create an external stage pointing to the source:
   ```sql
   CREATE OR REPLACE STAGE imaging_stage
     URL = 's3://bucket/dicom/'
     STORAGE_INTEGRATION = imaging_integration;
   ```

2. Create a file format for DICOM metadata (typically JSON/Parquet exports from PACS):
   ```sql
   CREATE OR REPLACE FILE FORMAT dicom_json_format
     TYPE = 'JSON'
     STRIP_OUTER_ARRAY = TRUE;
   ```

3. Create the raw landing table:
   ```sql
   CREATE OR REPLACE TABLE dicom_raw (
     file_path VARCHAR,
     metadata VARIANT,
     ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
   );
   ```

**Output:** Stage, file format, and raw table created

### Step 3: Build Ingestion Pipeline

**Goal:** Create automated pipeline based on user's ingestion pattern.

**If one-time load:**
```sql
COPY INTO dicom_raw (file_path, metadata)
  FROM (
    SELECT metadata$filename, $1
    FROM @imaging_stage
  )
  FILE_FORMAT = dicom_json_format
  ON_ERROR = 'CONTINUE';
```

**If continuous ingestion — use Dynamic Tables:**
```sql
CREATE OR REPLACE DYNAMIC TABLE dicom_studies
  TARGET_LAG = '10 minutes'
  WAREHOUSE = imaging_wh
AS
SELECT
  metadata:StudyInstanceUID::STRING AS study_uid,
  metadata:PatientID::STRING AS patient_id,
  metadata:PatientName::STRING AS patient_name,
  metadata:StudyDate::STRING AS study_date,
  metadata:Modality::STRING AS modality,
  metadata:StudyDescription::STRING AS study_description,
  metadata:SeriesInstanceUID::STRING AS series_uid,
  metadata:SOPInstanceUID::STRING AS sop_instance_uid,
  metadata:Rows::INT AS image_rows,
  metadata:Columns::INT AS image_columns,
  metadata:BitsAllocated::INT AS bits_allocated,
  metadata:BodyPartExamined::STRING AS body_part,
  metadata:InstitutionName::STRING AS institution,
  file_path,
  ingested_at
FROM dicom_raw;
```

**If event-driven — use Streams + Tasks:**
```sql
CREATE OR REPLACE STREAM dicom_raw_stream ON TABLE dicom_raw;

CREATE OR REPLACE TASK process_dicom_metadata
  WAREHOUSE = imaging_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('dicom_raw_stream')
AS
  INSERT INTO dicom_studies_processed
  SELECT
    metadata:StudyInstanceUID::STRING AS study_uid,
    metadata:PatientID::STRING AS patient_id,
    metadata:StudyDate::STRING AS study_date,
    metadata:Modality::STRING AS modality,
    metadata
  FROM dicom_raw_stream;
```

### Step 4: Validate Ingestion

**Goal:** Verify data landed correctly.

**Actions:**
```sql
SELECT COUNT(*) AS total_records,
       COUNT(DISTINCT metadata:StudyInstanceUID) AS unique_studies,
       COUNT(DISTINCT metadata:PatientID) AS unique_patients,
       MIN(metadata:StudyDate::STRING) AS earliest_study,
       MAX(metadata:StudyDate::STRING) AS latest_study
FROM dicom_raw;
```

**Validation Checklist:**
- Record count matches expected volume
- Study UIDs are unique per study
- Patient IDs are populated
- No critical NULL fields

## Stopping Points

- After Step 1 if source is unclear
- After Step 2 before creating objects (get approval)
- After Step 4 to confirm data quality

## Output

- External stage configured
- Raw landing table with DICOM metadata
- Automated pipeline (Dynamic Table or Stream/Task) for continuous ingestion
- Validation summary
