---
name: dicom-analytics
description: "DICOM metadata analytics, radiology report NLP, and imaging search using Cortex AI functions and Cortex Search on Snowflake."
parent_skill: hcls-provider-imaging
---

# DICOM Analytics & Metadata Intelligence

## When to Load

Healthcare-imaging router: After user intent matches ANALYTICS.

## Prerequisites

- DICOM metadata ingested (run `dicom-ingestion` skill first if needed)
- Cortex AI functions available (COMPLETE, EXTRACT, SUMMARIZE)
- Cortex Search service available for semantic search

## Workflow

### Step 0: Query Data Model Knowledge (Auto — Injected by Router)

The healthcare-imaging router automatically runs this step before loading this skill. The search results from `DICOM_MODEL_SEARCH_SVC` provide source table definitions for building analytics.

**Query source table definitions for analytical views:**
```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "study series patient modality body part date description columns for analytics", "columns": ["table_name", "column_name", "data_type", "description", "dicom_tag", "relationships"]}'
);
```

**Use the results to:**
- Build analytical Dynamic Tables with correct source column names and join keys
- Reference accurate column descriptions in Cortex AI prompts
- Ensure GROUP BY / aggregation columns exist in the source tables
- Map relationships for multi-table joins (e.g., study → series → instance)

**If search service is unavailable**, fall back to the schema in `dicom-parser/SKILL.md`.

### Step 1: Understand Analytics Goals

**Ask** user:
```
What imaging analytics do you need?
1. Study-level dashboards (volume, modality mix, turnaround times)
2. Radiology report NLP (extract findings, impressions, diagnoses)
3. Semantic search across imaging metadata and reports
4. Population-level imaging trends
5. Anomaly detection (missing metadata, duplicates, quality issues)
```

### Step 2: Build Analytical Views

**Goal:** Create curated analytical layers.

**Study Volume Analytics:**
```sql
CREATE OR REPLACE DYNAMIC TABLE imaging_study_metrics
  TARGET_LAG = '30 minutes'
  WAREHOUSE = analytics_wh
AS
SELECT
  DATE_TRUNC('day', TRY_TO_DATE(study_date, 'YYYYMMDD')) AS study_day,
  modality,
  body_part,
  institution,
  COUNT(DISTINCT study_uid) AS study_count,
  COUNT(DISTINCT patient_id) AS patient_count,
  COUNT(DISTINCT series_uid) AS series_count,
  COUNT(*) AS image_count
FROM dicom_studies
GROUP BY 1, 2, 3, 4;
```

### Step 3: Radiology Report NLP with Cortex AI

**Goal:** Extract structured findings from unstructured radiology reports.

**Extract clinical entities:**
```sql
CREATE OR REPLACE DYNAMIC TABLE radiology_findings
  TARGET_LAG = '1 hour'
  WAREHOUSE = analytics_wh
AS
SELECT
  study_uid,
  patient_id,
  report_text,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    report_text,
    'What are the key findings?'
  ) AS key_findings,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    report_text,
    'What is the impression or diagnosis?'
  ) AS impression,
  SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    report_text,
    'Are there any critical or urgent findings?'
  ) AS critical_findings,
  SNOWFLAKE.CORTEX.SENTIMENT(report_text) AS report_sentiment
FROM radiology_reports;
```

**Summarize lengthy reports:**
```sql
SELECT
  study_uid,
  SNOWFLAKE.CORTEX.SUMMARIZE(report_text) AS report_summary
FROM radiology_reports
WHERE LENGTH(report_text) > 500;
```

### Step 4: Cortex Search for Imaging Metadata

**Goal:** Enable semantic search across imaging studies and reports.

**Create Cortex Search Service:**
```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE imaging_search_svc
  ON imaging_search_corpus
  WAREHOUSE = analytics_wh
  TARGET_LAG = '1 hour'
AS (
  SELECT
    study_uid,
    patient_id,
    modality,
    study_description,
    body_part,
    report_text,
    CONCAT(
      'Study: ', study_description,
      ' Modality: ', modality,
      ' Body Part: ', body_part,
      ' Report: ', COALESCE(report_text, '')
    ) AS search_text
  FROM dicom_studies_with_reports
);
```

**Query the search service (via Cortex Agent or API):**
```sql
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'imaging_search_svc',
    '{"query": "chest CT with pulmonary nodule", "columns": ["study_uid", "modality", "study_description"], "limit": 10}'
  )
);
```

### Step 5: Data Quality & Anomaly Detection

**Goal:** Identify imaging data quality issues.

```sql
SELECT
  'Missing Patient ID' AS issue,
  COUNT(*) AS count
FROM dicom_studies WHERE patient_id IS NULL
UNION ALL
SELECT
  'Missing Modality',
  COUNT(*)
FROM dicom_studies WHERE modality IS NULL
UNION ALL
SELECT
  'Duplicate Study UID',
  COUNT(*)
FROM (
  SELECT study_uid FROM dicom_studies
  GROUP BY study_uid HAVING COUNT(*) > 1
);
```

## Stopping Points

- After Step 1 to confirm analytics scope
- After Step 3 before creating Cortex AI pipelines (cost implications)
- After Step 4 before creating Search service

## Output

- Analytical Dynamic Tables for study metrics
- NLP-enriched radiology findings table
- Cortex Search service for semantic imaging search
- Data quality summary

## Evidence Grounding: PubMed CKE

Invoke `$cke-pubmed` when radiology research context enriches imaging analytics:

- Search for imaging biomarkers, modality-specific diagnostic criteria, evidence-based imaging guidelines
- Augment Cortex AI extraction with published radiology evidence
- Compare institutional imaging patterns against published utilization studies

See `$cke-pubmed` for setup, query patterns, and the imaging research context SQL pattern.
