---
name: imaging-governance
description: "HIPAA-compliant governance for medical imaging data: PHI masking, de-identification, classification, row-access policies, and audit trails on Snowflake."
parent_skill: hcls-provider-imaging
---

# Imaging Data Governance & HIPAA Compliance

## When to Load

Healthcare-imaging router: After user intent matches GOVERNANCE.

## Prerequisites

- Imaging metadata tables exist in Snowflake
- ACCOUNTADMIN or SECURITYADMIN role access for policy creation
- Understanding of HIPAA Safe Harbor de-identification requirements

## Workflow

### Step 0: Query Data Model Knowledge for PHI Columns (Auto — Injected by Router)

The healthcare-imaging router automatically runs this step before loading this skill. The search results from `DICOM_MODEL_SEARCH_SVC` identify all PHI-containing columns across the DICOM data model.

**Query PHI columns:**
```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "PHI protected health information patient name ID birth date identifiers", "columns": ["table_name", "column_name", "data_type", "contains_phi", "description", "dicom_tag"]}'
);
```

**Use the results to:**
- Automatically identify every column flagged `contains_phi = Y` across all 18 tables
- Generate masking policies targeting the exact PHI columns (no manual enumeration)
- Scope de-identification pipelines to the correct columns
- Verify HIPAA Safe Harbor coverage against the data model reference

**If search service is unavailable**, fall back to the hardcoded HIPAA 18 identifiers list below.

### Step 1: Assess Governance Requirements

**Ask** user:
```
What governance capabilities do you need?
1. PHI masking (mask patient name, MRN, DOB in queries)
2. DICOM de-identification (remove/hash HIPAA 18 identifiers)
3. Sensitive data classification (auto-detect PHI columns)
4. Role-based access policies (restrict imaging data by role)
5. Audit trails (who accessed what imaging data)
6. All of the above
```

### Step 2: Classify Sensitive Imaging Data

**Goal:** Auto-detect PHI in imaging metadata tables.

**Invoke** the `sensitive-data-classification` skill for SYSTEM$CLASSIFY.

```sql
SELECT SYSTEM$CLASSIFY('imaging_db.imaging_schema.dicom_studies', {'auto_tag': true});
```

**HIPAA Safe Harbor — 18 identifiers to protect in DICOM:**
- Patient Name, Patient ID (MRN), Date of Birth, Study Date
- Institution Name, Referring Physician, Address/ZIP
- Phone, Email, SSN, Medical Record Numbers
- Device Serial Numbers, Unique Identifiers (UIDs)

### Step 3: Create Masking Policies for PHI

**Goal:** Mask PHI fields based on role.

**Invoke** the `data-policy` skill for masking policy best practices.

```sql
CREATE OR REPLACE MASKING POLICY phi_string_mask AS (val STRING)
RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('PHI_AUTHORIZED') THEN val
    ELSE '***MASKED***'
  END;

CREATE OR REPLACE MASKING POLICY phi_date_mask AS (val DATE)
RETURNS DATE ->
  CASE
    WHEN IS_ROLE_IN_SESSION('PHI_AUTHORIZED') THEN val
    ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)
  END;

ALTER TABLE dicom_studies MODIFY COLUMN patient_name
  SET MASKING POLICY phi_string_mask;
ALTER TABLE dicom_studies MODIFY COLUMN patient_id
  SET MASKING POLICY phi_string_mask;
ALTER TABLE dicom_studies MODIFY COLUMN study_date
  SET MASKING POLICY phi_date_mask;
```

### Step 4: Row-Access Policies

**Goal:** Restrict imaging data visibility by institution or department.

```sql
CREATE OR REPLACE ROW ACCESS POLICY imaging_institution_policy
AS (institution_val VARCHAR) RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('IMAGING_ADMIN')
  OR institution_val IN (
    SELECT institution FROM imaging_role_mapping
    WHERE role_name = CURRENT_ROLE()
  );

ALTER TABLE dicom_studies ADD ROW ACCESS POLICY imaging_institution_policy
  ON (institution);
```

### Step 5: DICOM De-Identification Pipeline

**Goal:** Create a de-identified copy of imaging metadata for research.

```sql
CREATE OR REPLACE TABLE dicom_studies_deidentified AS
SELECT
  SHA2(study_uid, 256) AS study_uid_hash,
  SHA2(patient_id, 256) AS patient_id_hash,
  '***' AS patient_name,
  DATE_FROM_PARTS(YEAR(TRY_TO_DATE(study_date, 'YYYYMMDD')), 1, 1) AS study_year,
  modality,
  body_part,
  image_rows,
  image_columns,
  bits_allocated
FROM dicom_studies;
```

### Step 6: Audit Trail Setup

**Goal:** Monitor PHI access via Snowflake ACCESS_HISTORY.

**Invoke** the `data-governance` skill for audit queries.

```sql
SELECT
  user_name,
  query_start_time,
  direct_objects_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE ARRAY_CONTAINS('DICOM_STUDIES'::VARIANT, 
  TRANSFORM(direct_objects_accessed, o -> o:objectName))
ORDER BY query_start_time DESC
LIMIT 100;
```

## Stopping Points

- After Step 1 to confirm scope
- After Step 2 before applying tags (review classification results)
- After Step 3 before applying masking policies (get security approval)
- After Step 4 before applying row-access policies

## Output

- PHI columns classified and tagged
- Masking policies applied to all PHI fields
- Row-access policies for institutional data segregation
- De-identified research dataset
- Audit query templates for compliance monitoring
