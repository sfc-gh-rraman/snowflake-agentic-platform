---
name: hcls-provider-imaging
description: "**[REQUIRED]** Use for ALL DICOM medical imaging tasks on Snowflake. This is the entry point for healthcare imaging solutions combining platform skills with clinical imaging workflows. Triggers: DICOM, medical imaging, radiology, imaging pipeline, PACS, imaging viewer, imaging AI, imaging governance, HIPAA imaging, clinical images, pathology images, imaging metadata, imaging ML, imaging model, imaging analytics, healthcare imaging, imaging data lake, imaging FHIR, imaging study, imaging series, data model knowledge, DICOM schema reference, model repository."
---

# Healthcare Imaging Solutions on Snowflake

## Setup

1. **Load** `references/dicom-standards.md` for DICOM domain context
2. **Verify** Snowflake connection is active and target database/schema exist
3. **Run Preflight Check** for Data Model Knowledge (see below)

## Preflight Check (REQUIRED -- Run at Skill Load)

Before routing to any sub-skill, verify the DICOM Data Model Knowledge repository is available:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "test", "columns": ["CONTENT"], "limit": 1}'
);
```

| Result | Status | Behavior |
|--------|--------|----------|
| Returns results | READY | Step 0 (Data Model Knowledge pre-step) will use dynamic search results |
| Error / does not exist | MISSING | Step 0 skipped -- sub-skills fall back to hardcoded schema definitions from SKILL.md references. Inform user: "DICOM data model search service not available -- using hardcoded schemas" |

This preflight runs ONCE at router load. The result determines whether Step 0 below executes or is skipped.

## Intent Detection

| Intent | Triggers | Load |
|--------|----------|------|
| PARSE | "parse DICOM", "extract DICOM tags", "DICOM schema", "DICOM data model", "pydicom", "DICOM to Snowflake", "build DICOM tables" | `dicom-parser/SKILL.md` |
| INGEST | "ingest DICOM", "imaging pipeline", "load images", "PACS integration", "stage DICOM", "stream images", "dynamic table imaging" | `dicom-ingestion/SKILL.md` |
| ANALYTICS | "imaging analytics", "metadata extraction", "imaging search", "Cortex search imaging", "study analytics", "radiology NLP", "report extraction" | `dicom-analytics/SKILL.md` |
| VIEWER | "imaging viewer", "Streamlit imaging", "DICOM viewer", "imaging app", "imaging dashboard", "radiology UI", "deploy viewer" | `imaging-viewer/SKILL.md` |
| GOVERNANCE | "imaging governance", "HIPAA", "PHI masking", "imaging audit", "imaging classification", "imaging access policy", "de-identification" | `imaging-governance/SKILL.md` |
| ML | "imaging model", "train imaging", "imaging classification ML", "pathology model", "radiology AI", "deploy imaging model", "imaging inference" | `imaging-ml/SKILL.md` |
| MODEL_KNOWLEDGE | "data model reference", "DICOM schema lookup", "generate DDL from model", "what columns", "model repository", "PHI columns", "table relationships" | `data-model-knowledge/SKILL.md` |

## Data Model Knowledge — Automatic Pre-Step (Conditional on Preflight)

**CRITICAL:** For intents PARSE, INGEST, ANALYTICS, and GOVERNANCE, **execute Step 0 if preflight status is READY**. If preflight status is MISSING, skip Step 0 and let sub-skills use hardcoded schema definitions.

### Step 0: Query Data Model Knowledge (automatic for PARSE, INGEST, ANALYTICS, GOVERNANCE)

Before generating any DDL, building pipelines, creating analytics views, or applying governance policies:

1. **Query the DICOM model search service** for relevant tables/columns:
```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "<context from user request>", "columns": ["table_name", "column_name", "data_type", "constraints", "description", "dicom_tag", "contains_phi", "relationships"]}'
);
```

2. **Use the search results** — not hardcoded DDL — as the source of truth for:
   - Table names and column definitions (PARSE, INGEST)
   - Column types and DICOM tag mappings (INGEST, ANALYTICS)
   - PHI column identification (GOVERNANCE)
   - Foreign key relationships (all)

3. **Pass results to the sub-skill** as grounding context.

| Intent | Step 0 Query Focus | What Gets Grounded |
|--------|-------------------|--------------------|
| PARSE | All tables + columns for requested scope | CREATE TABLE DDL statements |
| INGEST | Target table columns + data types + relationships | COPY INTO mappings, Dynamic Table SELECT lists |
| ANALYTICS | Source table columns + descriptions | Analytical views, Cortex AI extraction prompts |
| GOVERNANCE | PHI-flagged columns across all tables | Masking policy targets, de-identification scope |

## Workflow

```
Start
  |
  v
Run Preflight Check (DICOM_MODEL_SEARCH_SVC)
  |
  v
Detect Intent from table above
  |
  v
Is intent PARSE, INGEST, ANALYTICS, or GOVERNANCE?
  |                              |
  YES                            NO
  |                              |
  v                              v
  Preflight READY?               Skip Step 0
  |          |                   (VIEWER, ML, MODEL_KNOWLEDGE)
  YES        NO                  |
  |          |                   |
  v          v                   |
  Step 0:    Skip Step 0         |
  Query      (use hardcoded      |
  DICOM_MODEL schemas)           |
  _SEARCH_SVC                    |
  |          |                   |
  v          v                   v
  +---> PARSE -----> dicom-parser/SKILL.md (DDL grounded by search results OR hardcoded)
  |
  +---> INGEST ----> dicom-ingestion/SKILL.md (pipelines grounded by search results OR hardcoded)
  |
  +---> ANALYTICS -> dicom-analytics/SKILL.md (views grounded by search results OR hardcoded)
  |
  +---> GOVERNANCE > imaging-governance/SKILL.md (PHI columns from search results OR hardcoded)
  |
  +---> VIEWER ----> imaging-viewer/SKILL.md
  |
  +---> ML --------> imaging-ml/SKILL.md
  |
  +---> MODEL_KNOWLEDGE -> data-model-knowledge/SKILL.md (direct search queries)
```

## Cross-Cutting Concerns

All sub-skills should apply these platform patterns:

- **Data Model Knowledge (Preflight-Conditional Pre-Step)**: For PARSE, INGEST, ANALYTICS, and GOVERNANCE intents, the router **runs a preflight check** on `DICOM_MODEL_SEARCH_SVC`. If READY, it queries the Cortex Search Service before loading the sub-skill to ground all schema-dependent work in the latest data model. If MISSING, sub-skills fall back to hardcoded DDL from their SKILL.md references.
- **DICOM Parsing**: The `dicom-parser` sub-skill contains a comprehensive 18-table DICOM data model and a pydicom-based parser script. Use it as the foundation before ingestion or analytics.
- **Data Engineering**: Dynamic Tables for incremental refresh, Streams/Tasks for event-driven pipelines
- **AI/ML**: Cortex AI functions (COMPLETE, EXTRACT, SENTIMENT), Cortex Search for imaging metadata, ML Registry for models
- **Apps**: Streamlit in Snowflake for dashboards, SPCS for compute-heavy imaging workloads
- **Security**: Data masking for PHI, row-access policies per role, SYSTEM$CLASSIFY for PII detection, audit trails via ACCESS_HISTORY

## Stopping Points

- After intent detection if ambiguous
- Before creating any database objects
- Before deploying apps or models
