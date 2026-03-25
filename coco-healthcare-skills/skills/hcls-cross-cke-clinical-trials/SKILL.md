---
name: hcls-cross-cke-clinical-trials
description: "Cortex Knowledge Extension: Clinical Trials Research Database. RAG-based semantic search across ClinicalTrials.gov data via Snowflake Marketplace shared Cortex Search Service. Triggers: ClinicalTrials.gov, trial search, trial design, similar trials, trial feasibility, eligibility criteria, competitor trials, clinical trial registry, trial protocol comparison."
---

# CKE: Clinical Trials Research Database

This skill provides access to the **Clinical Trials Research Database** Cortex Knowledge Extension (CKE) from the Snowflake Marketplace. It is a shared Cortex Search Service that enables RAG-based semantic search across ClinicalTrials.gov data directly in Snowflake -- no data is copied into your account.

## Preflight Check (REQUIRED -- Run Before Any Query)

Before executing any Clinical Trials search, verify the Marketplace listing is installed:

```sql
SELECT COUNT(*) FROM CLINICAL_TRIALS_EMBEDDINGS.SHARED.CLINICAL_TRIALS_SEARCH_CORPUS LIMIT 1;
```

| Result | Status | Action |
|--------|--------|--------|
| Returns a count | READY | Proceed with queries using `CLINICAL_TRIALS_EMBEDDINGS` as the CKE database |
| `SQL compilation error: does not exist` | MISSING | Guide user through Setup below, then retry |
| Other error (permissions, etc.) | ERROR | Show error, suggest `GRANT IMPORTED PRIVILEGES ON DATABASE CLINICAL_TRIALS_EMBEDDINGS TO ROLE <role>` |

### Fallback (When MISSING)

If the listing is not installed and the user cannot install it now:
- **Inform the user**: "Clinical Trials CKE is not available in this account. ClinicalTrials.gov search is unavailable."
- **Continue without trial search enrichment** -- domain skills should still function for their primary task
- **Suggest alternative**: "You can search ClinicalTrials.gov manually at https://clinicaltrials.gov/ and paste relevant trial details into the conversation"

### Auto-Detection for Domain Skills

When a domain skill (clinical-trial-protocol, claims-data-analysis, etc.) wants to invoke this CKE:
1. Run the preflight probe above
2. If READY -- execute the CKE query and enrich the domain result
3. If MISSING -- skip enrichment, log a note: "Clinical Trials CKE not available -- skipping trial search enrichment"
4. Never fail the parent skill just because a CKE is unavailable

## Marketplace Details

| Field | Value |
|-------|-------|
| **Listing ID** | `GZSTZ67BY9ORD` |
| **Service Name** | `<CKE_DB>.SHARED.CKE_CLINICAL_TRIALS_SERVICE` |
| **Type** | Shared Cortex Search Service |
| **Columns** | `chunk`, `document_title`, `source_url` |

> **Note:** `<CKE_DB>` is the database name assigned when you install the listing from Marketplace (e.g., `CLINICAL_TRIALS_RESEARCH_DATABASE`).

## Setup (One-Time)

1. Navigate to **Snowflake Marketplace** and search for `Clinical Trials Research Database` (or listing `GZSTZ67BY9ORD`)
2. Click **Get** to install -- no data is copied; a shared Cortex Search Service appears in your account
3. Note the database name assigned (e.g., `CLINICAL_TRIALS_RESEARCH_DATABASE`)
4. Replace `<CKE_DB>` in the query patterns below with this database name

## Query Patterns

### Basic Search (SQL)

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  '<CKE_DB>.SHARED.CKE_CLINICAL_TRIALS_SERVICE',
  '{"query": "<natural language question>", "columns": ["chunk", "document_title", "source_url"]}'
);
```

### Cortex Agent API Tool Spec

```json
{
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "clinical_trials_search",
        "spec": {
          "service_name": "<CKE_DB>.SHARED.CKE_CLINICAL_TRIALS_SERVICE",
          "max_results": 5,
          "title_column": "document_title",
          "id_column": "source_url"
        }
      }
    }
  ]
}
```

## Use Cases by Domain Skill

This CKE is designed to be invoked on-demand by domain skills when trial registry evidence adds value. The calling skill decides when and what to query.

| Domain Skill | When to Invoke `$cke-clinical-trials` | Example Query |
|---|---|---|
| `$clinical-trial-protocol-skill` | Research similar/competing trials, benchmark eligibility criteria, reference endpoint definitions | `"phase 3 trial pancreatic cancer pembrolizumab"` |
| `$claims-data-analysis` | Feasibility analysis, RWE benchmarking against trial protocols, site selection support | `"type 2 diabetes GLP-1 agonist phase 3 recruiting"` |
| `$survival-analysis` | Reference endpoint definitions, compare survival outcomes with published trial results | `"overall survival progression-free survival NSCLC immunotherapy"` |

## Integration Patterns

### Pattern 1: Protocol Research (Clinical Trial Protocol)

Search for similar trials when designing a new protocol:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  '<CKE_DB>.SHARED.CKE_CLINICAL_TRIALS_SERVICE',
  '{"query": "phase 3 trial pancreatic cancer pembrolizumab eligibility criteria", "columns": ["chunk", "document_title", "source_url"]}'
);
```

**When to use in protocol generation:**
- **Step 1 (Research Similar Protocols):** Search for similar/competing trials by condition, intervention, phase
- **Step 2 (Protocol Foundation):** Benchmark eligibility criteria from comparable trials
- **Step 4 (Protocol Operations):** Reference endpoint definitions from similar study designs

### Pattern 2: Feasibility Analysis (Claims / RWE)

Cross-reference claims-based cohorts with active clinical trials:

```sql
WITH claims_cohort AS (
  SELECT DISTINCT member_id, dx1 AS condition_code
  FROM CLAIMS.MEDICAL
  WHERE dx1 LIKE 'E11%'  -- Type 2 Diabetes
),
trial_landscape AS (
  SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '<CKE_DB>.SHARED.CKE_CLINICAL_TRIALS_SERVICE',
    '{"query": "type 2 diabetes recruiting eligibility criteria", "columns": ["chunk", "document_title", "source_url"]}'
  ) AS matching_trials
)
SELECT
  COUNT(DISTINCT c.member_id) AS eligible_patients,
  t.matching_trials
FROM claims_cohort c, trial_landscape t
GROUP BY t.matching_trials;
```

### Pattern 3: Endpoint Benchmarking (Survival Analysis)

Reference published trial endpoints when designing survival analyses:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  '<CKE_DB>.SHARED.CKE_CLINICAL_TRIALS_SERVICE',
  '{"query": "overall survival progression-free survival NSCLC pembrolizumab endpoint definition", "columns": ["chunk", "document_title", "source_url"]}'
);
```
