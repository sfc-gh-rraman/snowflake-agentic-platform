---
name: hcls-cross-cke-pubmed
description: "Cortex Knowledge Extension: PubMed Biomedical Research Corpus. RAG-based semantic search across PubMed biomedical literature via Snowflake Marketplace shared Cortex Search Service. Triggers: PubMed, biomedical literature, drug mechanism, clinical evidence, research papers, medical literature, literature review, biomedical research, drug-event association, radiology research."
---

# CKE: PubMed Biomedical Research Corpus

This skill provides access to the **PubMed Biomedical Research Corpus** Cortex Knowledge Extension (CKE) from the Snowflake Marketplace. It is a shared Cortex Search Service that enables RAG-based semantic search across PubMed biomedical literature directly in Snowflake -- no data is copied into your account.

## Preflight Check (REQUIRED -- Run Before Any Query)

Before executing any PubMed search, verify the Marketplace listing is installed:

```sql
SELECT COUNT(*) FROM PUBMED_ABSTRACTS_EMBEDDINGS.SHARED.PUBMED_SEARCH_CORPUS LIMIT 1;
```

| Result | Status | Action |
|--------|--------|--------|
| Returns a count | READY | Proceed with queries using `PUBMED_ABSTRACTS_EMBEDDINGS` as the CKE database |
| `SQL compilation error: does not exist` | MISSING | Guide user through Setup below, then retry |
| Other error (permissions, etc.) | ERROR | Show error, suggest `GRANT IMPORTED PRIVILEGES ON DATABASE PUBMED_ABSTRACTS_EMBEDDINGS TO ROLE <role>` |

### Fallback (When MISSING)

If the listing is not installed and the user cannot install it now:
- **Inform the user**: "PubMed CKE is not available in this account. Biomedical literature search is unavailable."
- **Continue without PubMed enrichment** -- domain skills should still function for their primary task
- **Suggest alternative**: "You can search PubMed manually at https://pubmed.ncbi.nlm.nih.gov/ and paste relevant abstracts into the conversation"

### Auto-Detection for Domain Skills

When a domain skill (pharmacovigilance, clinical-nlp, etc.) wants to invoke this CKE:
1. Run the preflight probe above
2. If READY -- execute the CKE query and enrich the domain result
3. If MISSING -- skip enrichment, log a note: "PubMed CKE not available -- skipping literature enrichment"
4. Never fail the parent skill just because a CKE is unavailable

## Marketplace Details

| Field | Value |
|-------|-------|
| **Listing ID** | `GZSTZ67BY9OQW` |
| **Service Name** | `<CKE_DB>.SHARED.CKE_PUBMED_SERVICE` |
| **Type** | Shared Cortex Search Service |
| **Columns** | `chunk`, `document_title`, `source_url` |

> **Note:** `<CKE_DB>` is the database name assigned when you install the listing from Marketplace (e.g., `PUBMED_BIOMEDICAL_RESEARCH_CORPUS`).

## Setup (One-Time)

1. Navigate to **Snowflake Marketplace** and search for `PubMed Biomedical Research Corpus` (or listing `GZSTZ67BY9OQW`)
2. Click **Get** to install -- no data is copied; a shared Cortex Search Service appears in your account
3. Note the database name assigned (e.g., `PUBMED_BIOMEDICAL_RESEARCH_CORPUS`)
4. Replace `<CKE_DB>` in the query patterns below with this database name

## Query Patterns

### Basic Search (SQL)

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  '<CKE_DB>.SHARED.CKE_PUBMED_SERVICE',
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
        "name": "pubmed_search",
        "spec": {
          "service_name": "<CKE_DB>.SHARED.CKE_PUBMED_SERVICE",
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

This CKE is designed to be invoked on-demand by domain skills when evidence grounding adds value. The calling skill decides when and what to query.

| Domain Skill | When to Invoke `$cke-pubmed` | Example Query |
|---|---|---|
| `$pharmacovigilance` | After signal detection (PRR/ROR), search for published drug-event associations and mechanism evidence | `"{drug_name} {reaction} adverse event mechanism"` |
| `$healthcare-imaging` (dicom-analytics) | When enriching imaging analytics with radiology research context, imaging biomarkers, diagnostic criteria | `"pulmonary nodule CT screening Lung-RADS classification"` |
| `$clinical-nlp` | Entity disambiguation, terminology validation, grounding LLM prompts with biomedical context | `"drug interaction classification clinical text"` |
| `$scientific-problem-selection` | Literature landscape review, novelty assessment, gap identification for research ideas | `"CRISPR base editing sickle cell disease clinical outcomes"` |

## Integration Patterns

### Pattern 1: Signal Enrichment (Pharmacovigilance)

Annotate FAERS safety signals with published literature evidence:

```sql
WITH faers_signals AS (
  SELECT drug_name, reaction_pt, prr, ror
  FROM drug_safety_signals
  WHERE prr > 2 AND ror > 2
),
literature_evidence AS (
  SELECT
    s.drug_name,
    s.reaction_pt,
    s.prr,
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      '<CKE_DB>.SHARED.CKE_PUBMED_SERVICE',
      '{"query": "' || s.drug_name || ' ' || s.reaction_pt || ' adverse event mechanism", "columns": ["chunk", "document_title", "source_url"]}'
    ) AS pubmed_evidence
  FROM faers_signals s
)
SELECT * FROM literature_evidence;
```

### Pattern 2: LLM Prompt Grounding (Clinical NLP)

Ground Cortex AI extraction prompts with biomedical context:

```sql
WITH pubmed_context AS (
  SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '<CKE_DB>.SHARED.CKE_PUBMED_SERVICE',
    '{"query": "drug interaction classification clinical text", "columns": ["chunk"]}'
  ) AS literature_context
)
SELECT
  n.note_id,
  SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-70b',
    'Using this biomedical reference context: ' || p.literature_context::STRING ||
    ' Extract medications and potential drug interactions from this clinical note: ' || n.note_text
  ) AS enriched_extraction
FROM clinical_notes n, pubmed_context p
LIMIT 10;
```

### Pattern 3: Imaging Research Context (DICOM Analytics)

Enrich radiology findings with published evidence:

```sql
SELECT
  r.study_uid,
  r.key_findings,
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '<CKE_DB>.SHARED.CKE_PUBMED_SERVICE',
    '{"query": "' || r.key_findings::STRING || ' radiology evidence", "columns": ["chunk", "document_title", "source_url"]}'
  ) AS literature_context
FROM radiology_findings r
WHERE r.critical_findings IS NOT NULL
LIMIT 10;
```

### Pattern 4: Research Landscape Survey (Scientific Problem Selection)

When evaluating a research idea, survey the literature:

1. Search for the core topic to gauge publication volume and recency
2. Search for the specific approach/method to assess novelty
3. Search for competing approaches to understand alternatives
4. Use results to inform problem evaluation and risk assessment
