# Industry Solutions Architect for Health Sciences

An **Industry Solutions Architect** is a composable skill-based system on [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) that builds end-to-end healthcare and life sciences data and AI pipelines on Snowflake — delivering business outcomes from a single natural language conversation.

Instead of writing boilerplate pipelines from scratch, a solutions architect describes the business problem. An orchestrator agent understands the healthcare domain, selects the right combination of industry skills and Snowflake platform capabilities, and composes them into a working solution — from data ingestion through governance to analytics and applications.

### What makes this different

- **Composable skills, not monolithic scripts.** Each skill encodes deep domain expertise (DICOM imaging, FHIR interoperability, FAERS pharmacovigilance, genomics pipelines, claims analytics) as a reusable building block.
- **Orchestrator-driven composition.** A single orchestrator agent detects intent, routes across healthcare business domains, and chains multiple skills together with Snowflake platform skills to deliver complete solutions.
- **Knowledge-grounded.** Cortex Knowledge Extensions (CKEs) provide on-demand RAG search over PubMed and ClinicalTrials.gov. A Data Model Knowledge repository grounds schema generation in live reference models via Cortex Search.
- **Governance by default.** HIPAA guardrails — PHI masking, row-access policies, audit trails, de-identification — are enforced as cross-cutting concerns across every workflow.
- **Scales like lego blocks.** New skills snap into the framework as independent building blocks. Router skills cluster related capabilities under a business function (e.g., imaging router with parse/ingest/analytics/governance sub-skills). Adding a new domain or business function is just adding another skill directory — the orchestrator picks it up automatically.

### Example

A user asks:

> *"Design a Phase III clinical trial for a novel GLP-1 receptor agonist targeting Type 2 Diabetes with cardiovascular outcome endpoints."*

The orchestrator automatically composes multiple skills into a solution chain: research problem validation → competitor trial search (ClinicalTrials.gov CKE) → literature review (PubMed CKE) → protocol generation → survival endpoint design → claims-based feasibility analysis. No skill names needed.

## Architecture

```
+------------------------------------------------------------------+
|  ORCHESTRATOR AGENT: health-sciences-incubator.md                |
|  Intent Detection → Domain Routing → Skill Composition           |
+------------------------------------------------------------------+
       |            |             |             |
       v            v             v             v
  +---------+  +---------+  +---------+  +-----------+
  |Provider |  |Provider |  | Pharma  |  |  Pharma   |
  |Imaging  |  |ClinData |  |DrugSafe |  | Genomics  |
  |         |  |         |  |         |  |           |
  +---------+  +---------+  +---------+  +-----------+
       |   +--------+ +--------+ +----------+    |
       |   |Claims  | |  Lab   | |Research  |    |
       |   |        | |        | |          |    |
       |   +--------+ +--------+ +----------+    |
       |                                          |
       |     SHARED KNOWLEDGE (on-demand)         |
       |     hcls-cross-cke-pubmed                |
       |     hcls-cross-cke-clinical-trials       |
+------------------------------------------------------------------+
|  SNOWFLAKE PLATFORM SKILLS (bundled)                             |
|  Dynamic Tables | Cortex AI | Streamlit | SPCS | dbt | ML       |
|  Governance | Cortex Search | Cortex Agent | React App           |
+------------------------------------------------------------------+
       ^
       |
+------------------------------------------------------------------+
|  DATA MODEL KNOWLEDGE REPOSITORY                                 |
|  Cortex Search over reference models (DICOM: 18 tables, 222 col)|
|  Auto pre-step: grounds DDL, COPY INTO, masking in live schema   |
+------------------------------------------------------------------+
```

**How it works:**

1. User describes a healthcare business problem in natural language
2. Orchestrator detects the domain from trigger keywords and context
3. One or more industry skills are selected and composed into a pipeline
4. Skills invoke Snowflake platform skills for infrastructure (Dynamic Tables, Cortex AI, Streamlit, etc.)
5. For schema-dependent tasks, Data Model Knowledge auto-fires to ground outputs in live reference models
6. CKEs are invoked on-demand when literature or trial evidence adds value
7. HIPAA governance guardrails are applied across all workflows

## Getting Started

### Prerequisites

- [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) CLI or IDE, authenticated to your Snowflake account
- A Snowflake account with [Cortex AI functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) enabled
- For clinical document skills: `AI_PARSE_DOCUMENT`, `AI_EXTRACT`, `AI_AGG` access
- For search/agent skills: Cortex Search and Cortex Agent access
- For genomics skills: local Python environment with relevant packages

### Step 1: Clone the Repository

```bash
git clone https://github.com/sfc-gh-jrag/coco-healthcare-skills.git
cd coco-healthcare-skills
```

### Step 2: Register Skills

Open your Cortex Code skills configuration file:

```
~/.snowflake/cortex/skills.json
```

Add the healthcare skills as a `local` entry. Replace `<ABSOLUTE_PATH_TO_REPO>` with the full path to your cloned repo (e.g., `/Users/yourname/coco-healthcare-skills`):

```json
{
  "local": [
    {
      "path": "<ABSOLUTE_PATH_TO_REPO>/skills",
      "skills": [
        { "name": "hcls-provider-imaging", "relative_path": "hcls-provider-imaging" },
        { "name": "hcls-provider-imaging-dicom-parser", "relative_path": "hcls-provider-imaging-dicom-parser" },
        { "name": "hcls-provider-cdata-fhir", "relative_path": "hcls-provider-cdata-fhir" },
        { "name": "hcls-provider-cdata-clinical-nlp", "relative_path": "hcls-provider-cdata-clinical-nlp" },
        { "name": "hcls-provider-cdata-omop", "relative_path": "hcls-provider-cdata-omop" },
        { "name": "hcls-provider-cdata-clinical-docs", "relative_path": "hcls-provider-cdata-clinical-docs" },
        { "name": "hcls-provider-claims-data-analysis", "relative_path": "hcls-provider-claims-data-analysis" },
        { "name": "hcls-pharma-dsafety-pharmacovigilance", "relative_path": "hcls-pharma-dsafety-pharmacovigilance" },
        { "name": "hcls-pharma-dsafety-clinical-trial-protocol", "relative_path": "hcls-pharma-dsafety-clinical-trial-protocol" },
        { "name": "hcls-pharma-genomics-nextflow", "relative_path": "hcls-pharma-genomics-nextflow" },
        { "name": "hcls-pharma-genomics-variant-annotation", "relative_path": "hcls-pharma-genomics-variant-annotation" },
        { "name": "hcls-pharma-genomics-single-cell-qc", "relative_path": "hcls-pharma-genomics-single-cell-qc" },
        { "name": "hcls-pharma-genomics-scvi-tools", "relative_path": "hcls-pharma-genomics-scvi-tools" },
        { "name": "hcls-pharma-genomics-survival-analysis", "relative_path": "hcls-pharma-genomics-survival-analysis" },
        { "name": "hcls-pharma-lab-allotrope", "relative_path": "hcls-pharma-lab-allotrope" },
        { "name": "hcls-cross-research-problem-selection", "relative_path": "hcls-cross-research-problem-selection" },
        { "name": "hcls-cross-cke-pubmed", "relative_path": "hcls-cross-cke-pubmed" },
        { "name": "hcls-cross-cke-clinical-trials", "relative_path": "hcls-cross-cke-clinical-trials" }
      ],
      "added_at": "2026-03-19T00:00:00.000Z"
    }
  ]
}
```

> **Note**: If your `skills.json` already has entries (e.g., `remote`, `marketplace`, `stage`), merge the `local` array into the existing file — don't overwrite it. See `skills.json.template` for a clean starting point.

### Step 3: Create the Agent Profile

Create the incubator profile JSON at:

```
~/.snowflake/cortex/profiles/health-sciences-incubator.json
```

```json
{
  "name": "health-sciences-incubator",
  "description": "Health Sciences incubator profile for experimental skill development on Snowflake. Orchestrates skills across medical imaging, clinical data, drug safety, claims/RWE, genomics, and lab data.",
  "ownerTeam": "HCLS",
  "version": "1",
  "skillRepos": [
    {
      "source": "github:sfc-gh-jrag/coco-healthcare-skills",
      "ref": "main",
      "skills_path": "skills"
    }
  ],
  "mcpServers": {},
  "commandRepos": [],
  "scripts": [],
  "hooks": null,
  "plugins": [],
  "envVars": {},
  "settingsOverrides": {},
  "systemPromptPath": "<ABSOLUTE_PATH_TO_REPO>/agents/health-sciences-incubator.md",
  "localModified": true
}
```

Replace `<ABSOLUTE_PATH_TO_REPO>` with the full path to your cloned repo.

### Step 4: Validate

```bash
# Verify skills loaded
/skill                    # Should list hcls-* skills

# Verify orchestrator agent
/agents                   # Should show health-sciences-incubator

# Check profile details
cortex profile show health-sciences-incubator
```

### Step 5: Optional Dependencies

**Cortex Knowledge Extensions (CKEs):** Install PubMed and/or Clinical Trials CKE from Snowflake Marketplace for literature/trial evidence grounding. Skills work without CKEs but provide richer results with them.

**Data Model Knowledge:** Run `scripts/setup_dicom_model_knowledge_repo.sql` to create the Cortex Search Service over the DICOM data model.

### Step 6: Start Using

Ask healthcare questions in natural language. The orchestrator automatically routes to the right skills:

```
"I have DICOM files from our radiology department on S3.
 Build a pipeline to parse, ingest, and analyze the imaging metadata."

"Transform our FHIR R4 bundles into analytics-ready tables on Snowflake."

"Analyze FDA FAERS data for adverse events associated with aspirin."

"Design a Phase III clinical trial for a novel GLP-1 receptor agonist
 targeting Type 2 Diabetes with cardiovascular outcome endpoints."

"I have whole-genome sequencing FASTQs. Run variant calling and annotate
 pathogenic variants with ClinVar and gnomAD frequencies."

"Build a retrospective cohort of T2D patients from claims data and
 analyze treatment patterns and medication adherence."

"Build a real-world evidence study: cohort from claims, standardize
 to OMOP, run survival analysis, validate against published literature."
```

### Step 7: Keep Updated

```bash
cortex skill update                                  # Refresh skills from GitHub
cortex profile sync health-sciences-incubator        # Sync full profile
```

## Skills Inventory

### Provider > Clinical Research

| Skill | Description |
|-------|-------------|
| [hcls-provider-imaging](skills/hcls-provider-imaging/) | Router skill covering full DICOM imaging lifecycle (parse, ingest, analytics, viewer, governance, ML, data model knowledge) |
| [hcls-provider-imaging-dicom-parser](skills/hcls-provider-imaging-dicom-parser/) | Standalone DICOM metadata parser with comprehensive data model |

### Provider > Clinical Data Management

| Skill | Description |
|-------|-------------|
| [hcls-provider-cdata-fhir](skills/hcls-provider-cdata-fhir/) | Transform FHIR R4 resources (Patient, Observation, Condition, etc.) into analytics-ready Snowflake tables |
| [hcls-provider-cdata-clinical-nlp](skills/hcls-provider-cdata-clinical-nlp/) | Extract structured entities from clinical text (NER, ICD coding, medication extraction) via Cortex AI / spaCy |
| [hcls-provider-cdata-omop](skills/hcls-provider-cdata-omop/) | Transform EHR/claims data to OMOP CDM v5.4 with vocabulary mapping (SNOMED, LOINC, RxNorm) |

### Provider > Revenue Cycle

| Skill | Description |
|-------|-------------|
| [hcls-provider-claims-data-analysis](skills/hcls-provider-claims-data-analysis/) | Claims-based RWE: cohort building, utilization metrics, treatment patterns, medication adherence (PDC), HEDIS measures |

### Pharma > Drug Safety

| Skill | Description |
|-------|-------------|
| [hcls-pharma-dsafety-pharmacovigilance](skills/hcls-pharma-dsafety-pharmacovigilance/) | FDA FAERS adverse event analysis with PRR/ROR signal detection |
| [hcls-pharma-dsafety-clinical-trial-protocol](skills/hcls-pharma-dsafety-clinical-trial-protocol/) | Generate clinical trial protocols via waypoint architecture for FDA submissions (IDE/IND pathways) |

### Pharma > Genomics & Bioinformatics

| Skill | Description |
|-------|-------------|
| [hcls-pharma-genomics-nextflow](skills/hcls-pharma-genomics-nextflow/) | Run nf-core pipelines (rnaseq, sarek, atacseq) on sequencing data from local FASTQs or GEO/SRA |
| [hcls-pharma-genomics-variant-annotation](skills/hcls-pharma-genomics-variant-annotation/) | Annotate genomic variants with ClinVar pathogenicity, gnomAD allele frequencies, ACMG classification |
| [hcls-pharma-genomics-single-cell-qc](skills/hcls-pharma-genomics-single-cell-qc/) | Automated QC for single-cell RNA-seq using scverse best practices with MAD-based filtering |
| [hcls-pharma-genomics-scvi-tools](skills/hcls-pharma-genomics-scvi-tools/) | Deep learning single-cell analysis (scVI, scANVI, totalVI, PeakVI, MultiVI, veloVI) |
| [hcls-pharma-genomics-survival-analysis](skills/hcls-pharma-genomics-survival-analysis/) | Kaplan-Meier curves, Cox proportional hazards, time-to-event analysis with publication-ready plots |

### Pharma > Lab Operations

| Skill | Description |
|-------|-------------|
| [hcls-pharma-lab-allotrope](skills/hcls-pharma-lab-allotrope/) | Convert laboratory instrument files (PDF, CSV, Excel, TXT) to Allotrope Simple Model JSON/CSV |

### Cross-Industry

| Skill | Description |
|-------|-------------|
| [hcls-cross-research-problem-selection](skills/hcls-cross-research-problem-selection/) | Systematic research problem selection using Fischbach & Walsh decision trees |
| [hcls-cross-cke-pubmed](skills/hcls-cross-cke-pubmed/) | RAG-based semantic search over PubMed biomedical literature (Cortex Knowledge Extension) |
| [hcls-cross-cke-clinical-trials](skills/hcls-cross-cke-clinical-trials/) | RAG-based semantic search over ClinicalTrials.gov registry (Cortex Knowledge Extension) |

## Framework Architecture

### Skill Types

| Type | Description | Example |
|------|-------------|---------|
| **Router skill** | Detects user intent and routes to specialized sub-skills. Contains setup, preflight checks, and workflow orchestration. | `hcls-provider-imaging`, `hcls-provider-cdata-clinical-docs` |
| **Sub-skill** | Handles one specific task within a router. Loaded by the router, not directly by the user. | `dicom-parser`, `clinical-document-extraction` |
| **Standalone skill** | Self-contained skill with no router or sub-skills. | `hcls-provider-cdata-fhir`, `hcls-pharma-dsafety-pharmacovigilance` |

### CKE (Cortex Knowledge Extensions)

Several skills use **CKE** — dynamically discoverable metadata served via Cortex Search Services. Instead of hardcoding table schemas or document type definitions, skills query a search service at runtime to get the latest metadata.

```
Source of Truth (YAML / Excel)
    │
    └──> Backing Table (in DATA_MODEL_KNOWLEDGE schema)
            │
            └──> Cortex Search Service (CKE)
                    │
                    └──> Skills query at runtime via SEARCH_PREVIEW()
```

This means:
- **Schema changes propagate automatically** — edit the source, refresh the table, the search service updates within its TARGET_LAG
- **Skills are never stale** — they always query the latest metadata
- **Fallback is built in** — if the search service is down, skills fall back to local files on disk

### Router Pattern (DICOM + Clinical Docs)

Both router skills follow the same architecture:

1. **Preflight Check** — probe the CKE search service at skill load (READY / MISSING)
2. **Intent Detection** — classify what the user wants (parse, ingest, search, agent, etc.)
3. **Conditional Step 0** — if CKE is READY, query it for schema/spec context before routing
4. **Sub-skill Loading** — pass grounding context to the sub-skill
5. **Fallback** — if CKE is MISSING, sub-skills use local reference files

### Defense-in-Depth (Clinical Docs)

The clinical documents skill enforces a three-layer guardrail system:

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| 1 | `AGENTS.md` (profile-level rules) | Session-wide constraints |
| 2 | Gate micro-skills + Phase skills | Structural decomposition — model cannot skip steps |
| 3 | `hooks.json` | Hard blocks on DDL/DML without user confirmation |

Every decision point requires explicit user confirmation via `ask_user_question`. The pipeline is split into **Tier 1 gates** (pre-conditions) and **Tier 2 phases** (execution), with mandatory re-entry between phases.

**Gates (Tier 1 — pre-conditions):**

| Gate | Purpose |
|------|---------|
| `confirm-environment` | Validate Snowflake connection, database, schema, warehouse, stage |
| `confirm-doc-types` | Discover and confirm document types to process |
| `confirm-pipeline-config` | Review and approve extraction config before execution |

**Phases (Tier 2 — execution):**

| Phase | Purpose |
|-------|---------|
| `parse-and-refresh` | AI_PARSE_DOCUMENT + GENERATE_DYNAMIC_OBJECTS to build/refresh pipeline objects |
| `classify` | AI_COMPLETE-based document classification (single-page and AI_AGG multi-page) |
| `extract` | AI_EXTRACT type-specific field extraction (single-page and AI_AGG multi-page) |

## Skill Naming Convention and Organization

Skills follow a flat directory structure with a structured prefix encoding the taxonomy hierarchy:

```
hcls-{sub-industry}-{function}-{skill}
```

| Component | Values | Description |
|-----------|--------|-------------|
| `hcls` | Fixed prefix | Health Sciences industry identifier |
| sub-industry | `provider`, `pharma`, `payer`, `cross` | Customer segment |
| function | `imaging`, `cdata`, `dsafety`, `genomics`, `lab`, `claims`, `research`, `cke` | Business function |
| skill | `fhir`, `pharmacovigilance`, `nextflow`, etc. | Use case skill name |

Cortex Code requires skills at exactly **1 level of nesting** from the scan root: `skills/{name}/SKILL.md`. The five-level taxonomy (Industry > Sub-Industry > Business Function > Use Case Skill > Sub-Skill) is encoded in the name prefix, not directory depth.

### Skill Taxonomy Tree

```
Health Sciences
├── Provider
│   ├── Clinical Research
│   │   ├── hcls-provider-imaging (router + 7 sub-skills)
│   │   └── hcls-provider-imaging-dicom-parser (standalone)
│   ├── Clinical Data Management
│   │   ├── hcls-provider-cdata-fhir
│   │   ├── hcls-provider-cdata-clinical-nlp
│   │   ├── hcls-provider-cdata-omop
│   │   └── hcls-provider-cdata-clinical-docs (router + 5 sub-skills)
│   └── Revenue Cycle
│       └── hcls-provider-claims-data-analysis
│
├── Pharma
│   ├── Drug Safety
│   │   ├── hcls-pharma-dsafety-pharmacovigilance
│   │   └── hcls-pharma-dsafety-clinical-trial-protocol
│   ├── Genomics
│   │   ├── hcls-pharma-genomics-nextflow
│   │   ├── hcls-pharma-genomics-variant-annotation
│   │   ├── hcls-pharma-genomics-single-cell-qc
│   │   ├── hcls-pharma-genomics-scvi-tools
│   │   └── hcls-pharma-genomics-survival-analysis
│   └── Lab Operations
│       └── hcls-pharma-lab-allotrope
│
└── Cross-Industry
    ├── Research Strategy
    │   └── hcls-cross-research-problem-selection
    └── Knowledge Extensions
        ├── hcls-cross-cke-pubmed
        └── hcls-cross-cke-clinical-trials
```

### Skill Structure

```
hcls-{sub}-{func}-{skill}/
├── SKILL.md           # Main instructions (required)
├── scripts/           # Python helper scripts
├── references/        # Domain documentation
└── assets/            # Templates (optional)
```

### Sub-Skills (for router skills)

Router skills (e.g., `hcls-provider-imaging`) contain sub-skills nested inside:

```
hcls-provider-imaging/
├── SKILL.md                    # Router with intent detection + Step 0 pre-query
├── dicom-parser/SKILL.md       # Sub-skill (parent_skill: hcls-provider-imaging)
├── dicom-ingestion/SKILL.md
├── dicom-analytics/SKILL.md
├── imaging-viewer/SKILL.md
├── imaging-governance/SKILL.md
├── imaging-ml/SKILL.md
└── data-model-knowledge/SKILL.md
```

## Cross-Domain Composition Patterns

The orchestrator composes multiple skills for complex solutions:

| Pattern | Description |
|---------|-------------|
| Imaging + Clinical Integration | DICOM parse → FHIR ingest → Clinical NLP → PubMed enrichment → Streamlit |
| Clinical Data Warehouse | FHIR → OMOP CDM → Governance → Semantic views → Dashboards |
| Drug Safety Signal Detection | FAERS analysis → PubMed search → Clinical NLP → Claims correlation → Dashboard |
| Genomics + Clinical Outcomes | nf-core pipeline → Variant annotation → Survival analysis → ML models |
| Single-Cell Analysis Pipeline | scRNA-seq QC → scvi-tools integration → ML Registry |
| Real-World Evidence Study | Claims cohort → Clinical Trials search → OMOP → Survival → PubMed → Dashboard |
| Clinical Trial Design | Problem validation → Trial search → Literature review → Protocol → Power analysis → Feasibility |
| Lab Data Modernization | Allotrope conversion → Dynamic Tables pipeline → Analytics dashboard |
| Clinical Data App (React) | Domain skills → React/Next.js app → SPCS deployment → PHI masking |
| Document Intelligence | Clinical docs extraction → Search → Agent → Governance |

---

## Featured Solution: Clinical Document Intelligence

**Skill**: [`hcls-provider-cdata-clinical-docs`](skills/hcls-provider-cdata-clinical-docs/)

An end-to-end pipeline for extracting structured intelligence from clinical documents (PDF, DOCX, PNG, JPG, TIFF, TXT) using Snowflake Cortex AI.

### What It Does

```
Clinical PDFs on Stage
    → AI_PARSE_DOCUMENT (OCR / LAYOUT)
    → AI_EXTRACT (classify doc type + extract fields)
    → AI_AGG (handle multi-page split documents)
    → Pivot Views (one per doc type)
    → Cortex Search Service (full-text search)
    → Semantic View + Cortex Agent (natural language queries)
```

### Sub-Skills

| Sub-Skill | Purpose |
|-----------|---------|
| `clinical-document-extraction` | Orchestrator for the full extraction pipeline (gates + phases) |
| `clinical-docs-search` | Create a Cortex Search Service over parsed content |
| `clinical-docs-agent` | Create a Cortex Agent combining Analyst (Semantic View) + Search |
| `clinical-docs-viewer` | Build a Streamlit document viewer |
| `data-model-knowledge` | Query schema and doc type specs via CKE at runtime |

### CKE Architecture (Dual-Layer)

Clinical docs uses two CKE layers for dynamic metadata discovery:

| CKE Layer | Search Service | Answers |
|-----------|---------------|---------|
| **Schema CKE** | `CLINICAL_DOCS_MODEL_SEARCH_SVC` | "What tables exist?" "Which columns contain PHI?" |
| **Spec CKE** | `CLINICAL_DOCS_SPECS_SEARCH_SVC` | "What fields does a discharge summary have?" "What prompt extracts MRN?" |

```
document_type_specs.yaml (authoritative source of truth)
    │
    ├──> CLINICAL_DOCS_SPECS_REFERENCE table → Spec CKE (search service)
    │
    └──> EXTRACTION_CONFIG table (derived)
            └──> GENERATE_DYNAMIC_OBJECTS() stored procedure
                    ├── Pivot views, Semantic View, refresh task
                    ├── Step 7:  CLINICAL_DOCS_MODEL_REFERENCE → Schema CKE
                    └── Step 7b: CLINICAL_DOCS_SPECS_REFERENCE → Spec CKE
```

### Config-Driven Design

The pipeline is fully config-driven. To add a new document type:

1. Add an entry to `references/document_type_specs.yaml`
2. Seed the config table from the spec (INSERT rows or COPY INTO from CSV)
3. `CALL GENERATE_DYNAMIC_OBJECTS('{db}', '{schema}', '{warehouse}', '{stage}')` — one parameterized call does everything:
   - Config deduplication (removes duplicate rows from repeated loads)
   - Classification + type-specific extraction config seeding
   - Classification prompt update (LISTAGG of all discovered types)
   - New pivot view per doc type
   - MRN_PATIENT_MAPPING view creation
   - Refresh task rebuild with dynamic JOINs to all pivot views
   - Semantic View update (DIMENSIONS + METRICS from config table)
   - Schema CKE + Spec CKE corpus refresh

### Key Reference Files

| File | Purpose |
|------|---------|
| `references/document_type_specs.yaml` | Authoritative doc type definitions (fields, prompts, PHI flags) |
| `references/architecture.md` | Pipeline architecture and design decisions |
| `references/metadata_as_cke.md` | CKE pattern documentation and DICOM comparison |
| `references/cortex_ai_functions.md` | Cortex AI function reference (AI_PARSE_DOCUMENT, AI_EXTRACT, AI_AGG, AI_COMPLETE) |
| `references/supported_document_types.md` | Supported input formats and document type catalog |
| `clinical-document-extraction/scripts/dynamic_pipeline_setup.sql` | All DDL — tables, UDFs, CKE search services, GENERATE_DYNAMIC_OBJECTS proc |
| `clinical-document-extraction/scripts/proc_preprocess_clinical_docs.sql` | Preprocessing — splits large PDFs, populates DOCUMENT_HIERARCHY |
| `clinical-document-extraction/scripts/proc_parse_with_images.sql` | AI_PARSE_DOCUMENT — OCR/layout with optional image extraction |
| `clinical-document-extraction/scripts/proc_classify_metadata.sql` | AI_COMPLETE-based document classification |
| `clinical-document-extraction/scripts/proc_extract_type_specific.sql` | AI_EXTRACT — doc-type-specific field extraction |
| `clinical-document-extraction/scripts/proc_classify_aggregated.sql` | AI_AGG-based classification for multi-page split docs |
| `clinical-document-extraction/scripts/proc_extract_with_ai_agg.sql` | AI_AGG-based extraction for multi-page split docs |
| `clinical-document-extraction/scripts/stored_procedures.sql` | Modular stored procedure definitions for each pipeline step |

> **Note**: `dynamic_pipeline_setup.sql` is designed for **Snowsight worksheet execution** (single session). It will NOT work with `snow sql -f` due to session variable scoping, nested `$$` delimiters, and EXECUTE IMMEDIATE parsing. For CLI/CoCo execution, decompose into individual steps — see the execution notes in the file header.

---

## Featured Solution: DICOM Medical Imaging

**Skill**: [`hcls-provider-imaging`](skills/hcls-provider-imaging/)

A comprehensive DICOM imaging solution with an 18-table data model, metadata search, and ML-ready embeddings.

### Sub-Skills

| Sub-Skill | Purpose |
|-----------|---------|
| `dicom-parser` | Parse DICOM file metadata with pydicom, generate DDL from 18-table model |
| `dicom-ingestion` | Build ingestion pipelines (COPY INTO, Dynamic Tables, Streams + Tasks) |
| `dicom-analytics` | Imaging metadata analytics, Cortex Search, radiology NLP |
| `imaging-viewer` | Streamlit DICOM viewer |
| `imaging-governance` | HIPAA compliance, PHI masking, de-identification |
| `imaging-ml` | ML model training and deployment for imaging |
| `data-model-knowledge` | Query the DICOM data model via CKE at runtime |

### CKE Architecture

DICOM uses a single Schema CKE layer:

```
dicom_data_model_reference.xlsx → CSV → DICOM_MODEL_REFERENCE table
                                            → DICOM_MODEL_SEARCH_SVC (Cortex Search)
```

Sub-skills query the search service for table definitions, column types, DICOM tag mappings, and PHI indicators. DDL can be generated dynamically using `CORTEX.COMPLETE()` grounded by search results.

## Snowflake Objects

| Object | Fully Qualified Name |
|--------|---------------------|
| Data Model Table | `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_REFERENCE` |
| Cortex Search Service | `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC` |
| Stage | `UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.dicom_model_stage` |

## Repository Structure

```
coco-healthcare-skills/
├── agents/                              # Orchestrator agent files
│   ├── health-sciences-incubator.md     #   Incubator orchestrator (all skills)
│   └── health-sciences-solutions.md     #   Production orchestrator (approved only)
├── skills/                              # Flat skill directories
│   ├── hcls-provider-imaging/           #   Router + sub-skills inside
│   ├── hcls-provider-cdata-fhir/
│   ├── hcls-pharma-dsafety-pharmacovigilance/
│   ├── hcls-cross-cke-pubmed/
│   └── ...
├── templates/                           # Orchestrator generation templates
│   ├── orchestrator.md.j2              #   Shared Jinja2 template
│   ├── skills_incubator.yaml           #   Incubator skills registry
│   └── skills_production.yaml          #   Production skills registry
├── shared/                              # Shared infrastructure
│   └── preflight/                       #   Prerequisite checker pattern
├── references/                          # Data model spreadsheets
│   ├── dicom_data_model_reference.xlsx  #   DICOM 18-table model (source of truth)
│   └── dicom_model_search_corpus.csv    #   Pre-exported CKE corpus
├── scripts/                             # Setup, generation, and QA scripts
│   ├── generate_orchestrators.py        #   Generate agent profiles from templates
│   ├── setup_dicom_model_knowledge_repo.sql  # DICOM CKE search service setup
│   ├── generate_industry_framework_pdf.py    # ISF PDF generator
│   ├── generate_dicom_model_spreadsheet.py   # DICOM model spreadsheet generator
│   ├── export_search_corpus_csv.py      #   Export CKE corpus to CSV
│   ├── generate_pdf_guide.py            #   PDF guide generator
│   └── qa_validate_orchestrator.py      #   QA validation for orchestrator
├── Industry_Solutions_Framework.pdf     # ISF reference document
├── Healthcare_Solutions_on_Snowflake.pdf # Healthcare solutions overview
├── skills.json.template                 # Clean starting point for skills config
└── README.md
```

## Agent Profiles

Two orchestrator profiles in `agents/` control which skills are available and how requests are routed:

| Profile | File | Purpose |
|---------|------|---------|
| **Incubator** | `health-sciences-incubator.md` | All 18 skills enabled — rapid prototyping, demos, and development |
| **Production** | `health-sciences-solutions.md` | Production-grade skills only — skills graduate here after validation |

Profiles include routing rules (by sub-industry and task type), cross-domain composition patterns, CKE integration guidance, and HIPAA guardrails.

### How Profiles Work

Each profile is a Markdown file with YAML frontmatter (`name`, `description`, `tools`) that serves as a system prompt for Cortex Code. When you activate a profile via `/agents`, Cortex Code loads the system prompt and uses the routing rules inside it to direct your requests to the appropriate skill.

### Configuration Files

| File | Location | Purpose |
|------|----------|---------|
| `skills.json` | `~/.snowflake/cortex/skills.json` | Registers skill paths so Cortex Code can discover them |
| Profile JSON | `~/.snowflake/cortex/profiles/<profile-name>.json` | Defines the profile metadata, skill repos, and system prompt path |
| Agent Markdown | `agents/<profile-name>.md` (in this repo) | The actual system prompt with routing rules and skill taxonomy |

### Switching Between Profiles

Use `/agents` in Cortex Code to list and switch between registered profiles. Only one profile is active at a time.

## Skill Development Lifecycle

This repo is the **incubator** in a two-repo model:

```
Snowflake-Solutions/health-sciences-incubator    ← THIS REPO (Phase 0 & 1)
        │
        │  Skills mature here, then graduate ↓
        │
Snowflake-Solutions/cortex-code-skills           ← SFS production repo (Phase 2)
```

| Phase | Repo | Who | What |
|-------|------|-----|------|
| **Phase 0: Setup** | This repo (incubator) | Tiger Team | Create repo, guidelines, profile, orchestrator |
| **Phase 1: Incubate** | This repo (incubator) | Anyone (SEs, SAs, field) | Branch, create, test, iterate on skills |
| **Phase 2: Harden** | SFS cortex-code-skills | Tiger Team only | Audit, test, promote: draft → review → staging → production |
| **Phase 3: Publish** | Snowflake registry | Tiger Team | Publish production profile for field teams |
| **Phase 4: Consume** | Field environments | Field teams | `cortex profile add health-sciences-solutions` |

### Adding a New Skill

A contributor creates their skill directory under `skills/` and then refreshes the orchestrator so it can route to the new skill. This workflow is designed to become a skill itself — ask the orchestrator to "add a new skill" and it walks through these steps automatically.

1. Create the skill directory following the naming convention: `skills/hcls-{sub}-{func}-{skill}/`
2. Add `SKILL.md` with proper frontmatter (`name`, `description`, `tools`)
3. Register the skill in `templates/skills_incubator.yaml` (triggers, description, domain, and any sub-skills or CKE metadata)
4. Regenerate the orchestrator:
   ```bash
   python scripts/generate_orchestrators.py --profile incubator
   ```
5. Verify `agents/health-sciences-incubator.md` includes the new skill in the taxonomy tree and routing tables
6. Commit the skill directory, registry update, and regenerated orchestrator

For router skills with sub-skills, see `hcls-provider-cdata-clinical-docs/` or `hcls-provider-imaging/` as templates.

### Incubator Milestone Tagging

The incubator does **not** use semantic versioning. Instead, lightweight git tags called **milestones** mark known-good states for specific use cases or demos.

```
m{sequence}-{scope}-{optional-context}
```

| Tag | Meaning |
|-----|---------|
| `m1-imaging` | First stable milestone: imaging skills working end-to-end |
| `m2-imaging-genomics` | Added genomics skills on top of m1 |
| `m3-rwe-demo` | Stable point for a specific RWE customer demo |
| `m4-full-skills` | All skills reorganized and QA-validated |
| `m5-pre-sfs-batch1` | Snapshot before first batch submitted to SFS |

**When to create a milestone:**
- Domain skills pass QA validation for a demo
- Before submitting a batch to the SFS production repo
- Customer-specific engagement needing a frozen state
- After a major reorganization or refactor

**How field teams use milestones:**
```bash
cortex profile add health-sciences-incubator --ref m4-full-skills   # stable demo
cortex profile add health-sciences-incubator                         # bleeding edge (latest main)
```

**Key properties:**
- Zero overhead: `git tag m4-full-skills && git push --tags`
- Not semver — no compatibility promises, just "this worked when tagged"
- Deletable: `git tag -d m3-bad && git push --delete origin m3-bad`
- Incubator only — production uses proper semver (`v1.0.0`) on the SFS repo with immutable releases

## Documentation

| Document | Description |
|----------|-------------|
| [Industry Solutions Framework](Industry_Solutions_Framework.pdf) | Architecture, lifecycle, taxonomy, skills inventory, patterns, and getting started |
| [Healthcare Solutions on Snowflake](Healthcare_Solutions_on_Snowflake.pdf) | Healthcare solutions overview and value proposition |
| [agents/health-sciences-incubator.md](agents/health-sciences-incubator.md) | Orchestrator agent with routing rules, taxonomy tree, CKE integration, cross-domain patterns, and HIPAA guardrails |

## Contributing

This is the **incubator** — contributions are welcome from SEs, SAs, and field teams.

1. Branch from `main`
2. Create your skill under `skills/` following the `hcls-{sub}-{func}-{skill}` naming convention
3. Add `SKILL.md` with proper frontmatter (`name`, `description`, `tools`)
4. Include `scripts/`, `references/`, and `assets/` as needed
5. Test via: `cortex profile add health-sciences-incubator` or `cortex skill add <path>`
6. Push your branch and signal to Tiger Team when ready for Phase 2 promotion

## Acknowledgments

- [Snowflake Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code)
- [Anthropic Life Sciences](https://github.com/anthropics/life-sciences) — foundational genomics/research skills
- [scverse](https://scverse.org/) — single-cell analysis ecosystem
- [nf-core](https://nf-co.re/) — bioinformatics pipeline community
- [OHDSI](https://ohdsi.org/) — OMOP Common Data Model
- [HL7 FHIR](https://hl7.org/fhir/) — healthcare interoperability standard
- [FDA FAERS](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-public-dashboard) — pharmacovigilance data

## License

Apache License 2.0. See individual skill directories for specific licenses.

## Disclaimer

These skills are provided for educational and research purposes. They do not constitute medical, legal, or regulatory advice. Professional consultation is required for clinical applications.
