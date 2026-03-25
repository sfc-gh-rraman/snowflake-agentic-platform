# Industry Solutions Architect: Architecture

> Describe the business problem. The platform builds the solution.

## Vision

A composable, skill-based orchestrator on Cortex Code that receives natural language healthcare
requests and autonomously composes the right combination of industry skills and Snowflake platform
capabilities into end-to-end solutions -- from data ingestion through governance to analytics
and applications.

```
+-------------------------------------------------------------------------+
|  INPUT                                                                  |
|  "I have DICOM files from radiology, FHIR bundles from Epic, and      |
|   daily clinical reports. Build an imaging analytics platform with      |
|   document search, PHI governance, and a patient dashboard."            |
+-------------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------------+
|  ORCHESTRATOR (Generated System Prompt on Cortex Code)                  |
|                                                                         |
|  Intent Detection --> Domain Routing --> Skill Composition              |
|  --> Platform Affinities --> Plan Gate --> Step-by-Step Execution        |
+-------------------------------------------------------------------------+
        |            |             |             |             |
        v            v             v             v             v
   +--------+  +---------+  +---------+  +-----------+  +----------+
   |Provider |  |Provider |  | Pharma  |  |  Pharma   |  |  Cross-  |
   |Imaging  |  |ClinData |  |DrugSafe |  | Genomics  |  | Industry |
   +--------+  +---------+  +---------+  +-----------+  +----------+
        |            |             |             |             |
        +------+-----+------+-----+------+------+------+------+
               |            |            |             |
               v            v            v             v
   +---------------------------------------------------------------+
   |  SNOWFLAKE PLATFORM SKILLS (Bundled in Cortex Code)           |
   |  Dynamic Tables | Governance | Streamlit | SPCS | ML | dbt    |
   |  Cortex AI | Cortex Agent | Cortex Search | Semantic View     |
   +---------------------------------------------------------------+
               |
               v
   +---------------------------------------------------------------+
   |  CORTEX KNOWLEDGE EXTENSIONS (CKEs)                           |
   |  PubMed RAG | ClinicalTrials.gov RAG | Data Model Knowledge   |
   +---------------------------------------------------------------+
               |
               v
+-------------------------------------------------------------------------+
|  OUTPUT                                                                 |
|  +-- Curated Snowflake tables (FHIR, OMOP, DICOM, claims)             |
|  +-- Cortex Search services over clinical documents and imaging        |
|  +-- Semantic views for natural language analytics                     |
|  +-- Trained ML models registered in Snowflake ML Registry            |
|  +-- Cortex Agent wiring Search + Analyst + ML tools                  |
|  +-- Streamlit dashboards or React + SPCS applications                |
|  +-- HIPAA governance (masking, row-access, audit trails)             |
+-------------------------------------------------------------------------+
```

---

## Core Design Principles

| Principle | Description |
|-----------|-------------|
| **Composable Skills** | Independent building blocks, not monolithic scripts. Each skill encodes deep domain expertise. |
| **Orchestrator-Driven** | A generated system prompt detects intent, routes across domains, and chains skills into solutions. |
| **Plan-Gated** | No execution without explicit user approval. Every multi-step task goes through a plan gate. |
| **Generated, Not Hand-Edited** | Orchestrator is generated from YAML registry + Jinja2 template. Skills snap in automatically. |
| **Knowledge-Grounded** | CKEs provide RAG search over PubMed, ClinicalTrials.gov, and internal data models. |
| **Governance by Default** | HIPAA guardrails enforced as cross-cutting concerns across all workflows. |
| **Snowflake Native** | All data, compute, AI services, and governance run on Snowflake. |
| **Cortex Maximalist** | Leverage every Cortex capability: LLM, Search, Analyst, Agent, AI Functions. |

---

## Three-Layer Architecture

The system has three layers that work together to produce the orchestrator.

```
+------------------------------------------------------------------+
|  Layer 1: YAML REGISTRY                                          |
|  templates/skills_incubator.yaml                                 |
|                                                                  |
|  Single source of truth for all skills:                          |
|  - Skill names, triggers, descriptions, domains                  |
|  - Sub-skills and router relationships                           |
|  - Cross-domain composition patterns                             |
|  - Overlap declarations                                          |
|  - CKE metadata (data sources, used_by, invoke_when)             |
|  - Profile metadata (name, description, intro text)              |
+------------------------------------------------------------------+
                          |
                          v
+------------------------------------------------------------------+
|  Layer 2: JINJA2 TEMPLATE                                        |
|  templates/orchestrator.md.j2                                    |
|                                                                  |
|  Parameterized Markdown defining orchestrator structure:          |
|  - Plan-then-Execute protocol (static)                           |
|  - Platform Skill Selection algorithm (static)                   |
|  - Routing rules (static)                                        |
|  - Skill taxonomy tree (from registry)                           |
|  - Routing tables (from registry)                                |
|  - Cross-domain patterns (from registry)                         |
|  - CKE routing (from registry)                                   |
|  - Guardrails and anti-patterns (static)                         |
+------------------------------------------------------------------+
                          |
                          v
+------------------------------------------------------------------+
|  Layer 3: GENERATED ORCHESTRATOR AGENT                           |
|  agents/health-sciences-incubator.md                             |
|                                                                  |
|  Complete Markdown file with YAML frontmatter that Cortex Code   |
|  loads as a system prompt. NEVER hand-edited.                    |
+------------------------------------------------------------------+
```

### Generation Flow

```
skills_incubator.yaml --+
                        |--> generate_orchestrators.py --> health-sciences-incubator.md
orchestrator.md.j2   --+
```

```bash
python scripts/generate_orchestrators.py --profile incubator
```

### Twin Orchestrator Model

| Property | Incubator | Production |
|----------|-----------|------------|
| Registry | `skills_incubator.yaml` | `skills_production.yaml` |
| Output | `health-sciences-incubator.md` | `health-sciences-solutions.md` |
| Skills | All (experimental + mature) | Graduated only |
| Audience | SEs, SAs, contributors | Field teams, customers |
| Gate | Skills land via PR to main | Skills graduate via Tiger Team review |

---

## Plan-then-Execute Protocol

Every health sciences task follows a mandatory two-phase protocol.

### Phase 1: Plan (Mandatory Gate)

```
User Request
    |
    v
1. Identify sub-industry (Provider, Pharma, Payer)
    |
    v
2. Route by task if sub-industry is ambiguous
    |
    v
3. Scan Skill Routing Tables for trigger keyword matches
    |
    v
4. Check Cross-Domain Patterns if request spans business functions
    |
    v
5. Evaluate Platform Affinities for each skill in the plan
    |
    v
6. Build numbered solution plan:
   - Skill name
   - What it produces
   - Dependencies
   - Governance applicability
    |
    v
7. Present plan to user via ask_user_question
    |
    v
8. WAIT for explicit approval
```

### Phase 2: Execute (Only After Approval)

```
Approved Plan
    |
    v
1. Execute each step in approved order
    |
    v
2. Invoke skills via `skill` tool (never bypass with raw SQL/Bash)
    |
    v
3. Run preflight checks (CKEs, Data Model Knowledge auto-detect)
    |
    v
4. Apply governance guardrails on all patient/clinical data
    |
    v
5. Enrich with CKEs when plan calls for evidence grounding
    |
    v
6. Report back after each major step for user course-correction
    |
    v
7. Test and validate before declaring success
```

### Lightweight Gate

For simple single-skill queries, informational questions, or follow-up steps within an
already-approved plan, the gate can be a single sentence + confirmation.

### Example

```
User: "Design a Phase III trial for a GLP-1 receptor agonist for T2D"

Orchestrator builds plan:
  1. $hcls-cross-cke-clinical-trials   --> search for STEP trial references
  2. $hcls-cross-cke-pubmed            --> review STEP trial publications
  3. $hcls-pharma-genomics-survival-analysis --> power analysis for endpoints
  4. $hcls-pharma-dsafety-clinical-trial-protocol --> generate protocol
  5. cortex-ai-functions               --> AI_COMPLETE for narrative (affinity)

Orchestrator presents plan via ask_user_question --> user approves --> execute
```

---

## Routing Logic

The orchestrator uses a four-step algorithm to determine which skills to invoke.

### Step 1: Route by Sub-Industry

| Customer Type | Sub-Industry | Examples |
|---------------|--------------|----------|
| Hospital, health system, clinic, IDN | Provider | Epic, Cerner, clinical research orgs |
| Pharma, biotech, CRO | Pharma | Drug development, trials, genomics |
| Health plan, TPA, PBM | Payer | Claims adjudication, member analytics |

### Step 2: Route by Task (Disambiguation)

When the customer straddles sub-industries, route by the TASK being performed:

| Task Type | Route To | Regardless Of |
|-----------|----------|---------------|
| Clinical data / EHR | Provider > Clinical Data Management | Customer type |
| Drug safety / adverse events | Pharma > Drug Safety | Customer type |
| Imaging workflows | Provider > Clinical Research | Customer type |
| Genomic analysis | Pharma > Genomics | Customer type |
| Claims analysis | Provider > Revenue Cycle | Context-dependent |

### Step 3: Cross-Industry Skills

Available to ALL sub-industries:

- `hcls-cross-research-problem-selection` -- scientific problem validation
- `hcls-cross-cke-pubmed` -- PubMed biomedical literature search
- `hcls-cross-cke-clinical-trials` -- ClinicalTrials.gov registry search

### Step 4: Accept Overlaps

Some skills serve multiple sub-industries:

- `claims-data-analysis` -- Provider (revenue cycle) + Payer (claims processing)
- `survival-analysis` -- Pharma (clinical outcomes) + Provider (clinical research)
- `clinical-nlp` -- Provider (EHR extraction) + Pharma (safety narrative mining)
- `clinical-docs` -- Provider (document intelligence) + Pharma (safety narrative extraction)

---

## Skill Taxonomy

Skills are organized in a five-level hierarchy:

```
Industry / Sub-Industry / Business Function / Use Case Skill / Sub-Skill
```

Naming convention encodes hierarchy in a flat directory structure:

```
hcls-{sub-industry}-{function}-{skill}
```

### Skill Types

| Type | Description | Example |
|------|-------------|---------|
| **Router** | Detects intent, routes to sub-skills. Has setup, preflight, workflow. | `hcls-provider-imaging` (7 sub-skills) |
| **Sub-skill** | Handles one task within a router. Loaded by router, not user. | `dicom-parser`, `clinical-docs-search` |
| **Standalone** | Self-contained, no router or sub-skills. | `hcls-provider-cdata-fhir` |

### Full Taxonomy

```
Health Sciences
|-- Provider
|   |-- Clinical Research
|   |   |-- hcls-provider-imaging (router + 7 sub-skills)
|   |   +-- hcls-provider-imaging-dicom-parser (standalone)
|   |-- Clinical Data Management
|   |   |-- hcls-provider-cdata-fhir
|   |   |-- hcls-provider-cdata-clinical-nlp
|   |   |-- hcls-provider-cdata-omop
|   |   +-- hcls-provider-cdata-clinical-docs (router + 5 sub-skills)
|   +-- Revenue Cycle
|       +-- hcls-provider-claims-data-analysis
|
|-- Pharma
|   |-- Drug Safety
|   |   |-- hcls-pharma-dsafety-pharmacovigilance
|   |   +-- hcls-pharma-dsafety-clinical-trial-protocol
|   |-- Genomics
|   |   |-- hcls-pharma-genomics-nextflow
|   |   |-- hcls-pharma-genomics-variant-annotation
|   |   |-- hcls-pharma-genomics-single-cell-qc
|   |   |-- hcls-pharma-genomics-scvi-tools
|   |   +-- hcls-pharma-genomics-survival-analysis
|   +-- Lab Operations
|       +-- hcls-pharma-lab-allotrope
|
|-- Payer
|   +-- Claims Processing
|       +-- (future skills)
|
+-- Cross-Industry
    |-- Research Strategy: hcls-cross-research-problem-selection
    +-- Knowledge Extensions: cke-pubmed, cke-clinical-trials
```

### Skill Structure

```
hcls-{sub}-{func}-{skill}/
+-- SKILL.md           # Main instructions (required)
+-- scripts/           # Python helper scripts
+-- references/        # Domain documentation
+-- assets/            # Templates (optional)
```

### Router Skill Internal Structure

```
hcls-provider-imaging/
+-- SKILL.md                    # Router: intent detection + Step 0 pre-query
+-- dicom-parser/SKILL.md       # Sub-skill
+-- dicom-ingestion/SKILL.md
+-- dicom-analytics/SKILL.md
+-- imaging-viewer/SKILL.md
+-- imaging-governance/SKILL.md
+-- imaging-ml/SKILL.md
+-- data-model-knowledge/SKILL.md
```

---

## Platform Affinities

Platform affinities are a declarative mechanism for industry skills to declare which Snowflake
platform skills enhance them and under what conditions.

### How It Works

Each SKILL.md declares `platform_affinities` in its YAML frontmatter:

```yaml
---
name: hcls-provider-cdata-fhir
platform_affinities:
  produces: [tables, views, stages]
  benefits_from:
    - skill: dynamic-tables
      when: "incremental refresh or ongoing FHIR feeds"
    - skill: data-governance
      when: "FHIR tables contain PHI"
    - skill: developing-with-streamlit
      when: "user wants a patient data dashboard"
---
```

### Affinity Evaluation Algorithm

```
For each domain skill in the plan:
    |
    v
1. Read platform_affinities from SKILL.md frontmatter
    |
    v
2. For each benefits_from entry:
   evaluate 'when' condition against user's request
    |
    v
3. If condition matches: add platform skill as follow-on step
    |
    v
4. Deduplicate: if multiple skills trigger the same platform skill, include once
```

### 10 Platform Skills Available

| Platform Skill | When to Include |
|----------------|-----------------|
| `dynamic-tables` | Incremental refresh, ongoing data feeds, streaming pipelines |
| `data-governance` | PHI/PII present, masking policies, row-access policies, audit |
| `data-quality` | Data validation, conformance checks, completeness monitoring |
| `semantic-view` | Natural language queries, analytics layer, BI integration |
| `developing-with-streamlit` | Dashboards, viewers, interactive UIs |
| `deploy-to-spcs` | Container services, GPU compute, custom viewers |
| `machine-learning` | Model training, registry, deployment, inference |
| `cortex-ai-functions` | AI_PARSE_DOCUMENT, AI_COMPLETE, AI_EXTRACT, text analytics |
| `cortex-agent` | Conversational agents over domain data |
| `search-optimization` | Full-text or semantic search over extracted content |

### Worked Example

```
User: "Build a FHIR data pipeline with a patient dashboard and PHI masking"

1. $hcls-provider-cdata-fhir selected (triggers: FHIR, HL7, Patient resource)
2. Read affinities: produces=[tables, views, stages]
3. Evaluate: dynamic-tables when 'incremental refresh'    --> YES (pipeline = ongoing)
4. Evaluate: data-governance when 'PHI present'           --> YES (user said PHI masking)
5. Evaluate: developing-with-streamlit when 'dashboard'   --> YES
6. Final plan: FHIR ingest -> Dynamic Tables -> Governance -> Streamlit dashboard
```

---

## Cortex Knowledge Extensions (CKEs)

CKEs are standalone composable skills backed by Cortex Search Services. They provide on-demand
RAG search over external knowledge corpora.

### External CKEs (Marketplace)

| CKE Skill | Data Source | Invoke When |
|-----------|-------------|-------------|
| `hcls-cross-cke-pubmed` | PubMed biomedical literature | Drug-event associations, radiology research, clinical NLP context, research landscape review |
| `hcls-cross-cke-clinical-trials` | ClinicalTrials.gov registry | Trial design benchmarking, feasibility analysis, eligibility criteria, endpoint definitions |

### Internal CKEs (Data Model Knowledge)

Router skills use internal CKE layers -- Cortex Search Services over their own data models.
These auto-fire as a pre-step (Step 0) to ground DDL generation, extraction config, and
schema queries in live reference models.

| Router | Search Service | What It Answers |
|--------|---------------|-----------------|
| `hcls-provider-imaging` | `DICOM_MODEL_SEARCH_SVC` | Table definitions, column types, DICOM tags, PHI indicators |
| `hcls-provider-cdata-clinical-docs` | `CLINICAL_DOCS_MODEL_SEARCH_SVC` + `CLINICAL_DOCS_SPECS_SEARCH_SVC` | Schema + doc type specs, extraction prompts, field definitions |

### Preflight Pattern

Before invoking any CKE, the skill runs a probe query to verify the Marketplace listing
is installed. If MISSING, the skill skips CKE enrichment gracefully and continues with its
primary task. Skills work without CKEs but provide richer results with them.

```python
checker = PreflightChecker(conn)
checker.add_cortex_search(
    name="DICOM Model Search Service",
    svc_fqn="UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC",
    setup="Run: scripts/setup_dicom_model_knowledge_repo.sql",
    fallback="Skill will use hardcoded DICOM schema definitions instead of dynamic search.",
    required=False,
)
results = checker.run()
# Status: READY | MISSING | ERROR | SKIPPED
```

---

## Cross-Domain Composition Patterns

When a user's request spans multiple business functions, the orchestrator composes skills
using predefined patterns. Patterns are guides, not rigid scripts -- the orchestrator adapts
them to the user's actual request.

| Pattern | Skill Chain |
|---------|-------------|
| **Imaging + Clinical Integration** | DICOM parse -> FHIR ingest -> Clinical NLP -> PubMed enrichment -> UI |
| **Clinical Data Warehouse (OMOP)** | FHIR ingest -> OMOP CDM transform -> HIPAA governance -> Semantic views |
| **Drug Safety Signal Detection** | FAERS analysis -> PubMed literature -> Clinical NLP -> Claims correlation |
| **Genomics + Clinical Outcomes** | nf-core pipeline -> Variant annotation -> Survival analysis -> ML models |
| **Single-Cell Analysis Pipeline** | scRNA-seq QC -> scvi-tools integration -> ML Registry |
| **Real-World Evidence Study** | Claims cohort -> ClinicalTrials.gov -> OMOP -> Survival -> PubMed validation |
| **Clinical Trial Design** | Problem validation -> Trial search -> Literature -> Protocol -> Power analysis |
| **Lab Data Modernization** | Allotrope conversion -> Dynamic Tables -> Analytics dashboard |
| **Clinical Data Application (React)** | Domain skills -> React/Next.js app -> SPCS deployment -> PHI masking |
| **Clinical Document Intelligence** | Clinical docs extraction -> NLP enrichment -> PubMed -> FHIR -> Governance -> Semantic views |

### Adapting Patterns

- Skip steps that don't apply
- Reorder when the user already has intermediate outputs
- Combine patterns when the request spans multiple
- Add steps for capabilities not in the pattern (e.g., governance)
- Always ask if the adaptation is unclear

---

## Defense-in-Depth: Guardrail Architecture

### Three-Layer Guardrail System

Complex skills (e.g., clinical-docs) enforce a three-layer guardrail system:

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| 1 | `AGENTS.md` (profile-level rules) | Session-wide constraints |
| 2 | Gate micro-skills + Phase skills | Structural decomposition -- model cannot skip steps |
| 3 | `hooks.json` | Hard blocks on DDL/DML without user confirmation |

### Gate and Phase Pattern

```
+-- Tier 1: Gates (Pre-conditions) ---------+
|                                            |
|  confirm-environment                       |
|    Validate connection, db, schema, wh     |
|                                            |
|  confirm-doc-types                         |
|    Discover and confirm document types      |
|                                            |
|  confirm-pipeline-config                   |
|    Review and approve extraction config     |
|                                            |
+-- Tier 2: Phases (Execution) -------------+
|                                            |
|  parse-and-refresh                         |
|    AI_PARSE_DOCUMENT + dynamic objects      |
|                                            |
|  classify                                  |
|    AI_COMPLETE-based document classification|
|                                            |
|  extract                                   |
|    AI_EXTRACT type-specific field extraction|
|                                            |
+--------------------------------------------+
```

### HIPAA Guardrails (Always Enforced)

- Always apply HIPAA governance before exposing any patient data
- Never store or display PHI without masking policies in place
- Always use `IS_ROLE_IN_SESSION()` (not `CURRENT_ROLE()`) in masking/row-access policies
- Always recommend audit trails via `ACCESS_HISTORY` for PHI-containing tables
- Prefer de-identified datasets for analytics and ML training
- Always validate FHIR/HL7/OMOP data quality before building downstream tables
- For genomic data: ensure proper consent tracking and data use agreements
- For FAERS/pharmacovigilance: note limitations of spontaneous reporting data

### Anti-Patterns

- Do NOT use clinical-nlp on raw files (PDF, DOCX, images) -- use clinical-docs first
- Do NOT use survival-analysis without a defined cohort -- build the cohort first
- Do NOT invoke CKEs for non-evidence tasks (pipeline construction, SQL generation)
- Do NOT skip preflight checks -- they run automatically
- Do NOT force-follow a pattern when the request only partially matches -- adapt it
- Do NOT use imaging-dicom-parser (standalone) for full imaging workflows -- use the router
- Do NOT bypass the plan gate for multi-step pipelines or patient data workflows

---

## Shared Infrastructure

### Preflight Checker

Skills use a shared preflight checker to verify Snowflake dependencies before execution:

```
shared/preflight/
+-- checker.py     # PreflightChecker class
+-- configs.py     # Dependency configurations
+-- __init__.py
```

The checker supports:
- Snowflake tables (`add_table`)
- Cortex Search services (`add_cortex_search`)
- Marketplace listings (`add_marketplace_listing`)

Each dependency has: `name`, `probe_sql`, `setup_instructions`, `fallback`, `required`.

Status values: `READY` | `MISSING` | `ERROR` | `SKIPPED`

### Generation Pipeline

```
1. Edit YAML registry  (templates/skills_incubator.yaml)
2. Edit Jinja2 template (templates/orchestrator.md.j2)   -- structural changes only
3. Run: python scripts/generate_orchestrators.py --profile incubator
4. Script loads YAML, builds SkillObj instances, groups by domain, renders template
5. Output: agents/health-sciences-incubator.md
6. If --profile both: drift check between incubator and production
```

### QA Validation (12-Check Suite)

```bash
python scripts/qa_validate_orchestrator.py
```

| Check | What It Validates |
|-------|-------------------|
| 1. $refs -> SKILL.md name | Every $ref in orchestrator points to a SKILL.md with matching name |
| 2. SKILL.md -> orchestrator ref | Every top-level SKILL.md is referenced in the orchestrator |
| 3. Folder name == SKILL.md name | Directory name matches the name: field in SKILL.md frontmatter |
| 4. Imaging sub-skills | All imaging sub-skill directories exist and are referenced |
| 5. Taxonomy tree entries | Skills in the taxonomy tree exist in the filesystem |
| 6. Reference consistency | Counts $ref occurrences per skill |
| 7. Standalone skills | Standalone skills exist and are referenced |
| 8. Twin drift | Structural differences between incubator and production orchestrators |
| 9. Registry bidirectional | Every registry entry has a directory and vice versa |
| 10. Platform affinities | All SKILL.md files have valid platform_affinities frontmatter |
| 11. CKE used_by | CKE used_by references point to real skills |
| 12. Overlap entries | Overlap skills exist and are referenced in orchestrator |

---

## Observability and Audit

### Execution Logging

Each orchestrator session produces artifacts that can be logged to Snowflake tables for
auditability -- critical in healthcare where regulatory compliance demands traceability.

```sql
CREATE TABLE IF NOT EXISTS ORCHESTRATOR_EXECUTION_LOG (
    session_id STRING,
    plan_id STRING,
    user_request TEXT,
    detected_domain STRING,
    plan_steps VARIANT,
    plan_approved BOOLEAN,
    started_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at TIMESTAMP_NTZ,
    status STRING
);

CREATE TABLE IF NOT EXISTS SKILL_EXECUTION_LOG (
    session_id STRING,
    step_number NUMBER,
    skill_name STRING,
    skill_type STRING,
    input_context VARIANT,
    artifacts_produced VARIANT,
    governance_applied VARIANT,
    preflight_status STRING,
    started_at TIMESTAMP_NTZ,
    completed_at TIMESTAMP_NTZ,
    status STRING,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS GOVERNANCE_AUDIT_LOG (
    session_id STRING,
    skill_name STRING,
    governance_action STRING,
    target_object STRING,
    policy_type STRING,
    policy_definition VARIANT,
    applied_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### What Gets Logged

| Component | Captured Data |
|-----------|---------------|
| Orchestrator plan | User request, detected domain, generated plan, approval status |
| Skill execution | Skill name, artifacts produced, governance applied, preflight status |
| CKE invocations | Query text, result count, source (PubMed/ClinicalTrials), latency |
| Governance actions | Policy type, target table, role-based access rules applied |
| Platform skill usage | Which platform skills were triggered by affinities |

---

## Technology Stack

| Layer | Technology | Role |
|-------|------------|------|
| **Runtime** | Cortex Code (CoCo) | IDE and agent execution environment |
| **Orchestration** | Generated Markdown system prompt | Intent detection, routing, skill composition |
| **Industry Skills** | SKILL.md + Python scripts + references | Domain expertise as composable building blocks |
| **Platform Skills** | CoCo bundled skills | Dynamic Tables, Governance, Streamlit, SPCS, ML, dbt, Cortex AI/Agent/Search |
| **Knowledge** | Cortex Search Services | RAG over PubMed, ClinicalTrials.gov, data models |
| **LLM** | Snowflake Cortex LLM | Code generation, entity extraction, classification, summarization |
| **Search** | Cortex Search | Document retrieval, CKE queries, data model knowledge |
| **Semantic Queries** | Cortex Analyst + Semantic Views | Natural language queries over curated tables |
| **Agent Orchestration** | Cortex Agent | Conversational agents combining Search + Analyst + ML |
| **ML** | Snowpark ML + Registry | Model training, versioning, inference functions |
| **App Hosting** | SPCS or Streamlit in Snowflake | React/FastAPI containers or native Streamlit apps |
| **Governance** | Masking Policies, Row-Access Policies, ACCESS_HISTORY | HIPAA compliance |
| **Generation** | YAML + Jinja2 + Python | Orchestrator generation pipeline |
| **Validation** | 12-check QA suite | Orchestrator integrity verification |

---

## Skill Development Lifecycle

```
Phase 0: Setup         Phase 1: Incubate     Phase 2: Harden        Phase 3: Publish
Tiger Team             Anyone (SE/SA/field)   Tiger Team only        Tiger Team
                       |                      |                      |
Create repo,     -->   Branch, create,   -->  Audit, test,      --> Publish to
guidelines,            test, iterate          promote: draft >       Snowflake registry
profile, orchestrator  on skills              review > staging >
                                              production

                                                                     Phase 4: Consume
                                                                     Field teams
                                                                     |
                                                                     cortex profile add
                                                                     health-sciences-solutions
```

### Two-Repo Model

```
coco-healthcare-skills (incubator)       cortex-code-skills (production)
  - All skills, experimental + mature      - Graduated skills only
  - Skills land via PR to main             - Skills land via Tiger Team review
  - Milestone tagging (m1-imaging, etc.)   - Semantic versioning (v1.0.0)
```

### Adding a New Skill

1. Create `skills/hcls-{sub}-{func}-{skill}/` directory
2. Add `SKILL.md` with proper frontmatter (`name`, `description`, `tools`, `platform_affinities`)
3. Register in `templates/skills_incubator.yaml` (triggers, description, domain)
4. Regenerate: `python scripts/generate_orchestrators.py --profile incubator`
5. Verify with: `python scripts/qa_validate_orchestrator.py`
6. Commit skill directory + registry update + regenerated orchestrator

### Milestone Tagging

The incubator uses lightweight git tags (not semver):

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

---

## Cortex Service Utilization

Every step in the platform leverages Snowflake Cortex services:

| Workflow Phase | Cortex Services Used |
|----------------|---------------------|
| **Orchestrator Planning** | CoCo LLM (intent detection), skill routing tables (keyword matching) |
| **Data Ingestion** | Cortex AI Functions (AI_PARSE_DOCUMENT for PDFs/images) |
| **Document Processing** | Cortex AI (AI_EXTRACT, AI_COMPLETE, AI_AGG for classification/extraction) |
| **Clinical NLP** | Cortex LLM (entity extraction, ICD coding, medication parsing) |
| **Search Indexing** | Cortex Search (service creation over documents, DICOM metadata, CKEs) |
| **Semantic Modeling** | Cortex Analyst (semantic view generation for natural language queries) |
| **ML Training** | Snowpark ML (model training), ML Registry (model versioning) |
| **Agent Creation** | Cortex Agent (wiring Search + Analyst + ML inference tools) |
| **Evidence Grounding** | Cortex Search (CKE queries over PubMed, ClinicalTrials.gov) |
| **Governance** | Masking Policies, Row-Access Policies, ACCESS_HISTORY audit |
| **App Generation** | CoCo LLM (Streamlit/React code generation via platform skills) |
| **Deployment** | SPCS (container deployment), Streamlit in Snowflake (native apps) |

---

## Repository Structure

```
snowflake-agentic-platform/
+-- docs/
|   +-- ARCHITECTURE.md                # This document
|   +-- EXECUTIVE_VISION.md            # Executive summary and vision
|   +-- DEVOPS.md                      # CI/CD, testing, observability
|
+-- coco-healthcare-skills/            # Industry skills repository
    +-- agents/                        # Generated orchestrator agents
    |   +-- health-sciences-incubator.md
    |   +-- health-sciences-solutions.md
    +-- skills/                        # Flat skill directories
    |   +-- hcls-provider-imaging/
    |   +-- hcls-provider-imaging-dicom-parser/
    |   +-- hcls-provider-cdata-fhir/
    |   +-- hcls-provider-cdata-clinical-nlp/
    |   +-- hcls-provider-cdata-omop/
    |   +-- hcls-provider-cdata-clinical-docs/
    |   +-- hcls-provider-claims-data-analysis/
    |   +-- hcls-pharma-dsafety-pharmacovigilance/
    |   +-- hcls-pharma-dsafety-clinical-trial-protocol/
    |   +-- hcls-pharma-genomics-nextflow/
    |   +-- hcls-pharma-genomics-variant-annotation/
    |   +-- hcls-pharma-genomics-single-cell-qc/
    |   +-- hcls-pharma-genomics-scvi-tools/
    |   +-- hcls-pharma-genomics-survival-analysis/
    |   +-- hcls-pharma-lab-allotrope/
    |   +-- hcls-cross-research-problem-selection/
    |   +-- hcls-cross-cke-pubmed/
    |   +-- hcls-cross-cke-clinical-trials/
    +-- templates/                     # Orchestrator generation templates
    |   +-- orchestrator.md.j2
    |   +-- skills_incubator.yaml
    |   +-- skills_production.yaml
    +-- shared/                        # Shared infrastructure
    |   +-- preflight/
    +-- scripts/                       # Generation, setup, QA scripts
    |   +-- generate_orchestrators.py
    |   +-- qa_validate_orchestrator.py
    |   +-- setup_dicom_model_knowledge_repo.sql
    +-- references/                    # Data model spreadsheets
    +-- README.md
```

---

## Evolution from Original Architecture

This architecture supersedes the original LangGraph-based agent platform design. Key concepts
were preserved and adapted:

| Original Concept | Current Implementation | Notes |
|------------------|----------------------|-------|
| Meta-Agent (Planner) | Orchestrator system prompt | Declarative (YAML + Jinja2) vs imperative (Python) |
| Agent Registry (Cortex Search) | `skills_incubator.yaml` + trigger routing tables | Could add Cortex Search for fuzzy matching in future |
| Sub-agent state machines | Router skills with sub-skills | Same decomposition, simpler runtime |
| Validation Agent (5 check suites) | Preflight checker + plan-gate protocol + defense-in-depth | Room to add data quality / semantic validation skills |
| Agent communication (3 layers) | Platform affinities + artifact references | Declarative composition vs runtime message passing |
| LangGraph checkpointing | CoCo session state + plan-gate re-entry | No custom checkpointing needed |
| App Code Generator | `build-react-app` + `deploy-to-spcs` platform skills | Leverages bundled CoCo skills |
| LangSmith observability | Execution logging tables + QA validation | Can add LangSmith if needed |
| Self-healing retry | Preflight fallbacks + orchestrator course-correction | Skills degrade gracefully when dependencies missing |
| Domain agnostic | Domain-specific (healthcare) with extensible framework | Trade-off: depth over breadth |

### What the Framework Enables Beyond Healthcare

The three-layer architecture (YAML registry + Jinja2 template + generated orchestrator) is
industry-agnostic. To create an orchestrator for a different industry:

1. Create a new YAML registry (e.g., `skills_energy.yaml`) with industry-specific skills
2. Reuse the same Jinja2 template (routing rules, plan-gate protocol, guardrails adapt)
3. Generate a new orchestrator (e.g., `energy-solutions.md`)
4. Register new industry skills under `skills/`

The orchestrator logic guide, generation pipeline, QA validation, and preflight infrastructure
are all reusable across industries.

---

## Next Steps

1. **Enrich validation** -- Add a cross-cutting `hcls-cross-validation` skill implementing
   data completeness, schema consistency, and semantic validation checks from original design
2. **Execution logging** -- Implement the observability tables for auditability
3. **Payer skills** -- Extend the taxonomy with Payer > Claims Processing skills
4. **Cortex Search routing** -- Augment keyword-based routing with Cortex Search over skill
   capabilities for fuzzy intent matching
5. **Improvement loop** -- Formalize an `hcls-cross-improvement` skill that re-invokes the
   orchestrator with delta requests against existing artifacts
6. **Multi-industry expansion** -- Apply the framework to Energy, Financial Services, or
   other verticals using the same three-layer pattern

---

*Document Version: 2.0*
*Created: March 4, 2026*
*Revised: March 25, 2026*
*Supersedes: v1.0 (LangGraph-based agent platform)*
