# Snowflake Agentic Platform

> Describe your healthcare problem. The platform builds the solution.

A composable, skill-based orchestrator on Cortex Code that receives natural language healthcare
requests and autonomously composes the right combination of industry skills and Snowflake platform
capabilities into end-to-end solutions.

## What It Does

```
INPUT:  "I have DICOM files from radiology, FHIR bundles from Epic, and
         daily clinical reports. Build an imaging analytics platform with
         document search, PHI governance, and a patient dashboard."
      + Snowflake account with Cortex

OUTPUT: - Curated Snowflake tables (DICOM metadata, FHIR resources, OMOP CDM)
        - Cortex Search over clinical documents and imaging metadata
        - Semantic views for natural language analytics
        - ML models registered in Snowflake ML Registry
        - Cortex Agent combining Search + Analyst + ML tools
        - Streamlit dashboard or React + SPCS application
        - HIPAA governance (masking, row-access, audit trails)
```

## Core Principles

| Principle | Description |
|-----------|-------------|
| **Composable Skills** | Independent building blocks encoding deep domain expertise. |
| **Orchestrator-Driven** | Generated system prompt detects intent, routes, and chains skills. |
| **Plan-Gated** | No execution without explicit user approval. |
| **Generated, Not Hand-Edited** | Orchestrator produced from YAML registry + Jinja2 template. |
| **Knowledge-Grounded** | CKEs provide RAG over PubMed, ClinicalTrials.gov, and data models. |
| **Governance by Default** | HIPAA guardrails enforced across all workflows. |
| **Cortex Maximalist** | Leverage every Cortex capability: LLM, Search, Analyst, Agent, AI Functions. |

## Architecture

```
+------------------------------------------------------------------+
|  ORCHESTRATOR AGENT (Generated System Prompt on Cortex Code)     |
|  Intent Detection --> Domain Routing --> Skill Composition        |
+------------------------------------------------------------------+
       |            |             |             |
       v            v             v             v
  +---------+  +---------+  +---------+  +-----------+
  |Provider |  |Provider |  | Pharma  |  |  Pharma   |
  |Imaging  |  |ClinData |  |DrugSafe |  | Genomics  |
  +---------+  +---------+  +---------+  +-----------+
       |   +--------+ +--------+ +----------+    |
       |   |Claims  | |  Lab   | |Research  |    |
       |   +--------+ +--------+ +----------+    |
       |                                          |
       |     SHARED KNOWLEDGE (on-demand CKEs)    |
       |     PubMed | ClinicalTrials.gov           |
+------------------------------------------------------------------+
|  SNOWFLAKE PLATFORM SKILLS (10 bundled)                          |
|  Dynamic Tables | Governance | Streamlit | SPCS | ML | dbt       |
|  Cortex AI | Agent | Search | Semantic View                      |
+------------------------------------------------------------------+
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for full architecture details.

## Skill Inventory (18 skills)

### Provider

| Skill | Description |
|-------|-------------|
| `hcls-provider-imaging` | Router: DICOM imaging lifecycle (parse, ingest, analytics, viewer, governance, ML) |
| `hcls-provider-imaging-dicom-parser` | Standalone DICOM metadata parser with 18-table data model |
| `hcls-provider-cdata-fhir` | FHIR R4 resources to analytics-ready Snowflake tables |
| `hcls-provider-cdata-clinical-nlp` | Structured entity extraction from clinical text |
| `hcls-provider-cdata-omop` | EHR/claims to OMOP CDM v5.4 with vocabulary mapping |
| `hcls-provider-cdata-clinical-docs` | Router: clinical document intelligence (extraction, search, agent, viewer) |
| `hcls-provider-claims-data-analysis` | Claims-based RWE: cohort building, treatment patterns, HEDIS |

### Pharma

| Skill | Description |
|-------|-------------|
| `hcls-pharma-dsafety-pharmacovigilance` | FDA FAERS adverse event analysis with PRR/ROR signal detection |
| `hcls-pharma-dsafety-clinical-trial-protocol` | Clinical trial protocol generation for FDA submissions |
| `hcls-pharma-genomics-nextflow` | nf-core pipelines (rnaseq, sarek, atacseq) on sequencing data |
| `hcls-pharma-genomics-variant-annotation` | Variant annotation with ClinVar, gnomAD, ACMG classification |
| `hcls-pharma-genomics-single-cell-qc` | Automated QC for single-cell RNA-seq |
| `hcls-pharma-genomics-scvi-tools` | Deep learning single-cell analysis (scVI, scANVI, totalVI, etc.) |
| `hcls-pharma-genomics-survival-analysis` | Kaplan-Meier, Cox regression, time-to-event analysis |
| `hcls-pharma-lab-allotrope` | Lab instrument files to Allotrope Simple Model JSON/CSV |

### Cross-Industry

| Skill | Description |
|-------|-------------|
| `hcls-cross-research-problem-selection` | Scientific problem selection using Fischbach & Walsh |
| `hcls-cross-cke-pubmed` | RAG over PubMed biomedical literature |
| `hcls-cross-cke-clinical-trials` | RAG over ClinicalTrials.gov registry |

## Repository Structure

```
snowflake-agentic-platform/
+-- docs/                              # Architecture & guides
|   +-- ARCHITECTURE.md
|   +-- EXECUTIVE_VISION.md
|   +-- DEVOPS.md
+-- coco-healthcare-skills/            # Industry skills repository
    +-- agents/                        # Generated orchestrator agents
    +-- skills/                        # 18 healthcare skills
    +-- templates/                     # YAML registry + Jinja2 template
    +-- shared/                        # Preflight checker infrastructure
    +-- scripts/                       # Generation, setup, QA scripts
    +-- references/                    # Data model spreadsheets
    +-- README.md                      # Detailed getting started guide
```

## Tech Stack

| Layer | Technology |
|-------|------------|
| Runtime | Cortex Code (CoCo) |
| Orchestration | Generated Markdown system prompt |
| Industry Skills | SKILL.md + Python scripts + domain references |
| Platform Skills | CoCo bundled (Dynamic Tables, Governance, Streamlit, SPCS, ML, etc.) |
| Knowledge | Cortex Search (PubMed CKE, ClinicalTrials.gov CKE, Data Model CKEs) |
| LLM | Snowflake Cortex LLM |
| AI Functions | AI_PARSE_DOCUMENT, AI_EXTRACT, AI_COMPLETE, AI_AGG |
| Search | Cortex Search |
| Semantic | Cortex Analyst + Semantic Views |
| Agents | Cortex Agent |
| ML | Snowpark ML + Registry |
| Governance | Masking Policies, Row-Access Policies, ACCESS_HISTORY |
| Generation | YAML + Jinja2 + Python |

## Getting Started

See [coco-healthcare-skills/README.md](coco-healthcare-skills/README.md) for detailed setup instructions.

Quick start:
1. Clone the repo
2. Register skills in `~/.snowflake/cortex/skills.json`
3. Create the agent profile
4. Activate with `/agents` in Cortex Code
5. Ask healthcare questions in natural language

## Documentation

- [Architecture](docs/ARCHITECTURE.md) -- Full system design
- [Executive Vision](docs/EXECUTIVE_VISION.md) -- Vision and roadmap
- [DevOps](docs/DEVOPS.md) -- CI/CD, testing, observability
- [Skills README](coco-healthcare-skills/README.md) -- Detailed skill inventory and setup
