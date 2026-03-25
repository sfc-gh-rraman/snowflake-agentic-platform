# Industry Solutions Architect: Executive Vision

---

## Slide 1: The Problem

### Building Healthcare AI Solutions on Snowflake is Hard

```
+-------------------------------------------------------------------------+
|                     CURRENT STATE: MANUAL ASSEMBLY                      |
+-------------------------------------------------------------------------+
|                                                                         |
|  Customer has:                                                          |
|  +-- Raw healthcare data (DICOM images, FHIR bundles, clinical PDFs)   |
|  +-- A use case ("build an imaging analytics platform with search")    |
|  +-- Snowflake account with Cortex                                     |
|                                                                         |
|  To get to a working solution, they must:                               |
|                                                                         |
|  1  Data Engineering        |  Weeks of work                            |
|     +-- Ingest DICOM, parse FHIR, transform to OMOP CDM               |
|                                                                         |
|  2  Document Processing     |  Days of work                             |
|     +-- OCR, classification, entity extraction, chunking               |
|                                                                         |
|  3  Search & RAG Setup      |  Days of work                             |
|     +-- Cortex Search config, document indexing, CKE setup             |
|                                                                         |
|  4  ML Engineering          |  Weeks of work                            |
|     +-- Feature engineering, model training, registry                  |
|                                                                         |
|  5  Governance              |  Days of work                             |
|     +-- PHI masking, row-access policies, HIPAA audit trails           |
|                                                                         |
|  6  App Development         |  Weeks of work                            |
|     +-- Streamlit/React, Cortex Agent, SPCS deployment                 |
|                                                                         |
|                                                                         |
|  TOTAL: 2-3 months | Multiple specialists | High failure rate          |
|                                                                         |
+-------------------------------------------------------------------------+
```

**Pain Points:**
- Too many manual steps requiring specialized healthcare + Snowflake expertise
- Each step is disconnected -- errors compound
- Long time-to-value discourages adoption
- Customers underutilize Cortex capabilities
- HIPAA compliance is afterthought, not built-in

---

## Slide 2: What We've Built

### Composable Skills with an AI Orchestrator

```
+-------------------------------------------------------------------------+
|           INDUSTRY SOLUTIONS ARCHITECT ON CORTEX CODE                   |
+-------------------------------------------------------------------------+
|                                                                         |
|  INPUT                                                                  |
|  +-- "I have DICOM files from radiology, FHIR bundles from Epic,      |
|  |    and daily clinical reports. Build an imaging analytics platform   |
|  |    with document search, PHI governance, and a patient dashboard."  |
|  +-- Snowflake account                                                 |
|                                                                         |
|                              |                                          |
|                              v                                          |
|                                                                         |
|  +---------------------------------------------------------------+    |
|  |              ORCHESTRATOR (Generated System Prompt)             |    |
|  |                                                                 |    |
|  |  Intent Detection --> Domain Routing --> Skill Composition      |    |
|  |  --> Platform Affinities --> Plan Gate --> Execute               |    |
|  +---------------------------------------------------------------+    |
|                              |                                          |
|                              v                                          |
|                                                                         |
|  +-------+  +-------+  +-------+  +-------+  +-------+  +-------+    |
|  |Imaging|  | FHIR  |  |Clin   |  |Pharma |  |Genomic|  | Cross |    |
|  |Router |  |Ingest |  |Docs   |  |Safety |  |Pipes  |  |CKEs   |    |
|  +-------+  +-------+  +-------+  +-------+  +-------+  +-------+    |
|       |          |          |          |          |          |          |
|       +----+-----+----+----+----+-----+----+----+----+-----+          |
|            |          |         |          |         |                  |
|            v          v         v          v         v                  |
|  +---------------------------------------------------------------+    |
|  |  SNOWFLAKE PLATFORM SKILLS (10 bundled)                        |    |
|  |  Dynamic Tables | Governance | Streamlit | SPCS | ML | dbt     |    |
|  |  Cortex AI | Agent | Search | Semantic View                    |    |
|  +---------------------------------------------------------------+    |
|                              |                                          |
|                              v                                          |
|                                                                         |
|  OUTPUT (Composed Solution)                                             |
|  +-- Curated tables (DICOM metadata, FHIR resources, OMOP CDM)        |
|  +-- Cortex Search over clinical documents and imaging                 |
|  +-- Semantic views for natural language analytics                     |
|  +-- ML models registered in Snowflake ML Registry                    |
|  +-- Cortex Agent combining Search + Analyst + ML                     |
|  +-- Streamlit dashboard or React app on SPCS                         |
|  +-- HIPAA governance enforced across all artifacts                   |
|                                                                         |
|  RESULT: 2-3 months --> hours | Composable | Governance by default     |
|                                                                         |
+-------------------------------------------------------------------------+
```

**What We Demonstrated:**
- **Skill-based composition** -- 18 healthcare skills snap together like building blocks
- **Orchestrator-driven** -- Natural language in, composed solution out
- **Knowledge-grounded** -- PubMed and ClinicalTrials.gov RAG for evidence
- **Governance by default** -- HIPAA guardrails enforced across every workflow
- **Plan-gated** -- No execution without user approval

---

## Slide 3: How It Works

### The Architecture

```
+-------------------------------------------------------------------------+
|                   THREE-LAYER DECLARATIVE ARCHITECTURE                   |
+-------------------------------------------------------------------------+
|                                                                         |
|  Layer 1: YAML SKILL REGISTRY                                          |
|  +---------------------------------------------------------------+    |
|  |  skills_incubator.yaml                                         |    |
|  |  - 18 skills with triggers, domains, sub-skills                |    |
|  |  - 10 cross-domain composition patterns                        |    |
|  |  - Overlap declarations and CKE metadata                       |    |
|  +---------------------------------------------------------------+    |
|                              |                                          |
|                              v                                          |
|  Layer 2: JINJA2 TEMPLATE                                               |
|  +---------------------------------------------------------------+    |
|  |  orchestrator.md.j2                                            |    |
|  |  - Plan-then-Execute protocol                                  |    |
|  |  - Routing rules (sub-industry, task, cross-domain, overlaps)  |    |
|  |  - Platform affinity evaluation                                |    |
|  |  - Guardrails and anti-patterns                                |    |
|  +---------------------------------------------------------------+    |
|                              |                                          |
|                              v                                          |
|  Layer 3: GENERATED ORCHESTRATOR                                        |
|  +---------------------------------------------------------------+    |
|  |  health-sciences-incubator.md                                  |    |
|  |  - Complete system prompt loaded into Cortex Code              |    |
|  |  - NEVER hand-edited; all changes flow through Layers 1-2     |    |
|  +---------------------------------------------------------------+    |
|                                                                         |
+-------------------------------------------------------------------------+
```

**Key Innovation: The orchestrator is generated, not coded.**
- Add a new skill? Edit the YAML, regenerate, done.
- Change routing logic? Edit the template, regenerate, done.
- QA validation? 12-check suite ensures structural integrity.

---

## Slide 4: Skill Coverage

### 18 Skills Across Healthcare

```
Health Sciences
|-- Provider
|   |-- Clinical Research: DICOM imaging (7 sub-skills) + standalone parser
|   |-- Clinical Data Mgmt: FHIR, Clinical NLP, OMOP, Clinical Docs (5 sub-skills)
|   +-- Revenue Cycle: Claims analysis / RWE
|
|-- Pharma
|   |-- Drug Safety: Pharmacovigilance (FAERS), Clinical Trial Protocol
|   |-- Genomics: Nextflow, Variant Annotation, Single-Cell QC, scvi-tools, Survival
|   +-- Lab Ops: Allotrope instrument standardization
|
+-- Cross-Industry
    |-- Research: Problem selection (Fischbach & Walsh)
    +-- Knowledge: PubMed CKE, ClinicalTrials.gov CKE
```

**Each skill delivers deep domain expertise:**
- DICOM imaging: 18-table data model, PHI de-identification, ML-ready embeddings
- Clinical Docs: AI_PARSE_DOCUMENT + AI_EXTRACT pipeline with defense-in-depth guardrails
- Pharmacovigilance: PRR/ROR disproportionality signal detection on FDA FAERS
- Genomics: nf-core pipelines (rnaseq, sarek, atacseq) + variant annotation + survival analysis
- Claims: Cohort building, HEDIS measures, medication adherence (PDC), utilization metrics

---

## Slide 5: Where We're Going

### From Healthcare to Multi-Industry

```
+-------------------------------------------------------------------------+
|                   MULTI-INDUSTRY EXPANSION                              |
+-------------------------------------------------------------------------+
|                                                                         |
|  The framework is industry-agnostic:                                    |
|                                                                         |
|  YAML Registry    + Jinja2 Template  = Generated Orchestrator           |
|                                                                         |
|  skills_hcls.yaml + orchestrator.md.j2 = health-sciences-incubator.md  |
|  skills_energy.yaml + orchestrator.md.j2 = energy-solutions.md         |
|  skills_finserv.yaml + orchestrator.md.j2 = finserv-solutions.md       |
|                                                                         |
|  Shared infrastructure:                                                 |
|  - Preflight checker pattern                                            |
|  - CKE (Cortex Knowledge Extension) pattern                            |
|  - Platform affinity mechanism                                          |
|  - Plan-then-Execute protocol                                           |
|  - 12-check QA validation                                               |
|  - Twin orchestrator model (incubator / production)                     |
|  - Skill development lifecycle (incubate -> harden -> publish)          |
|                                                                         |
|  ROADMAP:                                                               |
|                                                                         |
|  Q2 2026: Payer skills (claims adjudication, member analytics)          |
|  Q2 2026: Cross-cutting validation skill (data quality, semantic)       |
|  Q3 2026: Execution logging for regulatory audit trails                 |
|  Q3 2026: Cortex Search routing (fuzzy intent matching)                 |
|  Q3 2026: Energy / Financial Services pilot orchestrators               |
|  Q4 2026: Self-improvement loop (delta re-execution)                    |
|                                                                         |
+-------------------------------------------------------------------------+
```

---

## Summary: The Journey

| | **Today (Manual)** | **Solutions Architect** | **Multi-Industry Vision** |
|---|---|---|---|
| **Input** | Raw data + deep expertise | Natural language + Snowflake account | Natural language + any data |
| **Process** | Weeks of manual work | Orchestrated skill composition | Orchestrated across industries |
| **Output** | Inconsistent results | Composed solution with governance | Enterprise AI solutions |
| **Time** | 2-3 months | Hours | Hours |
| **Skill Required** | Multiple specialists | Solutions architect | Anyone |
| **Governance** | Afterthought | Built-in (HIPAA) | Built-in (industry-specific) |
| **Adaptability** | Start over | Swap skills, re-compose | Add industry, add skills |

---

## The Ask

1. **Validate the approach** -- Is skill-based composition the right architecture?
2. **Expand coverage** -- Prioritize Payer skills and cross-cutting validation
3. **Early adopters** -- Customers to co-develop domain skills with
4. **Multi-industry** -- Greenlight Energy or Financial Services pilot

---

*"Describe your healthcare problem. The architect builds the solution."*
