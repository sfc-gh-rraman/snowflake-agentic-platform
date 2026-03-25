#!/usr/bin/env python3
from fpdf import FPDF

BLUE = (44, 100, 160)
DARK = (40, 40, 40)
GRAY = (100, 100, 100)
WHITE = (255, 255, 255)
LIGHT_BG = (245, 247, 250)
TABLE_HEADER_BG = (44, 100, 160)
TABLE_ALT_BG = (240, 244, 248)

class PDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(*GRAY)
            self.cell(0, 6, "Healthcare Solutions on Snowflake  |  Cortex Code Skills & Profile Guide", align="C")
            self.ln(8)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*GRAY)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, num, title):
        self.set_font("Helvetica", "B", 20)
        self.set_text_color(*BLUE)
        self.cell(0, 12, f"{num}. {title}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*BLUE)
        self.line(self.l_margin, self.get_y(), self.l_margin + 60, self.get_y())
        self.ln(4)

    def sub_title(self, title):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(*BLUE)
        self.cell(0, 9, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def sub_sub_title(self, title):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*DARK)
        self.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*DARK)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def bullet(self, text, bold_prefix=""):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*DARK)
        indent = 10
        if bold_prefix:
            label = text.rstrip()
            desc = bold_prefix
            self.set_x(self.l_margin)
            self.cell(indent, 5, "  -  ")
            self.set_font("Helvetica", "B", 10)
            bw = self.get_string_width(label + " ")
            remaining = self.w - self.r_margin - self.get_x()
            if bw + 20 > remaining:
                self.set_font("Helvetica", "", 10)
                self.multi_cell(0, 5, label + " " + desc)
            else:
                self.cell(bw, 5, label + " ")
                self.set_font("Helvetica", "", 10)
                self.multi_cell(0, 5, desc)
        else:
            self.set_x(self.l_margin)
            self.cell(indent, 5, "  -  ")
            self.multi_cell(0, 5, text)
        self.ln(0.5)

    def code_block(self, code):
        self.set_fill_color(*LIGHT_BG)
        self.set_font("Courier", "", 8)
        self.set_text_color(50, 50, 50)
        lines = code.strip().split("\n")
        for line in lines:
            self.cell(0, 4.5, "  " + line, fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(3)
        self.set_text_color(*DARK)

    def table(self, headers, rows, col_widths=None):
        if col_widths is None:
            w = (self.w - self.l_margin - self.r_margin) / len(headers)
            col_widths = [w] * len(headers)

        def draw_header():
            self.set_font("Helvetica", "B", 8)
            self.set_fill_color(*TABLE_HEADER_BG)
            self.set_text_color(*WHITE)
            for i, h in enumerate(headers):
                self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
            self.ln()
            self.set_font("Helvetica", "", 8)
            self.set_text_color(*DARK)

        draw_header()
        for ri, row in enumerate(rows):
            max_h = 7
            for ci, cell in enumerate(row):
                lines = self.multi_cell(col_widths[ci], 5, cell, dry_run=True, output="LINES")
                h = len(lines) * 5 + 2
                if h > max_h:
                    max_h = h
            if self.get_y() + max_h > self.h - 25:
                self.add_page()
                draw_header()
            x_start = self.l_margin
            y_start = self.get_y()
            fill_clr = TABLE_ALT_BG if ri % 2 == 1 else WHITE
            for ci, cell in enumerate(row):
                x = x_start + sum(col_widths[:ci])
                self.set_fill_color(*fill_clr)
                self.rect(x, y_start, col_widths[ci], max_h, "DF")
                self.set_xy(x + 1, y_start + 1)
                self.set_text_color(*DARK)
                self.multi_cell(col_widths[ci] - 2, 5, cell)
            self.set_y(y_start + max_h)
        self.ln(3)

    def check_page_break(self, h=40):
        if self.get_y() + h > self.h - 25:
            self.add_page()


def build_pdf():
    pdf = PDF("P", "mm", "Letter")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pw = pdf.w - pdf.l_margin - pdf.r_margin

    # --- PAGE 1: Title ---
    pdf.add_page()
    pdf.ln(50)
    pdf.set_font("Helvetica", "B", 32)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 14, "Healthcare Solutions", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 14, "on Snowflake", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_draw_color(*BLUE)
    pdf.line(pw * 0.3 + pdf.l_margin, pdf.get_y(), pw * 0.7 + pdf.l_margin, pdf.get_y())
    pdf.ln(8)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 8, "Cortex Code Skills & Profile Architecture Guide", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(12)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 7, "A modular, skill-based approach to building healthcare", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "data solutions using Snowflake platform capabilities", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)
    pdf.set_font("Helvetica", "I", 10)
    pdf.cell(0, 7, "Built with Cortex Code  |  March 2026", align="C", new_x="LMARGIN", new_y="NEXT")

    # --- PAGE 2: TOC ---
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 12, "Table of Contents", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*BLUE)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + 55, pdf.get_y())
    pdf.ln(6)
    toc = [
        ("1.", "Executive Summary", "3"),
        ("2.", "Architecture Overview", "4"),
        ("3.", "Healthcare Skills Inventory", "5"),
        ("4.", "Orchestrator Agent Profile", "8"),
        ("5.", "Cortex Knowledge Extensions (CKEs)", "9"),
        ("6.", "Data Model Knowledge Repository", "11"),
        ("7.", "Snowflake Platform Skills Integration", "13"),
        ("8.", "Cross-Domain Solution Patterns", "15"),
        ("9.", "Walkthrough: DICOM Pipeline (End-to-End)", "17"),
        ("10.", "Governance & Guardrails", "19"),
        ("11.", "Getting Started", "20"),
    ]
    for num, title, pg in toc:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(*BLUE)
        pdf.cell(10, 8, num)
        pdf.set_font("Helvetica", "", 12)
        pdf.set_text_color(*DARK)
        pdf.cell(pw - 25, 8, title)
        pdf.cell(15, 8, pg, align="R")
        pdf.ln()
    pdf.ln(4)

    # --- Section 1: Executive Summary ---
    pdf.add_page()
    pdf.section_title("1", "Executive Summary")
    pdf.body("This document describes a modular, skill-based architecture for building healthcare data solutions on Snowflake using Cortex Code. The system consists of:")
    pdf.bullet("17 skills: 15 healthcare domain skills + 2 shared CKE skills, organized in 8 categories")
    pdf.bullet("1 orchestrator agent profile (healthcare-solutions) that routes requests to the right skill(s)")
    pdf.bullet("Integration with Snowflake platform skills (Dynamic Tables, Cortex AI, Streamlit, dbt, governance)")
    pdf.bullet("2 Cortex Knowledge Extensions (CKEs) as standalone composable skills (PubMed, ClinicalTrials.gov)")
    pdf.bullet("1 Data Model Knowledge Repository (Cortex Search over DICOM 18-table reference model)")
    pdf.bullet("9 cross-domain composition patterns for complex multi-skill solutions")
    pdf.bullet("Cross-cutting prerequisite pattern: auto-fires data model queries before schema-dependent tasks")
    pdf.ln(2)
    pdf.body("The architecture enables a healthcare solutions architect to quickly build end-to-end solutions by composing domain-specific skills with Snowflake platform capabilities, while enforcing HIPAA governance guardrails across all workflows.")
    pdf.sub_title("Key Benefits")
    pdf.bullet("Modularity: ", "Reusable skills that encode domain expertise and best practices")
    pdf.bullet("Intelligent Routing: ", "Single orchestrator routes to the right skill based on natural language intent")
    pdf.bullet("Platform Integration: ", "Healthcare skills invoke Snowflake platform skills for infrastructure")
    pdf.bullet("Composability: ", "Cross-domain patterns compose multiple skills for complex use cases")
    pdf.bullet("Governance by Default: ", "HIPAA guardrails enforced as cross-cutting concerns")
    pdf.bullet("Knowledge-Grounded: ", "CKEs provide RAG-based biomedical evidence; Cortex Search grounds schemas in live data model definitions")

    # --- Section 2: Architecture Overview ---
    pdf.add_page()
    pdf.section_title("2", "Architecture Overview")
    pdf.body("The architecture follows a layered model: an orchestrator agent profile at the top, healthcare domain skill categories in the middle (with shared knowledge CKE skills invoked on-demand), Snowflake platform skills at the foundation, and a data model knowledge repository feeding schema context upward.")
    pdf.sub_title("Layered Architecture")
    pdf.code_block("""+------------------------------------------------------------------+
|              ORCHESTRATOR AGENT PROFILE                           |
|              healthcare-solutions.md                              |
|  Intent Detection -> Domain Routing -> Skill Composition         |
+------------------------------------------------------------------+
       |              |              |              |
       v              v              v              v
+-------------+ +------------+ +------------+ +------------+
| Medical     | | Clinical   | | Drug       | | Genomics & |
| Imaging     | | Data / EHR | | Safety     | | Bioinform. |
| (7 sub-     | | (3 skills) | | (2 skills) | | (5 skills) |
| skills)     | |            | |            | |            |
+-------------+ +------------+ +------------+ +------------+
       |     +----------+ +----------+ +----------+      |
       |     | Claims & | | Lab Data | | Research |      |
       |     | RWE (1)  | | (1)      | | (1)      |      |
       |     +----------+ +----------+ +----------+      |
       |              |              |              |
       |   +----------------------------------------------+
       |   | SHARED KNOWLEDGE (composable, on-demand)     |
       |   | $cke-pubmed        $cke-clinical-trials      |
       |   | Domain skills invoke when evidence adds value|
       |   +----------------------------------------------+
       |              |              |              |
       v              v              v              v
+------------------------------------------------------------------+
|              SNOWFLAKE PLATFORM SKILLS                            |
| Dynamic Tables | Cortex AI | Streamlit | SPCS | dbt | ML        |
| React App | Governance | Cortex Search | Cortex Agent | Analyst  |
+------------------------------------------------------------------+
       ^
       |
+------------------------------------------------------------------+
|         DATA MODEL KNOWLEDGE REPOSITORY                          |
| Excel -> CSV -> Table -> Cortex Search Service                   |
| DICOM_MODEL_SEARCH_SVC (auto pre-step for schema tasks)          |
+------------------------------------------------------------------+""")
    pdf.sub_title("How It Works")
    pdf.table(
        ["Step", "Action", "Component"],
        [
            ["1", "User sends a natural language request", "Cortex Code CLI"],
            ["2", "Orchestrator detects healthcare business domain from trigger keywords", "healthcare-solutions.md"],
            ["3", "Matching domain skill is invoked (e.g., $healthcare-imaging for DICOM)", "Domain Skill Router"],
            ["4", "For schema-dependent intents (PARSE, INGEST, ANALYTICS, GOVERNANCE), auto-query Data Model Knowledge Repository via Cortex Search", "DICOM_MODEL_SEARCH_SVC"],
            ["5", "Domain skills may invoke CKEs (PubMed, Clinical Trials) for evidence grounding", "CKE Cortex Search Services"],
            ["6", "Domain skills invoke Snowflake platform skills for infrastructure", "Dynamic Tables, Streamlit, etc."],
            ["7", "HIPAA governance guardrails applied as cross-cutting concerns", "Masking, RLS, Audit"],
        ],
        [pw * 0.07, pw * 0.58, pw * 0.35],
    )

    # --- Section 3: Healthcare Skills Inventory ---
    pdf.add_page()
    pdf.section_title("3", "Healthcare Skills Inventory")
    pdf.body("The collection contains 17 skills organized into 8 categories: 7 healthcare business domains + 1 shared knowledge category for CKEs. The healthcare-imaging skill is a collection with 7 sub-skills.")

    pdf.sub_title("Repository Directory Structure")
    pdf.code_block("""skills/
+-- medical-imaging/           healthcare-imaging (router + 7 sub-skills), dicom-parser
+-- clinical-data-ehr/         fhir-data-transformation, clinical-nlp, omop-cdm-modeling
+-- drug-safety/               pharmacovigilance, clinical-trial-protocol-skill
+-- claims-rwe/                claims-data-analysis
+-- genomics-bioinformatics/   nextflow-development, variant-annotation, single-cell-rna-qc,
|                              scvi-tools, survival-analysis
+-- lab-instrument-data/       instrument-data-to-allotrope
+-- research-strategy/         scientific-problem-selection
+-- shared-knowledge/          cke-pubmed, cke-clinical-trials""")

    pdf.sub_title("3.1 Medical Imaging & Radiology")
    pdf.body("Repo path: skills/medical-imaging/")
    pdf.body("The healthcare-imaging skill collection is a router that detects intent and routes to 7 sub-skills covering the full imaging lifecycle from parsing to AI, plus a data model knowledge service.")
    pdf.table(
        ["Sub-Skill", "Triggers", "Description"],
        [
            ["dicom-parser", "DICOM parse, extract tags, pydicom, DICOM data model", "18-table DICOM data model + pydicom parser script"],
            ["dicom-ingestion", "Ingest DICOM, imaging pipeline, load images, stage DICOM", "Stages, COPY, Dynamic Tables, Streams/Tasks"],
            ["dicom-analytics", "Imaging analytics, radiology NLP, report extraction", "Cortex AI NLP on reports, Cortex Search, study metrics"],
            ["imaging-viewer", "Imaging viewer, Streamlit imaging, DICOM viewer", "Streamlit dashboard + SPCS pixel viewer"],
            ["imaging-governance", "HIPAA imaging, PHI masking, imaging audit", "Masking policies, classification, row-access, audit"],
            ["imaging-ml", "Imaging model, pathology model, radiology AI", "ML training, Model Registry, SQL inference"],
            ["data-model-knowledge", "Data model reference, DICOM schema lookup, PHI columns", "Cortex Search over 18-table DICOM model (auto pre-step)"],
        ],
        [pw * 0.2, pw * 0.4, pw * 0.4],
    )

    pdf.sub_title("3.2 Clinical Data & EHR")
    pdf.body("Repo path: skills/clinical-data-ehr/")
    pdf.table(
        ["Skill", "Triggers", "Description"],
        [
            ["fhir-data-transformation", "FHIR, HL7, Patient, Observation, Bundle, ndjson", "Transforms FHIR R4 resources into analytics-ready relational tables"],
            ["clinical-nlp", "Clinical NLP, NER, clinical notes, ICD coding, med extraction", "Extracts structured data from clinical text via Cortex AI / spaCy"],
            ["omop-cdm-modeling", "OMOP, CDM, OHDSI, vocabulary mapping, SNOMED, LOINC", "Transforms EHR/claims to OMOP CDM v5.4 with vocab mapping"],
        ],
        [pw * 0.22, pw * 0.38, pw * 0.4],
    )

    pdf.sub_title("3.3 Drug Safety & Pharmacovigilance")
    pdf.body("Repo path: skills/drug-safety/")
    pdf.table(
        ["Skill", "Triggers", "Description"],
        [
            ["pharmacovigilance", "FAERS, adverse events, drug safety, MedDRA, PRR, ROR", "FDA FAERS analysis for drug safety signal detection"],
            ["clinical-trial-protocol-skill", "Clinical trial protocol, design study, FDA submission", "Generates clinical trial protocols via waypoint architecture"],
        ],
        [pw * 0.22, pw * 0.38, pw * 0.4],
    )

    pdf.check_page_break(50)
    pdf.sub_title("3.4 Claims & Real-World Evidence")
    pdf.body("Repo path: skills/claims-rwe/")
    pdf.table(
        ["Skill", "Triggers", "Description"],
        [
            ["claims-data-analysis", "Claims, RWE, 837/835, utilization, HEDIS, PDC", "Cohort building, utilization metrics, treatment patterns, adherence"],
        ],
        [pw * 0.22, pw * 0.38, pw * 0.4],
    )

    pdf.check_page_break(60)
    pdf.sub_title("3.5 Genomics & Bioinformatics")
    pdf.body("Repo path: skills/genomics-bioinformatics/")
    pdf.table(
        ["Skill", "Triggers", "Description"],
        [
            ["nextflow-development", "nf-core, Nextflow, FASTQ, variant calling, GEO, SRA", "Runs nf-core pipelines (rnaseq, sarek, atacseq) on sequencing data"],
            ["variant-annotation", "VCF, ClinVar, gnomAD, pathogenic variants, ACMG", "Annotates genomic variants with pathogenicity and frequency data"],
            ["single-cell-rna-qc", "scRNA-seq, QC, scanpy, MAD-based filtering", "Automated QC for single-cell RNA-seq using scverse best practices"],
            ["scvi-tools", "scVI, scANVI, totalVI, batch correction, integration", "Deep learning single-cell analysis using scvi-tools VAE models"],
            ["survival-analysis", "Kaplan-Meier, Cox regression, hazard ratio, PFS, OS", "Time-to-event analysis with publication-ready plots"],
        ],
        [pw * 0.22, pw * 0.38, pw * 0.4],
    )

    pdf.check_page_break(40)
    pdf.sub_title("3.6 Lab & Instrument Data")
    pdf.body("Repo path: skills/lab-instrument-data/")
    pdf.table(
        ["Skill", "Triggers", "Description"],
        [
            ["instrument-data-to-allotrope", "Instrument files, Allotrope, ASM, LIMS, ELN", "Converts lab instrument outputs to Allotrope Simple Model JSON/CSV"],
        ],
        [pw * 0.22, pw * 0.38, pw * 0.4],
    )

    pdf.sub_title("3.7 Research Strategy")
    pdf.body("Repo path: skills/research-strategy/")
    pdf.table(
        ["Skill", "Triggers", "Description"],
        [
            ["scientific-problem-selection", "Research problem, project ideation, evaluate project", "Systematic problem selection via Fischbach & Walsh decision trees"],
        ],
        [pw * 0.22, pw * 0.38, pw * 0.4],
    )

    # --- Section 4: Orchestrator Agent Profile ---
    pdf.add_page()
    pdf.section_title("4", "Orchestrator Agent Profile")
    pdf.body("The healthcare-solutions agent profile is a custom Cortex Code agent defined as a markdown file at ~/.snowflake/cortex/agents/healthcare-solutions.md. It serves as the single entry point for all healthcare tasks, detecting intent and routing to the appropriate skill(s).")
    pdf.sub_title("Profile Structure")
    pdf.code_block("""---
name: healthcare-solutions
description: "Healthcare industry solutions architect for Snowflake.
  Orchestrates skills across medical imaging, clinical data, drug safety,
  claims/RWE, genomics, and lab data. Integrates CKEs for PubMed and
  ClinicalTrials.gov research."
tools: ["*"]
---
# Healthcare Solutions Profile
You are a Healthcare Solutions Architect specializing in building
end-to-end healthcare data solutions on Snowflake.
## Skill Routing         [Intent detection tables for 7 business domains]
## CKE Tools             [PubMed + Clinical Trials search integration]
## Cross-Domain Patterns [9 composition patterns for multi-skill solutions]
## Guardrails            [HIPAA governance rules across all workflows]""")
    pdf.sub_title("Routing Mechanism")
    pdf.body("The profile uses trigger keywords in the user's request to determine the healthcare business domain. Each domain maps to one or more skills via $skill-name invocation syntax. Domain skills invoke CKE skills ($cke-pubmed, $cke-clinical-trials) on-demand when evidence grounding is beneficial.")
    pdf.table(
        ["Domain", "Example Request", "Skill Invoked", "Platform / CKE"],
        [
            ["Medical Imaging", '"Parse these DICOM files"', "$healthcare-imaging", "Dynamic Tables, SPCS"],
            ["Clinical / EHR", '"Transform FHIR bundles"', "$fhir-data-transformation", "Dynamic Tables, dbt"],
            ["Drug Safety", '"Analyze FAERS for drug X"', "$pharmacovigilance", "Cortex AI, PubMed CKE"],
            ["Claims / RWE", '"Build RWE cohort"', "$claims-data-analysis", "Cortex Analyst, Trials CKE"],
            ["Genomics", '"Annotate VCF variants"', "$variant-annotation", "ML Registry"],
            ["Lab Data", '"Convert to Allotrope"', "$instrument-data-to-allotrope", "Dynamic Tables"],
            ["Research", '"Evaluate this research idea"', "$scientific-problem-selection", "PubMed CKE"],
        ],
        [pw * 0.18, pw * 0.27, pw * 0.28, pw * 0.27],
    )

    # --- Section 5: CKEs ---
    pdf.add_page()
    pdf.section_title("5", "Cortex Knowledge Extensions (CKEs)")
    pdf.body("CKEs are Snowflake Marketplace shared Cortex Search Services that provide RAG-based literature search. In this architecture, CKEs are implemented as standalone composable skills under shared-knowledge/ -- domain skills invoke them on-demand via $cke-pubmed or $cke-clinical-trials when evidence grounding adds value.")

    pdf.sub_title("Architecture: CKEs as Composable Skills")
    pdf.body("Rather than embedding CKE connection details, query patterns, and SQL in every domain skill, CKEs are encapsulated as standalone skills. This eliminates duplication and makes adding new CKEs trivial.")
    pdf.table(
        ["Approach", "Where CKE lives", "How skills access it", "Trade-off"],
        [
            ["Orchestrator-level", "Agent profile", "Agent injects CKE context", "Coupling: orchestrator must know when to query"],
            ["Skill-level (embedded)", "Each skill's SKILL.md", "Each skill queries CKE directly", "Duplication: CKE details in 6+ skills"],
            ["Composable skills (chosen)", "shared-knowledge/ category", "Skills invoke $cke-pubmed etc.", "Clean: single definition, on-demand composition"],
        ],
        [pw * 0.2, pw * 0.22, pw * 0.28, pw * 0.3],
    )

    pdf.sub_title("Available CKE Skills")
    pdf.body("Repo path: skills/shared-knowledge/")
    pdf.table(
        ["CKE Skill", "Data Source", "Marketplace ID", "Service Name"],
        [
            ["$cke-pubmed", "PubMed biomedical literature", "GZSTZ67BY9OQW", "<CKE_DB>.SHARED.CKE_PUBMED_SERVICE"],
            ["$cke-clinical-trials", "ClinicalTrials.gov registry", "GZSTZ67BY9ORD", "<CKE_DB>.SHARED.CKE_CLINICAL_TRIALS_SERVICE"],
        ],
        [pw * 0.2, pw * 0.25, pw * 0.2, pw * 0.35],
    )

    pdf.sub_title("CKE Routing: Which Domain Skills Invoke Which CKE")
    pdf.table(
        ["CKE Skill", "Trigger Keywords", "Domain Skills That Invoke It"],
        [
            ["$cke-pubmed", "PubMed, biomedical literature, drug mechanism, clinical evidence, research papers", "$pharmacovigilance, $clinical-nlp, $scientific-problem-selection, dicom-analytics"],
            ["$cke-clinical-trials", "ClinicalTrials.gov, trial search, trial design, feasibility, eligibility", "$clinical-trial-protocol-skill, $claims-data-analysis, $survival-analysis"],
        ],
        [pw * 0.18, pw * 0.4, pw * 0.42],
    )

    pdf.sub_title("How Domain Skills Invoke CKEs")
    pdf.body("Domain skills contain a lightweight 'Evidence Grounding' section that describes when to invoke the CKE skill and what to query. The CKE skill encapsulates all Marketplace setup, query patterns, and integration SQL.")
    pdf.code_block("""## Evidence Grounding: PubMed CKE   (in pharmacovigilance/SKILL.md)

Invoke $cke-pubmed when evidence grounding adds value:
- After signal detection (PRR/ROR > 2), search for drug-event associations
- Cross-reference disproportionality findings with case reports

See $cke-pubmed for setup, query patterns, and integration SQL.""")

    pdf.sub_title("CKE Skill Contents")
    pdf.body("Each CKE skill SKILL.md encapsulates:")
    pdf.bullet("Marketplace details (listing ID, service name, columns)")
    pdf.bullet("One-time setup instructions")
    pdf.bullet("SQL query patterns (basic search + Cortex Agent API tool spec)")
    pdf.bullet("Use cases by domain skill (table of when/what to query)")
    pdf.bullet("Integration patterns with full SQL examples (signal enrichment, prompt grounding, feasibility analysis)")

    pdf.check_page_break(30)
    pdf.sub_title("Key Design Decision: On-Demand vs Auto Pre-Step")
    pdf.body("CKEs differ from the data-model-knowledge cross-cutting prerequisite. Data-model-knowledge auto-fires (Step 0) because schema context is always needed for DICOM tasks. CKEs are on-demand -- the domain skill decides when literature/trial evidence adds value. This keeps CKEs composable rather than mandatory.")

    # --- Section 6: Data Model Knowledge Repository ---
    pdf.add_page()
    pdf.section_title("6", "Data Model Knowledge Repository")
    pdf.body("Reference data models are stored as searchable knowledge repositories on Snowflake using Cortex Search. Skills query the repository at runtime to get the latest table/column definitions instead of relying on hardcoded schemas.")

    pdf.sub_title("Architecture")
    pdf.code_block("""Excel Spreadsheet (dicom_data_model_reference.xlsx)
    |
    v  export_search_corpus_csv.py
CSV (dicom_model_search_corpus.csv)      222 rows, 18 tables
    |
    v  COPY INTO
Snowflake Table (DICOM_MODEL_REFERENCE)
    |
    v  Cortex Search Service
DICOM_MODEL_SEARCH_SVC  <-- Skills query this at runtime""")

    pdf.sub_title("DICOM Model: 18 Tables, 222 Columns")
    pdf.body("The DICOM data model covers the full imaging hierarchy plus supporting entities:")
    pdf.table(
        ["Category", "Tables", "Purpose"],
        [
            ["Core Hierarchy", "dicom_patient, dicom_study, dicom_series, dicom_instance, dicom_frame", "Patient -> Study -> Series -> Instance -> Frame"],
            ["Technical Context", "dicom_equipment, dicom_image_pixel, dicom_image_plane", "Scanner/device details, pixel encoding, spatial positioning"],
            ["Workflow", "dicom_procedure_step", "Requested/performed procedure mapping"],
            ["Dose", "dicom_dose_summary", "CT/radiography exposure parameters"],
            ["Derived Objects", "dicom_segmentation_metadata, dicom_structured_report_header", "SEG and SR SOP metadata"],
            ["Storage", "dicom_file_location", "Physical file location and access URIs"],
            ["Generic Elements", "dicom_element, dicom_sequence_item", "Long-tail tags and sequence items"],
            ["ML / Embeddings", "image_embedding, embedding_model, embedding_evaluation", "Vector representations and model catalog"],
        ],
        [pw * 0.18, pw * 0.42, pw * 0.4],
    )

    pdf.sub_title("Cross-Cutting Prerequisite (Auto Pre-Step)")
    pdf.body("For DICOM-related intents (PARSE, INGEST, ANALYTICS, GOVERNANCE), the healthcare-imaging router automatically executes Step 0 before dispatching to the sub-skill. This grounds all schema work in the latest data model.")
    pdf.code_block("""SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "<context from user request>",
      "columns": ["table_name", "column_name", "data_type",
                   "constraints", "description", "dicom_tag",
                   "contains_phi", "relationships"]}'
);""")

    pdf.table(
        ["Intent", "Step 0 Query Focus", "What Gets Grounded"],
        [
            ["PARSE", "All tables + columns for requested scope", "CREATE TABLE DDL statements"],
            ["INGEST", "Target table columns + data types + relationships", "COPY INTO mappings, Dynamic Table SELECT lists"],
            ["ANALYTICS", "Source table columns + descriptions", "Analytical views, Cortex AI extraction prompts"],
            ["GOVERNANCE", "PHI-flagged columns across all tables", "Masking policy targets, de-identification scope"],
        ],
        [pw * 0.18, pw * 0.4, pw * 0.42],
    )

    pdf.check_page_break(30)
    pdf.sub_title("DDL Generation from Search Results")
    pdf.code_block("""WITH model_knowledge AS (
    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
        '{"query": "dose summary radiation CT exposure",
          "columns": ["table_name","column_name","data_type","constraints"]}'
    ) AS context
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-70b',
    'Generate Snowflake CREATE TABLE DDL from this data model reference. '
    || 'Use exact column names, data types, and constraints. Reference: '
    || context::STRING
) AS generated_ddl
FROM model_knowledge;""")

    pdf.sub_title("Extending to Other Data Models")
    pdf.table(
        ["Domain", "Spreadsheet", "Search Service"],
        [
            ["DICOM Imaging", "dicom_data_model_reference.xlsx", "DICOM_MODEL_SEARCH_SVC (live)"],
            ["FHIR R4", "fhir_r4_resource_model.xlsx", "FHIR_MODEL_SEARCH_SVC (planned)"],
            ["OMOP CDM v5.4", "omop_cdm_v54_model.xlsx", "OMOP_MODEL_SEARCH_SVC (planned)"],
            ["FAERS", "faers_data_model.xlsx", "FAERS_MODEL_SEARCH_SVC (planned)"],
            ["Claims (837/835)", "claims_data_model.xlsx", "CLAIMS_MODEL_SEARCH_SVC (planned)"],
        ],
        [pw * 0.25, pw * 0.35, pw * 0.4],
    )

    # --- Section 7: Platform Skills Integration ---
    pdf.add_page()
    pdf.section_title("7", "Snowflake Platform Skills Integration")
    pdf.body("Healthcare domain skills leverage Snowflake's bundled platform skills for infrastructure, compute, AI, governance, and visualization.")

    pdf.sub_title("7.1 Data Engineering")
    pdf.sub_sub_title("Dynamic Tables")
    pdf.body("Used for incremental data pipelines with automatic refresh:")
    pdf.bullet("DICOM metadata ingestion: ", "Dynamic Tables with 10-minute TARGET_LAG for continuous imaging data refresh")
    pdf.bullet("FHIR resource flattening: ", "Incremental FHIR JSON bundles into relational tables")
    pdf.bullet("Imaging analytics: ", "Study volume metrics refreshed every 30 minutes")
    pdf.bullet("Claims data: ", "Incremental claims aggregation")

    pdf.sub_sub_title("Streams & Tasks")
    pdf.bullet("DICOM ingestion: ", "Stream on raw landing table triggers processing of new imaging metadata")
    pdf.bullet("HL7v2 messages: ", "Stream on raw HL7 triggers ADT event parsing")

    pdf.sub_sub_title("dbt Projects")
    pdf.bullet("OMOP CDM: ", "dbt for vocabulary mapping and CDM table materialization")
    pdf.bullet("Claims analytics: ", "dbt for cohort tables, utilization summaries, HEDIS measures")

    pdf.check_page_break(50)
    pdf.sub_title("7.2 AI & Machine Learning")
    pdf.sub_sub_title("Cortex AI Functions")
    pdf.bullet("EXTRACT_ANSWER: ", "dicom-analytics extracts findings/impressions from radiology reports")
    pdf.bullet("SUMMARIZE: ", "Condenses lengthy radiology reports for dashboards")
    pdf.bullet("COMPLETE: ", "clinical-nlp uses LLM for complex entity extraction; DDL generation from model search")
    pdf.bullet("SENTIMENT: ", "Flags potentially critical radiology findings")

    pdf.sub_sub_title("Cortex Search")
    pdf.bullet("Imaging metadata search: ", "Semantic search over studies/reports (e.g., 'chest CT with pulmonary nodule')")
    pdf.bullet("Data model knowledge: ", "DICOM_MODEL_SEARCH_SVC provides live schema definitions to skills at runtime")
    pdf.bullet("CKE integration: ", "PubMed and Clinical Trials shared search services for evidence grounding")

    pdf.sub_sub_title("ML Registry & Model Deployment")
    pdf.bullet("imaging-ml: ", "Trains imaging classifiers, registers in ML Registry for SQL inference")
    pdf.bullet("survival-analysis: ", "Models registered for reproducible time-to-event analysis")

    pdf.check_page_break(50)
    pdf.sub_title("7.3 Applications & Visualization")
    pdf.sub_sub_title("Streamlit in Snowflake")
    pdf.bullet("Imaging dashboard, pharmacovigilance signals, claims analytics, clinical data explorer")
    pdf.sub_sub_title("Snowpark Container Services (SPCS)")
    pdf.bullet("DICOM pixel viewer (OHIF/Cornerstone.js), deep learning inference on GPU, bioinformatics pipelines")

    pdf.sub_title("7.4 Security & Governance")
    pdf.bullet("Sensitive Data Classification: ", "SYSTEM$CLASSIFY on all clinical tables for PHI auto-detection")
    pdf.bullet("Data Masking: ", "IS_ROLE_IN_SESSION() based masking for patient names, IDs, dates")
    pdf.bullet("Row-Access Policies: ", "Institutional data segregation by role")
    pdf.bullet("Audit Trails: ", "ACCESS_HISTORY monitoring for all PHI-containing tables")

    # --- Section 8: Cross-Domain Solution Patterns ---
    pdf.add_page()
    pdf.section_title("8", "Cross-Domain Solution Patterns")
    pdf.body("The most powerful capability of the orchestrator is composing multiple skills for solutions that span healthcare business domains. Below are 9 pre-defined composition patterns.")

    patterns = [
        ("Pattern 1: Imaging + Clinical Integration", [
            "$healthcare-imaging (dicom-parser) - Build imaging metadata tables",
            "$fhir-data-transformation - Ingest FHIR DiagnosticReport/ImagingStudy",
            "$clinical-nlp - Extract findings from radiology reports",
            "$cke-pubmed - Enrich with radiology research context",
            "Platform: developing-with-streamlit OR build-react-app",
        ]),
        ("Pattern 2: Clinical Data Warehouse (OMOP)", [
            "$fhir-data-transformation - Ingest FHIR bundles",
            "$omop-cdm-modeling - Transform to OMOP CDM with vocabulary mapping",
            "Platform: sensitive-data-classification, data-policy - HIPAA governance",
            "Platform: semantic-view-optimization - Semantic views for analytics",
            "Platform: developing-with-streamlit - Clinical dashboards",
        ]),
        ("Pattern 3: Drug Safety Signal Detection", [
            "$pharmacovigilance - Load and analyze FAERS data",
            "$cke-pubmed - Search literature for drug-event associations",
            "$clinical-nlp - Extract adverse events from narrative text",
            "$claims-data-analysis - Correlate with claims-based utilization",
            "Platform: developing-with-streamlit - Safety signal dashboard",
        ]),
        ("Pattern 4: Genomics + Clinical Outcomes", [
            "$nextflow-development - Run nf-core pipeline on sequencing data",
            "$variant-annotation - Annotate variants with ClinVar/gnomAD",
            "$survival-analysis - Correlate variants with patient outcomes",
            "Platform: machine-learning - Train predictive models",
        ]),
        ("Pattern 5: Single-Cell Analysis Pipeline", [
            "$single-cell-rna-qc - QC and filter scRNA-seq data",
            "$scvi-tools - Deep learning integration and batch correction",
            "Platform: machine-learning - Register models in Snowflake ML Registry",
        ]),
        ("Pattern 6: Real-World Evidence Study", [
            "$claims-data-analysis - Build cohorts from claims data",
            "$cke-clinical-trials - Cross-reference with registered trials",
            "$omop-cdm-modeling - Standardize to OMOP CDM",
            "$survival-analysis - Time-to-event outcomes analysis",
            "$clinical-nlp - Enrich with unstructured clinical data",
            "$cke-pubmed - Validate findings against published literature",
            "Platform: developing-with-streamlit - Study results dashboard",
        ]),
        ("Pattern 7: Clinical Trial Design", [
            "$scientific-problem-selection - Validate research problem",
            "$cke-clinical-trials - Search for similar/competing trials",
            "$cke-pubmed - Review biomedical literature for evidence",
            "$clinical-trial-protocol-skill - Generate protocol document",
            "$survival-analysis - Power analysis and endpoint design",
            "$claims-data-analysis - Feasibility analysis from claims data",
        ]),
        ("Pattern 8: Lab Data Modernization", [
            "$instrument-data-to-allotrope - Standardize instrument outputs",
            "Platform: dynamic-tables - Incremental pipeline for lab data",
            "Platform: developing-with-streamlit - Lab analytics dashboard",
        ]),
        ("Pattern 9: Clinical Data Application (React)", [
            "Domain skills - Prepare backend data (FHIR, OMOP, imaging, claims)",
            "Platform: build-react-app - Build React/Next.js app with Snowflake data",
            "Platform: deploy-to-spcs - Deploy containerized app to SPCS",
            "Platform: data-policy - Enforce PHI masking at the API layer",
        ]),
    ]
    for title, steps in patterns:
        pdf.check_page_break(15 + len(steps) * 6)
        pdf.sub_sub_title(title)
        for i, step in enumerate(steps):
            pdf.bullet(f"Step {i+1}: {step}")
        pdf.ln(2)

    # --- Section 9: Walkthrough ---
    pdf.add_page()
    pdf.section_title("9", "Walkthrough: DICOM Pipeline (End-to-End)")
    pdf.body("This walkthrough shows how the cross-cutting prerequisite pattern works when a user asks: 'Build the entire DICOM ingestion to analytics pipeline.' The router auto-queries the data model knowledge repository before each sub-skill.")

    pdf.sub_title("Step 0: Auto Pre-Step (Router)")
    pdf.body("The healthcare-imaging router detects INGEST + ANALYTICS intents. Before dispatching, it queries DICOM_MODEL_SEARCH_SVC:")
    pdf.code_block("""SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "study series instance patient columns for ingestion",
      "columns": ["table_name","column_name","data_type","constraints",
                   "dicom_tag","relationships"]}'
);""")
    pdf.body("The search results (table names, column definitions, DICOM tags, FK relationships) become the grounding context for subsequent steps.")

    pdf.sub_title("Step 1: Create Schema ($healthcare-imaging -> dicom-parser)")
    pdf.body("The dicom-parser sub-skill receives the search results and uses them to generate DDL. Instead of hardcoded CREATE TABLE statements, it calls Cortex AI COMPLETE with the model reference:")
    pdf.code_block("""SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
    'Generate Snowflake CREATE TABLE DDL from this reference: '
    || <search_results>::STRING
) AS generated_ddl;""")

    pdf.sub_title("Step 2: Build Ingestion Pipeline ($healthcare-imaging -> dicom-ingestion)")
    pdf.body("The dicom-ingestion sub-skill uses the same grounding context to build accurate COPY INTO column mappings and Dynamic Table SELECT lists with correct column names, types, and DICOM tag paths.")
    pdf.code_block("""CREATE OR REPLACE DYNAMIC TABLE dicom_studies
  TARGET_LAG = '10 minutes'  WAREHOUSE = imaging_wh
AS
SELECT
  -- Column list grounded by DICOM_MODEL_SEARCH_SVC results
  metadata:StudyInstanceUID::STRING AS study_instance_uid,
  metadata:PatientID::STRING AS patient_id,
  ...
FROM dicom_raw;""")

    pdf.check_page_break(40)
    pdf.sub_title("Step 3: Build Analytics ($healthcare-imaging -> dicom-analytics)")
    pdf.body("Analytical Dynamic Tables reference the model-accurate column names from Step 0. PubMed CKE is available for research context enrichment.")

    pdf.sub_title("Step 4: Apply Governance ($healthcare-imaging -> imaging-governance)")
    pdf.body("The governance sub-skill receives PHI column flags (contains_phi = 'Y') from the search results and auto-generates masking policies for every identified PHI column across all 18 tables.")
    pdf.code_block("""-- PHI columns identified by Step 0 search results:
-- dicom_patient.patient_name, dicom_patient.patient_id,
-- dicom_patient.patient_birth_date, dicom_study.referring_physician,
-- dicom_equipment.device_serial_number, ...

ALTER TABLE dicom_patient MODIFY COLUMN patient_name
  SET MASKING POLICY phi_string_mask;
ALTER TABLE dicom_patient MODIFY COLUMN patient_id
  SET MASKING POLICY phi_string_mask;
-- ... (auto-generated for all PHI columns found)""")

    # --- Section 10: Governance ---
    pdf.add_page()
    pdf.section_title("10", "Governance & Guardrails")
    pdf.body("All healthcare skills enforce these governance rules as cross-cutting concerns. The orchestrator profile embeds these as mandatory guardrails.")
    pdf.table(
        ["Guardrail", "Implementation"],
        [
            ["HIPAA PHI Protection", "All patient data tables require masking policies before analytics or dashboard exposure"],
            ["Masking Policy Pattern", "Always use IS_ROLE_IN_SESSION() (not CURRENT_ROLE()) for role-based masking"],
            ["Audit Trails", "ACCESS_HISTORY queries monitor all PHI access; templates in imaging-governance"],
            ["De-identification", "Prefer de-identified datasets for ML/analytics; use SHA2 hashing for UIDs"],
            ["Data Classification", "Run SYSTEM$CLASSIFY on all clinical tables to auto-detect PHI before pipelines"],
            ["Data Model Grounding", "Query DICOM_MODEL_SEARCH_SVC to identify PHI columns from reference model"],
            ["Row-Access Policies", "Restrict data by institution/department using role mapping tables"],
            ["Data Quality", "Always validate FHIR/HL7/OMOP data quality before building downstream tables"],
            ["Genomic Data", "Ensure proper consent tracking and data use agreements"],
            ["FAERS Limitations", "Always note limitations of spontaneous reporting data"],
        ],
        [pw * 0.3, pw * 0.7],
    )

    # --- Section 11: Getting Started ---
    pdf.add_page()
    pdf.section_title("11", "Getting Started")
    pdf.sub_title("Installation")
    pdf.body("1. Clone the skills repository:")
    pdf.code_block("git clone <repo-url> coco-healthcare-skills")
    pdf.body("2. Register skills in ~/.snowflake/cortex/skills.json. A ready-to-use template is included in the repo at skills.json.template -- just replace the path placeholder:")
    pdf.code_block("""{
  "local": [{
    "path": "<ABSOLUTE_PATH_TO_REPO>/skills",
    "skills": [
      {"name": "healthcare-imaging",
       "relative_path": "medical-imaging/healthcare-imaging"},
      {"name": "claims-data-analysis",
       "relative_path": "claims-rwe/claims-data-analysis"},
      {"name": "clinical-nlp",
       "relative_path": "clinical-data-ehr/clinical-nlp"},
      {"name": "clinical-trial-protocol-skill",
       "relative_path": "drug-safety/clinical-trial-protocol-skill"},
      {"name": "fhir-data-transformation",
       "relative_path": "clinical-data-ehr/fhir-data-transformation"},
      {"name": "instrument-data-to-allotrope",
       "relative_path": "lab-instrument-data/instrument-data-to-allotrope"},
      {"name": "nextflow-development",
       "relative_path": "genomics-bioinformatics/nextflow-development"},
      {"name": "omop-cdm-modeling",
       "relative_path": "clinical-data-ehr/omop-cdm-modeling"},
      {"name": "pharmacovigilance",
       "relative_path": "drug-safety/pharmacovigilance"},
      {"name": "scientific-problem-selection",
       "relative_path": "research-strategy/scientific-problem-selection"},
      {"name": "scvi-tools",
       "relative_path": "genomics-bioinformatics/scvi-tools"},
      {"name": "single-cell-rna-qc",
       "relative_path": "genomics-bioinformatics/single-cell-rna-qc"},
      {"name": "survival-analysis",
       "relative_path": "genomics-bioinformatics/survival-analysis"},
      {"name": "variant-annotation",
       "relative_path": "genomics-bioinformatics/variant-annotation"},
      {"name": "cke-pubmed",
       "relative_path": "shared-knowledge/cke-pubmed"},
      {"name": "cke-clinical-trials",
       "relative_path": "shared-knowledge/cke-clinical-trials"}
    ]
  }]
}""")
    pdf.body("3. Copy the agent profile:")
    pdf.code_block("""cp coco-healthcare-skills/agents/healthcare-solutions.md \\
    ~/.snowflake/cortex/agents/""")
    pdf.body("4. Set up CKEs (optional): Install PubMed CKE and/or Clinical Trials CKE from Snowflake Marketplace.")
    pdf.body("5. Set up Data Model Knowledge Repository: Run scripts/setup_dicom_model_knowledge_repo.sql to create the Cortex Search Service.")
    pdf.body("6. Update skills (git pull): Since all skills are registered from the repo path, running git pull updates every skill automatically.")
    pdf.body("7. Verify in Cortex Code:")
    pdf.code_block("""/agents   # Should show healthcare-solutions
/skill    # Should show all healthcare skills""")

    pdf.sub_title("Usage Examples")
    pdf.code_block("""# Invoke the orchestrator for any healthcare task:
$healthcare-imaging parse these DICOM files from S3
$fhir-data-transformation load Patient and Observation bundles
$pharmacovigilance analyze FAERS data for aspirin adverse events
$claims-data-analysis build a T2D cohort from claims
$variant-annotation annotate this VCF with ClinVar
$survival-analysis run KM analysis on treatment outcomes""")

    pdf.sub_title("File Locations")
    pdf.table(
        ["Component", "Path"],
        [
            ["Agent Profile", "~/.snowflake/cortex/agents/healthcare-solutions.md"],
            ["Skills Config", "~/.snowflake/cortex/skills.json"],
            ["Skills Config Template", "coco-healthcare-skills/skills.json.template"],
            ["Skills (repo root)", "coco-healthcare-skills/skills/"],
            ["  medical-imaging/", "healthcare-imaging (router + 7 sub-skills)"],
            ["  clinical-data-ehr/", "fhir-data-transformation, clinical-nlp, omop-cdm-modeling"],
            ["  drug-safety/", "pharmacovigilance, clinical-trial-protocol-skill"],
            ["  claims-rwe/", "claims-data-analysis"],
            ["  genomics-bioinformatics/", "nextflow-development, variant-annotation, single-cell-rna-qc, scvi-tools, survival-analysis"],
            ["  lab-instrument-data/", "instrument-data-to-allotrope"],
            ["  research-strategy/", "scientific-problem-selection"],
            ["  shared-knowledge/", "cke-pubmed, cke-clinical-trials"],
            ["Reference Model", "references/dicom_data_model_reference.xlsx"],
            ["Setup SQL", "scripts/setup_dicom_model_knowledge_repo.sql"],
        ],
        [pw * 0.3, pw * 0.7],
    )

    pdf.sub_title("Snowflake Objects")
    pdf.table(
        ["Object", "Fully Qualified Name"],
        [
            ["Data Model Table", "UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_REFERENCE"],
            ["Cortex Search Service", "UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC"],
            ["Stage", "UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.dicom_model_stage"],
        ],
        [pw * 0.3, pw * 0.7],
    )

    out_path = "/Users/mgandhirajan/Documents/CoCo/HCLS/coco-healthcare-skills/Healthcare_Solutions_on_Snowflake.pdf"
    pdf.output(out_path)
    print(f"PDF generated: {out_path}")
    print(f"Total pages: {pdf.page_no()}")


if __name__ == "__main__":
    build_pdf()
