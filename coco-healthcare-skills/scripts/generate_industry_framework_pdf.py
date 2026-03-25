#!/usr/bin/env python3
from fpdf import FPDF
import os

OUTPUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "Industry_Solutions_Framework.pdf")


class FrameworkPDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 10, "Industry Solutions Framework for Cortex Code Skills", align="C")
            self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(0, 100, 180)
        self.cell(0, 12, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 100, 180)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def subsection_title(self, title):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(50, 50, 50)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def bullet(self, text, indent=10):
        x = self.get_x()
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.set_x(x + indent)
        self.cell(5, 5.5, "-")
        self.multi_cell(0, 5.5, text)
        self.ln(0.5)

    def sub_bullet(self, text, indent=20):
        x = self.get_x()
        self.set_font("Helvetica", "", 9.5)
        self.set_text_color(80, 80, 80)
        self.set_x(x + indent)
        self.cell(5, 5, "  ")
        self.multi_cell(0, 5, text)
        self.ln(0.5)

    def table_header(self, cols, widths):
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 100, 180)
        self.set_text_color(255, 255, 255)
        for i, col in enumerate(cols):
            self.cell(widths[i], 7, col, border=1, fill=True, align="C")
        self.ln()

    def table_row(self, cols, widths, fill=False):
        self.set_font("Helvetica", "", 8.5)
        self.set_text_color(40, 40, 40)
        if fill:
            self.set_fill_color(240, 245, 250)
        else:
            self.set_fill_color(255, 255, 255)
        max_lines = 1
        cell_texts = []
        for i, col in enumerate(cols):
            lines = self.multi_cell(widths[i], 5, col, dry_run=True, output="LINES")
            cell_texts.append(lines)
            max_lines = max(max_lines, len(lines))
        row_height = max_lines * 5
        y_start = self.get_y()
        x_start = self.get_x()
        for i, lines in enumerate(cell_texts):
            self.set_xy(x_start + sum(widths[:i]), y_start)
            self.cell(widths[i], row_height, "", border=1, fill=fill)
            for j, line in enumerate(lines):
                self.set_xy(x_start + sum(widths[:i]) + 1, y_start + j * 5)
                self.cell(widths[i] - 2, 5, line)
        self.set_xy(x_start, y_start + row_height)


def build_pdf():
    pdf = FrameworkPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # Title page
    pdf.add_page()
    pdf.ln(50)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(0, 100, 180)
    pdf.cell(0, 15, "Industry Solutions Framework", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 18)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 12, "Cortex Code Skills Development Lifecycle", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)
    pdf.set_draw_color(0, 100, 180)
    pdf.line(60, pdf.get_y(), pdf.w - 60, pdf.get_y())
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, "Health Sciences", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, "Industry Solutions Tiger Team", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)
    pdf.set_font("Helvetica", "I", 10)
    pdf.cell(0, 8, "March 18, 2026", align="C", new_x="LMARGIN", new_y="NEXT")

    # Executive Summary
    pdf.add_page()
    pdf.section_title("Executive Summary")
    pdf.body_text(
        "This document defines the Industry Solutions Framework for developing, hardening, "
        "and distributing Cortex Code skills tailored to Health Sciences (covering both "
        "healthcare and life sciences). The framework establishes a two-repo model separating "
        "rapid experimentation from production-grade skill delivery, with a Tiger Team serving "
        "as the quality gateway."
    )
    pdf.body_text(
        "The end goal: Field teams run a single command to get a curated, tested set of "
        "health sciences skills that solve standard problems across the industry."
    )
    pdf.ln(2)
    pdf.set_font("Courier", "", 10)
    pdf.set_text_color(0, 100, 180)
    pdf.cell(0, 6, "  cortex profile add health-sciences-solutions", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(40, 40, 40)
    pdf.ln(4)
    pdf.body_text(
        "This single command delivers all approved health sciences skills, an orchestrator "
        "that routes across 7 business domains, Cortex Knowledge Extensions for PubMed and "
        "ClinicalTrials.gov, and data model knowledge repositories - all validated through "
        "a rigorous promotion pipeline."
    )

    pdf.ln(2)
    pdf.subsection_title("Two Profiles")
    pdf.body_text(
        "The framework uses two distinct profiles to support different stages of the lifecycle:"
    )
    w_prof = [45, 70, 70]
    pdf.table_header(["", "Incubator Profile", "Production Profile"], w_prof)
    prof_rows = [
        ["Name", "health-sciences-\nincubator", "health-sciences-\nsolutions"],
        ["Purpose", "Test experimental\nskills during Phase 1", "Deliver approved\nproduction skills"],
        ["Skills source", "Incubator repo\n(all draft skills)", "SFS skills repo\n(production only)"],
        ["Who uses", "Contributors &\ntesters", "Field teams for\ncustomer engagements"],
        ["Quality", "Experimental -\nmay be unstable", "Validated, tested,\nproduction-grade"],
        ["Command", "cortex profile add\nhealth-sciences-incubator", "cortex profile add\nhealth-sciences-solutions"],
    ]
    for i, row in enumerate(prof_rows):
        pdf.table_row(row, w_prof, fill=(i % 2 == 0))
    pdf.ln(6)

    # Two-Repo Model
    pdf.section_title("Two-Repo Model")
    pdf.body_text(
        "The framework uses two distinct repositories to separate experimentation from production:"
    )
    w = [45, 70, 70]
    pdf.table_header(["", "Incubator Repo", "SFS Skills Repo"], w)
    rows = [
        ["Repo", "Snowflake-Solutions/\nhealth-sciences-incubator", "Snowflake-Solutions/\ncortex-code-skills"],
        ["Purpose", "Rapid skill development\n& experimentation", "Approved, production-\ngrade skills"],
        ["Who contributes", "Anyone (SEs, SAs, field)", "Tiger Team only"],
        ["Branch/PR rules", "Open branching,\nlightweight review", "Formal PR, CI gates,\npromotion stages"],
        ["Quality bar", "Low - iterate fast,\nbreak things", "High - best practices,\nevidence, testing"],
        ["Skills flow", "Skills created &\nrefined here", "Mature skills graduate\nhere from incubator"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, fill=(i % 2 == 0))
    pdf.ln(6)

    # Phase 0
    pdf.add_page()
    pdf.section_title("Phase 0: Framework Setup")
    pdf.bold_text("Accountable: Tiger Team")
    pdf.body_text("Tiger Team establishes the foundation for the entire lifecycle.")
    pdf.bullet("Create industry skills INCUBATOR repo in Snowflake-Solutions GitHub")
    pdf.sub_bullet("e.g., Snowflake-Solutions/health-sciences-incubator")
    pdf.sub_bullet("Open access - any SE/SA can branch, commit, iterate")
    pdf.bullet("Define skill development guidelines & templates for incubator")
    pdf.bullet("Define quality bar & promotion criteria for graduation to SFS skills repo")
    pdf.bullet("Set up lightweight CI in incubator (structure checks, linting - no blocking gates)")
    pdf.bullet("Create incubator profile (health-sciences-incubator)")
    pdf.sub_bullet("References incubator repo skills for easy testing")
    pdf.sub_bullet("Publish to Snowflake registry for contributor access")
    pdf.bullet("Document the full lifecycle: incubator -> SFS skills repo -> profile -> field")
    pdf.ln(2)
    pdf.bold_text("Output: Incubator repo, incubator profile, guidelines, promotion criteria, lifecycle docs")

    # Phase 1
    pdf.ln(6)
    pdf.section_title("Phase 1: Incubate & Iterate")
    pdf.bold_text("Accountable: Individual Contributors (SEs, SAs, Field Teams)")
    pdf.bold_text("Repo: Snowflake-Solutions/health-sciences-incubator")
    pdf.ln(2)
    pdf.body_text(
        "Any SE, SA, or field team member can contribute to the incubator repo. "
        "This is a sandbox for rapid iteration with no formal approval gates."
    )
    pdf.bullet("Branch freely, no formal PR approval needed")
    pdf.bullet("Create, test, break, refine skills collaboratively")
    pdf.bullet("Share work-in-progress with peers for feedback")
    pdf.bullet("Use skill-development skill to scaffold new skills")
    pdf.bullet("Test skills via incubator profile:")
    pdf.sub_bullet("cortex profile add health-sciences-incubator")
    pdf.sub_bullet("Profile auto-loads all incubator skills for testing")
    pdf.sub_bullet("cortex profile sync health-sciences-incubator to get latest changes")
    pdf.bullet("Or test individually: cortex skill add /path/to/incubator/skills/my-skill")
    pdf.bullet("When skill is mature -> signal to Tiger Team for Phase 2 pickup")
    pdf.ln(2)
    pdf.bold_text("Gate: None - this is a sandbox for rapid iteration")
    pdf.bold_text("Exit Criteria: Contributor believes skill is ready, notifies Tiger Team")

    # Phase 2
    pdf.add_page()
    pdf.section_title("Phase 2: Harden & Promote")
    pdf.bold_text("Accountable: Tiger Team Only")
    pdf.bold_text("Repo: Snowflake-Solutions/cortex-code-skills")
    pdf.ln(2)
    pdf.body_text(
        "Tiger Team picks up mature skills from the incubator and drives them through "
        "the full SFS approval pipeline. Only Tiger Team members can submit and merge "
        "promotion PRs in the SFS skills repo."
    )

    pdf.subsection_title("Step 1: Evaluate & Accept")
    pdf.bullet("Review skill from incubator for relevance & quality potential")
    pdf.bullet("Decide: accept, request changes, or decline")
    pdf.bullet("Fork/copy skill into Snowflake-Solutions/cortex-code-skills")

    pdf.subsection_title("Step 2: Audit & Refine")
    pdf.bullet("Audit against SKILL_BEST_PRACTICES.md")
    pdf.bullet("Refine workflows, stopping points, guardrails")
    pdf.bullet("Ensure skill composes well with other industry skills")
    pdf.bullet("Validate no overlap/conflict with bundled product skills")

    pdf.subsection_title("Step 3: Test & Validate")
    pdf.bullet("Test with realistic customer scenarios")
    pdf.bullet("Gather testers (3+ for staging, 5+ for production)")
    pdf.bullet("Record customer impact evidence (skill_evidence.yaml)")
    pdf.bullet("Run overlap detection (blocking at staging+)")

    pdf.subsection_title("Step 4: Submit & Promote via PR")
    pdf.bullet("Submit PR to Snowflake-Solutions/cortex-code-skills at 'draft' stage")
    pdf.bullet("Pass full CI pipeline (evidence, overlap, best practices)")
    pdf.bullet("Promote: draft -> review -> staging -> production")
    pdf.bullet("Only Tiger Team members can merge promotion PRs")

    pdf.subsection_title("Step 5: Integrate into Production Profile")
    pdf.bullet("Add approved skill to health-sciences-solutions profile")
    pdf.bullet("Update orchestrator/router if needed")
    pdf.bullet("Wire CKEs, data model knowledge, cross-domain patterns")

    # Promotion Stages
    pdf.ln(4)
    pdf.subsection_title("Promotion Stages")
    w2 = [30, 30, 45, 40, 40]
    pdf.table_header(["Stage", "Min Testers", "Min Customers\n(cust-facing)", "Overlap", "Best Practices"], w2)
    stages = [
        ["draft", "0", "0", "Warnings", "Warnings"],
        ["review", "1", "0", "Warnings", "Warnings"],
        ["staging", "3", "1", "Blocking", "Warnings"],
        ["production", "5", "1", "Blocking", "Blocking"],
    ]
    for i, row in enumerate(stages):
        pdf.table_row(row, w2, fill=(i % 2 == 0))

    # Phase 3
    pdf.add_page()
    pdf.section_title("Phase 3: Publish & Distribute")
    pdf.bold_text("Accountable: Tiger Team")
    pdf.ln(2)
    pdf.body_text(
        "Tiger Team publishes the industry profile to the Snowflake registry, "
        "making it available to all field teams with a single command."
    )
    pdf.bullet("Production profile (health-sciences-solutions) references only production-stage skills")
    pdf.bullet("Includes orchestrator system prompt (domain routing, CKE integration)")
    pdf.bullet("Publish to Snowflake registry: cortex profile publish health-sciences-solutions")
    pdf.bullet("Maintain incubator profile separately for ongoing experimentation")
    pdf.bullet("Document prerequisites (Marketplace CKEs, Cortex Search setup, etc.)")
    pdf.bullet("Announce availability to field organization")
    pdf.ln(2)
    pdf.bold_text("Output: Published profile in Snowflake registry, onboarding docs")

    # Phase 4
    pdf.ln(6)
    pdf.section_title("Phase 4: Consume & Feedback")
    pdf.bold_text("Accountable: Field Teams (consumers), Tiger Team (support & updates)")
    pdf.ln(2)
    pdf.body_text(
        "Field SEs and SAs consume the published profile for customer engagements "
        "and feed improvements back into the incubator."
    )
    pdf.bullet("cortex profile add health-sciences-solutions")
    pdf.bullet("Uses approved, production-grade skills for customer engagements")
    pdf.bullet("Files issues / feedback on existing skills")
    pdf.bullet("Contributes improvements back to INCUBATOR -> feeds Phase 1")
    pdf.bullet("cortex profile sync health-sciences-solutions for updates")
    pdf.bullet("Can also add incubator profile to preview upcoming skills:")    
    pdf.sub_bullet("cortex profile add health-sciences-incubator")
    pdf.ln(2)
    pdf.bold_text("Feedback loop: Field issues -> incubator repo -> Tiger Team triage -> Phase 2")

    # Skill Taxonomy
    pdf.add_page()
    pdf.section_title("Skill Taxonomy")
    pdf.body_text(
        "Skills are organized in a five-level hierarchy that maps from the broadest "
        "industry classification down to individual capabilities."
    )
    pdf.ln(2)
    pdf.subsection_title("Hierarchy Definition")
    pdf.set_font("Courier", "B", 11)
    pdf.set_text_color(0, 100, 180)
    pdf.cell(0, 7, "  Industry / Sub-Industry / Business Function / Use Case Skill / Sub-Skill", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(40, 40, 40)
    pdf.ln(4)

    w_tax = [30, 55, 55, 45]
    pdf.table_header(["Level", "Example", "Description", "Applies To"], w_tax)
    tax_rows = [
        ["Industry", "Health Sciences", "Top-level domain", "Both repos"],
        ["Sub-Industry", "Provider, Pharma,\nPayer", "Customer segment", "Incubator\nstructure"],
        ["Business\nFunction", "Clinical Research,\nDrug Safety", "Functional area\nwithin sub-industry", "Incubator\nstructure"],
        ["Use Case\nSkill", "healthcare-imaging,\npharmacovigilance", "Concrete skill\nsolving a use case", "Both repos"],
        ["Sub-Skill", "dicom-parser,\nimaging-viewer", "Leaf capability\nwithin a skill", "Skill internal"],
    ]
    for i, row in enumerate(tax_rows):
        pdf.table_row(row, w_tax, fill=(i % 2 == 0))

    pdf.ln(6)
    pdf.subsection_title("Sub-Industries")
    w_sub = [35, 55, 95]
    pdf.table_header(["Sub-Industry", "Customer Types", "Business Functions"], w_sub)
    sub_rows = [
        ["Provider", "Hospitals, health systems,\nclinics, IDNs", "Clinical Research, Clinical Data\nManagement, Revenue Cycle"],
        ["Pharma", "Pharma, biotech, CROs", "Drug Safety, Genomics,\nLab Operations"],
        ["Payer", "Health plans, TPAs, PBMs", "Claims Processing\n(future skills)"],
        ["Cross-Industry", "All of the above", "Research Strategy,\nKnowledge Extensions"],
    ]
    for i, row in enumerate(sub_rows):
        pdf.table_row(row, w_sub, fill=(i % 2 == 0))

    # Incubator Repo Structure
    pdf.add_page()
    pdf.section_title("Incubator Repo Structure")
    pdf.body_text(
        "The incubator repo uses the full hierarchy as directory nesting. "
        "This provides clear organization for browsing and discovery during development."
    )
    pdf.ln(2)
    pdf.set_font("Courier", "", 9)
    pdf.set_text_color(40, 40, 40)
    inc_tree = [
        "  skills/",
        "    health-sciences/",
        "      provider/",
        "        clinical-research/",
        "          healthcare-imaging/",
        "            dicom-parser/",
        "            dicom-ingestion/",
        "            dicom-analytics/",
        "            imaging-viewer/",
        "            imaging-governance/",
        "            imaging-ml/",
        "        clinical-data-management/",
        "          fhir-data-transformation/",
        "          clinical-nlp/",
        "          omop-cdm-modeling/",
        "        revenue-cycle/",
        "          claims-data-analysis/",
        "      pharma/",
        "        drug-safety/",
        "          pharmacovigilance/",
        "          clinical-trial-protocol/",
        "        genomics/",
        "          nextflow-development/",
        "          variant-annotation/",
        "          single-cell-rna-qc/",
        "          scvi-tools/",
        "          survival-analysis/",
        "        lab-operations/",
        "          instrument-data-to-allotrope/",
        "      payer/",
        "        claims-processing/",
        "      cross-industry/",
        "        research-strategy/",
        "          scientific-problem-selection/",
        "        knowledge-extensions/",
        "          cke-pubmed/",
        "          cke-clinical-trials/",
    ]
    for line in inc_tree:
        pdf.cell(0, 4.5, line, new_x="LMARGIN", new_y="NEXT")

    pdf.ln(4)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.body_text(
        "Leaf-level skills can appear directly under a business function "
        "(e.g., claims-data-analysis under revenue-cycle) without requiring "
        "an intermediate use-case-skill directory."
    )

    # SFS Repo Naming Convention
    pdf.add_page()
    pdf.section_title("SFS Repo Naming Convention")
    pdf.body_text(
        "The SFS skills repo (Snowflake-Solutions/cortex-code-skills) uses a flattened "
        "naming convention. Skills sit in a single skills/ directory with a structured "
        "prefix that encodes the taxonomy."
    )
    pdf.ln(2)
    pdf.subsection_title("Naming Pattern")
    pdf.set_font("Courier", "B", 12)
    pdf.set_text_color(0, 100, 180)
    pdf.cell(0, 8, "  hcls-{sub-industry}-{function}-{skill}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(40, 40, 40)
    pdf.ln(4)

    pdf.subsection_title("Naming Rules")
    w_nr = [30, 60, 95]
    pdf.table_header(["Component", "Values", "Notes"], w_nr)
    nr_rows = [
        ["hcls", "Fixed prefix", "Health Sciences industry\nidentifier"],
        ["sub-industry", "provider, pharma,\npayer, cross", "Maps to sub-industry\ntaxonomy level"],
        ["function", "imaging, cdata,\ndsafety, genomics,\nlab, claims, rwe,\nresearch, cke", "Short alias for\nbusiness function"],
        ["skill", "dicom-parser,\npharmacovigilance,\nnextflow, etc.", "Use case skill name\n(leaf level)"],
    ]
    for i, row in enumerate(nr_rows):
        pdf.table_row(row, w_nr, fill=(i % 2 == 0))

    pdf.ln(6)
    pdf.subsection_title("Example Mappings")
    w_ex = [93, 93]
    pdf.table_header(["Incubator Path", "SFS Name"], w_ex)
    ex_rows = [
        ["health-sciences/provider/\nclinical-research/healthcare-imaging/\ndicom-parser/", "hcls-provider-imaging-\ndicom-parser"],
        ["health-sciences/pharma/\ndrug-safety/pharmacovigilance/", "hcls-pharma-dsafety-\npharmacovigilance"],
        ["health-sciences/pharma/\ngenomics/variant-annotation/", "hcls-pharma-genomics-\nvariant-annotation"],
        ["health-sciences/provider/\nclinical-data-management/\nclinical-nlp/", "hcls-provider-cdata-\nclinical-nlp"],
        ["health-sciences/cross-industry/\nknowledge-extensions/cke-pubmed/", "hcls-cross-cke-\npubmed"],
        ["health-sciences/provider/\nrevenue-cycle/claims-data-analysis/", "hcls-provider-claims-\ndata-analysis"],
    ]
    for i, row in enumerate(ex_rows):
        pdf.table_row(row, w_ex, fill=(i % 2 == 0))

    # Orchestrator Routing Instructions
    pdf.ln(6)
    pdf.subsection_title("Orchestrator Routing Instructions")
    pdf.body_text(
        "The health-sciences-solutions orchestrator profile uses the following "
        "rules to route requests to the correct skills:"
    )
    pdf.bullet("Route by sub-industry FIRST: Hospital/clinic -> Provider, "
               "Pharma/biotech/CRO -> Pharma, Health plan/TPA/PBM -> Payer")
    pdf.bullet("When sub-industry is ambiguous: route by TASK, not customer type "
               "(e.g., imaging tasks always go to Provider > Clinical Research)")
    pdf.bullet("Cross-industry skills (CKEs, research strategy) are available to ALL sub-industries")
    pdf.bullet("Accept overlaps: some skills serve multiple sub-industries - "
               "route to the skill regardless of where it sits in the taxonomy")
    pdf.sub_bullet("claims-data-analysis: serves both Provider and Payer")
    pdf.sub_bullet("survival-analysis: serves both Pharma and Provider")
    pdf.sub_bullet("clinical-nlp: serves both Provider and Pharma")

    # Design Decisions Summary
    pdf.add_page()
    pdf.section_title("Design Decisions")
    pdf.body_text(
        "Six key design decisions were evaluated and resolved to guide the "
        "framework's implementation. Each decision was analyzed through the lens "
        "of the two-repo/two-profile lifecycle."
    )

    w_dd = [60, 125]
    pdf.table_header(["Decision", "Resolution"], w_dd)
    dd_rows = [
        ["1. Skill Taxonomy\n& Naming", "5-level hierarchy (Industry > Sub-Industry > Business Function >\nUse Case Skill > Sub-Skill). Incubator uses deep nesting;\nSFS repo uses flat hcls-{sub}-{func}-{skill} convention."],
        ["2. Orchestrator\n/ Router", "Twin orchestrators in twin profiles, generated from a single\nJinja2 template. Routing logic/taxonomy/guardrails identical;\nonly skill refs and profile metadata differ."],
        ["3. CKE\nPackaging", "Standalone SFS skills with prerequisite checker. Preflight\ndetects Marketplace listing, guides setup if missing,\ngraceful fallback if skipped."],
        ["4. Data Model\nKnowledge", "Same pattern as CKEs: preflight checker + setup scripts +\ngraceful fallback. Reusable for any skill needing\nSnowflake objects (OMOP vocab, FAERS ref data, etc.)."],
        ["5. Profile\nVersioning", "Production: semver (v1.0.0) on SFS repo, immutable releases.\nIncubator: milestone tags (m{n}-{scope}), lightweight,\ndeletable, for demo stability."],
        ["6. Contribution\nFlow", "Hybrid: field can file issues OR submit PRs on incubator.\nTiger Team triages. Only Tiger Team submits PRs to SFS repo."],
    ]
    for i, row in enumerate(dd_rows):
        pdf.table_row(row, w_dd, fill=(i % 2 == 0))

    # Twin Orchestrator Architecture
    pdf.add_page()
    pdf.section_title("Twin Orchestrator Architecture")
    pdf.body_text(
        "Both profiles have their own orchestrator .md file generated from a single "
        "Jinja2 template plus a shared YAML skills registry. This eliminates drift "
        "risk while allowing each profile to reference different skill sets."
    )
    pdf.ln(2)
    pdf.subsection_title("Template Pipeline")
    pdf.set_font("Courier", "", 9)
    pdf.set_text_color(40, 40, 40)
    tmpl_flow = [
        "  templates/skills_registry.yaml    (single source of truth)",
        "  templates/orchestrator.md.j2       (shared Jinja2 template)",
        "       |",
        "       +--[profile=incubator]-->  agents/health-sciences-incubator.md",
        "       |                          (all skills available)",
        "       |",
        "       +--[profile=production]--> agents/health-sciences-solutions.md",
        "                                  (approved skills only)",
    ]
    for line in tmpl_flow:
        pdf.cell(0, 4.5, line, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.subsection_title("What Differs")
    w_td = [62, 62, 62]
    pdf.table_header(["Section", "Incubator", "Production"], w_td)
    td_rows = [
        ["Profile name", "health-sciences-\nincubator", "health-sciences-\nsolutions"],
        ["Introduction", "Experimental,\nvalidate outputs", "Approved, tested,\nproduction-grade"],
        ["Skill availability", "ALL skills\n(approved + draft)", "Only approved=true\nskills"],
        ["Routing logic", "IDENTICAL", "IDENTICAL"],
        ["Taxonomy tree", "IDENTICAL", "IDENTICAL"],
        ["Guardrails", "IDENTICAL", "IDENTICAL"],
        ["Patterns", "ALL patterns", "Only patterns where\nall skills approved"],
    ]
    for i, row in enumerate(td_rows):
        pdf.table_row(row, w_td, fill=(i % 2 == 0))

    pdf.ln(4)
    pdf.subsection_title("Drift Prevention")
    pdf.bullet("Generate command: python scripts/generate_orchestrators.py --profile both")
    pdf.bullet("Built-in drift check compares structural sections between outputs")
    pdf.bullet("QA validator (scripts/qa_validate_orchestrator.py) cross-references against filesystem")
    pdf.bullet("Rule: NEVER hand-edit agents/*.md directly - always edit template + registry, then regenerate")

    # Preflight Checker Pattern
    pdf.add_page()
    pdf.section_title("Preflight Checker Pattern")
    pdf.body_text(
        "A reusable infrastructure pattern for skills that depend on external "
        "Snowflake objects (Marketplace listings, Cortex Search services, tables). "
        "Applies to CKEs, Data Model Knowledge, and any future dependency."
    )
    pdf.ln(2)
    pdf.set_font("Courier", "", 9.5)
    pdf.set_text_color(40, 40, 40)
    pf_flow = [
        "  Skill needs external dependency?",
        "    --> Preflight check (detect if exists)",
        "    --> Setup scripts (self-service provisioning)",
        "    --> Graceful fallback (skill works without, better with)",
    ]
    for line in pf_flow:
        pdf.cell(0, 5, line, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.subsection_title("Current Dependencies Using This Pattern")
    w_pf = [45, 45, 50, 45]
    pdf.table_header(["Dependency", "Type", "Preflight Check", "Fallback"], w_pf)
    pf_rows = [
        ["CKE PubMed", "Marketplace\nlisting", "Probe shared\nCortex Search svc", "No literature\nenrichment"],
        ["CKE Clinical\nTrials", "Marketplace\nlisting", "Probe shared\nCortex Search svc", "No trial\nbenchmarking"],
        ["DICOM Data\nModel Knowledge", "Table +\nCortex Search", "Probe table &\nsearch service", "Hardcoded schema\ndefinitions"],
    ]
    for i, row in enumerate(pf_rows):
        pdf.table_row(row, w_pf, fill=(i % 2 == 0))

    pdf.ln(4)
    pdf.subsection_title("Implementation")
    pdf.bullet("Module: shared/preflight/checker.py - reusable PreflightChecker class")
    pdf.bullet("Configs: shared/preflight/configs.py - pre-built checkers for each dependency")
    pdf.bullet("Report: prints READY/MISSING/ERROR status with setup instructions")
    pdf.bullet("API: checker.add_table(), checker.add_cortex_search(), checker.add_marketplace_listing()")

    # Milestone Tagging
    pdf.add_page()
    pdf.section_title("Incubator Milestone Tagging")
    pdf.body_text(
        "Lightweight git tags on the incubator repo that mark a known-good state "
        "for a specific use case or demo, without the overhead of formal semantic versioning."
    )
    pdf.ln(2)
    pdf.subsection_title("Naming Convention")
    pdf.set_font("Courier", "B", 11)
    pdf.set_text_color(0, 100, 180)
    pdf.cell(0, 7, "  m{sequence}-{scope}-{optional-context}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(40, 40, 40)
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    pdf.subsection_title("Example Milestones")
    w_ms = [55, 130]
    pdf.table_header(["Tag", "Meaning"], w_ms)
    ms_rows = [
        ["m1-imaging", "First stable milestone: imaging skills working end-to-end"],
        ["m2-imaging-genomics", "Added genomics skills on top of m1"],
        ["m3-rwe-demo", "Stable point for a specific RWE customer demo"],
        ["m4-full-17skills", "All 17 skills reorganized and QA-validated"],
        ["m5-pre-sfs-batch1", "Snapshot before first batch submitted to SFS"],
    ]
    for i, row in enumerate(ms_rows):
        pdf.table_row(row, w_ms, fill=(i % 2 == 0))

    pdf.ln(4)
    pdf.subsection_title("When to Create a Milestone")
    pdf.bullet("Domain skills pass QA validation for a demo")
    pdf.bullet("Before submitting a batch to SFS repo")
    pdf.bullet("Customer-specific engagement needing a frozen state")
    pdf.bullet("After a major reorganization or refactor")

    pdf.ln(2)
    pdf.subsection_title("How Field Teams Use Milestones")
    pdf.set_font("Courier", "", 9.5)
    pdf.set_text_color(40, 40, 40)
    ms_usage = [
        "  # Stable demo experience",
        "  cortex profile add health-sciences-incubator --ref m4-full-17skills",
        "",
        "  # Bleeding edge (latest main)",
        "  cortex profile add health-sciences-incubator",
    ]
    for line in ms_usage:
        pdf.cell(0, 5, line, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.subsection_title("Key Properties")
    pdf.bullet("Zero overhead: git tag m4-full-17skills && git push --tags")
    pdf.bullet("Self-documenting: tag name describes the scope")
    pdf.bullet("Not semver: no compatibility promises, just 'this worked when tagged'")
    pdf.bullet("Deletable: git tag -d m3-bad && git push --delete origin m3-bad")
    pdf.bullet("Incubator only: production uses proper semver (v1.0.0) on SFS repo")

    pdf.ln(4)
    pdf.subsection_title("Milestones vs Production Versions")
    w_mv = [50, 68, 68]
    pdf.table_header(["", "Incubator Milestones", "Production Versions"], w_mv)
    mv_rows = [
        ["Format", "m{n}-{scope}", "v{major}.{minor}.{patch}"],
        ["Promise", "This worked for\n{scope}", "Approved, tested,\nsupported"],
        ["Who creates", "Any contributor", "Tiger Team only"],
        ["Repo", "Incubator", "SFS skills repo"],
        ["Ceremony", "git tag + push", "SFS PR + review +\nskill_evidence.yaml"],
        ["Deletable", "Yes", "No (immutable)"],
    ]
    for i, row in enumerate(mv_rows):
        pdf.table_row(row, w_mv, fill=(i % 2 == 0))

    # Contribution Flow
    pdf.add_page()
    pdf.section_title("Contribution Flow")
    pdf.body_text(
        "Field teams can contribute improvements through two paths on the incubator repo. "
        "Tiger Team triages all contributions and drives promotion to SFS."
    )
    pdf.ln(2)
    pdf.subsection_title("Path A: Issue-Based (Low Barrier)")
    pdf.bullet("Field team member files an issue on the incubator repo")
    pdf.bullet("Describes the bug, enhancement, or new skill idea")
    pdf.bullet("Contributor (or Tiger Team) picks up and implements")
    pdf.bullet("Tiger Team reviews and promotes when ready")
    pdf.ln(2)
    pdf.subsection_title("Path B: PR-Based (Direct Contribution)")
    pdf.bullet("Field team member forks the incubator repo")
    pdf.bullet("Implements the change in their fork")
    pdf.bullet("Submits PR to incubator repo (lightweight review)")
    pdf.bullet("Tiger Team merges to incubator, later promotes to SFS")
    pdf.ln(2)
    pdf.set_font("Courier", "", 9)
    pdf.set_text_color(40, 40, 40)
    contrib_flow = [
        "  Field Team",
        "    |",
        "    +--[issue]--> Incubator Repo (issue tracker)",
        "    |                  |",
        "    +--[fork+PR]---> Incubator Repo (PR merge)",
        "                      |",
        "                      v",
        "               Tiger Team Triage",
        "                      |",
        "                      v",
        "               SFS Skills Repo (Tiger Team PR only)",
        "                      |",
        "                      v",
        "               Production Profile",
    ]
    for line in contrib_flow:
        pdf.cell(0, 4.5, line, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.subsection_title("Rules")
    pdf.bullet("Incubator repo: open to all field teams (issues + PRs)")
    pdf.bullet("SFS skills repo: Tiger Team only (formal PR with CI gates)")
    pdf.bullet("No direct contributions to SFS repo from field teams")
    pdf.bullet("Tiger Team triages all contributions before promotion")

    # Accountability Matrix
    pdf.add_page()
    pdf.section_title("Accountability Matrix")
    w3 = [38, 35, 35, 42, 38]
    pdf.table_header(["Phase", "Who Can Do", "Who Approves", "Repo", "Output"], w3)
    matrix = [
        ["0 - Framework\nSetup", "Tiger Team", "Tiger Team", "Both", "Repos, guidelines,\nCI"],
        ["1 - Incubate\n& Iterate", "Anyone", "Self-service", "Incubator", "Experimental\nskills"],
        ["2 - Harden\n& Promote", "Tiger Team\nonly", "Tiger Team", "SFS Skills", "Production-grade\nskills"],
        ["3 - Publish\n& Distribute", "Tiger Team", "Tiger Team", "N/A (registry)", "Published profile"],
        ["4 - Consume\n& Feedback", "Field teams", "Tiger Team\n(support)", "Incubator\n(feedback)", "Solutions,\nfeedback"],
    ]
    for i, row in enumerate(matrix):
        pdf.table_row(row, w3, fill=(i % 2 == 0))

    # Lifecycle Flow
    pdf.ln(10)
    pdf.section_title("Lifecycle Flow")
    pdf.set_font("Courier", "", 9)
    pdf.set_text_color(40, 40, 40)
    flow = [
        "  Anyone (Field SE/SA)",
        "       |",
        "       v",
        "  +---------------------------------+",
        "  | Phase 1: Incubator Repo         |",
        "  | + incubator profile for testing  |",
        "  +---------------------------------+",
        "       |  skill mature",
        "       v",
        "  +---------------------------------+",
        "  | Phase 2: Tiger Team Reviews      |",
        "  | (audit, test, promote via PR)    |",
        "  +---------------------------------+",
        "       |  production stage",
        "       v",
        "  +---------------------------------+",
        "  | Phase 3: Publish Profiles        |",
        "  | (production + incubator)         |",
        "  +---------------------------------+",
        "       |",
        "       v",
        "  +---------------------------------+",
        "  | Phase 4: Field Consumption       |",
        "  | (cortex profile add)             |",
        "  +----------------+----------------+",
        "                   |  feedback",
        "                   v",
        "            Back to Phase 1",
    ]
    for line in flow:
        pdf.cell(0, 4.5, line, new_x="LMARGIN", new_y="NEXT")

    pdf.output(OUTPUT_PATH)
    print(f"PDF generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    build_pdf()
