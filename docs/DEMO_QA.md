# Healthcare Agentic Platform — Demo Q&A

---

## Q1: Can the plan list the skills for each step?

> "When the initial plan is getting listed out — can we also list the skills for each step? That way, someone who is a little technical can understand the detail better."

### What We Have Today

Every task in the DAG already carries `skill_name` and `skill_type` metadata internally. The scenario definitions in `state.py` map each step to a specific CoCo skill:

| Step | Skill (`skill_name`) | Type (`skill_type`) |
|---|---|---|
| Build Dynamic Tables | `dynamic-tables` | platform |
| Train Risk Model | `machine-learning` | platform |
| Create Cortex Search | `cortex-search` | platform |
| Create Semantic View | `cortex-analyst` | platform |
| Generate React App | `build-react-app` | platform |
| Setup FAERS Data | `hcls-pharma-dsafety-pharmacovigilance` | standalone |
| Run Signal Detection | `hcls-pharma-dsafety-pharmacovigilance` | standalone |
| Build Safety Dashboard | `streamlit` | platform |
| Setup Clinical Docs | `hcls-provider-cdata-clinical-docs` | standalone |
| Create Doc Search | `cortex-search` | platform |
| Create Doc Agent | `cortex-agent` | platform |

The backend API (`/api/workflow`) already returns this metadata per task. The frontend DAG nodes currently show task name and status, but **do not** surface the skill name in the UI.

### What We Can Build Next

- **Skill badges on DAG nodes** — Show a small tag under each node label (e.g., "Dynamic Tables" node shows a `dynamic-tables` badge with the CoCo skill icon). This is a frontend-only change in `WorkflowGraph.tsx` — the data is already in the API response.
- **Plan summary table** — Before execution starts, render a table in the UI listing each step with its skill, a one-line description, and the Snowflake features it uses (e.g., "Dynamic Tables → `CREATE DYNAMIC TABLE`, incremental refresh"). This makes the plan legible to a technical audience.
- **Skill documentation links** — Each skill badge could link to the corresponding `SKILL.md` file in the skills registry, giving the viewer full implementation detail on demand.
- **Effort estimate column** — Add estimated duration per step based on historical Langfuse traces (e.g., "Build Dynamic Tables: ~15s, Train Risk Model: ~45s").

**Effort: ~2 hours** (frontend table component + skill badge styling).

---

## Q2: Can the user modify/enhance the plan before execution?

> "When the plan is presented, does the current agent have the ability to solicit user feedback and give inputs to modify/enhance the plan before it's finalized?"

### What We Have Today

The LangGraph engine already supports `interrupt_before` for human-in-the-loop gating. The architecture includes:

- **Plan Generation phase** — Every scenario runs a "Generate Plan" step before execution begins. The plan DAG is rendered in the UI before tasks start running.
- **LangGraph `interrupt_before`** — The graph compiler accepts a list of node IDs to pause at. We currently wire this for high-risk execution nodes (ML training, signal detection), not the plan gate itself.
- **Approval event infrastructure** — `LangGraphWorkflowState` has `_awaiting_approval`, `_approval_event`, and `_approval_result` fields with WebSocket broadcast for `approval_required` events.

However, **today** the workflow auto-starts execution after plan generation. There is no UI for the user to review the plan, add/remove steps, or provide feedback before the execute phase begins.

### What We Can Build Next

- **Plan Gate UI** — After the "Generate Plan" step completes, pause the workflow using `interrupt_before` on the first execution node. Show a confirmation modal: "Here's the plan — 5 steps using Dynamic Tables, ML, Search, Analyst, and React. Approve, modify, or cancel?"
- **Step toggle** — Let the user uncheck optional steps (e.g., "Skip React app generation, I just need the data pipeline"). The backend removes unchecked nodes from the `StateGraph` before compiling and resuming.
- **Parameter override** — Allow the user to modify step parameters before execution (e.g., "Use `llama3.1-70b` instead of `mistral-large2` for the risk model", or "Target `MY_WAREHOUSE` instead of `COMPUTE_WH`").
- **Chat-based plan refinement** — Route the plan to the `HEALTH_COPILOT_AGENT` for conversational refinement: "Add a data quality check before ML training" → agent suggests adding a validation step → user approves → graph is rebuilt.
- **Plan versioning** — Store plan snapshots in Snowflake so users can compare what was planned vs. what was executed.

**Effort: ~4 hours** (interrupt gate + approval UI + step toggle logic).

---

## Q3: Does this produce reusable code, or is it a business user analysis tool?

> "When the workflow runs, does each agent produce code that can be checked into a git repo? Or is it designed to be a business user agent that produces output — more like an ad-hoc analysis tool?"

### What We Have Today

**It's currently both, depending on the layer:**

| Layer | What It Produces | Reusable? |
|---|---|---|
| **LangGraph workflow engine** | Executes DDL/DML against Snowflake (CREATE DYNAMIC TABLE, CREATE CORTEX SEARCH SERVICE, etc.) | The **Snowflake objects** are persistent and production-grade — Dynamic Tables, ML models in the Registry, Search services, Semantic views. These survive beyond the session. |
| **Cortex Agent (copilot)** | Natural language answers, SQL queries, charts, data tables via SSE streaming | Ad-hoc analysis output — ephemeral, conversational. Not checked into git. |
| **Task functions** | Each task function contains the SQL/Python that creates Snowflake objects. This code lives in `backend/tasks/` | These are **reusable Python modules** that could be extracted and versioned. |
| **React app generation** | Generates a React dashboard scaffold and deploys to a Snowflake stage | The generated code is stored in `APPS.CLINICAL_DASHBOARD_CODE` (4 files) on a Snowflake stage. |
| **CoCo Skills** | 19 SKILL.md files encoding domain patterns | Fully version-controlled in `coco-healthcare-skills/skills/`. Already in a git-friendly format. |

So the workflow **creates persistent Snowflake objects** (not just analysis output), but the code that creates them is embedded in the orchestrator backend, not exported as standalone scripts.

### What We Can Build Next

- **Code export per step** — After each task completes, capture the exact SQL/Python executed and write it to a Snowflake stage as a versioned file (e.g., `@ORCHESTRATOR.CODE_EXPORTS/clinical_data_warehouse/01_dynamic_tables.sql`). The user can download the full pipeline as a git-ready folder.
- **dbt project generation** — For data pipeline scenarios, emit a `dbt` project with models, sources, and tests instead of raw SQL. This is immediately checkable into git and deployable via CI/CD.
- **Notebook export** — Generate a Snowflake Notebook (`.ipynb`) per scenario that reproduces the entire pipeline. This serves as both documentation and executable artifact.
- **Terraform/Pulumi output** — For infrastructure-heavy scenarios (search services, compute pools, agents), emit IaC definitions alongside the SQL.
- **Dual mode toggle** — Add a UI switch: "Demo mode" (runs everything, shows results) vs. "Export mode" (generates code artifacts without executing, packages for git).

**Effort: ~6 hours** (code capture per task + stage export + download endpoint).

---

## Q4: Can this serve both AE/SE demo persona and end-customer persona?

> "If we need to think of this as an end tool for AEs — will that work? Two personas: (1) AE/SE showcasing art-of-the-possible to advance a use case, (2) end-customer agent targeted at a certain persona."

### What We Have Today

The platform currently serves **Persona 1 (AE/SE demo)** well:

- **Dispatcher Agent** — AE describes a customer use case in natural language → agent routes to the correct pipeline → orchestrator URL is returned → AE walks the customer through the live DAG execution.
- **3 pre-built scenarios** — Clinical Data Warehouse, Drug Safety, Clinical Docs cover the most common healthcare asks.
- **19 CoCo skills** — Deep domain coverage across Provider, Pharma, Cross-Industry that an AE can reference during the conversation.
- **Copilot chat** — The `HEALTH_COPILOT_AGENT` lets the AE ask ad-hoc questions ("How many patients are high risk?", "Show me the top adverse events for statins") with live SQL, charts, and data tables.
- **SPCS deployment** — The entire app runs as a single SPCS service. No local setup required for the customer to see the demo.

For **Persona 2 (end-customer agent)**, the building blocks exist but are not yet packaged:

- The Cortex Agent, Semantic Model, and Search Services created by the orchestrator are **standalone Snowflake objects** that work independently of the orchestrator UI.
- A customer could use the `HEALTH_COPILOT_AGENT` directly from Snowflake Intelligence (the Snowsight AI assistant) without needing the React app.

### What We Can Build Next

**For Persona 1 (AE/SE):**
- **Industry template library** — Pre-built scenario packs per customer vertical (Payer: claims adjudication + member analytics; Provider: clinical data warehouse + imaging; Pharma: drug safety + clinical trials). AE picks the pack, orchestrator builds it in the customer's trial account.
- **One-click demo provisioning** — `snow app install` or a Streamlit setup wizard that provisions the database, loads sample data, deploys the SPCS service, and creates the agents — all from a single command.
- **Customizable branding** — Let AEs swap the logo, color scheme, and sample questions to match the customer's brand for the demo.

**For Persona 2 (end-customer):**
- **Snowflake Intelligence integration** — Move the copilot agent to `SNOWFLAKE_INTELLIGENCE.AGENTS` schema so it appears natively in the customer's Snowsight sidebar. No separate app needed.
- **Role-based access** — Add RBAC so different customer personas (clinician, data analyst, compliance officer) see different tools and data scopes on the same agent.
- **Customer data onboarding** — Replace the synthetic FHIR/FAERS data with a guided ingestion flow that connects to the customer's actual data sources (Epic FHIR API, FDA FAERS download, internal document stores).
- **Self-service scenario builder** — Let the customer define new scenarios (add tasks, pick skills, set dependencies) through a visual DAG editor, not just the 3 pre-built ones.
- **Native App packaging** — Package the entire platform as a Snowflake Native App (using declarative sharing) so it can be installed from the Marketplace with one click.

**Effort: Persona 1 enhancements ~8 hours, Persona 2 productization ~2-3 weeks.**

---

## Q5: How can we deploy this in our environment?

> "How can we deploy this in our env? Can you share your code? What's the best way for us to try this out?"

### What We Have Today

The full stack is self-contained and deployable:

| Component | Location | What It Is |
|---|---|---|
| Frontend | `orchestrator/frontend/` | React + Vite + TypeScript + Tailwind |
| Backend | `orchestrator/backend/` | FastAPI + LangGraph + Langfuse |
| Deployment | `orchestrator/deploy/` | Dockerfile, nginx.conf, supervisord.conf, deploy.sh |
| Skills | `coco-healthcare-skills/skills/` | 19 SKILL.md files |
| Agents | Snowflake objects | `HEALTH_COPILOT_AGENT`, `ORCHESTRATOR_DISPATCHER_AGENT` |
| Data | Snowflake objects | FHIR tables, FAERS data, clinical docs, ML models |

**Current deployment method:**
```bash
# 1. Clone the repo
# 2. Run the deploy script (builds Docker, pushes to Snowflake registry, creates SPCS service)
cd orchestrator && bash deploy/deploy.sh
# 3. Takes ~3 minutes, outputs the SPCS URL
```

Prerequisites:
- Snowflake account with Cortex AI enabled (Cortex Agent, Cortex Search, Cortex Analyst)
- SPCS compute pool (`CPU_X64_S`, 1-2 nodes)
- Docker installed locally for building the image
- `snow` CLI configured with a connection

### What We Can Build Next

- **One-command bootstrap** — A single `setup.sh` script that: creates the database/schemas, loads sample data, creates the semantic model + search services + agents, builds and deploys the SPCS service, and outputs the URL. Zero manual steps.
- **Snowflake Native App** — Package as a Native App with `manifest.yml` and `setup_script.sql`. Customer installs from Marketplace → auto-provisions everything. No Docker, no CLI, no code.
- **Environment variables** — Externalize all hardcoded values (database name, warehouse, schema) to env vars so the same image works across accounts without code changes.
- **Terraform module** — IaC definition for the full stack: compute pool, image repo, service, agents, search services, stages. `terraform apply` and done.
- **GitHub Actions CI/CD** — Automated pipeline: push to main → build Docker → push to registry → rolling update on SPCS service. No manual deploys.
- **Documentation site** — Auto-generated docs from the SKILL.md files + architecture diagrams + deployment guide.

**Recommended path for trying it out:**
1. **Fastest** — We share the SPCS URL and grant access to the live demo (5 minutes)
2. **Self-service** — We share the repo + a setup guide, you run `deploy.sh` in your account (30 minutes)
3. **Production** — We package as a Native App with Marketplace listing (future)

---
