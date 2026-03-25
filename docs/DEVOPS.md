# DevOps & Observability

> Operational guide for the CoCo Healthcare Skills platform.

## Observability Architecture

```
+-----------------------------------------------------------------------------+
|                      OBSERVABILITY ARCHITECTURE                              |
+-----------------------------------------------------------------------------+
|                                                                              |
|  EXECUTION LOGGING (Snowflake Tables)                                        |
|  |-- ORCHESTRATOR_EXECUTION_LOG  Plan lifecycle tracking                     |
|  |-- SKILL_EXECUTION_LOG         Per-skill execution details                 |
|  +-- GOVERNANCE_AUDIT_LOG        Immutable governance action trail            |
|                                                                              |
|  CORTEX SERVICE MONITORING                                                   |
|  |-- SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY                  |
|  |-- SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY                                   |
|  +-- ACCESS_HISTORY              PHI access audit trail                      |
|                                                                              |
|  QA VALIDATION                                                               |
|  |-- qa_validate_orchestrator.py  12-check structural integrity suite        |
|  +-- hcls-cross-validation        Data quality validation skill              |
|                                                                              |
|  PREFLIGHT CHECKS                                                            |
|  +-- shared/preflight/            Dependency verification before execution   |
|                                                                              |
+-----------------------------------------------------------------------------+
```

## Execution Logging

### Setup

Run the DDL script to create observability tables:

```sql
-- In your target database
USE DATABASE <YOUR_DB>;
!source scripts/setup_observability_tables.sql
```

This creates:
- `OBSERVABILITY.ORCHESTRATOR_EXECUTION_LOG` — plan lifecycle
- `OBSERVABILITY.SKILL_EXECUTION_LOG` — per-skill execution
- `OBSERVABILITY.GOVERNANCE_AUDIT_LOG` — governance actions
- `OBSERVABILITY.ACTIVE_SESSIONS` — view of in-progress plans
- `OBSERVABILITY.SKILL_PERFORMANCE` — view of skill success rates
- `OBSERVABILITY.GOVERNANCE_SUMMARY` — daily governance action summary

### Python Logger

```python
from shared.observability.logger import ExecutionLogger

logger = ExecutionLogger(conn, database="MY_DB", schema="OBSERVABILITY")

plan_id = logger.log_plan_start(session_id, user_request, domain, steps)
logger.log_plan_approved(session_id, plan_id)
logger.log_skill_start(session_id, plan_id, 1, "hcls-provider-cdata-fhir")
logger.log_skill_complete(session_id, plan_id, 1, artifacts={"tables": ["PATIENT"]})
logger.log_governance_action(session_id, "hcls-provider-cdata-fhir", "MASKING_POLICY", "PATIENT", "PHI")
logger.log_plan_complete(session_id, plan_id)
```

### Monitoring Queries

```sql
-- Active sessions
SELECT * FROM OBSERVABILITY.ACTIVE_SESSIONS;

-- Skill performance (success rates, durations)
SELECT * FROM OBSERVABILITY.SKILL_PERFORMANCE;

-- Governance summary (daily action counts)
SELECT * FROM OBSERVABILITY.GOVERNANCE_SUMMARY;

-- Failed skills in last 24h
SELECT skill_name, error_message, started_at
FROM OBSERVABILITY.SKILL_EXECUTION_LOG
WHERE status = 'FAILED'
  AND started_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY started_at DESC;

-- Plans pending approval
SELECT session_id, plan_id, user_request, detected_domain
FROM OBSERVABILITY.ORCHESTRATOR_EXECUTION_LOG
WHERE status = 'PENDING' AND plan_approved = FALSE;
```

## QA Validation

### Orchestrator Structural Validation

```bash
python scripts/qa_validate_orchestrator.py --profile incubator
python scripts/qa_validate_orchestrator.py --profile both
```

12-check suite validates:

| Check | What It Validates |
|-------|-------------------|
| 1. $refs -> SKILL.md | Every $ref points to a SKILL.md with matching name |
| 2. SKILL.md -> orchestrator | Every top-level SKILL.md is referenced |
| 3. Folder == name | Directory name matches SKILL.md name: field |
| 4. Router sub-skills | Sub-skill directories exist and are referenced |
| 5. Taxonomy tree | Skills in taxonomy tree exist in filesystem |
| 6. Reference consistency | Occurrence counts per $ref |
| 7. Standalone skills | Standalone skills exist and are referenced |
| 8. Twin drift | Structural differences between incubator/production |
| 9. Skill directories | Every skill dir has SKILL.md |
| 10. Shared infrastructure | Preflight, observability, templates exist |
| 11. CKE used_by | CKE references point to real skills |
| 12. Overlap entries | Overlap skills exist in registry |

### Data Quality Validation

```bash
python skills/hcls-cross-validation/scripts/validate.py all \
    --table DB.SCHEMA.TABLE \
    --domain fhir \
    --connection default
```

Domains supported: `fhir`, `omop`, `dicom`, `clinical_docs`

## Defense-in-Depth Guardrails

### Three-Layer System

| Layer | Mechanism | File | Purpose |
|-------|-----------|------|---------|
| 1 | Profile rules | `agents/*.md` (YAML frontmatter) | Session-wide constraints |
| 2 | Gate + Phase skills | `skills/*/gates/`, `skills/*/phases/` | Structural decomposition |
| 3 | Hook blocks | `hooks.json` | DDL/DML confirmation before execution |

### hooks.json

The `hooks.json` file provides hard blocks on:
- DDL/DML statements (CREATE, ALTER, DROP, INSERT, etc.) — requires user confirmation
- Snowflake CLI commands — requires user confirmation
- Destructive system commands (rm -rf, sudo) — blocked entirely

## Testing Strategy

### Test Levels

```
+-----------------------------------------------------------------------------+
|  PREFLIGHT CHECKS                                                            |
|  +-- Dependency verification (tables, search services, Marketplace)          |
+-----------------------------------------------------------------------------+
|  QA VALIDATION (12-check suite)                                              |
|  +-- Orchestrator structural integrity                                       |
+-----------------------------------------------------------------------------+
|  DATA VALIDATION (hcls-cross-validation)                                     |
|  |-- Completeness checks (null rates, row counts, required fields)           |
|  |-- Schema consistency (expected columns, types, PKs)                       |
|  +-- Semantic validation (allowed values, references, domain rules)          |
+-----------------------------------------------------------------------------+
|  GENERATION PIPELINE VALIDATION                                              |
|  |-- Regenerate orchestrators: python scripts/generate_orchestrators.py      |
|  +-- Drift check: incubator vs production structural alignment               |
+-----------------------------------------------------------------------------+
|  END-TO-END SMOKE TESTS                                                      |
|  |-- Load CoCo profile, run sample requests, verify skill routing            |
|  +-- Preflight -> Plan -> Approve -> Execute -> Validate cycle               |
+-----------------------------------------------------------------------------+
```

## Generation Pipeline

### Regenerate Orchestrators

```bash
cd coco-healthcare-skills

python scripts/generate_orchestrators.py --profile incubator
python scripts/generate_orchestrators.py --profile production
python scripts/generate_orchestrators.py --profile both
```

### Full Build-and-Validate Cycle

```bash
python scripts/generate_orchestrators.py --profile both
python scripts/qa_validate_orchestrator.py --profile incubator
```

## CI/CD Pipeline

### GitHub Actions Workflow

```yaml
# .github/workflows/validate.yml
name: Validate Skills
on: [push, pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install pyyaml jinja2
      - name: Regenerate orchestrators
        run: python coco-healthcare-skills/scripts/generate_orchestrators.py --profile both
      - name: QA validation
        run: python coco-healthcare-skills/scripts/qa_validate_orchestrator.py --profile both
      - name: Check for uncommitted changes
        run: |
          if ! git diff --quiet coco-healthcare-skills/agents/; then
            echo "ERROR: Orchestrators are stale. Regenerate and commit."
            git diff --stat coco-healthcare-skills/agents/
            exit 1
          fi
```

### Adding a New Skill

```bash
# 1. Create skill directory
mkdir -p skills/hcls-{sub}-{func}-{skill}/scripts

# 2. Create SKILL.md with frontmatter (name, description, tools, platform_affinities)

# 3. Register in YAML registry
#    Edit templates/skills_incubator.yaml

# 4. Update domain order if new domain
#    Edit scripts/generate_orchestrators.py DOMAIN_ORDER

# 5. Update template if needed
#    Edit templates/orchestrator.md.j2

# 6. Regenerate + validate
python scripts/generate_orchestrators.py --profile incubator
python scripts/qa_validate_orchestrator.py --profile incubator
```

## Skill Versioning

### Milestone Tags (Incubator)

```
m{sequence}-{scope}-{context}
```

| Tag | Meaning |
|-----|---------|
| `m1-imaging` | Imaging skills working end-to-end |
| `m2-imaging-genomics` | Added genomics on top of m1 |
| `m5-pre-sfs-batch1` | Snapshot before SFS submission |
| `m6-observability` | Observability + validation infrastructure |

### Production (Semantic Versioning)

Skills graduated to `cortex-code-skills` (production repo) use semver: `v1.0.0`.

## Monitoring & Alerts

### Key Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Skill failure rate | SKILL_EXECUTION_LOG | >10% over 1 hour |
| Plan rejection rate | ORCHESTRATOR_EXECUTION_LOG | >50% (plan quality issue) |
| Governance actions/day | GOVERNANCE_AUDIT_LOG | Drop to 0 (guardrails bypassed?) |
| Preflight failures | Skill logs | >3 consecutive MISSING |
| Cortex API errors | CORTEX_FUNCTIONS_USAGE_HISTORY | >5% error rate |

### Cortex Service Cost Monitoring

```sql
SELECT
    function_name,
    model_name,
    DATE_TRUNC('day', start_time) AS day,
    SUM(tokens) AS total_tokens,
    COUNT(*) AS call_count
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY function_name, model_name, day
ORDER BY day DESC, total_tokens DESC;
```

---

*Document Version: 2.0*
*Created: March 4, 2026*
*Revised: March 25, 2026*
*Supersedes: v1.0 (LangSmith/LangGraph-based observability)*
