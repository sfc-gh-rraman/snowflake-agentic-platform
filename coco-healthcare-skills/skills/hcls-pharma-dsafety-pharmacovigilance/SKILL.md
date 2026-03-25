---
name: hcls-pharma-dsafety-pharmacovigilance
description: Analyze FDA FAERS adverse event data for drug safety signal detection. Use when investigating adverse drug reactions, detecting safety signals, analyzing drug-event associations, or building pharmacovigilance dashboards. Triggers include FAERS, adverse events, drug safety, pharmacovigilance, ADR, signal detection, MedDRA, drug reactions, safety surveillance.
---

# Pharmacovigilance & FAERS Analysis

Analyze FDA Adverse Event Reporting System (FAERS) data for drug safety surveillance and signal detection.

## When to Use This Skill

- Analyzing FDA FAERS quarterly data files
- Detecting drug safety signals
- Investigating specific drug-adverse event associations
- Building pharmacovigilance dashboards
- Calculating disproportionality metrics (PRR, ROR, EBGM)

## FAERS Data Overview

FDA FAERS contains spontaneous adverse event reports from healthcare providers, consumers, and manufacturers.

### FAERS File Structure

| File | Description | Key Fields |
|------|-------------|------------|
| `DEMO` | Demographics | primaryid, caseid, age, sex, reporter_country |
| `DRUG` | Drug information | primaryid, drugname, prod_ai, role_cod (PS/SS/C/I) |
| `REAC` | Reactions (MedDRA PT) | primaryid, pt (preferred term) |
| `OUTC` | Outcomes | primaryid, outc_cod (DE, LT, HO, DS, CA, RI, OT) |
| `RPSR` | Report sources | primaryid, rpsr_cod |
| `THER` | Therapy dates | primaryid, start_dt, end_dt |
| `INDI` | Indications | primaryid, indi_pt |

### Drug Role Codes

| Code | Meaning |
|------|---------|
| PS | Primary Suspect |
| SS | Secondary Suspect |
| C | Concomitant |
| I | Interacting |

### Outcome Codes

| Code | Meaning |
|------|---------|
| DE | Death |
| LT | Life-Threatening |
| HO | Hospitalization |
| DS | Disability |
| CA | Congenital Anomaly |
| RI | Required Intervention |
| OT | Other Serious |

## Quick Start

### Download FAERS Data

```bash
# Download quarterly ASCII files from FDA
# https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html

wget https://fis.fda.gov/content/Exports/faers_ascii_2024q1.zip
unzip faers_ascii_2024q1.zip
```

### Load to Snowflake

```sql
-- Create FAERS schema
CREATE SCHEMA IF NOT EXISTS FAERS;

-- Create stage
CREATE OR REPLACE STAGE faers_stage;
PUT file://./faers_ascii_2024q1/*.txt @faers_stage;

-- Load DEMO table
CREATE OR REPLACE TABLE FAERS.DEMO (
    primaryid VARCHAR,
    caseid VARCHAR,
    caseversion VARCHAR,
    i_f_code VARCHAR,
    event_dt VARCHAR,
    mfr_dt VARCHAR,
    init_fda_dt VARCHAR,
    fda_dt VARCHAR,
    rept_cod VARCHAR,
    auth_num VARCHAR,
    mfr_num VARCHAR,
    mfr_sndr VARCHAR,
    lit_ref VARCHAR,
    age FLOAT,
    age_cod VARCHAR,
    age_grp VARCHAR,
    sex VARCHAR,
    e_sub VARCHAR,
    wt FLOAT,
    wt_cod VARCHAR,
    rept_dt VARCHAR,
    to_mfr VARCHAR,
    occp_cod VARCHAR,
    reporter_country VARCHAR,
    occr_country VARCHAR
);

COPY INTO FAERS.DEMO FROM @faers_stage/DEMO
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '$' SKIP_HEADER = 1);

-- Load DRUG table
CREATE OR REPLACE TABLE FAERS.DRUG (
    primaryid VARCHAR,
    caseid VARCHAR,
    drug_seq VARCHAR,
    role_cod VARCHAR,
    drugname VARCHAR,
    prod_ai VARCHAR,
    val_vbm VARCHAR,
    route VARCHAR,
    dose_vbm VARCHAR,
    cum_dose_chr VARCHAR,
    cum_dose_unit VARCHAR,
    dechal VARCHAR,
    rechal VARCHAR,
    lot_num VARCHAR,
    exp_dt VARCHAR,
    nda_num VARCHAR,
    dose_amt VARCHAR,
    dose_unit VARCHAR,
    dose_form VARCHAR,
    dose_freq VARCHAR
);

COPY INTO FAERS.DRUG FROM @faers_stage/DRUG
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '$' SKIP_HEADER = 1);

-- Load REAC table
CREATE OR REPLACE TABLE FAERS.REAC (
    primaryid VARCHAR,
    caseid VARCHAR,
    pt VARCHAR,
    drug_rec_act VARCHAR
);

COPY INTO FAERS.REAC FROM @faers_stage/REAC
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '$' SKIP_HEADER = 1);

-- Load OUTC table
CREATE OR REPLACE TABLE FAERS.OUTC (
    primaryid VARCHAR,
    caseid VARCHAR,
    outc_cod VARCHAR
);

COPY INTO FAERS.OUTC FROM @faers_stage/OUTC
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '$' SKIP_HEADER = 1);
```

## Signal Detection Methods

### Proportional Reporting Ratio (PRR)

```sql
-- Calculate PRR for a drug-event combination
WITH drug_event_counts AS (
    SELECT
        d.drugname,
        r.pt AS reaction,
        COUNT(DISTINCT d.primaryid) AS de_count
    FROM FAERS.DRUG d
    JOIN FAERS.REAC r ON d.primaryid = r.primaryid
    WHERE d.role_cod = 'PS'
    GROUP BY d.drugname, r.pt
),
drug_totals AS (
    SELECT drugname, COUNT(DISTINCT primaryid) AS drug_total
    FROM FAERS.DRUG WHERE role_cod = 'PS'
    GROUP BY drugname
),
event_totals AS (
    SELECT pt AS reaction, COUNT(DISTINCT primaryid) AS event_total
    FROM FAERS.REAC
    GROUP BY pt
),
total_reports AS (
    SELECT COUNT(DISTINCT primaryid) AS total FROM FAERS.DRUG WHERE role_cod = 'PS'
)
SELECT 
    dec.drugname,
    dec.reaction,
    dec.de_count,
    dt.drug_total,
    et.event_total,
    tr.total,
    -- PRR = (a/b) / (c/d)
    -- a = reports with drug AND event
    -- b = reports with drug
    -- c = reports with event (without drug)
    -- d = total reports (without drug)
    (dec.de_count::FLOAT / dt.drug_total) / 
    ((et.event_total - dec.de_count)::FLOAT / (tr.total - dt.drug_total)) AS prr,
    -- Chi-square for significance
    POWER(dec.de_count - (dt.drug_total * et.event_total / tr.total), 2) /
    (dt.drug_total * et.event_total / tr.total) AS chi_square
FROM drug_event_counts dec
JOIN drug_totals dt ON dec.drugname = dt.drugname
JOIN event_totals et ON dec.reaction = et.reaction
CROSS JOIN total_reports tr
WHERE dec.de_count >= 3  -- Minimum case threshold
ORDER BY prr DESC;
```

### Reporting Odds Ratio (ROR)

```sql
-- ROR = (a/c) / (b/d) = ad/bc
SELECT 
    drugname,
    reaction,
    de_count AS a,
    (drug_total - de_count) AS b,
    (event_total - de_count) AS c,
    (total - drug_total - event_total + de_count) AS d,
    (de_count * (total - drug_total - event_total + de_count))::FLOAT /
    ((drug_total - de_count) * (event_total - de_count)) AS ror,
    -- 95% CI
    EXP(LN((de_count * (total - drug_total - event_total + de_count))::FLOAT /
        ((drug_total - de_count) * (event_total - de_count))) - 
        1.96 * SQRT(1.0/de_count + 1.0/(drug_total - de_count) + 
                    1.0/(event_total - de_count) + 
                    1.0/(total - drug_total - event_total + de_count))) AS ror_lower,
    EXP(LN((de_count * (total - drug_total - event_total + de_count))::FLOAT /
        ((drug_total - de_count) * (event_total - de_count))) + 
        1.96 * SQRT(1.0/de_count + 1.0/(drug_total - de_count) + 
                    1.0/(event_total - de_count) + 
                    1.0/(total - drug_total - event_total + de_count))) AS ror_upper
FROM drug_event_summary
WHERE de_count >= 3;
```

### Signal Thresholds

| Metric | Signal Threshold | Strong Signal |
|--------|------------------|---------------|
| PRR | ≥ 2 | ≥ 5 |
| ROR | Lower 95% CI > 1 | Lower 95% CI > 2 |
| Chi-square | ≥ 4 | ≥ 10 |
| Case count | ≥ 3 | ≥ 5 |

## Common Analyses

### Top Adverse Events for a Drug

```sql
SELECT 
    r.pt AS adverse_event,
    COUNT(DISTINCT d.primaryid) AS report_count,
    COUNT(DISTINCT CASE WHEN o.outc_cod = 'DE' THEN d.primaryid END) AS death_count,
    COUNT(DISTINCT CASE WHEN o.outc_cod IN ('DE', 'LT', 'HO') THEN d.primaryid END) AS serious_count
FROM FAERS.DRUG d
JOIN FAERS.REAC r ON d.primaryid = r.primaryid
LEFT JOIN FAERS.OUTC o ON d.primaryid = o.primaryid
WHERE UPPER(d.drugname) LIKE '%METFORMIN%'
  AND d.role_cod = 'PS'
GROUP BY r.pt
ORDER BY report_count DESC
LIMIT 20;
```

### Drugs Associated with Specific Event

```sql
SELECT 
    d.drugname,
    COUNT(DISTINCT d.primaryid) AS report_count
FROM FAERS.DRUG d
JOIN FAERS.REAC r ON d.primaryid = r.primaryid
WHERE r.pt = 'Rhabdomyolysis'
  AND d.role_cod = 'PS'
GROUP BY d.drugname
ORDER BY report_count DESC
LIMIT 20;
```

### Time Trend Analysis

```sql
SELECT 
    SUBSTRING(demo.fda_dt, 1, 4) AS year,
    COUNT(DISTINCT d.primaryid) AS report_count
FROM FAERS.DRUG d
JOIN FAERS.DEMO demo ON d.primaryid = demo.primaryid
WHERE UPPER(d.drugname) LIKE '%OZEMPIC%'
  AND d.role_cod = 'PS'
  AND demo.fda_dt IS NOT NULL
GROUP BY SUBSTRING(demo.fda_dt, 1, 4)
ORDER BY year;
```

## MedDRA Hierarchy

FAERS uses MedDRA (Medical Dictionary for Regulatory Activities) for coding adverse events:

```
System Organ Class (SOC)
  └── High Level Group Term (HLGT)
      └── High Level Term (HLT)
          └── Preferred Term (PT)  ← FAERS REAC.pt
              └── Lowest Level Term (LLT)
```

To aggregate by SOC, you need the MedDRA hierarchy file (licensed separately).

## Best Practices

1. **Deduplicate cases**: Use most recent `caseversion` per `caseid`
2. **Focus on primary suspect**: Filter `role_cod = 'PS'` for signal detection
3. **Normalize drug names**: Use `prod_ai` (active ingredient) when possible
4. **Consider reporting bias**: Newer drugs have more reports (Weber effect)
5. **Validate signals**: Signals require clinical validation before action

## Reference Files

- `references/faers_schema.md` - Complete FAERS schema
- `references/meddra_mapping.md` - MedDRA hierarchy guide
- `references/signal_detection.md` - Detailed signal detection methods

## Requirements

```
pandas>=2.0.0
snowflake-connector-python>=3.0.0
scipy>=1.10.0  # For statistical tests
```

## Evidence Grounding: PubMed CKE

Invoke `$cke-pubmed` when evidence grounding adds value to safety signal analysis:

- After signal detection (PRR/ROR > 2), search for published drug-event associations and mechanism evidence
- Cross-reference disproportionality findings with case reports and clinical studies
- Literature-based validation of signal detection results

See `$cke-pubmed` for setup, query patterns, and the full FAERS signal enrichment SQL pattern.
