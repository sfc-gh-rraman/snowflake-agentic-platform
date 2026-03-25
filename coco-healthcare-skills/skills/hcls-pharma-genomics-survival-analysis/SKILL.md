---
name: hcls-pharma-genomics-survival-analysis
description: Perform survival analysis for clinical outcomes including Kaplan-Meier curves, Cox proportional hazards regression, and time-to-event modeling. Use when analyzing patient survival, time to disease progression, treatment duration, or any censored time-to-event data. Triggers include survival analysis, Kaplan-Meier, KM curve, Cox regression, hazard ratio, time-to-event, censoring, progression-free survival, overall survival, PFS, OS.
---

# Survival Analysis

Perform survival analysis for clinical outcomes using Kaplan-Meier estimation and Cox proportional hazards regression.

## When to Use This Skill

- Analyzing overall survival (OS) or progression-free survival (PFS)
- Comparing survival between treatment groups
- Estimating hazard ratios for risk factors
- Creating publication-ready survival curves
- Time-to-event analysis with censored data

## Key Concepts

### Survival Data Structure

| Column | Description | Example |
|--------|-------------|---------|
| `time` | Time to event or censoring | 365 (days) |
| `event` | Event occurred (1) or censored (0) | 1 = death, 0 = alive at last follow-up |
| `group` | Treatment or comparison group | "Treatment A", "Control" |
| `covariates` | Risk factors for Cox regression | age, stage, biomarker |

### Censoring

- **Right censoring**: Patient lost to follow-up or study ends before event
- **Event = 0**: Patient was censored (didn't experience event during observation)
- **Event = 1**: Patient experienced the event (death, progression, etc.)

## Quick Start

### Kaplan-Meier Survival Curves

```python
python scripts/survival_analysis.py \
    --input clinical_data.csv \
    --time-col survival_days \
    --event-col death \
    --group-col treatment_arm \
    --output km_plot.png
```

### Cox Proportional Hazards

```python
python scripts/cox_regression.py \
    --input clinical_data.csv \
    --time-col survival_days \
    --event-col death \
    --covariates "age,stage,treatment" \
    --output cox_results.csv
```

## Kaplan-Meier Analysis

### Basic KM Curve

```python
import pandas as pd
from lifelines import KaplanMeierFitter
import matplotlib.pyplot as plt

df = pd.read_csv('clinical_data.csv')

kmf = KaplanMeierFitter()
kmf.fit(df['time'], df['event'], label='Overall Survival')

kmf.plot_survival_function()
plt.xlabel('Time (days)')
plt.ylabel('Survival Probability')
plt.title('Kaplan-Meier Survival Curve')
plt.savefig('km_curve.png', dpi=300, bbox_inches='tight')
```

### Comparing Groups (Log-Rank Test)

```python
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test

fig, ax = plt.subplots(figsize=(10, 6))

for group in df['treatment'].unique():
    mask = df['treatment'] == group
    kmf = KaplanMeierFitter()
    kmf.fit(df.loc[mask, 'time'], df.loc[mask, 'event'], label=group)
    kmf.plot_survival_function(ax=ax)

# Log-rank test
group_a = df[df['treatment'] == 'Treatment A']
group_b = df[df['treatment'] == 'Treatment B']

results = logrank_test(
    group_a['time'], group_b['time'],
    group_a['event'], group_b['event']
)
print(f"Log-rank p-value: {results.p_value:.4f}")
```

### Median Survival & Confidence Intervals

```python
kmf.fit(df['time'], df['event'])

print(f"Median survival: {kmf.median_survival_time_:.1f} days")
print(f"95% CI: {kmf.confidence_interval_median_survival_time_}")

# Survival at specific timepoints
print(f"1-year survival: {kmf.predict(365):.1%}")
print(f"5-year survival: {kmf.predict(1825):.1%}")
```

## Cox Proportional Hazards Regression

### Basic Cox Model

```python
from lifelines import CoxPHFitter

cph = CoxPHFitter()
cph.fit(df, duration_col='time', event_col='event')

cph.print_summary()
cph.plot()
```

### Multivariable Cox Model

```python
# Select columns for model
cols = ['time', 'event', 'age', 'stage', 'treatment', 'biomarker']
df_model = df[cols].dropna()

# Encode categorical variables
df_model = pd.get_dummies(df_model, columns=['stage', 'treatment'], drop_first=True)

cph = CoxPHFitter()
cph.fit(df_model, duration_col='time', event_col='event')

# Results
print(cph.summary[['coef', 'exp(coef)', 'p', 'exp(coef) lower 95%', 'exp(coef) upper 95%']])
```

### Hazard Ratio Interpretation

| HR | Interpretation |
|----|----------------|
| HR = 1.0 | No effect |
| HR > 1.0 | Increased risk (e.g., HR=2.0 means 2x risk) |
| HR < 1.0 | Decreased risk (e.g., HR=0.5 means 50% reduction) |

### Forest Plot

```python
from lifelines.plotting import plot_covariate_groups

fig, ax = plt.subplots(figsize=(8, 6))
cph.plot(ax=ax)
plt.title('Cox Regression - Hazard Ratios')
plt.savefig('forest_plot.png', dpi=300, bbox_inches='tight')
```

## Snowflake SQL for Survival Data Prep

```sql
-- Prepare survival data from clinical tables
WITH patient_outcomes AS (
    SELECT 
        p.patient_id,
        p.treatment_arm,
        p.enrollment_date,
        p.age_at_enrollment,
        p.disease_stage,
        d.death_date,
        COALESCE(d.death_date, CURRENT_DATE) AS end_date,
        CASE WHEN d.death_date IS NOT NULL THEN 1 ELSE 0 END AS death_event
    FROM patients p
    LEFT JOIN deaths d ON p.patient_id = d.patient_id
)
SELECT 
    patient_id,
    treatment_arm,
    age_at_enrollment,
    disease_stage,
    DATEDIFF('day', enrollment_date, end_date) AS survival_days,
    death_event
FROM patient_outcomes;
```

## Clinical Endpoints

### Overall Survival (OS)

- **Event**: Death from any cause
- **Censoring**: Alive at last follow-up, lost to follow-up

### Progression-Free Survival (PFS)

- **Event**: Disease progression OR death
- **Censoring**: No progression and alive at last assessment

### Time to Treatment Failure (TTF)

- **Event**: Progression, death, OR treatment discontinuation
- **Censoring**: Still on treatment without progression

## Model Diagnostics

### Check Proportional Hazards Assumption

```python
# Schoenfeld residuals test
cph.check_assumptions(df_model, p_value_threshold=0.05, show_plots=True)
```

### Concordance Index (C-index)

```python
# Model discrimination (0.5 = random, 1.0 = perfect)
print(f"C-index: {cph.concordance_index_:.3f}")
```

## Publication-Ready Plots

### KM Plot with Risk Table

```python
from lifelines.plotting import add_at_risk_counts

fig, ax = plt.subplots(figsize=(10, 8))

kmf_list = []
for group in ['Treatment', 'Control']:
    mask = df['arm'] == group
    kmf = KaplanMeierFitter()
    kmf.fit(df.loc[mask, 'time'], df.loc[mask, 'event'], label=group)
    kmf.plot_survival_function(ax=ax, ci_show=True)
    kmf_list.append(kmf)

add_at_risk_counts(*kmf_list, ax=ax)
plt.xlabel('Time (months)')
plt.ylabel('Survival Probability')
plt.title('Overall Survival by Treatment Arm')
plt.savefig('km_with_risk_table.png', dpi=300, bbox_inches='tight')
```

## Reference Files

- `references/survival_endpoints.md` - Clinical endpoint definitions
- `references/cox_assumptions.md` - Checking PH assumptions
- `references/sample_size.md` - Power calculations for survival studies

## Requirements

```
lifelines>=0.27.0
pandas>=2.0.0
matplotlib>=3.7.0
numpy>=1.24.0
```
