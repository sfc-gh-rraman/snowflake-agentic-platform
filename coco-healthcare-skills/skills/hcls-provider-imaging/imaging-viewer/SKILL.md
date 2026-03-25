---
name: imaging-viewer
description: "Build DICOM imaging viewer and dashboard applications using Streamlit in Snowflake and SPCS for compute-heavy rendering."
parent_skill: hcls-provider-imaging
---

# Medical Imaging Viewer & Dashboard

## When to Load

Healthcare-imaging router: After user intent matches VIEWER.

## Prerequisites

- DICOM metadata tables exist in Snowflake (run `dicom-ingestion` first if needed)
- Streamlit in Snowflake enabled on the account
- For DICOM pixel rendering: SPCS (Snowpark Container Services) available

## Workflow

### Step 1: Determine Viewer Requirements

**Ask** user:
```
What type of imaging application do you need?
1. Metadata dashboard (study volumes, modality charts, turnaround metrics)
2. Study browser (search and browse imaging studies with metadata)
3. DICOM pixel viewer (render actual images via SPCS)
4. Radiology report viewer with NLP insights
5. Combination of the above
```

### Step 2: Build Streamlit Metadata Dashboard

**Goal:** Create a Streamlit app for imaging analytics.

**Key components:**
- Study volume trends (line chart by date)
- Modality distribution (bar chart)
- Institution breakdown
- Patient demographics
- Search/filter by date range, modality, body part

**Invoke** the `developing-with-streamlit` skill for Streamlit best practices.

**Core app pattern:**
```python
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Medical Imaging Dashboard")

col1, col2 = st.columns(2)
with col1:
    modality = st.multiselect("Modality", ["CT", "MR", "XR", "US", "MG", "PT"])
with col2:
    date_range = st.date_input("Study Date Range", [])

query = """
SELECT study_day, modality, study_count, patient_count
FROM imaging_study_metrics
WHERE 1=1
"""
if modality:
    query += f" AND modality IN ({','.join([repr(m) for m in modality])})"

df = session.sql(query).to_pandas()
st.line_chart(df, x="STUDY_DAY", y="STUDY_COUNT", color="MODALITY")
```

### Step 3: Build Study Browser

**Goal:** Interactive study search and detail view.

**Components:**
- Search bar with Cortex Search integration
- Study list with sortable columns
- Study detail panel (metadata, series list, report)
- Link to DICOM viewer for pixel rendering (if SPCS available)

### Step 4: DICOM Pixel Viewer via SPCS (Advanced)

**Goal:** Deploy a containerized DICOM viewer for actual image rendering.

**This requires SPCS and involves:**
1. Container image with a DICOM rendering library (e.g., Cornerstone.js, OHIF Viewer)
2. SPCS service specification
3. Ingress endpoint for the viewer

**Invoke** the `deploy-to-spcs` skill for container deployment.

**Service spec pattern:**
```yaml
spec:
  containers:
    - name: dicom-viewer
      image: /db/schema/repo/dicom-viewer:latest
      resources:
        requests:
          memory: 4Gi
          cpu: 2
      env:
        SNOWFLAKE_WAREHOUSE: imaging_wh
  endpoints:
    - name: viewer
      port: 8080
      public: true
```

### Step 5: Deploy and Test

**Actions:**
1. Deploy Streamlit app to Snowflake
2. Verify data connectivity
3. Test filters and visualizations
4. Validate role-based access (only authorized roles see PHI)

## Stopping Points

- After Step 1 to confirm scope
- After Step 2 before deploying (review app code)
- After Step 4 before SPCS deployment (cost/infra approval)

## Output

- Streamlit imaging dashboard deployed to Snowflake
- Optional: SPCS-based DICOM pixel viewer
- Role-based access configured
