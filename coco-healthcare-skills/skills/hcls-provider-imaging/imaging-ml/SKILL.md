---
name: imaging-ml
description: "Train and deploy ML models for medical imaging use cases on Snowflake: imaging classification, anomaly detection, pathology models, and radiology AI using Cortex ML and Model Registry."
parent_skill: hcls-provider-imaging
---

# Medical Imaging ML Models

## When to Load

Healthcare-imaging router: After user intent matches ML.

## Prerequisites

- Imaging metadata and/or feature data in Snowflake
- Snowflake ML functions available (snowflake-ml-python)
- For deep learning: SPCS with GPU compute pools

## Workflow

### Step 1: Define ML Use Case

**Ask** user:
```
What imaging ML task do you need?
1. Imaging metadata classification (predict modality, body part, urgency)
2. Anomaly detection (flag unusual imaging patterns or metadata)
3. Report text classification (categorize radiology reports)
4. Feature extraction from imaging metadata for downstream models
5. Deep learning model deployment (pre-trained imaging model via SPCS)
```

### Step 2: Prepare Training Data

**Goal:** Create ML-ready datasets from imaging tables.

**Invoke** the `machine-learning` skill for ML workflow best practices.

```sql
CREATE OR REPLACE VIEW imaging_ml_features AS
SELECT
  study_uid,
  modality,
  body_part,
  image_rows,
  image_columns,
  bits_allocated,
  DATEDIFF('year', TRY_TO_DATE(study_date, 'YYYYMMDD'), CURRENT_DATE()) AS study_age_years,
  CASE WHEN critical_finding IS NOT NULL THEN 1 ELSE 0 END AS is_critical,
  institution
FROM dicom_studies_enriched
WHERE modality IS NOT NULL AND body_part IS NOT NULL;
```

**Train/test split:**
```sql
CREATE OR REPLACE TABLE imaging_train AS
  SELECT * FROM imaging_ml_features SAMPLE (80);
CREATE OR REPLACE TABLE imaging_test AS
  SELECT * FROM imaging_ml_features
  WHERE study_uid NOT IN (SELECT study_uid FROM imaging_train);
```

### Step 3: Train Model with Snowflake ML

**Goal:** Train classification or anomaly detection models.

**Classification (predict urgency/critical finding):**
```python
from snowflake.ml.modeling.classification import RandomForestClassifier
from snowflake.snowpark import Session

session = Session.builder.config("connection_name", "default").create()

train_df = session.table("imaging_train")
test_df = session.table("imaging_test")

model = RandomForestClassifier(
    input_cols=["IMAGE_ROWS", "IMAGE_COLUMNS", "BITS_ALLOCATED", "STUDY_AGE_YEARS"],
    label_cols=["IS_CRITICAL"],
    output_cols=["PREDICTED_CRITICAL"]
)

model.fit(train_df)
predictions = model.predict(test_df)
```

**Anomaly detection:**
```python
from snowflake.ml.modeling.anomaly_detection import IsolationForest

model = IsolationForest(
    input_cols=["IMAGE_ROWS", "IMAGE_COLUMNS", "BITS_ALLOCATED"],
    output_cols=["ANOMALY_SCORE"],
    contamination=0.05
)
model.fit(train_df)
```

### Step 4: Register Model in Snowflake ML Registry

**Goal:** Version and deploy the model.

```python
from snowflake.ml.registry import Registry

registry = Registry(session=session, database_name="IMAGING_DB", schema_name="ML")

mv = registry.log_model(
    model=model,
    model_name="imaging_critical_classifier",
    version_name="v1",
    sample_input_data=train_df.limit(10),
    comment="Predicts critical findings from imaging metadata features"
)
```

### Step 5: Deploy for SQL Inference

**Goal:** Make model available for SQL-based predictions.

```sql
SELECT
  study_uid,
  modality,
  body_part,
  IMAGING_DB.ML.imaging_critical_classifier!PREDICT(
    IMAGE_ROWS, IMAGE_COLUMNS, BITS_ALLOCATED, STUDY_AGE_YEARS
  ) AS predicted_critical
FROM dicom_studies_enriched
WHERE study_date >= '20250101';
```

### Step 6: Deep Learning via SPCS (Advanced)

**Goal:** Deploy pre-trained imaging models (e.g., CheXNet, pathology classifiers) on GPU.

**Invoke** the `deploy-to-spcs` skill.

**Pattern:**
1. Package model in Docker container with inference API
2. Push to Snowflake image repository
3. Create SPCS service with GPU compute pool
4. Expose inference endpoint

## Stopping Points

- After Step 1 to confirm ML task
- After Step 2 to validate training data quality
- After Step 4 before registering (review model metrics)
- After Step 5 before enabling SQL inference (cost review)

## Output

- ML-ready feature tables
- Trained and registered model in Snowflake ML Registry
- SQL inference function for real-time predictions
- Optional: SPCS-deployed deep learning model
