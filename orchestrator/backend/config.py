"""Environment configuration for the orchestrator backend.

All hardcoded database/warehouse/schema names are centralized here
with environment variable overrides.
"""

import os


SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "AGENTIC_PLATFORM")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "ORCHESTRATOR")
SNOWFLAKE_CONNECTION_NAME = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")

FHIR_SCHEMA = os.environ.get("FHIR_SCHEMA", "FHIR_DEMO")
ANALYTICS_SCHEMA = os.environ.get("ANALYTICS_SCHEMA", "ANALYTICS")
ML_SCHEMA = os.environ.get("ML_SCHEMA", "ML")
CORTEX_SCHEMA = os.environ.get("CORTEX_SCHEMA", "CORTEX")
DRUG_SAFETY_SCHEMA = os.environ.get("DRUG_SAFETY_SCHEMA", "DRUG_SAFETY")
CLINICAL_DOCS_SCHEMA = os.environ.get("CLINICAL_DOCS_SCHEMA", "CLINICAL_DOCS")
APPS_SCHEMA = os.environ.get("APPS_SCHEMA", "APPS")

CORTEX_MODEL = os.environ.get("CORTEX_MODEL", "mistral-large2")
CORTEX_AGENT_NAME = os.environ.get("CORTEX_AGENT_NAME", "HEALTH_COPILOT_AGENT")

SEMANTIC_MODEL_STAGE = os.environ.get(
    "SEMANTIC_MODEL_STAGE",
    f"@{SNOWFLAKE_DATABASE}.{CORTEX_SCHEMA}.SEMANTIC_MODELS/health_semantic_model.yaml"
)
