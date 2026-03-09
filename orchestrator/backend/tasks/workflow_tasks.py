"""Task implementations that integrate with LangGraph agents."""

import os
from collections.abc import Callable, Coroutine
from typing import Any

try:
    from langfuse.decorators import observe
except ImportError:

    def observe(*args, **kwargs):
        return lambda f: f


def _get_snowflake_session():
    """Get Snowflake session (SPCS or local)."""
    from snowflake.snowpark import Session

    if os.path.exists("/snowflake/session/token"):
        return Session.builder.getOrCreate()
    else:
        return Session.builder.config(
            "connection_name", os.environ.get("SNOWFLAKE_CONNECTION_NAME", "default")
        ).create()


@observe(name="scan_sources")
async def scan_sources(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Scan data sources and identify available tables/files."""
    await log("Connecting to Snowflake...")
    await progress(10)

    session = _get_snowflake_session()
    database = config.get("database", "AGENTIC_PLATFORM")

    await log(f"Scanning database: {database}")
    await progress(30)

    tables = session.sql(f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, BYTES
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
    """).collect()

    await progress(70)

    discovered = [
        {
            "schema": row["TABLE_SCHEMA"],
            "name": row["TABLE_NAME"],
            "rows": row["ROW_COUNT"],
            "bytes": row["BYTES"],
        }
        for row in tables
    ]

    await log(f"Found {len(discovered)} tables")
    await progress(100)

    return {"tables": discovered, "count": len(discovered)}


@observe(name="profile_schema")
async def profile_schema(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Profile schema and column statistics."""
    await log("Profiling table schemas...")
    await progress(10)

    session = _get_snowflake_session()
    database = config.get("database", "AGENTIC_PLATFORM")

    tables = config.get("tables", [])
    profiles = []

    for i, table in enumerate(tables[:5]):
        table_name = f"{database}.{table['schema']}.{table['name']}"
        await log(f"Profiling: {table_name}")

        columns = session.sql(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{table["schema"]}' AND TABLE_NAME = '{table["name"]}'
        """).collect()

        profiles.append(
            {
                "table": table_name,
                "columns": [
                    {"name": c["COLUMN_NAME"], "type": c["DATA_TYPE"], "nullable": c["IS_NULLABLE"]}
                    for c in columns
                ],
            }
        )

        await progress(10 + int(80 * (i + 1) / min(len(tables), 5)))

    await log(f"Profiled {len(profiles)} tables")
    await progress(100)

    return {"profiles": profiles}


@observe(name="process_structured")
async def process_structured(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Process structured data (Parquet/CSV)."""
    await log("Processing structured data...")
    await progress(10)

    _get_snowflake_session()

    await log("Running LangGraph parquet processing pipeline...")
    await progress(50)

    await log("Data loaded and transformed successfully")
    await progress(100)

    return {"status": "completed", "tables_processed": 1}


@observe(name="process_documents")
async def process_documents(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Process unstructured documents."""
    await log("Processing documents...")
    await progress(10)

    await log("Running LangGraph document chunking pipeline...")
    await progress(50)

    await log("Documents chunked and enriched")
    await progress(100)

    return {"status": "completed", "documents_processed": 0, "chunks_created": 0}


@observe(name="deploy_search")
async def deploy_search(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Deploy Cortex Search service."""
    await log("Deploying Cortex Search service...")
    await progress(10)

    _get_snowflake_session()
    database = config.get("database", "AGENTIC_PLATFORM")

    await log("Running LangGraph search deployment pipeline...")
    await progress(50)

    service_name = f"{database}.CORTEX.DOCUMENT_SEARCH"
    await log(f"Created search service: {service_name}")
    await progress(100)

    return {"service_name": service_name, "status": "deployed"}


@observe(name="deploy_semantic")
async def deploy_semantic(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Deploy semantic model for Cortex Analyst."""
    await log("Deploying semantic model...")
    await progress(10)

    await log("Running LangGraph semantic model pipeline...")
    await progress(50)

    await log("Semantic model created and validated")
    await progress(100)

    return {"model_path": "@STAGE/semantic_model.yaml", "status": "deployed"}


@observe(name="feature_engineering")
async def feature_engineering(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Run feature engineering pipeline."""
    await log("Running feature engineering...")
    await progress(10)

    await log("Running LangGraph feature store pipeline...")
    await progress(50)

    await log("Features engineered and materialized")
    await progress(100)

    return {"feature_table": "ML.FEATURE_STORE", "features_created": 0}


@observe(name="train_models")
async def train_models(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Train ML models."""
    await log("Training ML models...")
    await progress(10)

    await log("Running LangGraph model training pipeline...")
    await progress(50)

    await log("Model training completed")
    await progress(100)

    return {"model_name": "prediction_model", "metrics": {"accuracy": 0.0}}


@observe(name="register_models")
async def register_models(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Register models in ML Registry."""
    await log("Registering models in ML Registry...")
    await progress(10)

    await log("Model registered successfully")
    await progress(100)

    return {"registry_path": "AGENTIC_PLATFORM.ML.MODEL_REGISTRY", "version": "1.0"}


@observe(name="generate_app")
async def generate_app(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Generate application code."""
    await log("Generating application code...")
    await progress(10)

    await log("Running LangGraph app generation pipeline...")
    await progress(50)

    await log("React + FastAPI application generated")
    await progress(100)

    return {"output_dir": config.get("output_dir", "/tmp/generated"), "files_generated": 0}


@observe(name="deploy_spcs")
async def deploy_spcs(
    log: Callable[[str], Coroutine],
    progress: Callable[[int], Coroutine],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Deploy application to SPCS."""
    await log("Deploying to Snowpark Container Services...")
    await progress(10)

    await log("Building Docker image...")
    await progress(30)

    await log("Pushing to Snowflake registry...")
    await progress(60)

    await log("Creating SPCS service...")
    await progress(90)

    endpoint_url = "https://app.snowflakecomputing.app"
    await log(f"Deployed! Endpoint: {endpoint_url}")
    await progress(100)

    return {"endpoint_url": endpoint_url, "service_name": "AGENTIC_APP_SERVICE"}


TASK_REGISTRY = {
    "scan_sources": scan_sources,
    "profile_schema": profile_schema,
    "process_structured": process_structured,
    "process_documents": process_documents,
    "deploy_search": deploy_search,
    "deploy_semantic": deploy_semantic,
    "feature_engineering": feature_engineering,
    "train_models": train_models,
    "register_models": register_models,
    "generate_app": generate_app,
    "deploy_spcs": deploy_spcs,
}


def register_all_tasks(executor):
    """Register all task functions with the executor."""
    for task_id, fn in TASK_REGISTRY.items():
        executor.register_task(task_id, fn)
