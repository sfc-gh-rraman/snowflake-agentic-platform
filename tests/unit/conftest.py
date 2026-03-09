"""Pytest fixtures for unit tests."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_snowflake_session():
    """Mock Snowflake session for unit tests."""
    session = MagicMock()
    session.sql.return_value.collect.return_value = []
    return session


@pytest.fixture
def mock_snowflake_connector():
    """Mock snowflake.connector for unit tests."""
    with patch("snowflake.connector.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("COL1",), ("COL2",)]
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_connect


@pytest.fixture
def mock_cortex_complete():
    """Mock Cortex COMPLETE function responses."""

    def _mock_complete(response: str):
        with patch("snowflake.connector.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (response,)
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            return mock_connect

    return _mock_complete


@pytest.fixture
def sample_parquet_state() -> dict[str, Any]:
    """Sample state for Parquet processor tests."""
    return {
        "stage_path": "@RAW.DATA_STAGE/sensors/",
        "target_schema": "CURATED",
        "database": "AGENTIC_PLATFORM",
        "files": [],
        "schemas": {},
        "profiles": {},
        "quality_issues": {},
        "column_mappings": {},
        "tables_created": [],
        "current_state": "SCAN",
        "errors": [],
        "messages": [],
    }


@pytest.fixture
def sample_document_state() -> dict[str, Any]:
    """Sample state for Document chunker tests."""
    return {
        "stage_path": "@RAW.DOCS_STAGE/reports/",
        "target_table": "DOC_CHUNKS",
        "database": "AGENTIC_PLATFORM",
        "schema": "DOCS",
        "documents": [],
        "structures": {},
        "chunks": [],
        "metadata": {},
        "chunk_table": None,
        "current_state": "EXTRACT",
        "errors": [],
        "messages": [],
    }


@pytest.fixture
def sample_ml_state() -> dict[str, Any]:
    """Sample state for ML model builder tests."""
    return {
        "source_table": "CURATED.SENSOR_DATA",
        "target_column": "FAILURE",
        "database": "AGENTIC_PLATFORM",
        "schema": "ML",
        "task_type": None,
        "features": [],
        "model_artifact": None,
        "evaluation_metrics": {},
        "registered_model": None,
        "explanations": {},
        "current_state": "TASK_CLASSIFICATION",
        "errors": [],
        "messages": [],
    }


@pytest.fixture
def sample_execution_plan() -> dict[str, Any]:
    """Sample execution plan for meta-agent tests."""
    return {
        "use_case": "Equipment Failure Prediction",
        "total_phases": 5,
        "phases": [
            {
                "phase": "infrastructure",
                "name": "Create Snowflake Infrastructure",
                "steps": [
                    {"action": "create_database", "target": "AGENTIC_PLATFORM"},
                    {"action": "create_schemas", "schemas": ["RAW", "CURATED", "ML"]},
                ],
            },
            {
                "phase": "preprocessing",
                "name": "Process Data",
                "steps": [
                    {"action": "process_parquet", "source": "@RAW.DATA_STAGE"},
                ],
            },
            {
                "phase": "ml_models",
                "name": "Train ML Models",
                "steps": [
                    {
                        "action": "train_model",
                        "model": "anomaly_detector",
                        "task": "anomaly_detection",
                    },
                ],
            },
            {
                "phase": "app_generation",
                "name": "Generate Application",
                "steps": [
                    {"action": "generate_backend", "framework": "fastapi"},
                    {"action": "generate_frontend", "framework": "react"},
                ],
            },
            {
                "phase": "deployment",
                "name": "Deploy to SPCS",
                "steps": [
                    {"action": "build_docker_image"},
                    {"action": "create_service"},
                ],
            },
        ],
    }


@pytest.fixture
def sample_agent_registry_entries() -> list[dict[str, Any]]:
    """Sample agent registry entries for tests."""
    return [
        {
            "AGENT_ID": "parquet_processor",
            "NAME": "Parquet Processor",
            "VERSION": "1.0.0",
            "CATEGORY": "preprocessing",
            "CAPABILITIES": ["ingest_parquet", "schema_inference", "data_profiling"],
        },
        {
            "AGENT_ID": "document_chunker",
            "NAME": "Document Chunker",
            "VERSION": "1.0.0",
            "CATEGORY": "preprocessing",
            "CAPABILITIES": ["pdf_extraction", "text_chunking", "metadata_extraction"],
        },
        {
            "AGENT_ID": "ml_model_builder",
            "NAME": "ML Model Builder",
            "VERSION": "1.0.0",
            "CATEGORY": "ml",
            "CAPABILITIES": ["classification", "regression", "anomaly_detection"],
        },
    ]
