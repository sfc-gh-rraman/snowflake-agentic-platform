"""Seed initial agent definitions into the registry."""

from .models import (
    AgentCapability,
    AgentCategory,
    AgentDefinition,
    AgentDependency,
    AgentTrigger,
    DependencyRelationship,
    StateMachine,
)
from .registry_query import AgentRegistryQuery


def get_parquet_processor_agent() -> AgentDefinition:
    return AgentDefinition(
        agent_id="parquet_processor",
        name="Parquet Processor",
        version="1.0.0",
        description="Processes parquet files from Snowflake stages into curated tables. "
        "Handles schema inference, profiling, quality checks, transformations, and loading.",
        category=AgentCategory.PREPROCESSING,
        capabilities=[
            AgentCapability(
                capability_id="ingest_parquet",
                name="Ingest Parquet Files",
                description="Load parquet files from stage into Snowflake tables with automatic schema inference",
                input_types=["parquet", "stage"],
                output_types=["table"],
                constraints={"max_file_size_gb": 10},
            ),
            AgentCapability(
                capability_id="profile_parquet",
                name="Profile Parquet Data",
                description="Generate statistics and data profile for parquet files",
                input_types=["parquet"],
                output_types=["data_profile"],
            ),
        ],
        triggers=[
            AgentTrigger(
                condition="input_type == 'parquet'",
                priority=90,
                description="Triggered when parquet files are discovered",
            ),
        ],
        state_machine=StateMachine(
            states=["SCAN", "SCHEMA_INFER", "PROFILE", "QUALITY_CHECK", "TRANSFORM", "LOAD"],
            initial_state="SCAN",
            transitions=[
                {"from": "SCAN", "to": "SCHEMA_INFER"},
                {"from": "SCHEMA_INFER", "to": "PROFILE"},
                {"from": "PROFILE", "to": "QUALITY_CHECK"},
                {"from": "QUALITY_CHECK", "to": "TRANSFORM"},
                {"from": "TRANSFORM", "to": "LOAD"},
            ],
        ),
    )


def get_document_chunker_agent() -> AgentDefinition:
    return AgentDefinition(
        agent_id="document_chunker",
        name="Document Chunker",
        version="1.0.0",
        description="Extracts text from documents (PDF, TXT, DOCX), analyzes structure, "
        "creates semantic chunks, and loads them for RAG/Cortex Search.",
        category=AgentCategory.PREPROCESSING,
        capabilities=[
            AgentCapability(
                capability_id="extract_pdf",
                name="Extract PDF Text",
                description="Extract text content from PDF documents",
                input_types=["pdf"],
                output_types=["text", "chunks"],
            ),
            AgentCapability(
                capability_id="chunk_documents",
                name="Chunk Documents",
                description="Split documents into semantic chunks suitable for embedding",
                input_types=["text", "pdf", "docx"],
                output_types=["chunks", "chunk_table"],
                constraints={"max_chunk_size": 16000},
            ),
        ],
        triggers=[
            AgentTrigger(
                condition="input_type in ['pdf', 'docx', 'txt']",
                priority=85,
                description="Triggered when document files are discovered",
            ),
        ],
        state_machine=StateMachine(
            states=["EXTRACT", "ANALYZE_STRUCTURE", "CHUNK", "ENRICH_METADATA", "LOAD_CHUNKS"],
            initial_state="EXTRACT",
            transitions=[
                {"from": "EXTRACT", "to": "ANALYZE_STRUCTURE"},
                {"from": "ANALYZE_STRUCTURE", "to": "CHUNK"},
                {"from": "CHUNK", "to": "ENRICH_METADATA"},
                {"from": "ENRICH_METADATA", "to": "LOAD_CHUNKS"},
            ],
        ),
    )


def get_ml_model_builder_agent() -> AgentDefinition:
    return AgentDefinition(
        agent_id="ml_model_builder",
        name="ML Model Builder",
        version="1.0.0",
        description="Automatically builds and registers ML models using Snowpark ML. "
        "Handles task classification, feature selection, training, evaluation, "
        "registry, and explainability.",
        category=AgentCategory.ML,
        capabilities=[
            AgentCapability(
                capability_id="train_classifier",
                name="Train Classification Model",
                description="Train a classification model on labeled data",
                input_types=["table", "features"],
                output_types=["ml_model", "ml_model_version"],
            ),
            AgentCapability(
                capability_id="train_regressor",
                name="Train Regression Model",
                description="Train a regression model for continuous target prediction",
                input_types=["table", "features"],
                output_types=["ml_model", "ml_model_version"],
            ),
            AgentCapability(
                capability_id="explain_model",
                name="Generate Model Explanations",
                description="Generate SHAP explanations for model predictions",
                input_types=["ml_model"],
                output_types=["shap_values", "feature_importance"],
            ),
        ],
        dependencies=[
            AgentDependency(
                agent_id="parquet_processor",
                relationship=DependencyRelationship.OPTIONAL,
                description="May need data from parquet processor",
            ),
        ],
        triggers=[
            AgentTrigger(
                condition="task_type in ['classification', 'regression'] and has_labeled_data",
                priority=70,
                description="Triggered when ML training is needed",
            ),
        ],
        state_machine=StateMachine(
            states=[
                "TASK_CLASSIFICATION",
                "FEATURE_SELECTION",
                "TRAINING",
                "EVALUATION",
                "REGISTRATION",
                "EXPLAINABILITY",
            ],
            initial_state="TASK_CLASSIFICATION",
            transitions=[
                {"from": "TASK_CLASSIFICATION", "to": "FEATURE_SELECTION"},
                {"from": "FEATURE_SELECTION", "to": "TRAINING"},
                {"from": "TRAINING", "to": "EVALUATION"},
                {"from": "EVALUATION", "to": "REGISTRATION"},
                {"from": "REGISTRATION", "to": "EXPLAINABILITY"},
            ],
        ),
    )


def get_app_code_generator_agent() -> AgentDefinition:
    return AgentDefinition(
        agent_id="app_code_generator",
        name="App Code Generator",
        version="1.0.0",
        description="Generates complete React + FastAPI applications from specifications. "
        "Creates frontend, backend, and deployment configurations.",
        category=AgentCategory.APP_GENERATION,
        capabilities=[
            AgentCapability(
                capability_id="generate_react",
                name="Generate React Frontend",
                description="Generate React TypeScript components, pages, and hooks",
                input_types=["app_spec"],
                output_types=["generated_code"],
            ),
            AgentCapability(
                capability_id="generate_fastapi",
                name="Generate FastAPI Backend",
                description="Generate FastAPI routes, models, and services",
                input_types=["app_spec"],
                output_types=["generated_code"],
            ),
            AgentCapability(
                capability_id="generate_deployment",
                name="Generate Deployment Configs",
                description="Generate Dockerfile, nginx config, and SPCS service spec",
                input_types=["app_spec"],
                output_types=["generated_code", "spcs_service"],
            ),
        ],
        dependencies=[
            AgentDependency(
                agent_id="ml_model_builder",
                relationship=DependencyRelationship.OPTIONAL,
                description="May integrate with ML models",
            ),
        ],
        triggers=[
            AgentTrigger(
                condition="has_app_spec and deployment_target == 'spcs'",
                priority=60,
                description="Triggered when app generation is needed",
            ),
        ],
        state_machine=StateMachine(
            states=[
                "APP_SPEC_GENERATION",
                "REACT_CODE_GENERATION",
                "FASTAPI_CODE_GENERATION",
                "DEPLOYMENT_CONFIG_GENERATION",
                "TEST_AND_VALIDATE",
            ],
            initial_state="APP_SPEC_GENERATION",
            transitions=[
                {"from": "APP_SPEC_GENERATION", "to": "REACT_CODE_GENERATION"},
                {"from": "REACT_CODE_GENERATION", "to": "FASTAPI_CODE_GENERATION"},
                {"from": "FASTAPI_CODE_GENERATION", "to": "DEPLOYMENT_CONFIG_GENERATION"},
                {"from": "DEPLOYMENT_CONFIG_GENERATION", "to": "TEST_AND_VALIDATE"},
            ],
        ),
    )


def get_cortex_search_builder_agent() -> AgentDefinition:
    return AgentDefinition(
        agent_id="cortex_search_builder",
        name="Cortex Search Builder",
        version="1.0.0",
        description="Creates Cortex Search services from document chunks. "
        "Configures embedding models, filter attributes, and search parameters.",
        category=AgentCategory.SEARCH,
        capabilities=[
            AgentCapability(
                capability_id="create_search_service",
                name="Create Cortex Search Service",
                description="Create a Cortex Search service over a chunk table",
                input_types=["chunk_table", "table"],
                output_types=["cortex_search_service"],
            ),
        ],
        dependencies=[
            AgentDependency(
                agent_id="document_chunker",
                relationship=DependencyRelationship.REQUIRES,
                description="Needs chunked documents to index",
            ),
        ],
        triggers=[
            AgentTrigger(
                condition="has_chunk_table and needs_search",
                priority=75,
                description="Triggered when search capability is needed",
            ),
        ],
    )


def get_semantic_model_generator_agent() -> AgentDefinition:
    return AgentDefinition(
        agent_id="semantic_model_generator",
        name="Semantic Model Generator",
        version="1.0.0",
        description="Generates semantic model YAML from table profiles. "
        "Identifies dimensions, facts, and creates verified queries.",
        category=AgentCategory.SEMANTIC,
        capabilities=[
            AgentCapability(
                capability_id="generate_semantic_model",
                name="Generate Semantic Model",
                description="Generate semantic model YAML from table metadata",
                input_types=["table", "data_profile"],
                output_types=["semantic_model", "semantic_view"],
            ),
            AgentCapability(
                capability_id="generate_verified_queries",
                name="Generate Verified Queries",
                description="Generate verified queries for Cortex Analyst",
                input_types=["semantic_model"],
                output_types=["verified_queries"],
            ),
        ],
        triggers=[
            AgentTrigger(
                condition="has_structured_data and needs_analytics",
                priority=70,
                description="Triggered when text-to-SQL capability is needed",
            ),
        ],
    )


def seed_all_agents(registry: AgentRegistryQuery) -> None:
    agents = [
        get_parquet_processor_agent(),
        get_document_chunker_agent(),
        get_ml_model_builder_agent(),
        get_app_code_generator_agent(),
        get_cortex_search_builder_agent(),
        get_semantic_model_generator_agent(),
    ]

    for agent in agents:
        registry.register_agent(agent)
        print(f"Registered agent: {agent.name} ({agent.agent_id})")


if __name__ == "__main__":
    registry = AgentRegistryQuery()
    seed_all_agents(registry)
