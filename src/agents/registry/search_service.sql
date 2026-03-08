-- Cortex Search Service for Agent Registry
-- Enables semantic search over agent capabilities

USE DATABASE AGENTIC_PLATFORM;
USE SCHEMA REGISTRY;

CREATE OR REPLACE CORTEX SEARCH SERVICE AGENT_CAPABILITY_SEARCH
    ON chunk_text
    ATTRIBUTES agent_id, agent_name, category, capability_name, input_types, output_types
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 hour'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
AS (
    SELECT 
        chunk_id,
        agent_id,
        agent_name,
        agent_version,
        category,
        capability_id,
        capability_name,
        chunk_text,
        input_types,
        output_types,
        priority,
        full_definition
    FROM AGENT_CAPABILITY_CHUNKS
    WHERE TRUE  -- Placeholder for any filtering
);

COMMENT ON CORTEX SEARCH SERVICE AGENT_CAPABILITY_SEARCH IS 
    'Semantic search over agent capabilities for meta-agent planning';
