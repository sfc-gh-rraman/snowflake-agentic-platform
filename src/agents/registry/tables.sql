-- Agent Registry Tables for Snowflake Agentic Platform
-- Stores agent definitions and capabilities for Cortex Search discovery

USE DATABASE AGENTIC_PLATFORM;
USE SCHEMA REGISTRY;

-- Agent definitions table
CREATE TABLE IF NOT EXISTS AGENT_DEFINITIONS (
    agent_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    version VARCHAR(20) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    definition VARIANT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE AGENT_DEFINITIONS IS 'Master table of all registered agents and their full definitions';

-- Agent capability chunks for Cortex Search
CREATE TABLE IF NOT EXISTS AGENT_CAPABILITY_CHUNKS (
    chunk_id VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    agent_id VARCHAR(100) NOT NULL REFERENCES AGENT_DEFINITIONS(agent_id),
    agent_name VARCHAR(100) NOT NULL,
    agent_version VARCHAR(20) NOT NULL,
    category VARCHAR(50),
    capability_id VARCHAR(100),
    capability_name VARCHAR(200),
    chunk_text VARCHAR(16000) NOT NULL,
    input_types ARRAY,
    output_types ARRAY,
    priority INTEGER DEFAULT 50,
    full_definition VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE AGENT_CAPABILITY_CHUNKS IS 'Chunked agent capabilities for Cortex Search indexing';

CREATE INDEX IF NOT EXISTS idx_chunk_agent ON AGENT_CAPABILITY_CHUNKS(agent_id);
CREATE INDEX IF NOT EXISTS idx_chunk_category ON AGENT_CAPABILITY_CHUNKS(category);
