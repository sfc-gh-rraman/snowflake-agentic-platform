-- Schema Setup for Snowflake Agentic Platform
-- Creates medallion architecture: RAW → CURATED → ML → ANALYTICS

-- Create database (parameterized - replace AGENTIC_PLATFORM with your database name)
CREATE DATABASE IF NOT EXISTS AGENTIC_PLATFORM;

USE DATABASE AGENTIC_PLATFORM;

-- RAW: Landing zone for ingested data
CREATE SCHEMA IF NOT EXISTS RAW;
COMMENT ON SCHEMA RAW IS 'Landing zone for raw data ingestion from stages';

-- CURATED: Cleaned, typed, validated data
CREATE SCHEMA IF NOT EXISTS CURATED;
COMMENT ON SCHEMA CURATED IS 'Cleaned and validated data ready for processing';

-- ML: Model artifacts, feature stores, training data
CREATE SCHEMA IF NOT EXISTS ML;
COMMENT ON SCHEMA ML IS 'ML model artifacts, feature stores, and training datasets';

-- ANALYTICS: Consumption layer for apps and semantic models
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
COMMENT ON SCHEMA ANALYTICS IS 'Consumption layer for applications and reporting';

-- STATE: Agent execution state and checkpoints
CREATE SCHEMA IF NOT EXISTS STATE;
COMMENT ON SCHEMA STATE IS 'Agent execution state, checkpoints, and observability';

-- REGISTRY: Agent capability catalog
CREATE SCHEMA IF NOT EXISTS REGISTRY;
COMMENT ON SCHEMA REGISTRY IS 'Agent capability registry and Cortex Search catalog';

-- Create stages for data ingestion
CREATE STAGE IF NOT EXISTS RAW.DATA_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for incoming data files (parquet, csv, json)';

CREATE STAGE IF NOT EXISTS RAW.DOCUMENTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for document files (pdf, txt, docx)';

-- Create image repository for SPCS deployments
CREATE IMAGE REPOSITORY IF NOT EXISTS ANALYTICS.APP_IMAGES;

-- Grant usage to appropriate roles (customize as needed)
-- GRANT USAGE ON DATABASE AGENTIC_PLATFORM TO ROLE <your_role>;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE AGENTIC_PLATFORM TO ROLE <your_role>;
