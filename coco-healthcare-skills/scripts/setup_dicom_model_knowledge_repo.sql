-- =============================================================================
-- DICOM Data Model Knowledge Repository on Snowflake
-- Creates a Cortex Search Service over the DICOM 18-table data model reference
-- so skills can query latest model definitions at runtime.
-- =============================================================================

-- Step 1: Use target database and schema
USE DATABASE UNSTRUCTURED_HEALTHDATA;
CREATE SCHEMA IF NOT EXISTS DATA_MODEL_KNOWLEDGE;
USE SCHEMA DATA_MODEL_KNOWLEDGE;

-- Step 2: Create the data model reference table (flat search corpus)
CREATE OR REPLACE TABLE DICOM_MODEL_REFERENCE (
    search_text VARCHAR,
    table_name VARCHAR,
    column_name VARCHAR,
    data_type VARCHAR,
    dicom_tag VARCHAR,
    category VARCHAR,
    description VARCHAR,
    constraints VARCHAR,
    contains_phi VARCHAR,
    relationships VARCHAR
);

-- Step 3: Create stage and load the Excel-exported data
CREATE OR REPLACE STAGE dicom_model_stage;

-- Upload the Excel file (run from CLI):
-- PUT file:///path/to/dicom_data_model_reference.xlsx @dicom_model_stage;
--
-- Or load directly from the "Search Corpus (Flat)" sheet exported as CSV:
-- PUT file:///path/to/dicom_model_search_corpus.csv @dicom_model_stage;

-- Step 4: Load from CSV (exported from "Search Corpus (Flat)" sheet)
COPY INTO DICOM_MODEL_REFERENCE
FROM @dicom_model_stage/dicom_model_search_corpus.csv
FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
);

-- Step 5: Verify load
SELECT COUNT(*) AS row_count FROM DICOM_MODEL_REFERENCE;
SELECT table_name, COUNT(*) AS column_count
FROM DICOM_MODEL_REFERENCE
GROUP BY table_name
ORDER BY column_count DESC;

-- Step 6: Create Cortex Search Service over the data model
CREATE OR REPLACE CORTEX SEARCH SERVICE DICOM_MODEL_SEARCH_SVC
    ON search_text
    ATTRIBUTES table_name, column_name, data_type, category, contains_phi
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
AS (
    SELECT
        search_text,
        table_name,
        column_name,
        data_type,
        dicom_tag,
        category,
        description,
        constraints,
        contains_phi,
        relationships
    FROM DICOM_MODEL_REFERENCE
);

-- Step 7: Test the search service
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "patient demographics columns", "columns": ["table_name", "column_name", "data_type", "description", "contains_phi"]}'
);

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "radiation dose exposure CT scan", "columns": ["table_name", "column_name", "data_type", "description", "dicom_tag"]}'
);

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',
    '{"query": "columns that contain PHI protected health information", "columns": ["table_name", "column_name", "description", "contains_phi"]}'
);
