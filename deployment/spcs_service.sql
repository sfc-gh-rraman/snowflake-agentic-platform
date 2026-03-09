-- SPCS Service Specification for Agentic Platform
-- Deploy to Snowpark Container Services

-- 1. Create compute pool (if not exists)
CREATE COMPUTE POOL IF NOT EXISTS AGENTIC_COMPUTE_POOL
    MIN_NODES = 1
    MAX_NODES = 3
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300;

-- 2. Create image repository
CREATE IMAGE REPOSITORY IF NOT EXISTS AGENTIC_PLATFORM.ORCHESTRATOR.IMAGES;

-- 3. Create service (replace <REGISTRY> with your registry URL)
-- Get registry URL: SHOW IMAGE REPOSITORIES IN SCHEMA AGENTIC_PLATFORM.ORCHESTRATOR;

CREATE SERVICE IF NOT EXISTS AGENTIC_PLATFORM.ORCHESTRATOR.AGENTIC_PLATFORM_SERVICE
    IN COMPUTE POOL AGENTIC_COMPUTE_POOL
    FROM SPECIFICATION $$
spec:
  containers:
  - name: app
    image: <REGISTRY>/agentic-platform:latest
    env:
      SNOWFLAKE_DATABASE: AGENTIC_PLATFORM
      ENVIRONMENT: production
    resources:
      requests:
        memory: 2Gi
        cpu: 1
      limits:
        memory: 4Gi
        cpu: 2
  endpoints:
  - name: app
    port: 8000
    public: true
$$
    MIN_INSTANCES = 1
    MAX_INSTANCES = 3;

-- 4. Grant usage
GRANT USAGE ON SERVICE AGENTIC_PLATFORM.ORCHESTRATOR.AGENTIC_PLATFORM_SERVICE 
    TO ROLE PUBLIC;

-- 5. Get service status
SELECT SYSTEM$GET_SERVICE_STATUS('AGENTIC_PLATFORM.ORCHESTRATOR.AGENTIC_PLATFORM_SERVICE');

-- 6. Get endpoint URL
SHOW ENDPOINTS IN SERVICE AGENTIC_PLATFORM.ORCHESTRATOR.AGENTIC_PLATFORM_SERVICE;
