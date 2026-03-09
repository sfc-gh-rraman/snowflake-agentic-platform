-- SPCS Service Specification for Drilling Operations Dashboard
-- This SQL creates the SPCS service with all required configuration

-- 1. Ensure compute pool exists and is active
ALTER COMPUTE POOL PETRA_COMPUTE_POOL RESUME;

-- 2. Create the service
CREATE SERVICE IF NOT EXISTS DRILLING_OPS_DB.SPCS.DRILLING_DASHBOARD_SERVICE
  IN COMPUTE POOL PETRA_COMPUTE_POOL
  FROM SPECIFICATION $$
  spec:
    containers:
    - name: app
      image: /drilling_ops_db/spcs/drilling_app_images/drilling-dashboard:latest
      env:
        SNOWFLAKE_HOST: "{{ context().CURRENT_HOST }}"
        SNOWFLAKE_ACCOUNT: "{{ context().CURRENT_ACCOUNT }}"
      resources:
        requests:
          memory: 512M
          cpu: 0.5
        limits:
          memory: 1G
          cpu: 1
    endpoints:
    - name: app
      port: 8080
      public: true
  $$
  MIN_INSTANCES = 1
  MAX_INSTANCES = 2;

-- 3. Grant usage to roles
GRANT USAGE ON SERVICE DRILLING_OPS_DB.SPCS.DRILLING_DASHBOARD_SERVICE TO ROLE PUBLIC;

-- 4. Check service status
SELECT SYSTEM$GET_SERVICE_STATUS('DRILLING_OPS_DB.SPCS.DRILLING_DASHBOARD_SERVICE') as STATUS;

-- 5. Get service endpoint (once running)
-- SHOW ENDPOINTS IN SERVICE DRILLING_OPS_DB.SPCS.DRILLING_DASHBOARD_SERVICE;
