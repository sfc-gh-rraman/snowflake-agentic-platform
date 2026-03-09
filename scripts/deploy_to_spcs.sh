#!/bin/bash
# SPCS Deployment Script for Drilling Operations Dashboard

set -e

APP_NAME="drilling-dashboard"
IMAGE_TAG="latest"
REGISTRY="sfpscogs-rraman-aws-si.registry.snowflakecomputing.com/drilling_ops_db/spcs/drilling_app_images"
COMPUTE_POOL="PETRA_COMPUTE_POOL"

echo "=========================================="
echo "DRILLING OPS DASHBOARD - SPCS DEPLOYMENT"
echo "=========================================="

# Step 1: Build the Docker image
echo ""
echo "Step 1: Building Docker image..."
cd "$(dirname "$0")/.."
docker build -t ${APP_NAME}:${IMAGE_TAG} -f generated_app/Dockerfile generated_app/

# Step 2: Tag for Snowflake registry
echo ""
echo "Step 2: Tagging image for Snowflake registry..."
docker tag ${APP_NAME}:${IMAGE_TAG} ${REGISTRY}/${APP_NAME}:${IMAGE_TAG}

# Step 3: Login to Snowflake registry
echo ""
echo "Step 3: Logging into Snowflake registry..."
snow connection set-default my_snowflake
snow spcs image-registry login

# Step 4: Push to Snowflake
echo ""
echo "Step 4: Pushing image to Snowflake..."
docker push ${REGISTRY}/${APP_NAME}:${IMAGE_TAG}

# Step 5: Create service
echo ""
echo "Step 5: Creating SPCS service..."
snow sql -q "
CREATE SERVICE IF NOT EXISTS DRILLING_OPS_DB.SPCS.DRILLING_DASHBOARD_SERVICE
  IN COMPUTE POOL ${COMPUTE_POOL}
  FROM SPECIFICATION \$\$
  spec:
    containers:
    - name: app
      image: /${REGISTRY}/${APP_NAME}:${IMAGE_TAG}
      env:
        SNOWFLAKE_HOST: {{ context().CURRENT_HOST }}
        SNOWFLAKE_ACCOUNT: {{ context().CURRENT_ACCOUNT }}
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
  \$\$
  MIN_INSTANCES = 1
  MAX_INSTANCES = 2
"

# Step 6: Get endpoint URL
echo ""
echo "Step 6: Getting service endpoint..."
snow sql -q "
SELECT SYSTEM\$GET_SERVICE_STATUS('DRILLING_OPS_DB.SPCS.DRILLING_DASHBOARD_SERVICE') as STATUS
"

echo ""
echo "=========================================="
echo "Deployment complete!"
echo "=========================================="
