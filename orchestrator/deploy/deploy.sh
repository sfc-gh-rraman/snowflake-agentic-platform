#!/bin/bash
set -e

DATABASE="${DATABASE:-AGENTIC_PLATFORM}"
SCHEMA="${SCHEMA:-ORCHESTRATOR}"
IMAGE_REPO="${IMAGE_REPO:-ORCHESTRATOR_IMAGES}"
SERVICE_NAME="${SERVICE_NAME:-AGENTIC_ORCHESTRATOR}"
COMPUTE_POOL="${COMPUTE_POOL:-AGENTIC_COMPUTE_POOL}"
CONNECTION="${CONNECTION:-default}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Agentic Platform Orchestrator SPCS Deployment ==="
echo "Database: $DATABASE"
echo "Schema: $SCHEMA"
echo "Service: $SERVICE_NAME"
echo "Compute Pool: $COMPUTE_POOL"
echo "Connection: $CONNECTION"
echo ""

echo "Creating database and schema..."
snow sql -q "CREATE DATABASE IF NOT EXISTS $DATABASE" -c "$CONNECTION"
snow sql -q "CREATE SCHEMA IF NOT EXISTS $DATABASE.$SCHEMA" -c "$CONNECTION"

echo "Creating image repository..."
snow sql -q "CREATE IMAGE REPOSITORY IF NOT EXISTS $DATABASE.$SCHEMA.$IMAGE_REPO" -c "$CONNECTION"

echo "Getting repository URL..."
REPO_URL=$(snow sql -q "SHOW IMAGE REPOSITORIES LIKE '$IMAGE_REPO' IN SCHEMA $DATABASE.$SCHEMA" -c "$CONNECTION" --format json | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['repository_url'] if d else '')")

if [ -z "$REPO_URL" ]; then
    echo "ERROR: Could not get repository URL"
    exit 1
fi

echo "Repository URL: $REPO_URL"

IMAGE_NAME="$REPO_URL/orchestrator:latest"
echo ""
echo "Building Docker image: $IMAGE_NAME"

cd "$PROJECT_DIR"
docker build --platform linux/amd64 -t "$IMAGE_NAME" -f deploy/Dockerfile .

echo ""
echo "Logging into Snowflake registry..."
snow spcs image-registry login -c "$CONNECTION"

echo ""
echo "Pushing image to Snowflake registry..."
docker push "$IMAGE_NAME"

IMAGE_PATH="/$( echo $DATABASE | tr '[:upper:]' '[:lower:]' )/$( echo $SCHEMA | tr '[:upper:]' '[:lower:]' )/$( echo $IMAGE_REPO | tr '[:upper:]' '[:lower:]' )/orchestrator:latest"

echo ""
echo "Dropping existing service if any..."
snow sql -q "DROP SERVICE IF EXISTS $DATABASE.$SCHEMA.$SERVICE_NAME" -c "$CONNECTION" 2>/dev/null || true

echo ""
echo "Creating SPCS service..."
snow sql -q "
CREATE SERVICE $DATABASE.$SCHEMA.$SERVICE_NAME
IN COMPUTE POOL $COMPUTE_POOL
FROM SPECIFICATION \$\$
spec:
  containers:
  - name: orchestrator
    image: $IMAGE_PATH
    env:
      SNOWFLAKE_HOST: \"{{SNOWFLAKE_HOST}}\"
      SNOWFLAKE_ACCOUNT: \"{{SNOWFLAKE_ACCOUNT}}\"
    resources:
      requests:
        memory: 2Gi
        cpu: 1000m
      limits:
        memory: 4Gi
        cpu: 2000m
    readinessProbe:
      port: 8080
      path: /health
  endpoints:
  - name: app
    port: 8080
    public: true
\$\$
MIN_INSTANCES = 1
MAX_INSTANCES = 1
" -c "$CONNECTION"

echo ""
echo "Waiting for service to start..."
for i in {1..30}; do
    STATUS=$(snow sql -q "SELECT SYSTEM\$GET_SERVICE_STATUS('$DATABASE.$SCHEMA.$SERVICE_NAME')" -c "$CONNECTION" --format json | python3 -c "import sys,json; d=json.load(sys.stdin); s=json.loads(d[0][\"SYSTEM\\\$GET_SERVICE_STATUS('$DATABASE.$SCHEMA.$SERVICE_NAME')\"]); print(s[0]['status'] if s else 'UNKNOWN')" 2>/dev/null || echo "UNKNOWN")
    echo "  Status: $STATUS"
    if [ "$STATUS" = "READY" ]; then
        break
    fi
    sleep 10
done

echo ""
echo "Getting endpoint URL..."
for i in {1..12}; do
    ENDPOINT_URL=$(snow sql -q "SHOW ENDPOINTS IN SERVICE $DATABASE.$SCHEMA.$SERVICE_NAME" -c "$CONNECTION" --format json | python3 -c "import sys,json; d=json.load(sys.stdin); url=d[0]['ingress_url'] if d else ''; print(url if url and not url.startswith('Endpoint') else '')" 2>/dev/null || echo "")
    if [ -n "$ENDPOINT_URL" ]; then
        echo ""
        echo "=========================================="
        echo "  DEPLOYMENT COMPLETE!"
        echo "=========================================="
        echo ""
        echo "  Orchestrator URL: https://$ENDPOINT_URL"
        echo ""
        echo "  View in Langfuse: https://cloud.langfuse.com"
        echo ""
        echo "=========================================="
        exit 0
    fi
    echo "  Waiting for endpoint URL..."
    sleep 5
done

echo ""
echo "WARNING: Endpoint URL not yet available."
echo "Check Snowsight for service status:"
echo "  SHOW SERVICES LIKE '$SERVICE_NAME' IN SCHEMA $DATABASE.$SCHEMA;"
