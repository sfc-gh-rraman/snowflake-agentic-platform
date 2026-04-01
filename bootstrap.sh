#!/bin/bash
set -e

CONNECTION="${1:-my_snowflake}"
DATABASE="${DATABASE:-AGENTIC_PLATFORM}"

echo "============================================="
echo "  CoCo Healthcare Skills Orchestrator Setup"
echo "============================================="
echo ""
echo "Connection: $CONNECTION"
echo "Database:   $DATABASE"
echo ""

command -v snow >/dev/null 2>&1 || { echo "ERROR: Snowflake CLI (snow) not found. Install: https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker not found."; exit 1; }
command -v node >/dev/null 2>&1 || { echo "ERROR: Node.js not found."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: Python 3 not found."; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "[1/5] Setting up Snowflake objects..."
snow sql -f "$SCRIPT_DIR/orchestrator/deploy/setup.sql" -c "$CONNECTION"
echo "  Done."

echo ""
echo "[2/5] Installing Python dependencies..."
pip install -q -r "$SCRIPT_DIR/orchestrator/backend/requirements.txt"
echo "  Done."

echo ""
echo "[3/5] Installing frontend dependencies..."
cd "$SCRIPT_DIR/orchestrator/frontend"
npm install --silent
echo "  Done."

echo ""
echo "[4/5] Building frontend..."
npm run build --silent
echo "  Done."

echo ""
echo "[5/5] Deploying to SPCS..."
cd "$SCRIPT_DIR"
CONNECTION="$CONNECTION" DATABASE="$DATABASE" "$SCRIPT_DIR/orchestrator/deploy/deploy.sh"

echo ""
echo "============================================="
echo "  Setup Complete!"
echo "============================================="
echo ""
echo "To run locally instead of SPCS:"
echo ""
echo "  # Terminal 1: Backend"
echo "  SNOWFLAKE_CONNECTION_NAME=$CONNECTION python3 -m uvicorn orchestrator.backend.api.main:app \\"
echo "    --host 0.0.0.0 --port 8000 --reload --reload-dir orchestrator/backend \\"
echo "    --app-dir $SCRIPT_DIR"
echo ""
echo "  # Terminal 2: Frontend"
echo "  cd $SCRIPT_DIR/orchestrator/frontend && npm run dev"
echo ""
echo "  Open http://localhost:5173"
echo ""
