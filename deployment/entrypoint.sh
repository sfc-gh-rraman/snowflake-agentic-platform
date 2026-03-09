#!/bin/bash
set -e

if [ -f /snowflake/session/token ]; then
    export SNOWFLAKE_AUTHENTICATOR="OAUTH"
    export SNOWFLAKE_TOKEN=$(cat /snowflake/session/token)
    echo "Running in SPCS environment with OAuth token"
fi

case "$1" in
    serve)
        echo "Starting Agentic Platform API server..."
        python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
        ;;
    worker)
        echo "Starting background worker..."
        python -m src.workers.executor
        ;;
    cli)
        shift
        python -m src.cli.main "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
