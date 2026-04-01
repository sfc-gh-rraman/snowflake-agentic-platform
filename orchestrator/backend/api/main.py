"""FastAPI application for the orchestrator."""

import os
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import router, websocket_router

app = FastAPI(
    title="Agentic Platform Orchestrator",
    description="Visual workflow orchestrator for deploying AI applications on Snowflake",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")
app.include_router(websocket_router)


@app.get("/health")
async def health_check():
    result = {
        "status": "healthy",
        "service": "agentic-platform-orchestrator",
        "snowflake": "unknown",
    }

    try:
        import snowflake.connector
        start = time.time()
        if os.path.exists("/snowflake/session/token"):
            with open("/snowflake/session/token") as f:
                token = f.read().strip()
            conn = snowflake.connector.connect(
                host=os.environ.get("SNOWFLAKE_HOST", ""),
                account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                authenticator="oauth",
                token=token,
            )
        else:
            conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")
            conn = snowflake.connector.connect(connection_name=conn_name)

        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        latency = round((time.time() - start) * 1000, 1)
        result["snowflake"] = "connected"
        result["sf_latency_ms"] = latency
    except Exception as e:
        result["status"] = "degraded"
        result["snowflake"] = "unreachable"
        result["sf_error"] = str(e)[:200]

    return result


@app.get("/")
async def root():
    return {
        "name": "Agentic Platform Orchestrator",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
