"""FastAPI application for the orchestrator."""

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
    return {"status": "healthy", "service": "agentic-platform-orchestrator"}


@app.get("/")
async def root():
    return {
        "name": "Agentic Platform Orchestrator",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
