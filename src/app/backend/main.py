"""FastAPI backend for Orchestrator UI with dynamic plan execution."""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Agentic Platform Orchestrator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from .routes import dashboard, logs, plans

app.include_router(plans.router, prefix="/api/plans", tags=["Plans"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(logs.router, prefix="/api/logs", tags=["Logs"])

if os.path.exists("./static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
