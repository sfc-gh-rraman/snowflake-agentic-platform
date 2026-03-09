from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI(title="Drilling Operations Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from .routes import data, search, ml

app.include_router(data.router)
app.include_router(search.router)
app.include_router(ml.router)

if os.path.exists("./static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
