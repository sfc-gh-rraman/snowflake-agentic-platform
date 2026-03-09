"""Application code generator.

Generates React frontend and FastAPI backend from UseCaseConfig.
"""

import json
from pathlib import Path
from typing import Dict, List

from src.config import (
    UseCaseConfig,
    PageSpec,
    UIComponent,
    ComponentType,
    PageLayout,
    APIEndpoint,
)


def generate_app(config: UseCaseConfig, output_dir: Path) -> None:
    """Generate complete application code from UseCaseConfig.
    
    Args:
        config: The use case configuration
        output_dir: Directory to write generated code
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    _generate_backend(config, output_dir / "backend")
    _generate_frontend(config, output_dir / "frontend")
    _generate_spcs_config(config, output_dir)


def _generate_backend(config: UseCaseConfig, output_dir: Path) -> None:
    """Generate FastAPI backend."""
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "routes").mkdir(exist_ok=True)
    
    main_py = _generate_main_py(config)
    (output_dir / "main.py").write_text(main_py)
    
    routes_init = '"""API routes package."""\n'
    (output_dir / "routes" / "__init__.py").write_text(routes_init)
    
    chat_route = _generate_chat_route(config)
    (output_dir / "routes" / "chat.py").write_text(chat_route)
    
    data_route = _generate_data_route(config)
    (output_dir / "routes" / "data.py").write_text(data_route)
    
    if config.data.unstructured:
        search_route = _generate_search_route(config)
        (output_dir / "routes" / "search.py").write_text(search_route)
    
    if config.ml_models:
        predict_route = _generate_predict_route(config)
        (output_dir / "routes" / "predict.py").write_text(predict_route)
    
    requirements = _generate_requirements()
    (output_dir / "requirements.txt").write_text(requirements)
    
    config_py = _generate_config_py(config)
    (output_dir / "config.py").write_text(config_py)


def _generate_main_py(config: UseCaseConfig) -> str:
    """Generate main FastAPI application."""
    has_search = bool(config.data.unstructured)
    has_ml = bool(config.ml_models)
    
    search_import = "from routes import search" if has_search else ""
    predict_import = "from routes import predict" if has_ml else ""
    search_router = "app.include_router(search.router, prefix='/api', tags=['search'])" if has_search else ""
    predict_router = "app.include_router(predict.router, prefix='/api', tags=['predict'])" if has_ml else ""
    
    return f'''"""FastAPI backend for {config.app.name}."""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes import chat, data
{search_import}
{predict_import}
from config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Starting {config.app.name}...")
    yield
    print("Shutting down...")


app = FastAPI(
    title="{config.app.name}",
    description="{(config.app.description or config.domain.description)[:200].replace('"', "'")}",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat.router, prefix="/api", tags=["chat"])
app.include_router(data.router, prefix="/api", tags=["data"])
{search_router}
{predict_router}


@app.get("/health")
async def health():
    return {{"status": "healthy", "app": "{config.app.name}"}}


@app.get("/api/config")
async def get_config():
    """Return app configuration for frontend."""
    return {{
        "app_name": "{config.app.name}",
        "domain": "{config.domain.name}",
        "real_time_enabled": {str(config.app.real_time.enabled).lower()},
        "refresh_interval_ms": {config.app.real_time.refresh_interval_ms},
        "entities": settings.ENTITIES,
    }}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
'''


def _generate_chat_route(config: UseCaseConfig) -> str:
    """Generate chat route with Cortex Agent."""
    agents = [a for a in config.agents if a.agent_type.value == "orchestrator"]
    agent_name = agents[0].name if agents else "Assistant"
    capabilities = []
    for a in config.agents:
        capabilities.extend(a.capabilities[:2])
    caps_str = "\\n".join(f"- {c}" for c in capabilities[:6])
    
    return f'''"""Chat route with Cortex Agent."""

import os
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import snowflake.connector

router = APIRouter()


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    context: Optional[dict] = None


class ChatResponse(BaseModel):
    response: str
    session_id: str
    latency_ms: int
    sources: Optional[list] = None


def get_connection():
    """Get Snowflake connection."""
    if os.path.exists("/snowflake/session/token"):
        from snowflake.snowpark import Session
        return Session.builder.getOrCreate().connection
    else:
        return snowflake.connector.connect(
            connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        )


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Process a chat message through the {agent_name} agent."""
    start_time = time.time()
    session_id = request.session_id or str(uuid.uuid4())
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        system_prompt = """You are {agent_name}, an AI assistant for {config.domain.name}.
        
Your capabilities:
{caps_str}

Be helpful, accurate, and concise."""
        
        full_prompt = f"{{system_prompt}}\\n\\nUser: {{request.message}}"
        escaped = full_prompt.replace("'", "''")
        
        sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{{escaped}}')"
        cursor.execute(sql)
        result = cursor.fetchone()
        response_text = result[0] if result else "I could not process your request."
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        return ChatResponse(
            response=response_text,
            session_id=session_id,
            latency_ms=latency_ms,
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''


def _generate_data_route(config: UseCaseConfig) -> str:
    """Generate data route for queries."""
    primary_data = config.get_primary_structured_data()
    entity_col = primary_data.entity_column if primary_data else "entity_id"
    time_col = primary_data.time_column if primary_data else "timestamp"
    measures = primary_data.measures[:6] if primary_data else ["value"]
    table_name = primary_data.location if primary_data else "data"
    
    measures_list_str = ', '.join(f'"{m}"' for m in measures)
    measures_values_str = ', '.join(f'"{m}": row[{i+1}]' for i, m in enumerate(measures))
    
    return f'''"""Data routes for querying structured data."""

import os
import time
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

import snowflake.connector
from config import settings

router = APIRouter()


class EntitySummary(BaseModel):
    entity_id: str
    record_count: int
    latest_timestamp: Optional[str] = None
    metrics: dict


class TimeSeriesPoint(BaseModel):
    timestamp: str
    values: dict


def get_connection():
    """Get Snowflake connection."""
    if os.path.exists("/snowflake/session/token"):
        from snowflake.snowpark import Session
        return Session.builder.getOrCreate().connection
    else:
        return snowflake.connector.connect(
            connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        )


@router.get("/entities")
async def list_entities():
    """List all entities with summary stats."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        sql = f"""
            SELECT 
                {entity_col} as entity_id,
                COUNT(*) as record_count,
                MAX({time_col}) as latest_timestamp
            FROM {{settings.DATABASE}}.{{settings.CURATED_SCHEMA}}.{table_name.split('.')[-1]}
            GROUP BY {entity_col}
            ORDER BY {entity_col}
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        return [
            {{
                "entity_id": row[0],
                "record_count": row[1],
                "latest_timestamp": str(row[2]) if row[2] else None,
            }}
            for row in rows
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/entities/{{entity_id}}/timeseries")
async def get_timeseries(
    entity_id: str,
    limit: int = Query(default=1000, le=10000),
    offset: int = Query(default=0),
):
    """Get time series data for an entity."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        sql = f"""
            SELECT 
                {time_col},
                {measures_list_str}
            FROM {{settings.DATABASE}}.{{settings.CURATED_SCHEMA}}.{table_name.split('.')[-1]}
            WHERE {entity_col} = %s
            ORDER BY {time_col} DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(sql, (entity_id, limit, offset))
        rows = cursor.fetchall()
        
        return [
            {{
                "timestamp": str(row[0]),
                "values": {{
                    {measures_values_str}
                }}
            }}
            for row in rows
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/entities/{{entity_id}}/latest")
async def get_latest(entity_id: str):
    """Get latest readings for an entity."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        sql = f"""
            SELECT 
                {time_col},
                {measures_list_str}
            FROM {{settings.DATABASE}}.{{settings.CURATED_SCHEMA}}.{table_name.split('.')[-1]}
            WHERE {entity_col} = %s
            ORDER BY {time_col} DESC
            LIMIT 1
        """
        cursor.execute(sql, (entity_id,))
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Entity not found")
        
        return {{
            "entity_id": entity_id,
            "timestamp": str(row[0]),
            "values": {{
                {measures_values_str}
            }}
        }}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_summary():
    """Get overall summary statistics."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        sql = f"""
            SELECT 
                COUNT(DISTINCT {entity_col}) as entity_count,
                COUNT(*) as total_records,
                MIN({time_col}) as earliest,
                MAX({time_col}) as latest
            FROM {{settings.DATABASE}}.{{settings.CURATED_SCHEMA}}.{table_name.split('.')[-1]}
        """
        cursor.execute(sql)
        row = cursor.fetchone()
        
        return {{
            "entity_count": row[0],
            "total_records": row[1],
            "earliest_timestamp": str(row[2]) if row[2] else None,
            "latest_timestamp": str(row[3]) if row[3] else None,
        }}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''


def _generate_search_route(config: UseCaseConfig) -> str:
    """Generate search route for Cortex Search."""
    search_services = config.cortex_services.search
    service_name = search_services[0].name if search_services else "SEARCH_SERVICE"
    
    return f'''"""Search route for Cortex Search."""

import os
import time
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

import snowflake.connector
from config import settings

router = APIRouter()


class SearchRequest(BaseModel):
    query: str
    limit: int = 10
    filters: Optional[dict] = None


class SearchResult(BaseModel):
    document_name: str
    chunk_text: str
    score: float
    section: Optional[str] = None
    metadata: Optional[dict] = None


class SearchResponse(BaseModel):
    results: List[SearchResult]
    query: str
    latency_ms: int


def get_connection():
    """Get Snowflake connection."""
    if os.path.exists("/snowflake/session/token"):
        from snowflake.snowpark import Session
        return Session.builder.getOrCreate().connection
    else:
        return snowflake.connector.connect(
            connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        )


@router.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    """Search documents using Cortex Search."""
    start_time = time.time()
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        escaped_query = request.query.replace("'", "''")
        
        sql = f"""
            SELECT 
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    '{{settings.DATABASE}}.{{settings.CORTEX_SCHEMA}}.{service_name}',
                    '{{"query": "{{escaped_query}}", "columns": ["document_name", "chunk_text", "section"], "limit": {{request.limit}}}}'
                )
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        if result and result[0]:
            import json
            data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
            results = [
                SearchResult(
                    document_name=r.get("document_name", ""),
                    chunk_text=r.get("chunk_text", ""),
                    score=r.get("score", 0.0),
                    section=r.get("section"),
                )
                for r in data.get("results", [])
            ]
        else:
            results = []
        
        return SearchResponse(
            results=results,
            query=request.query,
            latency_ms=latency_ms,
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''


def _generate_predict_route(config: UseCaseConfig) -> str:
    """Generate prediction route for ML models."""
    models = config.ml_models
    model_names = [m.name for m in models]
    
    return f'''"""Prediction route for ML models."""

import os
import time
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import snowflake.connector
from config import settings

router = APIRouter()

AVAILABLE_MODELS = {model_names}


class PredictRequest(BaseModel):
    model_name: str
    features: Dict[str, Any]


class PredictResponse(BaseModel):
    model_name: str
    prediction: Any
    probability: Optional[float] = None
    latency_ms: int


def get_connection():
    """Get Snowflake connection."""
    if os.path.exists("/snowflake/session/token"):
        from snowflake.snowpark import Session
        return Session.builder.getOrCreate().connection
    else:
        return snowflake.connector.connect(
            connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        )


@router.get("/models")
async def list_models():
    """List available ML models."""
    return {{"models": AVAILABLE_MODELS}}


@router.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    """Make a prediction using a registered model."""
    start_time = time.time()
    
    if request.model_name not in AVAILABLE_MODELS:
        raise HTTPException(
            status_code=404, 
            detail=f"Model '{{request.model_name}}' not found. Available: {{AVAILABLE_MODELS}}"
        )
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        import json
        features_json = json.dumps(request.features)
        
        sql = f"""
            SELECT {{settings.DATABASE}}.{{settings.ML_SCHEMA}}.{{request.model_name.upper()}}_PREDICT(
                PARSE_JSON('{{features_json}}')
            )
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        prediction = result[0] if result else None
        probability = None
        
        if isinstance(prediction, dict):
            probability = prediction.get("probability")
            prediction = prediction.get("prediction", prediction)
        
        return PredictResponse(
            model_name=request.model_name,
            prediction=prediction,
            probability=probability,
            latency_ms=latency_ms,
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''


def _generate_requirements() -> str:
    """Generate requirements.txt."""
    return '''fastapi>=0.109.0
uvicorn>=0.27.0
snowflake-connector-python>=3.0.0
snowflake-snowpark-python>=1.20.0
pydantic>=2.0.0
python-dotenv>=1.0.0
'''


def _generate_config_py(config: UseCaseConfig) -> str:
    """Generate config.py with settings."""
    entities = []
    if config.data.structured:
        primary = config.data.structured[0]
        if primary.entity_column:
            entities = [primary.entity_column]
    
    return f'''"""Application configuration."""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    DATABASE: str = "{config.snowflake.database}"
    RAW_SCHEMA: str = "{config.snowflake.raw_schema}"
    CURATED_SCHEMA: str = "{config.snowflake.curated_schema}"
    ML_SCHEMA: str = "{config.snowflake.ml_schema}"
    DOCS_SCHEMA: str = "{config.snowflake.docs_schema}"
    CORTEX_SCHEMA: str = "{config.snowflake.cortex_schema}"
    ORCHESTRATOR_SCHEMA: str = "{config.snowflake.orchestrator_schema}"
    
    ENTITIES: list = None
    
    def __post_init__(self):
        self.DATABASE = os.getenv("SNOWFLAKE_DATABASE", self.DATABASE)
        self.ENTITIES = {entities if entities else []}


settings = Settings()
'''


def _generate_frontend(config: UseCaseConfig, output_dir: Path) -> None:
    """Generate React frontend."""
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "src").mkdir(exist_ok=True)
    (output_dir / "src" / "components").mkdir(exist_ok=True)
    (output_dir / "src" / "pages").mkdir(exist_ok=True)
    (output_dir / "src" / "hooks").mkdir(exist_ok=True)
    
    package_json = _generate_package_json(config)
    (output_dir / "package.json").write_text(package_json)
    
    app_tsx = _generate_app_tsx(config)
    (output_dir / "src" / "App.tsx").write_text(app_tsx)
    
    index_tsx = _generate_index_tsx(config)
    (output_dir / "src" / "index.tsx").write_text(index_tsx)
    
    for page in config.app.pages:
        page_tsx = _generate_page_tsx(page, config)
        page_filename = page.name.replace(" ", "") + ".tsx"
        (output_dir / "src" / "pages" / page_filename).write_text(page_tsx)
    
    api_hook = _generate_api_hook(config)
    (output_dir / "src" / "hooks" / "useApi.ts").write_text(api_hook)
    
    for component in _get_unique_components(config):
        comp_tsx = _generate_component_tsx(component, config)
        comp_filename = _component_type_to_filename(component)
        (output_dir / "src" / "components" / comp_filename).write_text(comp_tsx)


def _generate_package_json(config: UseCaseConfig) -> str:
    """Generate package.json."""
    app_name = config.app.name.lower().replace(" ", "-").replace("_", "-")
    
    return f'''{{
  "name": "{app_name}",
  "version": "1.0.0",
  "private": true,
  "dependencies": {{
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "react-router-dom": "^6.22.0",
    "recharts": "^2.12.0",
    "lucide-react": "^0.330.0",
    "@tanstack/react-query": "^5.17.0"
  }},
  "devDependencies": {{
    "@types/react": "^18.2.0",
    "@types/react-dom": "^18.2.0",
    "@vitejs/plugin-react": "^4.2.0",
    "typescript": "^5.3.0",
    "vite": "^5.0.0"
  }},
  "scripts": {{
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  }}
}}
'''


def _generate_app_tsx(config: UseCaseConfig) -> str:
    """Generate main App.tsx with routing."""
    routes = []
    imports = []
    
    for page in config.app.pages:
        page_name = page.name.replace(" ", "")
        imports.append(f"import {page_name} from './pages/{page_name}';")
        routes.append(f'        <Route path="{page.route}" element={{<{page_name} />}} />')
    
    return f'''import React from 'react';
import {{ BrowserRouter, Routes, Route, Link }} from 'react-router-dom';
import {{ QueryClient, QueryClientProvider }} from '@tanstack/react-query';

{chr(10).join(imports)}

const queryClient = new QueryClient();

function Navigation() {{
  return (
    <nav className="bg-gray-900 text-white p-4">
      <div className="flex items-center justify-between max-w-7xl mx-auto">
        <h1 className="text-xl font-bold">{config.app.name}</h1>
        <div className="flex gap-4">
{chr(10).join(f'          <Link to="{p.route}" className="hover:text-blue-400">{p.name}</Link>' for p in config.app.pages)}
        </div>
      </div>
    </nav>
  );
}}

export default function App() {{
  return (
    <QueryClientProvider client={{queryClient}}>
      <BrowserRouter>
        <div className="min-h-screen bg-gray-100">
          <Navigation />
          <main className="p-4">
            <Routes>
{chr(10).join(routes)}
            </Routes>
          </main>
        </div>
      </BrowserRouter>
    </QueryClientProvider>
  );
}}
'''


def _generate_index_tsx(config: UseCaseConfig) -> str:
    """Generate index.tsx entry point."""
    return '''import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
'''


def _generate_page_tsx(page: PageSpec, config: UseCaseConfig) -> str:
    """Generate a page component."""
    page_name = page.name.replace(" ", "")
    
    component_imports = []
    component_renders = []
    
    for comp in page.components:
        comp_name = _component_type_to_name(comp.component_type)
        component_imports.append(f"import {comp_name} from '../components/{comp_name}';")
        props = f'title="{comp.title}"' if comp.title else ""
        component_renders.append(f"        <{comp_name} {props} />")
    
    component_imports = list(set(component_imports))
    
    return f'''import React from 'react';
{chr(10).join(component_imports)}

export default function {page_name}() {{
  return (
    <div className="max-w-7xl mx-auto">
      <h2 className="text-2xl font-bold mb-4">{page.name}</h2>
      <p className="text-gray-600 mb-6">{page.description or ""}</p>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
{chr(10).join(component_renders)}
      </div>
    </div>
  );
}}
'''


def _generate_api_hook(config: UseCaseConfig) -> str:
    """Generate useApi hook."""
    return '''import { useQuery, useMutation } from '@tanstack/react-query';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

async function fetchApi<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
    ...options,
  });
  
  if (!response.ok) {
    throw new Error(`API error: ${response.statusText}`);
  }
  
  return response.json();
}

export function useEntities() {
  return useQuery({
    queryKey: ['entities'],
    queryFn: () => fetchApi<any[]>('/entities'),
  });
}

export function useEntityTimeseries(entityId: string) {
  return useQuery({
    queryKey: ['timeseries', entityId],
    queryFn: () => fetchApi<any[]>(`/entities/${entityId}/timeseries`),
    enabled: !!entityId,
  });
}

export function useChat() {
  return useMutation({
    mutationFn: (message: string) =>
      fetchApi<any>('/chat', {
        method: 'POST',
        body: JSON.stringify({ message }),
      }),
  });
}

export function useSearch() {
  return useMutation({
    mutationFn: (query: string) =>
      fetchApi<any>('/search', {
        method: 'POST',
        body: JSON.stringify({ query }),
      }),
  });
}

export function useSummary() {
  return useQuery({
    queryKey: ['summary'],
    queryFn: () => fetchApi<any>('/summary'),
  });
}
'''


def _get_unique_components(config: UseCaseConfig) -> List[ComponentType]:
    """Get unique component types across all pages."""
    types = set()
    for page in config.app.pages:
        for comp in page.components:
            types.add(comp.component_type)
    return list(types)


def _component_type_to_name(comp_type: ComponentType) -> str:
    """Convert component type to React component name."""
    mapping = {
        ComponentType.LIVE_GAUGES: "LiveGauges",
        ComponentType.ALERT_FEED: "AlertFeed",
        ComponentType.CHAT: "Chat",
        ComponentType.CONTEXT_PANEL: "ContextPanel",
        ComponentType.MULTI_LINE_CHART: "MultiLineChart",
        ComponentType.DAYS_VS_DEPTH: "DaysVsDepth",
        ComponentType.STATISTICS_TABLE: "StatisticsTable",
        ComponentType.DOCUMENT_SEARCH: "DocumentSearch",
        ComponentType.AI_BRIEFING: "AIBriefing",
        ComponentType.KPI_CARDS: "KPICards",
        ComponentType.HEATMAP: "Heatmap",
        ComponentType.PARAMETER_COMPARISON: "ParameterComparison",
    }
    return mapping.get(comp_type, "GenericComponent")


def _component_type_to_filename(comp_type: ComponentType) -> str:
    """Convert component type to filename."""
    return _component_type_to_name(comp_type) + ".tsx"


def _generate_component_tsx(comp_type: ComponentType, config: UseCaseConfig) -> str:
    """Generate a React component."""
    comp_name = _component_type_to_name(comp_type)
    
    if comp_type == ComponentType.CHAT:
        return _generate_chat_component()
    elif comp_type == ComponentType.LIVE_GAUGES:
        return _generate_gauges_component(config)
    elif comp_type == ComponentType.ALERT_FEED:
        return _generate_alert_component()
    elif comp_type == ComponentType.KPI_CARDS:
        return _generate_kpi_component()
    elif comp_type == ComponentType.DOCUMENT_SEARCH:
        return _generate_search_component()
    elif comp_type == ComponentType.STATISTICS_TABLE:
        return _generate_table_component()
    else:
        return _generate_generic_component(comp_name)


def _generate_chat_component() -> str:
    """Generate Chat component."""
    return '''import React, { useState } from 'react';
import { Send } from 'lucide-react';
import { useChat } from '../hooks/useApi';

interface Message {
  role: 'user' | 'assistant';
  content: string;
}

export default function Chat({ title = "AI Assistant" }: { title?: string }) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const chat = useChat();

  const handleSend = async () => {
    if (!input.trim()) return;
    
    const userMessage = { role: 'user' as const, content: input };
    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    
    try {
      const response = await chat.mutateAsync(input);
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: response.response },
      ]);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: 'Sorry, I encountered an error.' },
      ]);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow p-4 col-span-2">
      <h3 className="font-semibold mb-4">{title}</h3>
      
      <div className="h-64 overflow-y-auto border rounded p-2 mb-4 space-y-2">
        {messages.map((msg, i) => (
          <div
            key={i}
            className={`p-2 rounded ${
              msg.role === 'user' ? 'bg-blue-100 ml-8' : 'bg-gray-100 mr-8'
            }`}
          >
            {msg.content}
          </div>
        ))}
        {chat.isPending && (
          <div className="bg-gray-100 mr-8 p-2 rounded animate-pulse">
            Thinking...
          </div>
        )}
      </div>
      
      <div className="flex gap-2">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && handleSend()}
          placeholder="Ask a question..."
          className="flex-1 border rounded px-3 py-2"
        />
        <button
          onClick={handleSend}
          disabled={chat.isPending}
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600 disabled:opacity-50"
        >
          <Send size={20} />
        </button>
      </div>
    </div>
  );
}
'''


def _generate_gauges_component(config: UseCaseConfig) -> str:
    """Generate LiveGauges component."""
    return '''import React from 'react';
import { Activity } from 'lucide-react';

interface GaugeProps {
  label: string;
  value: number;
  unit: string;
  min?: number;
  max?: number;
}

function Gauge({ label, value, unit, min = 0, max = 100 }: GaugeProps) {
  const percentage = ((value - min) / (max - min)) * 100;
  
  return (
    <div className="bg-gray-50 rounded p-3">
      <div className="text-sm text-gray-600">{label}</div>
      <div className="text-2xl font-bold">{value.toFixed(1)} <span className="text-sm font-normal">{unit}</span></div>
      <div className="w-full bg-gray-200 rounded-full h-2 mt-2">
        <div
          className="bg-blue-500 rounded-full h-2 transition-all"
          style={{ width: `${Math.min(100, Math.max(0, percentage))}%` }}
        />
      </div>
    </div>
  );
}

export default function LiveGauges({ title = "Live Parameters" }: { title?: string }) {
  // In production, this would use real-time data
  const gauges = [
    { label: 'Metric 1', value: 45.2, unit: 'units', max: 100 },
    { label: 'Metric 2', value: 78.5, unit: 'units', max: 150 },
    { label: 'Metric 3', value: 120, unit: 'rpm', max: 200 },
    { label: 'Metric 4', value: 15.8, unit: 'kNm', max: 50 },
  ];

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <div className="flex items-center gap-2 mb-4">
        <Activity className="text-green-500" size={20} />
        <h3 className="font-semibold">{title}</h3>
      </div>
      
      <div className="grid grid-cols-2 gap-3">
        {gauges.map((gauge) => (
          <Gauge key={gauge.label} {...gauge} />
        ))}
      </div>
    </div>
  );
}
'''


def _generate_alert_component() -> str:
    """Generate AlertFeed component."""
    return '''import React from 'react';
import { AlertTriangle, AlertCircle, Info } from 'lucide-react';

interface Alert {
  id: string;
  severity: 'warning' | 'error' | 'info';
  message: string;
  timestamp: string;
}

export default function AlertFeed({ title = "Active Alerts" }: { title?: string }) {
  // In production, this would come from the API
  const alerts: Alert[] = [
    { id: '1', severity: 'warning', message: 'Parameter approaching threshold', timestamp: '2 min ago' },
    { id: '2', severity: 'info', message: 'System operating normally', timestamp: '15 min ago' },
  ];

  const getIcon = (severity: string) => {
    switch (severity) {
      case 'error': return <AlertCircle className="text-red-500" size={20} />;
      case 'warning': return <AlertTriangle className="text-yellow-500" size={20} />;
      default: return <Info className="text-blue-500" size={20} />;
    }
  };

  return (
    <div className="bg-white rounded-lg shadow p-4">
      <h3 className="font-semibold mb-4">{title}</h3>
      
      <div className="space-y-2">
        {alerts.map((alert) => (
          <div key={alert.id} className="flex items-start gap-2 p-2 bg-gray-50 rounded">
            {getIcon(alert.severity)}
            <div className="flex-1">
              <p className="text-sm">{alert.message}</p>
              <p className="text-xs text-gray-500">{alert.timestamp}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
'''


def _generate_kpi_component() -> str:
    """Generate KPICards component."""
    return '''import React from 'react';
import { TrendingUp, TrendingDown } from 'lucide-react';
import { useSummary } from '../hooks/useApi';

export default function KPICards({ title }: { title?: string }) {
  const { data: summary, isLoading } = useSummary();

  const kpis = [
    { label: 'Total Entities', value: summary?.entity_count || 0, trend: 'up' },
    { label: 'Total Records', value: summary?.total_records?.toLocaleString() || '0', trend: 'up' },
    { label: 'Latest Update', value: summary?.latest_timestamp?.split('T')[0] || '-', trend: null },
  ];

  if (isLoading) {
    return <div className="bg-white rounded-lg shadow p-4">Loading...</div>;
  }

  return (
    <div className="bg-white rounded-lg shadow p-4 col-span-full">
      {title && <h3 className="font-semibold mb-4">{title}</h3>}
      
      <div className="grid grid-cols-3 gap-4">
        {kpis.map((kpi) => (
          <div key={kpi.label} className="text-center p-4 bg-gray-50 rounded">
            <div className="text-sm text-gray-600">{kpi.label}</div>
            <div className="text-2xl font-bold flex items-center justify-center gap-2">
              {kpi.value}
              {kpi.trend === 'up' && <TrendingUp className="text-green-500" size={20} />}
              {kpi.trend === 'down' && <TrendingDown className="text-red-500" size={20} />}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
'''


def _generate_search_component() -> str:
    """Generate DocumentSearch component."""
    return '''import React, { useState } from 'react';
import { Search } from 'lucide-react';
import { useSearch } from '../hooks/useApi';

export default function DocumentSearch({ title = "Search" }: { title?: string }) {
  const [query, setQuery] = useState('');
  const search = useSearch();

  const handleSearch = () => {
    if (query.trim()) {
      search.mutate(query);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow p-4 col-span-full">
      <h3 className="font-semibold mb-4">{title}</h3>
      
      <div className="flex gap-2 mb-4">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
          placeholder="Search documents..."
          className="flex-1 border rounded px-3 py-2"
        />
        <button
          onClick={handleSearch}
          disabled={search.isPending}
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600"
        >
          <Search size={20} />
        </button>
      </div>
      
      {search.isPending && <div className="text-gray-500">Searching...</div>}
      
      {search.data?.results && (
        <div className="space-y-3">
          {search.data.results.map((result: any, i: number) => (
            <div key={i} className="p-3 bg-gray-50 rounded">
              <div className="font-medium">{result.document_name}</div>
              <div className="text-sm text-gray-600 mt-1">{result.chunk_text}</div>
              <div className="text-xs text-gray-400 mt-1">
                Score: {result.score?.toFixed(3)} | Section: {result.section}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
'''


def _generate_table_component() -> str:
    """Generate StatisticsTable component."""
    return '''import React from 'react';
import { useEntities } from '../hooks/useApi';

export default function StatisticsTable({ title = "Statistics" }: { title?: string }) {
  const { data: entities, isLoading } = useEntities();

  if (isLoading) {
    return <div className="bg-white rounded-lg shadow p-4">Loading...</div>;
  }

  return (
    <div className="bg-white rounded-lg shadow p-4 col-span-full overflow-x-auto">
      <h3 className="font-semibold mb-4">{title}</h3>
      
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b">
            <th className="text-left p-2">Entity</th>
            <th className="text-right p-2">Records</th>
            <th className="text-right p-2">Latest Update</th>
          </tr>
        </thead>
        <tbody>
          {entities?.map((entity: any) => (
            <tr key={entity.entity_id} className="border-b hover:bg-gray-50">
              <td className="p-2 font-medium">{entity.entity_id}</td>
              <td className="p-2 text-right">{entity.record_count?.toLocaleString()}</td>
              <td className="p-2 text-right text-gray-500">
                {entity.latest_timestamp?.split('T')[0] || '-'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
'''


def _generate_generic_component(comp_name: str) -> str:
    """Generate a generic placeholder component."""
    return f'''import React from 'react';

export default function {comp_name}({{ title = "{comp_name}" }}: {{ title?: string }}) {{
  return (
    <div className="bg-white rounded-lg shadow p-4">
      <h3 className="font-semibold mb-4">{{title}}</h3>
      <div className="text-gray-500">
        Component placeholder - customize as needed
      </div>
    </div>
  );
}}
'''


def _generate_spcs_config(config: UseCaseConfig, output_dir: Path) -> None:
    """Generate SPCS deployment configuration."""
    app_name = config.app.name.lower().replace(" ", "-").replace("_", "-")
    compute_pool = config.app.deployment.compute_pool or "COMPUTE_POOL"
    
    spcs_yaml = f'''# SPCS Service Configuration for {config.app.name}
# Generated by Agentic Platform

spec:
  containers:
    - name: {app_name}
      image: /{config.snowflake.database}/spcs/image_repo/{app_name}:latest
      env:
        SNOWFLAKE_DATABASE: {config.snowflake.database}
        SNOWFLAKE_WAREHOUSE: {config.app.deployment.warehouse or "COMPUTE_WH"}
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
'''
    
    (output_dir / "spcs_service.yaml").write_text(spcs_yaml)
    
    dockerfile = f'''# Dockerfile for {config.app.name}
FROM python:3.11-slim

WORKDIR /app

# Install backend dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/ .

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
'''
    
    (output_dir / "Dockerfile").write_text(dockerfile)
