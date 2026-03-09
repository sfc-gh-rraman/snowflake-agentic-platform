"""App Code Generator agent - generates React + FastAPI applications."""

import json
import os
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class AppGeneratorState(StrEnum):
    APP_SPEC_GENERATION = "APP_SPEC_GENERATION"
    REACT_CODE_GENERATION = "REACT_CODE_GENERATION"
    FASTAPI_CODE_GENERATION = "FASTAPI_CODE_GENERATION"
    DEPLOYMENT_CONFIG_GENERATION = "DEPLOYMENT_CONFIG_GENERATION"
    TEST_AND_VALIDATE = "TEST_AND_VALIDATE"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass
class AppSpec:
    app_name: str
    description: str
    pages: list[dict[str, Any]]
    data_sources: list[dict[str, Any]]
    features: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "app_name": self.app_name,
            "description": self.description,
            "pages": self.pages,
            "data_sources": self.data_sources,
            "features": self.features,
        }


APP_SPEC_PROMPT = """Generate an application specification for a data app.

Use Case: {use_case}
Available Data Sources:
- Tables: {tables}
- ML Models: {models}
- Search Services: {search_services}
- Semantic Models: {semantic_models}

Generate a JSON app specification:
{{
    "app_name": "App Name",
    "description": "What the app does",
    "pages": [
        {{
            "name": "Page Name",
            "route": "/route",
            "components": ["DataTable", "Chart", "SearchBox", "Form"],
            "data_sources": [
                {{"type": "sql", "query": "SELECT..."}},
                {{"type": "ml_model", "model_name": "MODEL"}},
                {{"type": "cortex_search", "service_name": "SERVICE"}},
                {{"type": "cortex_analyst", "semantic_model": "MODEL"}}
            ]
        }}
    ],
    "features": ["search", "analytics", "ml_predictions"]
}}

Return ONLY the JSON object."""


REACT_COMPONENT_TEMPLATE = """import React, {{ useState, useEffect }} from 'react';
import {{ Box, Container, Typography, Paper, CircularProgress }} from '@mui/material';

interface {component_name}Props {{
  title?: string;
}}

export const {component_name}: React.FC<{component_name}Props> = ({{ title = "{title}" }}) => {{
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {{
    const fetchData = async () => {{
      try {{
        const response = await fetch('{api_endpoint}');
        if (!response.ok) throw new Error('Failed to fetch');
        const result = await response.json();
        setData(result.data || []);
      }} catch (err) {{
        setError(err instanceof Error ? err.message : 'Unknown error');
      }} finally {{
        setLoading(false);
      }}
    }};
    fetchData();
  }}, []);

  if (loading) return <CircularProgress />;
  if (error) return <Typography color="error">{{error}}</Typography>;

  return (
    <Container maxWidth="lg">
      <Paper sx={{ {{ p: 3, mt: 2 }} }}>
        <Typography variant="h5" gutterBottom>{{title}}</Typography>
        {content}
      </Paper>
    </Container>
  );
}};

export default {component_name};
"""


FASTAPI_ROUTE_TEMPLATE = """from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import os
import snowflake.connector

router = APIRouter(prefix="/{route_prefix}", tags=["{tag}"])


def get_connection():
    if os.path.exists("/snowflake/session/token"):
        with open("/snowflake/session/token", "r") as f:
            token = f.read()
        return snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            authenticator="oauth",
            token=token,
        )
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    )


class QueryRequest(BaseModel):
    query: str
    params: Optional[Dict[str, Any]] = None


class DataResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int


{endpoints}
"""


DOCKERFILE_TEMPLATE = """FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./backend/
COPY frontend/build/ ./static/

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
"""


class AppCodeGenerator:
    """Generate complete React + FastAPI applications."""

    def __init__(
        self,
        connection_name: str | None = None,
        model: str = "mistral-large2",
        output_dir: str = "./generated_app",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.model = model
        self.output_dir = output_dir
        self._state = AppGeneratorState.APP_SPEC_GENERATION
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self):
        if os.path.exists("/snowflake/session/token"):
            from snowflake.snowpark import Session

            return Session.builder.getOrCreate()
        else:
            import snowflake.connector

            conn = snowflake.connector.connect(connection_name=self.connection_name)
            return conn

    def _execute(self, sql: str) -> str:
        if hasattr(self.session, "sql"):
            result = self.session.sql(sql).collect()
            return result[0][0] if result else ""
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
                return row[0] if row else ""
            finally:
                cursor.close()

    def _escape(self, text: str) -> str:
        return text.replace("'", "''").replace("\\", "\\\\")

    def generate_app_spec(
        self,
        use_case: str,
        tables: list[str],
        models: list[str] = None,
        search_services: list[str] = None,
        semantic_models: list[str] = None,
    ) -> AppSpec:
        self._state = AppGeneratorState.APP_SPEC_GENERATION

        prompt = APP_SPEC_PROMPT.format(
            use_case=use_case,
            tables=", ".join(tables) if tables else "None",
            models=", ".join(models) if models else "None",
            search_services=", ".join(search_services) if search_services else "None",
            semantic_models=", ".join(semantic_models) if semantic_models else "None",
        )

        escaped_prompt = self._escape(prompt)
        sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{self.model}',
                '{escaped_prompt}'
            ) as RESPONSE
        """

        try:
            response = self._execute(sql)
            start = response.find("{")
            end = response.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(response[start:end])
            else:
                data = self._default_spec(use_case, tables)
        except Exception:
            data = self._default_spec(use_case, tables)

        return AppSpec(
            app_name=data.get("app_name", "Generated App"),
            description=data.get("description", "Auto-generated application"),
            pages=data.get("pages", []),
            data_sources=[],
            features=data.get("features", []),
        )

    def _default_spec(self, use_case: str, tables: list[str]) -> dict[str, Any]:
        return {
            "app_name": "Data Explorer",
            "description": f"Application for: {use_case}",
            "pages": [
                {
                    "name": "Dashboard",
                    "route": "/",
                    "components": ["DataTable", "Chart"],
                    "data_sources": [{"type": "sql", "table": tables[0] if tables else ""}],
                },
                {
                    "name": "Search",
                    "route": "/search",
                    "components": ["SearchBox", "Results"],
                    "data_sources": [{"type": "cortex_search"}],
                },
            ],
            "features": ["search", "analytics"],
        }

    def generate_react_components(self, spec: AppSpec) -> dict[str, str]:
        self._state = AppGeneratorState.REACT_CODE_GENERATION

        components = {}

        app_tsx = f"""import React from 'react';
import {{ BrowserRouter, Routes, Route }} from 'react-router-dom';
import {{ ThemeProvider, createTheme, CssBaseline }} from '@mui/material';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';

const theme = createTheme({{
  palette: {{
    mode: 'light',
    primary: {{ main: '#1976d2' }},
  }},
}});

const App: React.FC = () => {{
  return (
    <ThemeProvider theme={{theme}}>
      <CssBaseline />
      <BrowserRouter>
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6">{spec.app_name}</Typography>
          </Toolbar>
        </AppBar>
        <Routes>
          {self._generate_routes(spec.pages)}
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  );
}};

export default App;
"""
        components["App.tsx"] = app_tsx

        for page in spec.pages:
            component_name = page.get("name", "Page").replace(" ", "")
            route = page.get("route", "/")
            page_components = page.get("components", [])

            content = self._generate_page_content(page_components)

            component = REACT_COMPONENT_TEMPLATE.format(
                component_name=component_name,
                title=page.get("name", "Page"),
                api_endpoint=f"/api{route}",
                content=content,
            )
            components[f"pages/{component_name}.tsx"] = component

        return components

    def _generate_routes(self, pages: list[dict]) -> str:
        routes = []
        for page in pages:
            name = page.get("name", "Page").replace(" ", "")
            route = page.get("route", "/")
            routes.append(f'<Route path="{route}" element={{<{name} />}} />')
        return "\n          ".join(routes)

    def _generate_page_content(self, components: list[str]) -> str:
        content_parts = []

        if "DataTable" in components:
            content_parts.append("""<Box sx={{ mt: 2 }}>
          {data.map((row, idx) => (
            <Paper key={idx} sx={{ p: 1, mb: 1 }}>
              {JSON.stringify(row)}
            </Paper>
          ))}
        </Box>""")

        if "SearchBox" in components:
            content_parts.append("""<Box sx={{ mb: 2 }}>
          <input type="text" placeholder="Search..." style={{ width: '100%', padding: 8 }} />
        </Box>""")

        if "Chart" in components:
            content_parts.append("""<Box sx={{ height: 300 }}>
          <Typography>Chart visualization placeholder</Typography>
        </Box>""")

        return (
            "\n        ".join(content_parts)
            if content_parts
            else "<Typography>No data</Typography>"
        )

    def generate_fastapi_backend(self, spec: AppSpec) -> dict[str, str]:
        self._state = AppGeneratorState.FASTAPI_CODE_GENERATION

        files = {}

        main_py = f'''from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI(title="{spec.app_name}")

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
'''
        files["main.py"] = main_py

        data_endpoints = """
@router.get("/")
async def list_data() -> DataResponse:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM AGENTIC_PLATFORM.ANALYTICS.DATA LIMIT 100")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return DataResponse(data=rows, count=len(rows))
    finally:
        cursor.close()
        conn.close()


@router.post("/query")
async def execute_query(request: QueryRequest) -> DataResponse:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(request.query)
        columns = [col[0] for col in cursor.description] if cursor.description else []
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()] if columns else []
        return DataResponse(data=rows, count=len(rows))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()
"""
        files["routes/data.py"] = FASTAPI_ROUTE_TEMPLATE.format(
            route_prefix="data",
            tag="Data",
            endpoints=data_endpoints,
        )

        search_endpoints = '''
class SearchRequest(BaseModel):
    query: str
    limit: int = 10


@router.post("/")
async def search(request: SearchRequest) -> DataResponse:
    import json
    conn = get_connection()
    cursor = conn.cursor()
    try:
        search_spec = json.dumps({
            "query": request.query,
            "columns": [],
            "limit": request.limit,
        }).replace("'", "''")
        cursor.execute(f"""
            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'AGENTIC_PLATFORM.ANALYTICS.DOCUMENT_SEARCH',
                '{search_spec}'
            )
        """)
        result = cursor.fetchone()
        if result:
            data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
            return DataResponse(data=data.get("results", []), count=len(data.get("results", [])))
        return DataResponse(data=[], count=0)
    finally:
        cursor.close()
        conn.close()
'''
        files["routes/search.py"] = FASTAPI_ROUTE_TEMPLATE.format(
            route_prefix="search",
            tag="Search",
            endpoints=search_endpoints,
        )

        ml_endpoints = '''
class PredictRequest(BaseModel):
    model_name: str
    features: Dict[str, Any]


@router.post("/predict")
async def predict(request: PredictRequest) -> Dict[str, Any]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT * FROM {request.model_name}!PREDICT(
                {request.features}
            )
        """)
        result = cursor.fetchone()
        return {"prediction": result[0] if result else None}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()
'''
        files["routes/ml.py"] = FASTAPI_ROUTE_TEMPLATE.format(
            route_prefix="ml",
            tag="ML",
            endpoints=ml_endpoints,
        )

        files["routes/__init__.py"] = ""

        return files

    def generate_deployment_config(self, spec: AppSpec) -> dict[str, str]:
        self._state = AppGeneratorState.DEPLOYMENT_CONFIG_GENERATION

        files = {}

        files["Dockerfile"] = DOCKERFILE_TEMPLATE

        files["requirements.txt"] = """fastapi==0.109.0
uvicorn==0.27.0
snowflake-connector-python==3.6.0
pydantic==2.5.3
python-multipart==0.0.6
"""

        spcs_spec = {
            "spec": {
                "containers": [
                    {
                        "name": spec.app_name.lower().replace(" ", "-"),
                        "image": f"/AGENTIC_PLATFORM/ANALYTICS/APP_IMAGES/{spec.app_name.lower().replace(' ', '_')}:latest",
                        "env": {
                            "SNOWFLAKE_ACCOUNT": "{{ context().CURRENT_ACCOUNT }}",
                            "SNOWFLAKE_HOST": "{{ context().CURRENT_HOST }}",
                        },
                    }
                ],
                "endpoints": [
                    {
                        "name": "app",
                        "port": 8080,
                        "public": True,
                    }
                ],
            }
        }
        files["spcs_service.yaml"] = json.dumps(spcs_spec, indent=2)

        return files

    def generate(
        self,
        use_case: str,
        tables: list[str],
        models: list[str] = None,
        search_services: list[str] = None,
        semantic_models: list[str] = None,
    ) -> dict[str, Any]:
        result = {
            "status": "started",
            "files": {},
        }

        try:
            spec = self.generate_app_spec(
                use_case, tables, models, search_services, semantic_models
            )
            result["spec"] = spec.to_dict()

            react_files = self.generate_react_components(spec)
            for path, content in react_files.items():
                result["files"][f"frontend/src/{path}"] = content

            backend_files = self.generate_fastapi_backend(spec)
            for path, content in backend_files.items():
                result["files"][f"backend/{path}"] = content

            deploy_files = self.generate_deployment_config(spec)
            result["files"].update(deploy_files)

            self._state = AppGeneratorState.COMPLETE
            result["status"] = "complete"

        except Exception as e:
            self._state = AppGeneratorState.FAILED
            result["status"] = "failed"
            result["error"] = str(e)

        return result


def generate_app(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node function for app generation."""
    generator = AppCodeGenerator()

    use_case = state.get("use_case", "Data exploration app")
    tables = state.get("tables", [])
    models = state.get("models")
    search_services = state.get("search_services")
    semantic_models = state.get("semantic_models")

    result = generator.generate(
        use_case=use_case,
        tables=tables,
        models=models,
        search_services=search_services,
        semantic_models=semantic_models,
    )

    return {
        "app_result": result,
        "generated_files": result.get("files", {}),
        "current_state": generator._state.value,
        "messages": state.get("messages", [])
        + [
            {
                "role": "system",
                "content": f"Generated app with {len(result.get('files', {}))} files",
            }
        ],
    }
