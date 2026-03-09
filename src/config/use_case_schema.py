"""Rich use case schema for agentic platform configuration.

This module defines the comprehensive schema for specifying use cases,
going far beyond simple "predict X from Y" to capture:
- Domain context and industry
- Target personas and their needs
- Structured and unstructured data assets
- Multi-agent architecture (Watchdog/Historian/Advisor pattern)
- ML model specifications with business value
- Full application UI/UX requirements
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator


class Industry(str, Enum):
    OIL_GAS = "oil_gas"
    MANUFACTURING = "manufacturing"
    HEALTHCARE = "healthcare"
    FINANCE = "finance"
    RETAIL = "retail"
    LOGISTICS = "logistics"
    UTILITIES = "utilities"
    TELECOMMUNICATIONS = "telecommunications"
    OTHER = "other"


class DataType(str, Enum):
    TIME_SERIES = "time_series"
    EVENT = "event"
    TRANSACTIONAL = "transactional"
    MASTER = "master"
    REFERENCE = "reference"
    SURVEY = "survey"


class DocumentType(str, Enum):
    PDF = "pdf"
    DOCX = "docx"
    TXT = "txt"
    MD = "markdown"
    HTML = "html"
    MIXED = "mixed"


class AgentType(str, Enum):
    WATCHDOG = "watchdog"
    HISTORIAN = "historian"
    ADVISOR = "advisor"
    ORCHESTRATOR = "orchestrator"
    ANALYST = "analyst"
    CUSTOM = "custom"


class MLTaskType(str, Enum):
    BINARY_CLASSIFICATION = "binary_classification"
    MULTI_CLASS_CLASSIFICATION = "multi_class_classification"
    REGRESSION = "regression"
    ANOMALY_DETECTION = "anomaly_detection"
    TIME_SERIES_FORECASTING = "time_series_forecasting"
    CLUSTERING = "clustering"


class ComponentType(str, Enum):
    LIVE_GAUGES = "live_gauges"
    ALERT_FEED = "alert_feed"
    CHAT = "chat"
    CONTEXT_PANEL = "context_panel"
    MULTI_LINE_CHART = "multi_line_chart"
    DAYS_VS_DEPTH = "days_vs_depth"
    STATISTICS_TABLE = "statistics_table"
    DOCUMENT_SEARCH = "document_search"
    AI_BRIEFING = "ai_briefing"
    KPI_CARDS = "kpi_cards"
    HEATMAP = "heatmap"
    PARAMETER_COMPARISON = "parameter_comparison"


class PageLayout(str, Enum):
    COMMAND_CENTER = "command_center"
    FLEET_OVERVIEW = "fleet_overview"
    KNOWLEDGE_BASE = "knowledge_base"
    DASHBOARD = "dashboard"
    SINGLE_COLUMN = "single_column"


class Domain(BaseModel):
    """Domain context for the use case."""
    name: str = Field(..., description="Human-readable domain name", examples=["Drilling Operations Intelligence"])
    industry: Industry = Field(..., description="Industry vertical")
    description: str = Field(..., description="Detailed description of the domain and use case")
    
    @field_validator('description')
    @classmethod
    def description_min_length(cls, v: str) -> str:
        if len(v) < 50:
            raise ValueError('Description should be at least 50 characters to capture sufficient context')
        return v


class PersonaNeed(BaseModel):
    """A specific need for a persona."""
    description: str
    priority: int = Field(default=1, ge=1, le=3, description="1=critical, 2=important, 3=nice-to-have")


class Persona(BaseModel):
    """Target user persona."""
    role: str = Field(..., description="Job title or role", examples=["Driller", "Plant Manager", "Quality Engineer"])
    department: Optional[str] = Field(None, description="Department or team")
    needs: List[str] = Field(..., min_length=1, description="What this persona needs from the system")
    primary_page: Optional[str] = Field(None, description="Main page this persona will use")


class StructuredDataAsset(BaseModel):
    """Structured data source (tables, parquet, CSV)."""
    name: str = Field(..., description="Logical name for this dataset")
    location: str = Field(..., description="Stage path or table name", examples=["@RAW.SENSOR_STAGE/*.parquet", "RAW.EVENTS"])
    data_type: DataType = Field(..., description="Type of data")
    volume: Optional[str] = Field(None, description="Approximate volume", examples=["12.5M rows", "500K records"])
    key_columns: List[str] = Field(default_factory=list, description="Primary/natural key columns")
    time_column: Optional[str] = Field(None, description="Timestamp column for time-series data")
    entity_column: Optional[str] = Field(None, description="Entity identifier column (well_id, machine_id, etc.)")
    measures: List[str] = Field(default_factory=list, description="Numeric measure columns")
    dimensions: List[str] = Field(default_factory=list, description="Categorical dimension columns")
    label_column: Optional[str] = Field(None, description="Target/label column for ML")


class UnstructuredDataAsset(BaseModel):
    """Unstructured data source (documents, PDFs)."""
    name: str = Field(..., description="Logical name for this dataset")
    location: str = Field(..., description="Stage path", examples=["@RAW.REPORTS_STAGE/*.pdf"])
    doc_type: DocumentType = Field(..., description="Document format")
    count: Optional[int] = Field(None, description="Approximate document count")
    content_description: str = Field(..., description="What these documents contain")
    chunk_size: int = Field(default=512, description="Target chunk size for RAG")
    chunk_overlap: int = Field(default=50, description="Chunk overlap for context preservation")


class DataAssets(BaseModel):
    """All data assets for the use case."""
    structured: List[StructuredDataAsset] = Field(default_factory=list)
    unstructured: List[UnstructuredDataAsset] = Field(default_factory=list)
    
    @field_validator('structured', 'unstructured')
    @classmethod
    def at_least_one_asset(cls, v: list, info) -> list:
        return v


class AgentCapability(BaseModel):
    """A specific capability an agent provides."""
    name: str
    description: str
    tools: List[str] = Field(default_factory=list, description="Cortex tools used")


class AgentSpec(BaseModel):
    """Specification for an agent in the multi-agent architecture."""
    name: str = Field(..., description="Agent name", examples=["Watchdog", "Historian", "Advisor"])
    agent_type: AgentType = Field(..., description="Agent archetype")
    purpose: str = Field(..., description="What this agent does")
    capabilities: List[str] = Field(default_factory=list, description="List of capabilities")
    tools: List[str] = Field(default_factory=list, description="Cortex/Snowflake tools", examples=["cortex_search", "cortex_analyst", "sql_query", "model_inference"])
    triggers: List[str] = Field(default_factory=list, description="What triggers this agent", examples=["anomaly_detection", "user_query", "threshold_breach"])
    alerts_on: List[str] = Field(default_factory=list, description="For watchdog: what conditions trigger alerts")
    sources: List[str] = Field(default_factory=list, description="Data sources this agent accesses")


class MLModelSpec(BaseModel):
    """Specification for an ML model."""
    name: str = Field(..., description="Model name", examples=["stuck_pipe_predictor", "defect_classifier"])
    task: MLTaskType = Field(..., description="ML task type")
    target: str = Field(..., description="Target column or variable")
    features: List[str] = Field(default_factory=list, description="Feature columns (auto-detected if empty)")
    source_table: Optional[str] = Field(None, description="Source table for training data")
    business_value: str = Field(..., description="Business impact of this model")
    threshold: Optional[float] = Field(None, description="Decision threshold for classification")
    retrain_schedule: Optional[str] = Field(None, description="Retraining schedule", examples=["weekly", "monthly"])


class UIComponent(BaseModel):
    """A UI component in a page."""
    component_type: ComponentType = Field(..., description="Type of component")
    title: Optional[str] = Field(None, description="Component title")
    metrics: List[str] = Field(default_factory=list, description="Metrics to display")
    refresh_ms: Optional[int] = Field(None, description="Refresh interval in milliseconds")
    source: Optional[str] = Field(None, description="Data source (agent, table, service)")
    config: Dict[str, Any] = Field(default_factory=dict, description="Additional configuration")


class PageSpec(BaseModel):
    """Specification for an application page."""
    name: str = Field(..., description="Page name", examples=["Command Center", "Fleet Overview"])
    route: str = Field(..., description="URL route", examples=["/", "/fleet", "/search"])
    layout: PageLayout = Field(..., description="Page layout template")
    description: Optional[str] = Field(None, description="What this page is for")
    components: List[UIComponent] = Field(default_factory=list, description="Components on this page")
    primary_for: List[str] = Field(default_factory=list, description="Which personas primarily use this page")


class APIEndpoint(BaseModel):
    """API endpoint specification."""
    path: str = Field(..., description="API path", examples=["/api/chat", "/api/fleet/summary"])
    method: str = Field(default="GET", description="HTTP method")
    handler: str = Field(..., description="Handler type", examples=["cortex_agent", "sql_query", "cortex_search", "model_inference"])
    description: Optional[str] = Field(None)
    config: Dict[str, Any] = Field(default_factory=dict)


class RealTimeConfig(BaseModel):
    """Real-time/streaming configuration."""
    enabled: bool = Field(default=False)
    websocket: bool = Field(default=False)
    refresh_interval_ms: int = Field(default=2000, description="Default refresh interval")
    parameters: List[str] = Field(default_factory=list, description="Parameters to stream")


class DeploymentConfig(BaseModel):
    """Deployment configuration."""
    target: str = Field(default="spcs", description="Deployment target")
    compute_pool: Optional[str] = Field(None, description="SPCS compute pool")
    warehouse: Optional[str] = Field(None, description="Snowflake warehouse")
    auth: str = Field(default="oauth", description="Authentication method")
    min_instances: int = Field(default=1)
    max_instances: int = Field(default=1)


class AppSpec(BaseModel):
    """Full application specification."""
    name: str = Field(..., description="Application name", examples=["Drilling Co-Pilot", "Quality Intelligence"])
    description: Optional[str] = Field(None)
    pages: List[PageSpec] = Field(default_factory=list)
    api_endpoints: List[APIEndpoint] = Field(default_factory=list)
    real_time: RealTimeConfig = Field(default_factory=RealTimeConfig)
    deployment: DeploymentConfig = Field(default_factory=DeploymentConfig)


class CortexSearchService(BaseModel):
    """Cortex Search service specification."""
    name: str = Field(..., description="Service name")
    source: str = Field(..., description="Source data asset name")
    embedding_model: str = Field(default="e5-base-v2")
    chunk_column: str = Field(default="chunk_text")
    columns_to_return: List[str] = Field(default_factory=list)


class CortexAnalystModel(BaseModel):
    """Cortex Analyst semantic model specification."""
    name: str = Field(..., description="Semantic model name")
    tables: List[str] = Field(..., description="Tables to include")
    verified_queries: List[Dict[str, str]] = Field(default_factory=list)


class CortexAgentConfig(BaseModel):
    """Cortex Agent configuration."""
    name: str = Field(..., description="Agent name")
    tools: List[str] = Field(default_factory=list, description="Tools available to agent")
    system_prompt: Optional[str] = Field(None)


class CortexServices(BaseModel):
    """All Cortex services for the use case."""
    search: List[CortexSearchService] = Field(default_factory=list)
    analyst: List[CortexAnalystModel] = Field(default_factory=list)
    agents: List[CortexAgentConfig] = Field(default_factory=list)


class SnowflakeConfig(BaseModel):
    """Snowflake connection and schema configuration."""
    database: str = Field(..., description="Target database")
    raw_schema: str = Field(default="RAW", description="Schema for raw data")
    curated_schema: str = Field(default="CURATED", description="Schema for curated data")
    ml_schema: str = Field(default="ML", description="Schema for ML artifacts")
    docs_schema: str = Field(default="DOCS", description="Schema for document chunks")
    analytics_schema: str = Field(default="ANALYTICS", description="Schema for analytics views")
    cortex_schema: str = Field(default="CORTEX", description="Schema for Cortex services")
    orchestrator_schema: str = Field(default="ORCHESTRATOR", description="Schema for orchestration state")


class UseCaseConfig(BaseModel):
    """Complete use case configuration.
    
    This is the master schema that captures everything needed to generate
    a comprehensive AI application like PETRA.
    """
    version: str = Field(default="1.0", description="Schema version")
    
    domain: Domain = Field(..., description="Domain context")
    personas: List[Persona] = Field(..., min_length=1, description="Target user personas")
    
    snowflake: SnowflakeConfig = Field(..., description="Snowflake configuration")
    data: DataAssets = Field(..., description="Data assets")
    
    agents: List[AgentSpec] = Field(default_factory=list, description="Multi-agent architecture")
    ml_models: List[MLModelSpec] = Field(default_factory=list, description="ML model specifications")
    
    cortex_services: CortexServices = Field(default_factory=CortexServices, description="Cortex service configs")
    app: AppSpec = Field(..., description="Application specification")
    
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    def has_real_time(self) -> bool:
        return self.app.real_time.enabled

    def has_search(self) -> bool:
        return len(self.data.unstructured) > 0

    def has_ml(self) -> bool:
        return len(self.ml_models) > 0

    def get_watchdog_agents(self) -> List[AgentSpec]:
        return [a for a in self.agents if a.agent_type == AgentType.WATCHDOG]

    def get_primary_structured_data(self) -> Optional[StructuredDataAsset]:
        time_series = [d for d in self.data.structured if d.data_type == DataType.TIME_SERIES]
        return time_series[0] if time_series else (self.data.structured[0] if self.data.structured else None)


def create_drilling_ops_example() -> UseCaseConfig:
    """Create the drilling_ops PETRA configuration as a reference example."""
    return UseCaseConfig(
        version="1.0",
        domain=Domain(
            name="Drilling Operations Intelligence",
            industry=Industry.OIL_GAS,
            description="""
            An intelligent co-pilot that watches drilling operations, remembers everything 
            that ever happened across 8 wells, proactively warns about potential issues 
            like stuck pipe, explains why based on historical incidents, and suggests 
            optimal drilling parameters based on offset well analysis.
            """
        ),
        personas=[
            Persona(
                role="Driller / Toolpusher",
                department="Operations",
                needs=[
                    "Real-time parameter monitoring with anomaly alerts",
                    "What should I do about this torque spike?",
                    "What happened last time at this depth?",
                ],
                primary_page="Command Center"
            ),
            Persona(
                role="Drilling Engineer",
                department="Engineering",
                needs=[
                    "How can I drill this section faster?",
                    "What parameters worked best for offset wells?",
                    "Generate the daily drilling report",
                ],
                primary_page="Fleet Overview"
            ),
            Persona(
                role="Drilling Superintendent",
                department="Management",
                needs=[
                    "How is my fleet performing?",
                    "What lessons have we learned?",
                    "Brief me for the morning meeting",
                ],
                primary_page="Toolbox Talk"
            ),
        ],
        snowflake=SnowflakeConfig(
            database="DRILLING_OPS_DB",
            raw_schema="RAW",
            curated_schema="CURATED",
            ml_schema="ML",
            docs_schema="DOCS",
            analytics_schema="ANALYTICS",
            cortex_schema="CORTEX",
            orchestrator_schema="ORCHESTRATOR",
        ),
        data=DataAssets(
            structured=[
                StructuredDataAsset(
                    name="drilling_time",
                    location="CURATED.DRILLING_TIME",
                    data_type=DataType.TIME_SERIES,
                    volume="12.5M rows",
                    key_columns=["WELL_NAME", "TIMESTAMP"],
                    time_column="TIMESTAMP",
                    entity_column="WELL_NAME",
                    measures=["ROP_M_HR", "WOB_KKGF", "RPM", "TORQUE_KNM", "SPP_KPA", "HOOKLOAD_KKGF", "MUD_FLOW_IN_LPM"],
                    dimensions=["WELL_NAME"],
                    label_column="STUCK_PIPE_FLAG",
                ),
                StructuredDataAsset(
                    name="drilling_depth",
                    location="CURATED.DRILLING_DEPTH",
                    data_type=DataType.SURVEY,
                    volume="737K rows",
                    key_columns=["WELL_NAME", "MEASURED_DEPTH_M"],
                    entity_column="WELL_NAME",
                    measures=["MEASURED_DEPTH_M", "TVD_M", "INCLINATION_DEG", "AZIMUTH_DEG", "D_EXPONENT"],
                ),
            ],
            unstructured=[
                UnstructuredDataAsset(
                    name="daily_drilling_reports",
                    location="@RAW.DOCUMENTS_STAGE/*.pdf",
                    doc_type=DocumentType.PDF,
                    count=1759,
                    content_description="Daily drilling reports with hourly operational narratives, incidents, lessons learned",
                    chunk_size=512,
                    chunk_overlap=50,
                ),
            ],
        ),
        agents=[
            AgentSpec(
                name="Watchdog",
                agent_type=AgentType.WATCHDOG,
                purpose="Continuous real-time monitoring and proactive alerting",
                capabilities=[
                    "Monitor current depth vs plan",
                    "Detect parameter anomalies (torque, SPP, ROP trends)",
                    "Pattern match against historical incidents",
                    "Generate proactive warnings before problems occur",
                ],
                tools=["sql_query", "model_inference"],
                alerts_on=["torque_spike", "pressure_anomaly", "rop_deviation", "stuck_pipe_risk"],
            ),
            AgentSpec(
                name="Historian",
                agent_type=AgentType.HISTORIAN,
                purpose="Institutional memory and knowledge retrieval",
                capabilities=[
                    "Search 1,759 daily drilling reports",
                    "Find similar historical incidents",
                    "Retrieve lessons learned",
                ],
                tools=["cortex_search", "sql_query"],
                sources=["daily_drilling_reports", "drilling_time"],
            ),
            AgentSpec(
                name="Advisor",
                agent_type=AgentType.ADVISOR,
                purpose="Decision support with explainable recommendations",
                capabilities=[
                    "Parameter recommendations based on offset wells",
                    "Risk assessment for current operation",
                    "Step-by-step procedures with context",
                    "What-if scenario analysis",
                ],
                tools=["cortex_analyst", "sql_query", "model_inference"],
                sources=["drilling_time", "drilling_depth"],
            ),
            AgentSpec(
                name="Orchestrator",
                agent_type=AgentType.ORCHESTRATOR,
                purpose="Coordinate multi-agent responses to user queries",
                capabilities=[
                    "Route queries to appropriate specialist agent",
                    "Combine insights from multiple agents",
                    "Maintain conversation context",
                ],
                tools=["cortex_search", "cortex_analyst", "model_inference"],
            ),
        ],
        ml_models=[
            MLModelSpec(
                name="stuck_pipe_predictor",
                task=MLTaskType.BINARY_CLASSIFICATION,
                target="STUCK_PIPE_FLAG",
                features=["ROP_M_HR", "WOB_KKGF", "RPM", "TORQUE_KNM", "SPP_KPA", "HOOKLOAD_KKGF"],
                source_table="CURATED.DRILLING_TIME",
                business_value="Avoid $500K-2M per stuck pipe incident",
                threshold=0.7,
            ),
            MLModelSpec(
                name="rop_optimizer",
                task=MLTaskType.REGRESSION,
                target="ROP_M_HR",
                features=["WOB_KKGF", "RPM", "TORQUE_KNM", "MUD_FLOW_IN_LPM", "HOLE_DEPTH_MD_M"],
                source_table="CURATED.DRILLING_TIME",
                business_value="15% ROP improvement = days saved per well",
            ),
            MLModelSpec(
                name="kick_detector",
                task=MLTaskType.BINARY_CLASSIFICATION,
                target="KICK_FLAG",
                features=["MUD_FLOW_IN_LPM", "MUD_DENSITY_IN", "MUD_DENSITY_OUT", "SPP_KPA"],
                source_table="CURATED.DRILLING_TIME",
                business_value="Safety critical - prevent blowouts",
                threshold=0.5,
            ),
        ],
        cortex_services=CortexServices(
            search=[
                CortexSearchService(
                    name="DDR_SEARCH_SERVICE",
                    source="daily_drilling_reports",
                    embedding_model="e5-base-v2",
                    columns_to_return=["well_name", "report_date", "chunk_text", "section"],
                ),
            ],
            analyst=[
                CortexAnalystModel(
                    name="DRILLING_ANALYST",
                    tables=["DRILLING_TIME", "DRILLING_DEPTH", "MODEL_METRICS", "FEATURE_IMPORTANCE"],
                ),
            ],
            agents=[
                CortexAgentConfig(
                    name="DRILLING_COPILOT",
                    tools=["DDR_SEARCH_SERVICE", "DRILLING_ANALYST", "stuck_pipe_predictor"],
                ),
            ],
        ),
        app=AppSpec(
            name="PETRA - Drilling Co-Pilot",
            description="Predictive Engineering & Technical Response Assistant",
            pages=[
                PageSpec(
                    name="Command Center",
                    route="/",
                    layout=PageLayout.COMMAND_CENTER,
                    description="Real-time drilling operations monitoring with chat",
                    components=[
                        UIComponent(
                            component_type=ComponentType.LIVE_GAUGES,
                            title="Live Parameters",
                            metrics=["rop", "wob", "rpm", "torque", "spp", "hookload"],
                            refresh_ms=2000,
                        ),
                        UIComponent(
                            component_type=ComponentType.ALERT_FEED,
                            title="Active Alerts",
                            source="watchdog_agent",
                        ),
                        UIComponent(
                            component_type=ComponentType.CHAT,
                            title="Co-Pilot Chat",
                            source="orchestrator_agent",
                        ),
                        UIComponent(
                            component_type=ComponentType.CONTEXT_PANEL,
                            title="Context",
                            config={"dynamic": True},
                        ),
                    ],
                    primary_for=["Driller / Toolpusher"],
                ),
                PageSpec(
                    name="Fleet Overview",
                    route="/fleet",
                    layout=PageLayout.FLEET_OVERVIEW,
                    description="Multi-well performance comparison",
                    components=[
                        UIComponent(
                            component_type=ComponentType.KPI_CARDS,
                            metrics=["total_wells", "total_depth", "avg_rop", "total_readings"],
                        ),
                        UIComponent(
                            component_type=ComponentType.DAYS_VS_DEPTH,
                            title="Days vs Depth",
                            config={"wells": "all", "y_reversed": True},
                        ),
                        UIComponent(
                            component_type=ComponentType.PARAMETER_COMPARISON,
                            title="Parameter vs Depth",
                            metrics=["rop", "wob", "rpm", "torque"],
                        ),
                        UIComponent(
                            component_type=ComponentType.STATISTICS_TABLE,
                            title="Well Statistics",
                        ),
                    ],
                    primary_for=["Drilling Engineer"],
                ),
                PageSpec(
                    name="Knowledge Base",
                    route="/search",
                    layout=PageLayout.KNOWLEDGE_BASE,
                    description="Semantic search across drilling reports",
                    components=[
                        UIComponent(
                            component_type=ComponentType.DOCUMENT_SEARCH,
                            title="DDR Search",
                            source="DDR_SEARCH_SERVICE",
                            config={"highlight_matches": True, "result_display": "card"},
                        ),
                    ],
                    primary_for=["Drilling Engineer", "Drilling Superintendent"],
                ),
                PageSpec(
                    name="Toolbox Talk",
                    route="/briefing",
                    layout=PageLayout.SINGLE_COLUMN,
                    description="AI-generated shift briefings",
                    components=[
                        UIComponent(
                            component_type=ComponentType.AI_BRIEFING,
                            title="Daily Briefing",
                            config={
                                "template": "toolbox_talk",
                                "sources": ["recent_incidents", "current_conditions", "lessons_learned"],
                            },
                        ),
                    ],
                    primary_for=["Drilling Superintendent"],
                ),
            ],
            api_endpoints=[
                APIEndpoint(path="/api/chat", method="POST", handler="cortex_agent"),
                APIEndpoint(path="/api/fleet/summary", method="GET", handler="sql_query"),
                APIEndpoint(path="/api/fleet/days-vs-depth", method="GET", handler="sql_query"),
                APIEndpoint(path="/api/fleet/well/{well_id}/timeseries", method="GET", handler="sql_query"),
                APIEndpoint(path="/api/search", method="POST", handler="cortex_search"),
                APIEndpoint(path="/api/predict/stuck_pipe", method="POST", handler="model_inference"),
                APIEndpoint(path="/api/briefing/generate", method="POST", handler="cortex_llm"),
            ],
            real_time=RealTimeConfig(
                enabled=True,
                websocket=True,
                refresh_interval_ms=2000,
                parameters=["rop", "wob", "rpm", "torque", "spp", "depth"],
            ),
            deployment=DeploymentConfig(
                target="spcs",
                compute_pool="PETRA_COMPUTE_POOL",
                warehouse="COMPUTE_WH",
                auth="oauth",
            ),
        ),
    )
