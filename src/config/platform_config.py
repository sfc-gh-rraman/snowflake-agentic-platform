"""Platform configuration for Snowflake connections and database settings.

This module provides centralized configuration that all agents and components use.
Configuration can be set via:
1. Environment variables
2. UseCaseConfig passed at runtime
3. Default values

Priority: Runtime config > Environment variables > Defaults
"""

import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class SnowflakeSettings:
    """Snowflake connection and database settings."""

    database: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_DATABASE", "AGENTIC_PLATFORM")
    )
    warehouse: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    connection_name: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    )

    raw_schema: str = "RAW"
    curated_schema: str = "CURATED"
    ml_schema: str = "ML"
    docs_schema: str = "DOCS"
    analytics_schema: str = "ANALYTICS"
    cortex_schema: str = "CORTEX"
    orchestrator_schema: str = "ORCHESTRATOR"
    state_schema: str = "STATE"

    def __post_init__(self):
        """Load from environment variables if not set."""
        self.raw_schema = os.getenv("SNOWFLAKE_RAW_SCHEMA", self.raw_schema)
        self.curated_schema = os.getenv("SNOWFLAKE_CURATED_SCHEMA", self.curated_schema)
        self.ml_schema = os.getenv("SNOWFLAKE_ML_SCHEMA", self.ml_schema)
        self.docs_schema = os.getenv("SNOWFLAKE_DOCS_SCHEMA", self.docs_schema)
        self.orchestrator_schema = os.getenv(
            "SNOWFLAKE_ORCHESTRATOR_SCHEMA", self.orchestrator_schema
        )

    def get_full_table_name(self, schema: str, table: str) -> str:
        """Get fully qualified table name."""
        return f"{self.database}.{schema}.{table}"

    def get_state_table(self, table: str) -> str:
        """Get fully qualified state table name."""
        return self.get_full_table_name(self.orchestrator_schema, table)

    def get_ml_table(self, table: str) -> str:
        """Get fully qualified ML table name."""
        return self.get_full_table_name(self.ml_schema, table)

    def get_cortex_service(self, service: str) -> str:
        """Get fully qualified Cortex service name."""
        return self.get_full_table_name(self.cortex_schema, service)

    @classmethod
    def from_use_case_config(cls, config: dict[str, Any]) -> "SnowflakeSettings":
        """Create settings from a UseCaseConfig dict."""
        snowflake_config = config.get("snowflake", {})
        return cls(
            database=snowflake_config.get(
                "database", os.getenv("SNOWFLAKE_DATABASE", "AGENTIC_PLATFORM")
            ),
            raw_schema=snowflake_config.get("raw_schema", "RAW"),
            curated_schema=snowflake_config.get("curated_schema", "CURATED"),
            ml_schema=snowflake_config.get("ml_schema", "ML"),
            docs_schema=snowflake_config.get("docs_schema", "DOCS"),
            analytics_schema=snowflake_config.get("analytics_schema", "ANALYTICS"),
            cortex_schema=snowflake_config.get("cortex_schema", "CORTEX"),
            orchestrator_schema=snowflake_config.get("orchestrator_schema", "ORCHESTRATOR"),
        )


_settings: SnowflakeSettings | None = None


def get_settings() -> SnowflakeSettings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = SnowflakeSettings()
    return _settings


def set_settings(settings: SnowflakeSettings) -> None:
    """Set the global settings instance."""
    global _settings
    _settings = settings


def configure_from_use_case(config: dict[str, Any]) -> SnowflakeSettings:
    """Configure settings from a UseCaseConfig dict."""
    settings = SnowflakeSettings.from_use_case_config(config)
    set_settings(settings)
    return settings


def reset_settings() -> None:
    """Reset settings to defaults."""
    global _settings
    _settings = None
