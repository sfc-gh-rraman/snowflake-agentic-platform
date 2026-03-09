"""YAML loader for use case configurations.

Loads and validates use case configurations from YAML files.
"""

from pathlib import Path
from typing import Any

import yaml

from .use_case_schema import UseCaseConfig


def load_use_case_yaml(path: str | Path) -> UseCaseConfig:
    """Load a use case configuration from a YAML file.

    Args:
        path: Path to the YAML file

    Returns:
        Validated UseCaseConfig instance

    Raises:
        FileNotFoundError: If the file doesn't exist
        yaml.YAMLError: If the YAML is invalid
        pydantic.ValidationError: If the config doesn't match the schema
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Use case config not found: {path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    return UseCaseConfig(**data)


def save_use_case_yaml(config: UseCaseConfig, path: str | Path) -> None:
    """Save a use case configuration to a YAML file.

    Args:
        config: UseCaseConfig instance to save
        path: Path to save the YAML file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data = config.model_dump(mode="json", exclude_none=True)

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def get_template_path(template_name: str) -> Path:
    """Get the path to a built-in template.

    Args:
        template_name: Name of the template (e.g., "drilling_ops")

    Returns:
        Path to the template YAML file
    """
    templates_dir = Path(__file__).parent.parent.parent / "config" / "templates"
    return templates_dir / f"{template_name}.yaml"


def list_templates() -> list[str]:
    """List available built-in templates.

    Returns:
        List of template names (without .yaml extension)
    """
    templates_dir = Path(__file__).parent.parent.parent / "config" / "templates"

    if not templates_dir.exists():
        return []

    return [f.stem for f in templates_dir.glob("*.yaml")]


def load_template(template_name: str) -> UseCaseConfig:
    """Load a built-in template.

    Args:
        template_name: Name of the template (e.g., "drilling_ops")

    Returns:
        UseCaseConfig instance from the template
    """
    path = get_template_path(template_name)
    return load_use_case_yaml(path)


def create_blank_template() -> dict[str, Any]:
    """Create a blank template dictionary for users to fill in.

    Returns:
        Dictionary with the structure of a use case config
    """
    return {
        "version": "1.0",
        "domain": {
            "name": "YOUR_DOMAIN_NAME",
            "industry": "manufacturing",  # Options: oil_gas, manufacturing, healthcare, etc.
            "description": "Describe your use case in detail (minimum 50 characters)",
        },
        "personas": [
            {
                "role": "Operator",
                "department": "Operations",
                "needs": [
                    "Real-time monitoring",
                    "Alert notifications",
                ],
                "primary_page": "Command Center",
            }
        ],
        "snowflake": {
            "database": "YOUR_DATABASE",
            "raw_schema": "RAW",
            "curated_schema": "CURATED",
            "ml_schema": "ML",
            "docs_schema": "DOCS",
        },
        "data": {
            "structured": [
                {
                    "name": "main_data",
                    "location": "@RAW.DATA_STAGE/*.parquet",
                    "data_type": "time_series",
                    "time_column": "TIMESTAMP",
                    "measures": ["metric1", "metric2"],
                }
            ],
            "unstructured": [],
        },
        "agents": [
            {
                "name": "Watchdog",
                "agent_type": "watchdog",
                "purpose": "Real-time monitoring",
                "tools": ["sql_query"],
            }
        ],
        "ml_models": [],
        "app": {
            "name": "Your Application Name",
            "pages": [{"name": "Dashboard", "route": "/", "layout": "dashboard", "components": []}],
            "deployment": {"target": "spcs"},
        },
    }


def validate_yaml_file(path: str | Path) -> tuple[bool, str | None]:
    """Validate a YAML file against the use case schema.

    Args:
        path: Path to the YAML file

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        load_use_case_yaml(path)
        return True, None
    except FileNotFoundError as e:
        return False, str(e)
    except yaml.YAMLError as e:
        return False, f"Invalid YAML syntax: {e}"
    except Exception as e:
        return False, f"Validation error: {e}"
