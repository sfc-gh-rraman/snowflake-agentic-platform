"""Integration tests for the agentic platform."""

import tempfile
from pathlib import Path

import pytest


class TestUseCaseConfig:
    """Tests for use case configuration loading and validation."""

    def test_load_drilling_ops_template(self):
        """Test loading the drilling_ops template."""
        from src.config import list_templates, load_template

        templates = list_templates()
        assert "drilling_ops" in templates

        config = load_template("drilling_ops")

        assert config.domain.name == "Drilling Operations Intelligence"
        assert config.domain.industry.value == "oil_gas"
        assert len(config.personas) == 3
        assert len(config.data.structured) == 2
        assert len(config.data.unstructured) == 1
        assert len(config.agents) == 4
        assert len(config.ml_models) == 3
        assert len(config.app.pages) == 4

    def test_create_drilling_ops_example(self):
        """Test the factory function creates valid config."""
        from src.config import create_drilling_ops_example

        config = create_drilling_ops_example()

        assert config.version == "1.0"
        assert config.snowflake.database == "DRILLING_OPS_DB"
        assert config.has_real_time() is True
        assert config.has_search() is True
        assert config.has_ml() is True

    def test_save_and_load_config(self):
        """Test round-trip save and load."""
        from src.config import create_drilling_ops_example, load_use_case_yaml, save_use_case_yaml

        config = create_drilling_ops_example()

        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            temp_path = Path(f.name)

        try:
            save_use_case_yaml(config, temp_path)

            loaded = load_use_case_yaml(temp_path)

            assert loaded.domain.name == config.domain.name
            assert len(loaded.personas) == len(config.personas)
            assert len(loaded.agents) == len(config.agents)
        finally:
            temp_path.unlink()

    def test_validate_yaml_file(self):
        """Test YAML validation."""
        from src.config import get_template_path, validate_yaml_file

        template_path = get_template_path("drilling_ops")
        is_valid, error = validate_yaml_file(template_path)

        assert is_valid is True
        assert error is None


class TestDDLGenerator:
    """Tests for DDL generation."""

    def test_generate_ddls(self):
        """Test DDL generation from config."""
        from src.config import create_drilling_ops_example
        from src.generators import generate_ddls

        config = create_drilling_ops_example()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            files = generate_ddls(config, output_dir)

            assert len(files) >= 5
            assert (output_dir / "01_setup.sql").exists()
            assert (output_dir / "02_stages.sql").exists()
            assert (output_dir / "03_tables.sql").exists()
            assert (output_dir / "04_state_tables.sql").exists()
            assert (output_dir / "05_cortex_search.sql").exists()

            setup_sql = (output_dir / "01_setup.sql").read_text()
            assert "DRILLING_OPS_DB" in setup_sql
            assert "CREATE DATABASE" in setup_sql


class TestAppGenerator:
    """Tests for application code generation."""

    def test_generate_app(self):
        """Test app code generation from config."""
        from src.config import create_drilling_ops_example
        from src.generators import generate_app

        config = create_drilling_ops_example()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            generate_app(config, output_dir)

            assert (output_dir / "backend" / "main.py").exists()
            assert (output_dir / "backend" / "requirements.txt").exists()
            assert (output_dir / "backend" / "routes" / "chat.py").exists()
            assert (output_dir / "backend" / "routes" / "data.py").exists()
            assert (output_dir / "backend" / "routes" / "search.py").exists()

            assert (output_dir / "frontend" / "package.json").exists()
            assert (output_dir / "frontend" / "src" / "App.tsx").exists()

            main_py = (output_dir / "backend" / "main.py").read_text()
            assert "PETRA" in main_py or "Drilling" in main_py


class TestPlatformConfig:
    """Tests for platform configuration."""

    def test_default_settings(self):
        """Test default settings."""
        from src.config import get_settings, reset_settings

        reset_settings()
        settings = get_settings()

        assert settings.raw_schema == "RAW"
        assert settings.ml_schema == "ML"
        assert settings.orchestrator_schema == "ORCHESTRATOR"

    def test_configure_from_use_case(self):
        """Test configuring from use case."""
        from src.config import configure_from_use_case, get_settings, reset_settings

        reset_settings()

        config_dict = {
            "snowflake": {
                "database": "TEST_DB",
                "raw_schema": "TEST_RAW",
                "ml_schema": "TEST_ML",
            }
        }

        configure_from_use_case(config_dict)
        settings = get_settings()

        assert settings.database == "TEST_DB"
        assert settings.raw_schema == "TEST_RAW"
        assert settings.ml_schema == "TEST_ML"

        reset_settings()


class TestMetaAgentIntegration:
    """Integration tests for meta-agent with config."""

    def test_build_execution_plan(self):
        """Test execution plan generation from config."""
        from src.config import create_drilling_ops_example
        from src.meta_agent.graph import _build_execution_plan

        config = create_drilling_ops_example()
        plan = _build_execution_plan(config)

        assert plan["use_case"] == "Drilling Operations Intelligence"
        assert plan["total_phases"] >= 4

        phase_names = [p["phase"] for p in plan["phases"]]
        assert "infrastructure" in phase_names
        assert "document_processing" in phase_names
        assert "ml_models" in phase_names
        assert "app_generation" in phase_names
        assert "deployment" in phase_names

    def test_extract_requirements(self):
        """Test requirements extraction from config."""
        from src.config import create_drilling_ops_example
        from src.meta_agent.graph import _extract_requirements

        config = create_drilling_ops_example()
        reqs = _extract_requirements(config)

        assert reqs["domain"] == "Drilling Operations Intelligence"
        assert reqs["industry"] == "oil_gas"
        assert len(reqs["personas"]) == 3
        assert len(reqs["agents_needed"]) == 4
        assert len(reqs["ml_models"]) == 3

    def test_extract_data_profile(self):
        """Test data profile extraction from config."""
        from src.config import create_drilling_ops_example
        from src.meta_agent.graph import _extract_data_profile

        config = create_drilling_ops_example()
        profile = _extract_data_profile(config)

        assert profile["database"] == "DRILLING_OPS_DB"
        assert len(profile["structured_assets"]) == 2
        assert len(profile["unstructured_assets"]) == 1
        assert profile["schemas"]["raw"] == "RAW"


class TestObservability:
    """Tests for observability module."""

    def test_triple_logger_initialization(self):
        """Test TripleLogger can be initialized."""
        from src.observability import TripleLogger

        logger = TripleLogger(
            langsmith_project="test-project",
            snowflake_database="TEST_DB",
        )

        assert logger.langsmith is not None
        assert logger.langfuse is not None
        assert logger.snowflake is not None

    def test_create_logger_factory(self):
        """Test create_logger factory function."""
        from src.observability import create_logger

        logger = create_logger(database="TEST_DB", schema="TEST_SCHEMA")

        assert logger.snowflake.database == "TEST_DB"
        assert logger.snowflake.schema == "TEST_SCHEMA"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
