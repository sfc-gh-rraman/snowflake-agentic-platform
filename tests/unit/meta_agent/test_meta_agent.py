"""Unit tests for Meta-Agent graph."""

from unittest.mock import patch

import pytest


@pytest.mark.unit
class TestMetaAgentState:
    """Tests for MetaAgentState structure."""

    def test_state_has_required_fields(self):
        """Test MetaAgentState has all required fields."""
        from src.meta_agent.state import MetaAgentState

        required_fields = [
            "use_case_description",
            "data_locations",
            "data_assets",
            "parsed_requirements",
            "data_profile",
            "available_agents",
            "execution_plan",
            "approval_status",
            "current_phase",
        ]

        state: MetaAgentState = {
            "use_case_description": "Test use case",
            "data_locations": [],
            "data_assets": [],
            "parsed_requirements": None,
            "data_profile": None,
            "available_agents": [],
            "execution_plan": None,
            "approval_status": "pending",
            "approval_feedback": None,
            "current_phase": "start",
            "error": None,
            "messages": [],
        }

        for field in required_fields:
            assert field in state


@pytest.mark.unit
class TestUseCaseParser:
    """Tests for use case parser tool."""

    def test_parser_extracts_intent(self, mock_cortex_complete):
        """Test parser extracts intent from description."""
        from src.meta_agent.tools.use_case_parser import parse_use_case

        state = {
            "use_case_description": "I need to predict equipment failures from sensor data",
            "messages": [],
        }

        with patch("src.meta_agent.tools.use_case_parser.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = (
                    '{"domain": "industrial", "intent": "predictive_maintenance", "ml_tasks": ["anomaly_detection"]}',
                )
                result = parse_use_case(state)

        assert "parsed_requirements" in result

    def test_parser_handles_empty_description(self):
        """Test parser handles empty description gracefully."""
        from src.meta_agent.tools.use_case_parser import parse_use_case

        state = {
            "use_case_description": "",
            "messages": [],
        }

        with patch("src.meta_agent.tools.use_case_parser.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = ("{}",)
                result = parse_use_case(state)

        assert "error" in result or "parsed_requirements" in result


@pytest.mark.unit
class TestDataScanner:
    """Tests for data scanner tool."""

    def test_scanner_lists_files(self, mock_snowflake_connector):
        """Test scanner lists files from stage."""
        from src.meta_agent.tools.data_scanner import scan_data

        state = {
            "data_locations": ["@RAW.DATA_STAGE"],
            "messages": [],
        }

        mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("sensors.parquet", 1000000, "2024-01-01"),
            ("reports.pdf", 500000, "2024-01-02"),
        ]
        mock_cursor.description = [("name",), ("size",), ("last_modified",)]

        with patch("src.meta_agent.tools.data_scanner.os.path.exists", return_value=False):
            result = scan_data(state)

        assert "data_assets" in result or "data_profile" in result

    def test_scanner_handles_multiple_locations(self, mock_snowflake_connector):
        """Test scanner handles multiple data locations."""
        from src.meta_agent.tools.data_scanner import scan_data

        state = {
            "data_locations": ["@RAW.DATA_STAGE", "@RAW.DOCS_STAGE"],
            "messages": [],
        }

        mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = []

        with patch("src.meta_agent.tools.data_scanner.os.path.exists", return_value=False):
            result = scan_data(state)

        assert "data_assets" in result or "data_profile" in result


@pytest.mark.unit
class TestAgentRegistryQuery:
    """Tests for agent registry query tool."""

    def test_registry_query_finds_agents(
        self, sample_agent_registry_entries, mock_snowflake_connector
    ):
        """Test registry query finds matching agents."""
        from src.meta_agent.tools.agent_registry_query import query_registry

        state = {
            "parsed_requirements": {
                "intent": "data_processing",
                "data_types": ["parquet", "pdf"],
            },
            "data_profile": {
                "structured_assets": [{"type": "parquet"}],
                "unstructured_assets": [{"doc_type": "pdf"}],
            },
            "messages": [],
        }

        mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            (entry["AGENT_ID"], entry["NAME"], entry["CAPABILITIES"])
            for entry in sample_agent_registry_entries
        ]

        with patch("src.meta_agent.tools.agent_registry_query.os.path.exists", return_value=False):
            result = query_registry(state)

        assert "available_agents" in result


@pytest.mark.unit
class TestPlanGenerator:
    """Tests for plan generator tool."""

    def test_generator_creates_valid_plan(self, sample_execution_plan, mock_cortex_complete):
        """Test plan generator creates valid execution plan."""
        from src.meta_agent.tools.plan_generator import generate_plan

        state = {
            "parsed_requirements": {"domain": "industrial", "ml_tasks": ["anomaly_detection"]},
            "data_profile": {"structured_assets": [{"name": "sensors"}]},
            "available_agents": ["parquet_processor", "ml_model_builder"],
            "messages": [],
        }

        with patch("src.meta_agent.tools.plan_generator.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                import json

                mock_conn.return_value.cursor.return_value.fetchone.return_value = (
                    json.dumps(sample_execution_plan),
                )
                result = generate_plan(state)

        assert "execution_plan" in result

    def test_generator_plan_has_phases(self, mock_cortex_complete):
        """Test generated plan has required phases."""
        from src.meta_agent.tools.plan_generator import generate_plan

        state = {
            "parsed_requirements": {"domain": "test"},
            "data_profile": {"structured_assets": []},
            "available_agents": ["parquet_processor"],
            "messages": [],
        }

        with patch("src.meta_agent.tools.plan_generator.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = (
                    '{"phases": [{"phase": "test"}]}',
                )
                result = generate_plan(state)

        plan = result.get("execution_plan")
        if plan:
            assert "phases" in plan


@pytest.mark.unit
class TestMetaAgentGraph:
    """Tests for meta-agent graph construction."""

    def test_graph_builds_successfully(self):
        """Test meta-agent graph builds without errors."""
        from src.meta_agent.graph import create_meta_agent_graph

        graph = create_meta_agent_graph()
        assert graph is not None

    def test_graph_has_correct_flow(self):
        """Test graph has correct node flow."""
        from src.meta_agent.graph import create_meta_agent_graph

        graph = create_meta_agent_graph()

        expected_nodes = [
            "parse_use_case",
            "scan_data",
            "query_registry",
            "generate_plan",
            "human_approval",
            "execute_plan",
        ]

        for node in expected_nodes:
            assert node in graph.nodes


@pytest.mark.unit
class TestApprovalFlow:
    """Tests for human approval flow."""

    def test_auto_approve_skips_waiting(self):
        """Test auto-approve skips waiting for approval."""
        from src.meta_agent.approval import should_wait_for_approval

        state = {"approval_status": "approved"}
        result = should_wait_for_approval(state)

        assert result == "execute_plan"

    def test_pending_waits_for_approval(self):
        """Test pending status waits for approval."""
        from src.meta_agent.approval import should_wait_for_approval

        state = {"approval_status": "pending"}
        result = should_wait_for_approval(state)

        assert result == "await_approval"

    def test_rejected_ends_flow(self):
        """Test rejected status ends the flow."""
        from src.meta_agent.approval import should_wait_for_approval

        state = {"approval_status": "rejected"}
        result = should_wait_for_approval(state)

        assert result == "end"
