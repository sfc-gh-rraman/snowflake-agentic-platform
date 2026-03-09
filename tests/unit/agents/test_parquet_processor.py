"""Unit tests for Parquet Processor agent."""

import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.unit
class TestParquetGraphState:
    """Tests for ParquetGraphState structure."""

    def test_initial_state_structure(self, sample_parquet_state):
        """Test that initial state has all required fields."""
        required_fields = [
            "stage_path", "target_schema", "database", "files", "schemas",
            "profiles", "quality_issues", "column_mappings", "tables_created",
            "current_state", "errors", "messages"
        ]
        for field in required_fields:
            assert field in sample_parquet_state

    def test_initial_state_values(self, sample_parquet_state):
        """Test initial state default values."""
        assert sample_parquet_state["current_state"] == "SCAN"
        assert sample_parquet_state["files"] == []
        assert sample_parquet_state["errors"] == []


@pytest.mark.unit
class TestParquetScanNode:
    """Tests for scan_files node function."""

    @patch("src.agents.preprocessing.parquet_graph.os.path.exists")
    def test_scan_node_updates_state(self, mock_exists, sample_parquet_state, mock_snowflake_connector):
        """Test scan node adds files to state."""
        mock_exists.return_value = False
        
        mock_snowflake_connector.return_value.cursor.return_value.fetchall.return_value = [
            ("file1.parquet", 1000, "2024-01-01"),
            ("file2.parquet", 2000, "2024-01-02"),
        ]
        mock_snowflake_connector.return_value.cursor.return_value.description = [
            ("name",), ("size",), ("last_modified",)
        ]

        from src.agents.preprocessing.parquet_graph import scan_files
        
        result = scan_files(sample_parquet_state)
        
        assert "files" in result
        assert result["current_state"] == "SCHEMA_INFER"

    def test_scan_node_handles_empty_stage(self, sample_parquet_state, mock_snowflake_connector):
        """Test scan node handles empty stage gracefully."""
        mock_snowflake_connector.return_value.cursor.return_value.fetchall.return_value = []
        
        from src.agents.preprocessing.parquet_graph import scan_files
        
        with patch("src.agents.preprocessing.parquet_graph.os.path.exists", return_value=False):
            result = scan_files(sample_parquet_state)
        
        assert result.get("files") == [] or "error" in str(result.get("errors", []))


@pytest.mark.unit
class TestParquetSchemaInferNode:
    """Tests for infer_schema node function."""

    def test_schema_infer_detects_types(self, sample_parquet_state, mock_snowflake_connector):
        """Test schema inference detects column types."""
        sample_parquet_state["files"] = [{"name": "test.parquet", "path": "@stage/test.parquet"}]
        
        mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("ID", "NUMBER"),
            ("TIMESTAMP", "TIMESTAMP_NTZ"),
            ("VALUE", "FLOAT"),
            ("CATEGORY", "VARCHAR"),
        ]
        mock_cursor.description = [("COLUMN_NAME",), ("DATA_TYPE",)]

        from src.agents.preprocessing.parquet_graph import infer_schema
        
        with patch("src.agents.preprocessing.parquet_graph.os.path.exists", return_value=False):
            result = infer_schema(sample_parquet_state)
        
        assert "schemas" in result
        assert result["current_state"] == "PROFILE"


@pytest.mark.unit
class TestParquetQualityNode:
    """Tests for quality_check node function."""

    def test_quality_check_detects_issues(self, sample_parquet_state, mock_snowflake_connector):
        """Test quality check identifies data issues."""
        sample_parquet_state["files"] = [{"name": "test.parquet"}]
        sample_parquet_state["schemas"] = {
            "test.parquet": {"ID": "NUMBER", "VALUE": "FLOAT"}
        }
        sample_parquet_state["profiles"] = {
            "test.parquet": {
                "row_count": 1000,
                "columns": {
                    "ID": {"null_count": 50, "unique_count": 950},
                    "VALUE": {"null_count": 100, "min": 0, "max": 1000},
                }
            }
        }

        from src.agents.preprocessing.parquet_graph import quality_check
        
        with patch("src.agents.preprocessing.parquet_graph.os.path.exists", return_value=False):
            result = quality_check(sample_parquet_state)
        
        assert "quality_issues" in result


@pytest.mark.unit
class TestParquetTransformNode:
    """Tests for transform node function."""

    def test_transform_generates_mappings(self, sample_parquet_state):
        """Test transform generates column mappings."""
        sample_parquet_state["schemas"] = {
            "test.parquet": {
                "user-id": "NUMBER",
                "First Name": "VARCHAR",
                "created_at": "TIMESTAMP_NTZ",
            }
        }

        from src.agents.preprocessing.parquet_graph import transform
        
        with patch("src.agents.preprocessing.parquet_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = ("{}",)
                result = transform(sample_parquet_state)
        
        assert "column_mappings" in result


@pytest.mark.unit
class TestParquetLoadNode:
    """Tests for load node function."""

    def test_load_creates_table(self, sample_parquet_state, mock_snowflake_connector):
        """Test load creates Snowflake table."""
        sample_parquet_state["files"] = [{"name": "test.parquet", "path": "@stage/test.parquet"}]
        sample_parquet_state["schemas"] = {
            "test.parquet": {"ID": "NUMBER", "VALUE": "FLOAT"}
        }
        sample_parquet_state["column_mappings"] = {
            "test.parquet": {"ID": "ID", "VALUE": "VALUE"}
        }

        from src.agents.preprocessing.parquet_graph import load
        
        with patch("src.agents.preprocessing.parquet_graph.os.path.exists", return_value=False):
            result = load(sample_parquet_state)
        
        assert "tables_created" in result
        assert result["current_state"] == "COMPLETE"


@pytest.mark.unit
class TestParquetGraphBuild:
    """Tests for graph construction."""

    def test_graph_builds_successfully(self):
        """Test that parquet graph builds without errors."""
        from src.agents.preprocessing.parquet_graph import build_parquet_graph
        
        graph = build_parquet_graph()
        assert graph is not None

    def test_graph_has_correct_nodes(self):
        """Test graph has all expected nodes."""
        from src.agents.preprocessing.parquet_graph import build_parquet_graph
        
        graph = build_parquet_graph()
        
        expected_nodes = ["scan", "schema_infer", "profile", "quality_check", "transform", "load"]
        for node in expected_nodes:
            assert node in graph.nodes
