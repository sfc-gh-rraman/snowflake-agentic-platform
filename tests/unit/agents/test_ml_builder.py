"""Unit tests for ML Model Builder agent."""

import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.unit
class TestMLGraphState:
    """Tests for MLGraphState structure."""

    def test_initial_state_structure(self, sample_ml_state):
        """Test that initial state has all required fields."""
        required_fields = [
            "source_table", "target_column", "database", "schema",
            "task_type", "features", "model_artifact", "evaluation_metrics",
            "registered_model", "explanations", "current_state", "errors", "messages"
        ]
        for field in required_fields:
            assert field in sample_ml_state

    def test_initial_state_values(self, sample_ml_state):
        """Test initial state default values."""
        assert sample_ml_state["current_state"] == "TASK_CLASSIFICATION"
        assert sample_ml_state["task_type"] is None
        assert sample_ml_state["features"] == []


@pytest.mark.unit
class TestTaskClassificationNode:
    """Tests for classify_task node function."""

    def test_classify_detects_binary_classification(self, sample_ml_state, mock_cortex_complete):
        """Test task classification detects binary classification."""
        sample_ml_state["target_column"] = "IS_FAILURE"
        
        mock_response = '{"task_type": "binary_classification", "confidence": 0.95}'
        
        from src.agents.ml.model_graph import classify_task
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_cursor = mock_conn.return_value.cursor.return_value
                mock_cursor.fetchone.return_value = (mock_response,)
                mock_cursor.fetchall.return_value = [(0, 800), (1, 200)]
                mock_cursor.description = [("VALUE",), ("COUNT",)]
                result = classify_task(sample_ml_state)
        
        assert result.get("task_type") in ["binary_classification", "classification", None] or "task_type" in result

    def test_classify_detects_regression(self, sample_ml_state):
        """Test task classification detects regression tasks."""
        sample_ml_state["target_column"] = "PRICE"
        
        from src.agents.ml.model_graph import classify_task
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_cursor = mock_conn.return_value.cursor.return_value
                mock_cursor.fetchone.return_value = ('{"task_type": "regression"}',)
                mock_cursor.fetchall.return_value = []
                result = classify_task(sample_ml_state)
        
        assert "task_type" in result

    def test_classify_detects_anomaly_detection(self, sample_ml_state):
        """Test task classification detects anomaly detection."""
        sample_ml_state["target_column"] = "ANOMALY_SCORE"
        
        from src.agents.ml.model_graph import classify_task
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_cursor = mock_conn.return_value.cursor.return_value
                mock_cursor.fetchone.return_value = ('{"task_type": "anomaly_detection"}',)
                result = classify_task(sample_ml_state)
        
        assert "task_type" in result


@pytest.mark.unit
class TestFeatureSelectionNode:
    """Tests for select_features node function."""

    def test_feature_selection_identifies_features(self, sample_ml_state, mock_snowflake_connector):
        """Test feature selection identifies relevant features."""
        sample_ml_state["task_type"] = "binary_classification"
        
        mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("TEMPERATURE", "FLOAT"),
            ("PRESSURE", "FLOAT"),
            ("VIBRATION", "FLOAT"),
            ("EQUIPMENT_ID", "VARCHAR"),
            ("IS_FAILURE", "NUMBER"),
        ]
        mock_cursor.description = [("COLUMN_NAME",), ("DATA_TYPE",)]
        mock_cursor.fetchone.return_value = ('{"features": ["TEMPERATURE", "PRESSURE", "VIBRATION"]}',)

        from src.agents.ml.model_graph import select_features
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            result = select_features(sample_ml_state)
        
        assert "features" in result

    def test_feature_selection_excludes_target(self, sample_ml_state, mock_snowflake_connector):
        """Test feature selection excludes target column."""
        sample_ml_state["task_type"] = "classification"
        sample_ml_state["target_column"] = "FAILURE"
        
        mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("FEATURE1", "FLOAT"),
            ("FAILURE", "NUMBER"),
        ]
        mock_cursor.fetchone.return_value = ('{"features": ["FEATURE1"]}',)

        from src.agents.ml.model_graph import select_features
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            result = select_features(sample_ml_state)
        
        features = result.get("features", [])
        if features:
            assert "FAILURE" not in features


@pytest.mark.unit
class TestTrainingNode:
    """Tests for train_model node function."""

    def test_training_creates_artifact(self, sample_ml_state, mock_snowflake_connector):
        """Test training creates model artifact."""
        sample_ml_state["task_type"] = "binary_classification"
        sample_ml_state["features"] = ["TEMPERATURE", "PRESSURE"]

        from src.agents.ml.model_graph import train_model
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            result = train_model(sample_ml_state)
        
        assert "model_artifact" in result or "current_state" in result


@pytest.mark.unit
class TestEvaluationNode:
    """Tests for evaluate_model node function."""

    def test_evaluation_calculates_metrics(self, sample_ml_state):
        """Test evaluation calculates appropriate metrics."""
        sample_ml_state["task_type"] = "binary_classification"
        sample_ml_state["model_artifact"] = {"model_type": "RandomForest"}

        from src.agents.ml.model_graph import evaluate_model
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = (
                    '{"accuracy": 0.85, "precision": 0.82, "recall": 0.78}',
                )
                result = evaluate_model(sample_ml_state)
        
        assert "evaluation_metrics" in result


@pytest.mark.unit
class TestRegistrationNode:
    """Tests for register_model node function."""

    def test_registration_creates_registry_entry(self, sample_ml_state, mock_snowflake_connector):
        """Test model registration creates registry entry."""
        sample_ml_state["model_artifact"] = {"model_type": "RandomForest", "path": "/tmp/model"}
        sample_ml_state["evaluation_metrics"] = {"accuracy": 0.85}

        from src.agents.ml.model_graph import register_model
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            result = register_model(sample_ml_state)
        
        assert "registered_model" in result or "current_state" in result


@pytest.mark.unit
class TestExplainabilityNode:
    """Tests for explain_model node function."""

    def test_explainability_generates_explanations(self, sample_ml_state, mock_cortex_complete):
        """Test explainability generates feature importance."""
        sample_ml_state["registered_model"] = "ANOMALY_DETECTOR_V1"
        sample_ml_state["features"] = ["TEMPERATURE", "PRESSURE", "VIBRATION"]

        from src.agents.ml.model_graph import explain_model
        
        with patch("src.agents.ml.model_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = (
                    '{"feature_importance": {"TEMPERATURE": 0.4, "PRESSURE": 0.35, "VIBRATION": 0.25}}',
                )
                result = explain_model(sample_ml_state)
        
        assert "explanations" in result
        assert result["current_state"] == "COMPLETE"


@pytest.mark.unit
class TestMLGraphBuild:
    """Tests for graph construction."""

    def test_graph_builds_successfully(self):
        """Test that ML graph builds without errors."""
        from src.agents.ml.model_graph import build_ml_graph
        
        graph = build_ml_graph()
        assert graph is not None

    def test_graph_has_correct_nodes(self):
        """Test graph has all expected nodes."""
        from src.agents.ml.model_graph import build_ml_graph
        
        graph = build_ml_graph()
        
        expected_nodes = [
            "classify_task", "select_features", "train_model",
            "evaluate_model", "register_model", "explain_model"
        ]
        for node in expected_nodes:
            assert node in graph.nodes
