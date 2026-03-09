"""Unit tests for Snowflake Checkpoint Saver."""

import pytest
from unittest.mock import MagicMock, patch
import json


@pytest.mark.unit
class TestSnowflakeCheckpointSaver:
    """Tests for SnowflakeCheckpointSaver."""

    def test_saver_initializes(self):
        """Test checkpoint saver initializes correctly."""
        with patch("src.state.snowflake_checkpoint.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(
                connection_name="default",
                database="TEST_DB",
                orchestrator_schema="ORCHESTRATOR",
            )
            
            from src.state.snowflake_checkpoint import SnowflakeCheckpointSaver
            
            saver = SnowflakeCheckpointSaver()
            assert saver.database == "TEST_DB"
            assert saver.schema == "ORCHESTRATOR"
            assert saver.table == "LANGGRAPH_CHECKPOINTS"

    def test_saver_put_creates_checkpoint(self, mock_snowflake_connector):
        """Test put method creates checkpoint in Snowflake."""
        with patch("src.state.snowflake_checkpoint.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(
                connection_name="default",
                database="TEST_DB",
                orchestrator_schema="ORCHESTRATOR",
            )
            
            from src.state.snowflake_checkpoint import SnowflakeCheckpointSaver
            
            saver = SnowflakeCheckpointSaver()
            
            config = {"configurable": {"thread_id": "test-thread"}}
            checkpoint = {
                "v": 1,
                "ts": "2024-01-01T00:00:00",
                "channel_values": {"key": "value"},
                "channel_versions": {},
                "versions_seen": {},
            }
            metadata = {}
            
            with patch.object(saver, "_create_session", return_value=mock_snowflake_connector.return_value):
                result = saver.put(config, checkpoint, metadata)
            
            assert "configurable" in result
            assert "checkpoint_id" in result["configurable"]

    def test_saver_get_retrieves_checkpoint(self, mock_snowflake_connector):
        """Test get_tuple retrieves checkpoint from Snowflake."""
        with patch("src.state.snowflake_checkpoint.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(
                connection_name="default",
                database="TEST_DB",
                orchestrator_schema="ORCHESTRATOR",
            )
            
            from src.state.snowflake_checkpoint import SnowflakeCheckpointSaver
            
            saver = SnowflakeCheckpointSaver()
            
            checkpoint_data = {
                "v": 1,
                "ts": "2024-01-01T00:00:00",
                "channel_values": {"test": "data"},
                "channel_versions": {},
                "versions_seen": {},
            }
            
            mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
            mock_cursor.fetchall.return_value = [{
                "CHECKPOINT_ID": "cp-123",
                "THREAD_ID": "test-thread",
                "PARENT_CHECKPOINT_ID": None,
                "CHECKPOINT_DATA": json.dumps(checkpoint_data),
                "METADATA": "{}",
                "CREATED_AT": None,
            }]
            mock_cursor.description = [
                ("CHECKPOINT_ID",), ("THREAD_ID",), ("PARENT_CHECKPOINT_ID",),
                ("CHECKPOINT_DATA",), ("METADATA",), ("CREATED_AT",),
            ]
            
            config = {"configurable": {"thread_id": "test-thread"}}
            
            with patch.object(saver, "_create_session", return_value=mock_snowflake_connector.return_value):
                with patch.object(saver, "_execute") as mock_execute:
                    mock_execute.return_value = [{
                        "CHECKPOINT_ID": "cp-123",
                        "THREAD_ID": "test-thread",
                        "PARENT_CHECKPOINT_ID": None,
                        "CHECKPOINT_DATA": checkpoint_data,
                        "METADATA": {},
                        "CREATED_AT": None,
                    }]
                    result = saver.get_tuple(config)
            
            assert result is not None
            assert result.checkpoint["channel_values"] == {"test": "data"}

    def test_saver_list_returns_checkpoints(self, mock_snowflake_connector):
        """Test list method returns checkpoint iterator."""
        with patch("src.state.snowflake_checkpoint.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(
                connection_name="default",
                database="TEST_DB",
                orchestrator_schema="ORCHESTRATOR",
            )
            
            from src.state.snowflake_checkpoint import SnowflakeCheckpointSaver
            
            saver = SnowflakeCheckpointSaver()
            
            config = {"configurable": {"thread_id": "test-thread"}}
            
            with patch.object(saver, "_execute") as mock_execute:
                mock_execute.return_value = [
                    {
                        "CHECKPOINT_ID": "cp-1",
                        "THREAD_ID": "test-thread",
                        "PARENT_CHECKPOINT_ID": None,
                        "CHECKPOINT_DATA": {"v": 1, "channel_values": {}},
                        "METADATA": {},
                        "CREATED_AT": None,
                    },
                    {
                        "CHECKPOINT_ID": "cp-2",
                        "THREAD_ID": "test-thread",
                        "PARENT_CHECKPOINT_ID": "cp-1",
                        "CHECKPOINT_DATA": {"v": 1, "channel_values": {}},
                        "METADATA": {},
                        "CREATED_AT": None,
                    },
                ]
                
                results = list(saver.list(config))
            
            assert len(results) == 2

    def test_saver_handles_spcs_environment(self):
        """Test saver uses SPCS session when available."""
        with patch("src.state.snowflake_checkpoint.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(
                connection_name="default",
                database="TEST_DB",
                orchestrator_schema="ORCHESTRATOR",
            )
            
            with patch("src.state.snowflake_checkpoint.os.path.exists") as mock_exists:
                mock_exists.return_value = True
                
                with patch("snowflake.snowpark.Session") as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.builder.getOrCreate.return_value = mock_session
                    
                    from src.state.snowflake_checkpoint import SnowflakeCheckpointSaver
                    
                    saver = SnowflakeCheckpointSaver()
                    session = saver._create_session()
                    
                    mock_session_class.builder.getOrCreate.assert_called_once()
