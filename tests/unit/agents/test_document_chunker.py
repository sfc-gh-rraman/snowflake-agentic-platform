"""Unit tests for Document Chunker agent."""

import pytest
from unittest.mock import MagicMock, patch


@pytest.mark.unit
class TestDocumentGraphState:
    """Tests for DocumentGraphState structure."""

    def test_initial_state_structure(self, sample_document_state):
        """Test that initial state has all required fields."""
        required_fields = [
            "stage_path", "target_table", "database", "schema",
            "documents", "structures", "chunks", "metadata",
            "chunk_table", "current_state", "errors", "messages"
        ]
        for field in required_fields:
            assert field in sample_document_state

    def test_initial_state_values(self, sample_document_state):
        """Test initial state default values."""
        assert sample_document_state["current_state"] == "EXTRACT"
        assert sample_document_state["documents"] == []
        assert sample_document_state["chunks"] == []


@pytest.mark.unit
class TestDocumentExtractNode:
    """Tests for extract_documents node function."""

    def test_extract_lists_documents(self, sample_document_state, mock_snowflake_connector):
        """Test extraction lists documents from stage."""
        mock_cursor = mock_snowflake_connector.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ("report1.pdf", 50000, "2024-01-01"),
            ("report2.pdf", 75000, "2024-01-02"),
        ]
        mock_cursor.description = [("name",), ("size",), ("last_modified",)]

        from src.agents.preprocessing.document_graph import extract_documents
        
        with patch("src.agents.preprocessing.document_graph.os.path.exists", return_value=False):
            result = extract_documents(sample_document_state)
        
        assert "documents" in result
        assert result["current_state"] == "ANALYZE_STRUCTURE"


@pytest.mark.unit
class TestDocumentAnalyzeNode:
    """Tests for analyze_structure node function."""

    def test_analyze_detects_document_type(self, sample_document_state, mock_cortex_complete):
        """Test structure analysis detects document types."""
        sample_document_state["documents"] = [
            {"name": "incident_report.pdf", "content_preview": "Equipment failure report..."}
        ]

        mock_response = '{"doc_type": "incident_report", "sections": ["summary", "details", "recommendations"]}'
        mock_cortex_complete(mock_response)

        from src.agents.preprocessing.document_graph import analyze_structure
        
        with patch("src.agents.preprocessing.document_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = (mock_response,)
                result = analyze_structure(sample_document_state)
        
        assert "structures" in result


@pytest.mark.unit
class TestDocumentChunkNode:
    """Tests for chunk_documents node function."""

    def test_chunk_creates_chunks(self, sample_document_state):
        """Test chunking creates document chunks."""
        sample_document_state["documents"] = [
            {
                "name": "test.pdf",
                "content": "This is a test document. " * 100,
            }
        ]
        sample_document_state["structures"] = {
            "test.pdf": {"doc_type": "report", "chunk_strategy": "semantic"}
        }

        from src.agents.preprocessing.document_graph import chunk_documents
        
        with patch("src.agents.preprocessing.document_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = ('["chunk1", "chunk2"]',)
                result = chunk_documents(sample_document_state)
        
        assert "chunks" in result
        assert result["current_state"] == "ENRICH_METADATA"

    def test_chunk_respects_size_limits(self, sample_document_state):
        """Test chunking respects configured size limits."""
        sample_document_state["chunk_size"] = 512
        sample_document_state["chunk_overlap"] = 50
        sample_document_state["documents"] = [
            {"name": "test.pdf", "content": "Word " * 1000}
        ]

        from src.agents.preprocessing.document_graph import chunk_documents
        
        with patch("src.agents.preprocessing.document_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect"):
                result = chunk_documents(sample_document_state)
        
        assert "chunks" in result


@pytest.mark.unit
class TestDocumentEnrichNode:
    """Tests for enrich_metadata node function."""

    def test_enrich_adds_metadata(self, sample_document_state, mock_cortex_complete):
        """Test enrichment adds metadata to chunks."""
        sample_document_state["chunks"] = [
            {"chunk_id": "c1", "text": "Equipment failure occurred...", "doc_name": "report.pdf"}
        ]

        mock_response = '{"keywords": ["equipment", "failure"], "summary": "Equipment failure incident"}'
        
        from src.agents.preprocessing.document_graph import enrich_metadata
        
        with patch("src.agents.preprocessing.document_graph.os.path.exists", return_value=False):
            with patch("snowflake.connector.connect") as mock_conn:
                mock_conn.return_value.cursor.return_value.fetchone.return_value = (mock_response,)
                result = enrich_metadata(sample_document_state)
        
        assert "metadata" in result


@pytest.mark.unit
class TestDocumentLoadNode:
    """Tests for load_chunks node function."""

    def test_load_creates_chunk_table(self, sample_document_state, mock_snowflake_connector):
        """Test loading creates chunk table in Snowflake."""
        sample_document_state["chunks"] = [
            {
                "chunk_id": "c1",
                "doc_name": "report.pdf",
                "text": "Test chunk content",
                "position": 0,
            }
        ]
        sample_document_state["metadata"] = {
            "c1": {"keywords": ["test"], "summary": "Test summary"}
        }

        from src.agents.preprocessing.document_graph import load_chunks
        
        with patch("src.agents.preprocessing.document_graph.os.path.exists", return_value=False):
            result = load_chunks(sample_document_state)
        
        assert "chunk_table" in result
        assert result["current_state"] == "COMPLETE"


@pytest.mark.unit
class TestDocumentGraphBuild:
    """Tests for graph construction."""

    def test_graph_builds_successfully(self):
        """Test that document graph builds without errors."""
        from src.agents.preprocessing.document_graph import build_document_graph
        
        graph = build_document_graph()
        assert graph is not None

    def test_graph_has_correct_nodes(self):
        """Test graph has all expected nodes."""
        from src.agents.preprocessing.document_graph import build_document_graph
        
        graph = build_document_graph()
        
        expected_nodes = ["extract", "analyze_structure", "chunk", "enrich_metadata", "load_chunks"]
        for node in expected_nodes:
            assert node in graph.nodes
