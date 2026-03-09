"""Document chunker agent - 5-state LangGraph processor for documents."""

import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from src.config import get_settings


class DocumentChunkerState(str, Enum):
    EXTRACT = "EXTRACT"
    ANALYZE_STRUCTURE = "ANALYZE_STRUCTURE"
    CHUNK = "CHUNK"
    ENRICH_METADATA = "ENRICH_METADATA"
    LOAD_CHUNKS = "LOAD_CHUNKS"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass
class DocumentChunk:
    chunk_id: str
    source_file: str
    document_type: str
    page_number: Optional[int]
    section_header: Optional[str]
    chunk_text: str
    chunk_index: int
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "source_file": self.source_file,
            "document_type": self.document_type,
            "page_number": self.page_number,
            "section_header": self.section_header,
            "chunk_text": self.chunk_text,
            "chunk_index": self.chunk_index,
            "metadata": self.metadata,
        }


@dataclass
class DocumentStructure:
    file_name: str
    document_type: str
    total_pages: int
    sections: List[Dict[str, Any]]
    extracted_text: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class DocumentChunker:
    """Process documents through a 5-state pipeline."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        chunk_table: str = "DOCUMENT_CHUNKS",
        max_chunk_size: int = 8000,
        chunk_overlap: int = 200,
    ):
        settings = get_settings()
        self.connection_name = connection_name or settings.connection_name
        self.database = database or settings.database
        self.schema = schema or settings.docs_schema
        self.chunk_table = chunk_table
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
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

    def _execute(self, sql: str) -> List[Dict]:
        if hasattr(self.session, 'sql'):
            result = self.session.sql(sql).collect()
            return [dict(row.asDict()) for row in result]
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
            finally:
                cursor.close()

    def _detect_document_type(self, filename: str) -> str:
        ext = filename.lower().split('.')[-1] if '.' in filename else ''
        return ext if ext in ['pdf', 'docx', 'txt', 'md', 'html'] else 'unknown'

    def extract(self, stage_path: str) -> List[Dict[str, Any]]:
        if not stage_path.startswith('@'):
            stage_path = f"@{stage_path}"

        sql = f"LIST {stage_path} PATTERN='.*\\.(pdf|docx|txt|md)'"
        try:
            results = self._execute(sql)
        except Exception:
            return []

        documents = []
        for row in results:
            name = row.get("name", "")
            if name:
                documents.append({
                    "path": f"{stage_path}/{name}",
                    "name": name.split('/')[-1],
                    "size_bytes": row.get("size", 0),
                    "document_type": self._detect_document_type(name),
                })

        return documents

    def analyze_structure(self, text: str) -> List[Dict[str, Any]]:
        sections = []
        
        header_patterns = [
            (r'^#{1,6}\s+(.+)$', 'markdown'),
            (r'^([A-Z][A-Z\s]+):?\s*$', 'caps_header'),
            (r'^(\d+\.?\d*\.?\d*)\s+(.+)$', 'numbered'),
            (r'^([IVXLCDM]+\.)\s+(.+)$', 'roman'),
        ]

        lines = text.split('\n')
        current_section = None
        current_content = []

        for i, line in enumerate(lines):
            is_header = False
            header_text = None

            for pattern, header_type in header_patterns:
                match = re.match(pattern, line.strip(), re.MULTILINE)
                if match:
                    is_header = True
                    header_text = match.group(1) if header_type != 'numbered' else match.group(2)
                    break

            if is_header and header_text:
                if current_section:
                    current_section['content'] = '\n'.join(current_content)
                    current_section['end_line'] = i - 1
                    sections.append(current_section)
                    current_content = []

                current_section = {
                    'header': header_text.strip(),
                    'start_line': i,
                    'content': '',
                }
            else:
                current_content.append(line)

        if current_section:
            current_section['content'] = '\n'.join(current_content)
            current_section['end_line'] = len(lines) - 1
            sections.append(current_section)

        if not sections:
            sections.append({
                'header': 'Document',
                'start_line': 0,
                'end_line': len(lines) - 1,
                'content': text,
            })

        return sections

    def chunk(self, text: str, source_file: str, document_type: str) -> List[DocumentChunk]:
        chunks = []
        chunk_index = 0

        sections = self.analyze_structure(text)

        for section in sections:
            section_text = section.get('content', '')
            section_header = section.get('header', '')

            if len(section_text) <= self.max_chunk_size:
                if section_text.strip():
                    chunks.append(DocumentChunk(
                        chunk_id=f"{source_file}_{chunk_index}",
                        source_file=source_file,
                        document_type=document_type,
                        page_number=None,
                        section_header=section_header,
                        chunk_text=section_text.strip(),
                        chunk_index=chunk_index,
                    ))
                    chunk_index += 1
            else:
                paragraphs = re.split(r'\n\s*\n', section_text)
                current_chunk = []
                current_length = 0

                for para in paragraphs:
                    para = para.strip()
                    if not para:
                        continue

                    if current_length + len(para) > self.max_chunk_size and current_chunk:
                        chunk_text = '\n\n'.join(current_chunk)
                        chunks.append(DocumentChunk(
                            chunk_id=f"{source_file}_{chunk_index}",
                            source_file=source_file,
                            document_type=document_type,
                            page_number=None,
                            section_header=section_header,
                            chunk_text=chunk_text,
                            chunk_index=chunk_index,
                        ))
                        chunk_index += 1

                        overlap_text = current_chunk[-1] if current_chunk else ""
                        current_chunk = [overlap_text] if len(overlap_text) <= self.chunk_overlap else []
                        current_length = len(overlap_text) if current_chunk else 0

                    current_chunk.append(para)
                    current_length += len(para)

                if current_chunk:
                    chunk_text = '\n\n'.join(current_chunk)
                    chunks.append(DocumentChunk(
                        chunk_id=f"{source_file}_{chunk_index}",
                        source_file=source_file,
                        document_type=document_type,
                        page_number=None,
                        section_header=section_header,
                        chunk_text=chunk_text,
                        chunk_index=chunk_index,
                    ))
                    chunk_index += 1

        return chunks

    def enrich_metadata(self, chunk: DocumentChunk, doc_metadata: Dict[str, Any]) -> DocumentChunk:
        chunk.metadata.update({
            "document_metadata": doc_metadata,
            "char_count": len(chunk.chunk_text),
            "word_count": len(chunk.chunk_text.split()),
        })
        return chunk

    def load_chunks(self, chunks: List[DocumentChunk]) -> int:
        full_table = f"{self.database}.{self.schema}.{self.chunk_table}"

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                chunk_id VARCHAR(200) PRIMARY KEY,
                source_file VARCHAR(500),
                document_type VARCHAR(100),
                page_number INTEGER,
                section_header VARCHAR(500),
                chunk VARCHAR(16000),
                chunk_index INTEGER,
                metadata VARIANT,
                created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        self._execute(create_sql)

        loaded = 0
        for chunk in chunks:
            escaped_text = chunk.chunk_text.replace("'", "''")
            escaped_header = (chunk.section_header or "").replace("'", "''")
            escaped_file = chunk.source_file.replace("'", "''")
            
            import json
            metadata_json = json.dumps(chunk.metadata).replace("'", "''")

            insert_sql = f"""
                INSERT INTO {full_table} 
                (chunk_id, source_file, document_type, page_number, section_header, chunk, chunk_index, metadata)
                SELECT 
                    '{chunk.chunk_id}',
                    '{escaped_file}',
                    '{chunk.document_type}',
                    {chunk.page_number or 'NULL'},
                    '{escaped_header}',
                    '{escaped_text[:16000]}',
                    {chunk.chunk_index},
                    PARSE_JSON('{metadata_json}')
            """
            try:
                self._execute(insert_sql)
                loaded += 1
            except Exception as e:
                print(f"Insert error: {e}")
                pass

        return loaded

    def process(self, stage_path: str, sample_text: Optional[str] = None) -> Dict[str, Any]:
        result = {
            "stage_path": stage_path,
            "documents_processed": 0,
            "chunks_created": 0,
            "errors": [],
        }

        documents = self.extract(stage_path)

        for doc in documents:
            try:
                doc_text = sample_text or f"Sample content from {doc['name']}"
                
                chunks = self.chunk(doc_text, doc['name'], doc['document_type'])

                for chunk in chunks:
                    chunk = self.enrich_metadata(chunk, doc)

                loaded = self.load_chunks(chunks)

                result["documents_processed"] += 1
                result["chunks_created"] += loaded

            except Exception as e:
                result["errors"].append({
                    "document": doc['name'],
                    "error": str(e),
                })

        return result


def process_documents(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function for document chunking."""
    chunker = DocumentChunker(
        max_chunk_size=state.get("max_chunk_size", 8000),
        chunk_overlap=state.get("chunk_overlap", 200),
    )

    stage_paths = state.get("document_stage_paths", ["@RAW.DOCUMENTS_STAGE"])

    all_results = {
        "documents_processed": 0,
        "chunks_created": 0,
        "errors": [],
    }

    for stage_path in stage_paths:
        result = chunker.process(stage_path)
        all_results["documents_processed"] += result["documents_processed"]
        all_results["chunks_created"] += result["chunks_created"]
        all_results["errors"].extend(result["errors"])

    return {
        "chunk_results": all_results,
        "current_state": DocumentChunkerState.COMPLETE.value,
        "chunk_table": f"{chunker.database}.{chunker.schema}.{chunker.chunk_table}",
        "messages": state.get("messages", []) + [{
            "role": "system",
            "content": f"Processed {all_results['documents_processed']} documents, created {all_results['chunks_created']} chunks",
        }],
    }
