"""LangGraph state machine for Document Chunker agent."""

from typing import Any, Dict, List, Literal, Optional, TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import BaseCheckpointSaver
import operator

from .document_chunker import DocumentChunker, DocumentChunk, DocumentChunkerState


class DocumentGraphState(TypedDict):
    stage_path: str
    database: str
    schema: str
    chunk_table: str
    max_chunk_size: int
    chunk_overlap: int
    documents: Annotated[List[Dict[str, Any]], operator.add]
    extracted_texts: Dict[str, str]
    structures: Dict[str, List[Dict[str, Any]]]
    chunks: Annotated[List[Dict[str, Any]], operator.add]
    enriched_chunks: Annotated[List[Dict[str, Any]], operator.add]
    chunks_loaded: int
    current_state: str
    errors: Annotated[List[str], operator.add]
    messages: Annotated[List[Dict[str, str]], operator.add]


def create_chunker(state: DocumentGraphState) -> DocumentChunker:
    return DocumentChunker(
        database=state.get("database", "AGENTIC_PLATFORM"),
        schema=state.get("schema", "DOCS"),
        chunk_table=state.get("chunk_table", "DOCUMENT_CHUNKS"),
        max_chunk_size=state.get("max_chunk_size", 8000),
        chunk_overlap=state.get("chunk_overlap", 200),
    )


def extract_node(state: DocumentGraphState) -> Dict[str, Any]:
    chunker = create_chunker(state)
    stage_path = state["stage_path"]
    
    try:
        documents = chunker.extract(stage_path)
        return {
            "documents": documents,
            "current_state": DocumentChunkerState.ANALYZE_STRUCTURE.value,
            "messages": [{"role": "system", "content": f"EXTRACT: Found {len(documents)} documents in {stage_path}"}],
        }
    except Exception as e:
        return {
            "current_state": DocumentChunkerState.FAILED.value,
            "errors": [f"EXTRACT failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"EXTRACT: Failed - {str(e)}"}],
        }


def analyze_structure_node(state: DocumentGraphState) -> Dict[str, Any]:
    chunker = create_chunker(state)
    structures = {}
    extracted_texts = state.get("extracted_texts", {})
    
    for doc in state.get("documents", []):
        doc_name = doc["name"]
        text = extracted_texts.get(doc_name, f"Sample content from {doc_name}")
        
        try:
            sections = chunker.analyze_structure(text)
            structures[doc_name] = sections
        except Exception as e:
            structures[doc_name] = [{"header": "Document", "content": text, "error": str(e)}]
    
    total_sections = sum(len(s) for s in structures.values())
    return {
        "structures": structures,
        "extracted_texts": extracted_texts,
        "current_state": DocumentChunkerState.CHUNK.value,
        "messages": [{"role": "system", "content": f"ANALYZE_STRUCTURE: Identified {total_sections} sections across {len(structures)} documents"}],
    }


def chunk_node(state: DocumentGraphState) -> Dict[str, Any]:
    chunker = create_chunker(state)
    all_chunks = []
    extracted_texts = state.get("extracted_texts", {})
    
    for doc in state.get("documents", []):
        doc_name = doc["name"]
        doc_type = doc.get("document_type", "unknown")
        text = extracted_texts.get(doc_name, f"Sample content from {doc_name}")
        
        try:
            chunks = chunker.chunk(text, doc_name, doc_type)
            all_chunks.extend([c.to_dict() for c in chunks])
        except Exception as e:
            pass
    
    return {
        "chunks": all_chunks,
        "current_state": DocumentChunkerState.ENRICH_METADATA.value,
        "messages": [{"role": "system", "content": f"CHUNK: Created {len(all_chunks)} chunks"}],
    }


def enrich_metadata_node(state: DocumentGraphState) -> Dict[str, Any]:
    chunker = create_chunker(state)
    enriched = []
    
    doc_lookup = {doc["name"]: doc for doc in state.get("documents", [])}
    
    for chunk_dict in state.get("chunks", []):
        chunk = DocumentChunk(
            chunk_id=chunk_dict["chunk_id"],
            source_file=chunk_dict["source_file"],
            document_type=chunk_dict["document_type"],
            page_number=chunk_dict.get("page_number"),
            section_header=chunk_dict.get("section_header"),
            chunk_text=chunk_dict["chunk_text"],
            chunk_index=chunk_dict["chunk_index"],
            metadata=chunk_dict.get("metadata", {}),
        )
        
        doc_meta = doc_lookup.get(chunk.source_file, {})
        chunk = chunker.enrich_metadata(chunk, doc_meta)
        enriched.append(chunk.to_dict())
    
    return {
        "enriched_chunks": enriched,
        "current_state": DocumentChunkerState.LOAD_CHUNKS.value,
        "messages": [{"role": "system", "content": f"ENRICH_METADATA: Enriched {len(enriched)} chunks with metadata"}],
    }


def load_chunks_node(state: DocumentGraphState) -> Dict[str, Any]:
    chunker = create_chunker(state)
    errors = []
    
    chunks_to_load = []
    for chunk_dict in state.get("enriched_chunks", []):
        chunks_to_load.append(DocumentChunk(
            chunk_id=chunk_dict["chunk_id"],
            source_file=chunk_dict["source_file"],
            document_type=chunk_dict["document_type"],
            page_number=chunk_dict.get("page_number"),
            section_header=chunk_dict.get("section_header"),
            chunk_text=chunk_dict["chunk_text"],
            chunk_index=chunk_dict["chunk_index"],
            metadata=chunk_dict.get("metadata", {}),
        ))
    
    try:
        loaded_count = chunker.load_chunks(chunks_to_load)
    except Exception as e:
        loaded_count = 0
        errors.append(f"LOAD_CHUNKS failed: {str(e)}")
    
    full_table = f"{chunker.database}.{chunker.schema}.{chunker.chunk_table}"
    
    return {
        "chunks_loaded": loaded_count,
        "chunk_table": full_table,
        "current_state": DocumentChunkerState.COMPLETE.value,
        "errors": errors,
        "messages": [{"role": "system", "content": f"LOAD_CHUNKS: Loaded {loaded_count} chunks to {full_table}"}],
    }


def should_continue(state: DocumentGraphState) -> Literal["analyze_structure", "chunk", "enrich_metadata", "load_chunks", "end"]:
    current = state.get("current_state", "")
    
    if current == DocumentChunkerState.FAILED.value:
        return "end"
    elif current == DocumentChunkerState.ANALYZE_STRUCTURE.value:
        return "analyze_structure"
    elif current == DocumentChunkerState.CHUNK.value:
        return "chunk"
    elif current == DocumentChunkerState.ENRICH_METADATA.value:
        return "enrich_metadata"
    elif current == DocumentChunkerState.LOAD_CHUNKS.value:
        return "load_chunks"
    else:
        return "end"


def build_document_graph(checkpointer: Optional[BaseCheckpointSaver] = None) -> StateGraph:
    graph = StateGraph(DocumentGraphState)
    
    graph.add_node("extract", extract_node)
    graph.add_node("analyze_structure", analyze_structure_node)
    graph.add_node("chunk", chunk_node)
    graph.add_node("enrich_metadata", enrich_metadata_node)
    graph.add_node("load_chunks", load_chunks_node)
    
    graph.set_entry_point("extract")
    
    graph.add_conditional_edges(
        "extract",
        should_continue,
        {
            "analyze_structure": "analyze_structure",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "analyze_structure",
        should_continue,
        {
            "chunk": "chunk",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "chunk",
        should_continue,
        {
            "enrich_metadata": "enrich_metadata",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "enrich_metadata",
        should_continue,
        {
            "load_chunks": "load_chunks",
            "end": END,
        }
    )
    graph.add_edge("load_chunks", END)
    
    return graph.compile(checkpointer=checkpointer)


def run_document_pipeline(
    stage_path: str,
    database: str = "AGENTIC_PLATFORM",
    schema: str = "DOCS",
    chunk_table: str = "DOCUMENT_CHUNKS",
    max_chunk_size: int = 8000,
    chunk_overlap: int = 200,
    extracted_texts: Optional[Dict[str, str]] = None,
    checkpointer: Optional[BaseCheckpointSaver] = None,
    thread_id: str = "document-default",
) -> Dict[str, Any]:
    graph = build_document_graph(checkpointer)
    
    initial_state: DocumentGraphState = {
        "stage_path": stage_path,
        "database": database,
        "schema": schema,
        "chunk_table": chunk_table,
        "max_chunk_size": max_chunk_size,
        "chunk_overlap": chunk_overlap,
        "documents": [],
        "extracted_texts": extracted_texts or {},
        "structures": {},
        "chunks": [],
        "enriched_chunks": [],
        "chunks_loaded": 0,
        "current_state": DocumentChunkerState.EXTRACT.value,
        "errors": [],
        "messages": [],
    }
    
    config = {"configurable": {"thread_id": thread_id}}
    result = graph.invoke(initial_state, config)
    
    return result
