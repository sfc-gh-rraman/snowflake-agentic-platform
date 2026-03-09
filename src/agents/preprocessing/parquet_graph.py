"""LangGraph state machine for Parquet Processor agent."""

from typing import Any, Dict, List, Literal, Optional, TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import BaseCheckpointSaver
import operator

from .parquet_processor import ParquetProcessor, ParquetFile, ParquetProcessorState


class ParquetGraphState(TypedDict):
    stage_path: str
    target_schema: str
    database: str
    files: Annotated[List[Dict[str, Any]], operator.add]
    schemas: Dict[str, Dict[str, str]]
    profiles: Dict[str, Dict[str, Any]]
    quality_issues: Dict[str, List[str]]
    column_mappings: Dict[str, Dict[str, str]]
    tables_created: Annotated[List[str], operator.add]
    current_state: str
    errors: Annotated[List[str], operator.add]
    messages: Annotated[List[Dict[str, str]], operator.add]


def create_processor(state: ParquetGraphState) -> ParquetProcessor:
    return ParquetProcessor(
        database=state.get("database", "AGENTIC_PLATFORM"),
        target_schema=state.get("target_schema", "RAW"),
    )


def scan_node(state: ParquetGraphState) -> Dict[str, Any]:
    processor = create_processor(state)
    stage_path = state["stage_path"]
    
    try:
        files = processor.scan(stage_path)
        file_dicts = [
            {
                "path": f.path,
                "name": f.name,
                "size_bytes": f.size_bytes,
            }
            for f in files
        ]
        return {
            "files": file_dicts,
            "current_state": ParquetProcessorState.SCHEMA_INFER.value,
            "messages": [{"role": "system", "content": f"SCAN: Found {len(files)} parquet files in {stage_path}"}],
        }
    except Exception as e:
        return {
            "current_state": ParquetProcessorState.FAILED.value,
            "errors": [f"SCAN failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"SCAN: Failed - {str(e)}"}],
        }


def schema_infer_node(state: ParquetGraphState) -> Dict[str, Any]:
    processor = create_processor(state)
    schemas = {}
    errors = []
    
    for file_dict in state.get("files", []):
        pf = ParquetFile(
            path=file_dict["path"],
            name=file_dict["name"],
            size_bytes=file_dict["size_bytes"],
        )
        try:
            pf = processor.infer_schema(pf)
            if pf.schema:
                schemas[pf.name] = pf.schema
        except Exception as e:
            errors.append(f"Schema inference failed for {pf.name}: {str(e)}")
    
    return {
        "schemas": schemas,
        "current_state": ParquetProcessorState.PROFILE.value if schemas else ParquetProcessorState.FAILED.value,
        "errors": errors,
        "messages": [{"role": "system", "content": f"SCHEMA_INFER: Inferred schemas for {len(schemas)} files"}],
    }


def profile_node(state: ParquetGraphState) -> Dict[str, Any]:
    processor = create_processor(state)
    profiles = {}
    
    for file_dict in state.get("files", []):
        pf = ParquetFile(
            path=file_dict["path"],
            name=file_dict["name"],
            size_bytes=file_dict["size_bytes"],
            schema=state.get("schemas", {}).get(file_dict["name"]),
        )
        if pf.schema:
            try:
                pf = processor.infer_schema(pf)
                profile = processor.profile(pf)
                profiles[pf.name] = profile
            except Exception:
                profiles[pf.name] = {"error": "Profile failed"}
    
    return {
        "profiles": profiles,
        "current_state": ParquetProcessorState.QUALITY_CHECK.value,
        "messages": [{"role": "system", "content": f"PROFILE: Generated profiles for {len(profiles)} files"}],
    }


def quality_check_node(state: ParquetGraphState) -> Dict[str, Any]:
    processor = create_processor(state)
    quality_issues = {}
    
    for file_dict in state.get("files", []):
        pf = ParquetFile(
            path=file_dict["path"],
            name=file_dict["name"],
            size_bytes=file_dict["size_bytes"],
            schema=state.get("schemas", {}).get(file_dict["name"]),
            row_count=state.get("profiles", {}).get(file_dict["name"], {}).get("row_count"),
        )
        issues = processor.quality_check(pf)
        if issues:
            quality_issues[pf.name] = issues
    
    total_issues = sum(len(v) for v in quality_issues.values())
    return {
        "quality_issues": quality_issues,
        "current_state": ParquetProcessorState.TRANSFORM.value,
        "messages": [{"role": "system", "content": f"QUALITY_CHECK: Found {total_issues} issues across {len(quality_issues)} files"}],
    }


def transform_node(state: ParquetGraphState) -> Dict[str, Any]:
    processor = create_processor(state)
    column_mappings = {}
    
    for file_dict in state.get("files", []):
        pf = ParquetFile(
            path=file_dict["path"],
            name=file_dict["name"],
            size_bytes=file_dict["size_bytes"],
            schema=state.get("schemas", {}).get(file_dict["name"]),
        )
        mapping = processor.transform(pf)
        if mapping:
            column_mappings[pf.name] = mapping
    
    return {
        "column_mappings": column_mappings,
        "current_state": ParquetProcessorState.LOAD.value,
        "messages": [{"role": "system", "content": f"TRANSFORM: Created column mappings for {len(column_mappings)} files"}],
    }


def load_node(state: ParquetGraphState) -> Dict[str, Any]:
    processor = create_processor(state)
    tables_created = []
    errors = []
    
    for file_dict in state.get("files", []):
        pf = ParquetFile(
            path=file_dict["path"],
            name=file_dict["name"],
            size_bytes=file_dict["size_bytes"],
            schema=state.get("schemas", {}).get(file_dict["name"]),
        )
        column_mapping = state.get("column_mappings", {}).get(pf.name)
        
        try:
            table_name = processor.load(pf, column_mapping)
            tables_created.append(table_name)
        except Exception as e:
            errors.append(f"Load failed for {pf.name}: {str(e)}")
    
    return {
        "tables_created": tables_created,
        "current_state": ParquetProcessorState.COMPLETE.value,
        "errors": errors,
        "messages": [{"role": "system", "content": f"LOAD: Created {len(tables_created)} tables"}],
    }


def should_continue(state: ParquetGraphState) -> Literal["schema_infer", "profile", "quality_check", "transform", "load", "end"]:
    current = state.get("current_state", "")
    
    if current == ParquetProcessorState.FAILED.value:
        return "end"
    elif current == ParquetProcessorState.SCHEMA_INFER.value:
        return "schema_infer"
    elif current == ParquetProcessorState.PROFILE.value:
        return "profile"
    elif current == ParquetProcessorState.QUALITY_CHECK.value:
        return "quality_check"
    elif current == ParquetProcessorState.TRANSFORM.value:
        return "transform"
    elif current == ParquetProcessorState.LOAD.value:
        return "load"
    else:
        return "end"


def build_parquet_graph(checkpointer: Optional[BaseCheckpointSaver] = None) -> StateGraph:
    graph = StateGraph(ParquetGraphState)
    
    graph.add_node("scan", scan_node)
    graph.add_node("schema_infer", schema_infer_node)
    graph.add_node("profile", profile_node)
    graph.add_node("quality_check", quality_check_node)
    graph.add_node("transform", transform_node)
    graph.add_node("load", load_node)
    
    graph.set_entry_point("scan")
    
    graph.add_conditional_edges(
        "scan",
        should_continue,
        {
            "schema_infer": "schema_infer",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "schema_infer",
        should_continue,
        {
            "profile": "profile",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "profile",
        should_continue,
        {
            "quality_check": "quality_check",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "quality_check",
        should_continue,
        {
            "transform": "transform",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "transform",
        should_continue,
        {
            "load": "load",
            "end": END,
        }
    )
    graph.add_edge("load", END)
    
    return graph.compile(checkpointer=checkpointer)


def run_parquet_pipeline(
    stage_path: str,
    target_schema: str = "RAW",
    database: str = "AGENTIC_PLATFORM",
    checkpointer: Optional[BaseCheckpointSaver] = None,
    thread_id: str = "parquet-default",
) -> Dict[str, Any]:
    graph = build_parquet_graph(checkpointer)
    
    initial_state: ParquetGraphState = {
        "stage_path": stage_path,
        "target_schema": target_schema,
        "database": database,
        "files": [],
        "schemas": {},
        "profiles": {},
        "quality_issues": {},
        "column_mappings": {},
        "tables_created": [],
        "current_state": ParquetProcessorState.SCAN.value,
        "errors": [],
        "messages": [],
    }
    
    config = {"configurable": {"thread_id": thread_id}}
    result = graph.invoke(initial_state, config)
    
    return result
