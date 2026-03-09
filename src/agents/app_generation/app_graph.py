"""LangGraph state machine for App Code Generator agent."""

from typing import Any, Dict, List, Literal, Optional, TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import BaseCheckpointSaver
import operator
import os

from .code_generator import AppCodeGenerator, AppGeneratorState, AppSpec


class AppGraphState(TypedDict):
    use_case: str
    tables: List[str]
    models: Optional[List[str]]
    search_services: Optional[List[str]]
    semantic_models: Optional[List[str]]
    output_dir: str
    llm_model: str
    app_spec: Optional[Dict[str, Any]]
    react_files: Dict[str, str]
    backend_files: Dict[str, str]
    deployment_files: Dict[str, str]
    validation_results: Dict[str, Any]
    current_state: str
    errors: Annotated[List[str], operator.add]
    messages: Annotated[List[Dict[str, str]], operator.add]


def create_generator(state: AppGraphState) -> AppCodeGenerator:
    return AppCodeGenerator(
        model=state.get("llm_model", "mistral-large2"),
        output_dir=state.get("output_dir", "./generated_app"),
    )


def app_spec_node(state: AppGraphState) -> Dict[str, Any]:
    generator = create_generator(state)
    
    try:
        spec = generator.generate_app_spec(
            use_case=state["use_case"],
            tables=state.get("tables", []),
            models=state.get("models"),
            search_services=state.get("search_services"),
            semantic_models=state.get("semantic_models"),
        )
        
        return {
            "app_spec": spec.to_dict(),
            "current_state": AppGeneratorState.REACT_CODE_GENERATION.value,
            "messages": [{"role": "system", "content": f"APP_SPEC_GENERATION: Generated spec for '{spec.app_name}' with {len(spec.pages)} pages"}],
        }
    except Exception as e:
        return {
            "current_state": AppGeneratorState.FAILED.value,
            "errors": [f"APP_SPEC_GENERATION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"APP_SPEC_GENERATION: Failed - {str(e)}"}],
        }


def react_code_node(state: AppGraphState) -> Dict[str, Any]:
    generator = create_generator(state)
    
    spec_dict = state.get("app_spec", {})
    spec = AppSpec(
        app_name=spec_dict.get("app_name", "Generated App"),
        description=spec_dict.get("description", ""),
        pages=spec_dict.get("pages", []),
        data_sources=spec_dict.get("data_sources", []),
        features=spec_dict.get("features", []),
    )
    
    try:
        react_files = generator.generate_react_components(spec)
        
        return {
            "react_files": react_files,
            "current_state": AppGeneratorState.FASTAPI_CODE_GENERATION.value,
            "messages": [{"role": "system", "content": f"REACT_CODE_GENERATION: Generated {len(react_files)} React files"}],
        }
    except Exception as e:
        return {
            "current_state": AppGeneratorState.FAILED.value,
            "errors": [f"REACT_CODE_GENERATION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"REACT_CODE_GENERATION: Failed - {str(e)}"}],
        }


def fastapi_code_node(state: AppGraphState) -> Dict[str, Any]:
    generator = create_generator(state)
    
    spec_dict = state.get("app_spec", {})
    spec = AppSpec(
        app_name=spec_dict.get("app_name", "Generated App"),
        description=spec_dict.get("description", ""),
        pages=spec_dict.get("pages", []),
        data_sources=spec_dict.get("data_sources", []),
        features=spec_dict.get("features", []),
    )
    
    try:
        backend_files = generator.generate_fastapi_backend(spec)
        
        return {
            "backend_files": backend_files,
            "current_state": AppGeneratorState.DEPLOYMENT_CONFIG_GENERATION.value,
            "messages": [{"role": "system", "content": f"FASTAPI_CODE_GENERATION: Generated {len(backend_files)} backend files"}],
        }
    except Exception as e:
        return {
            "current_state": AppGeneratorState.FAILED.value,
            "errors": [f"FASTAPI_CODE_GENERATION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"FASTAPI_CODE_GENERATION: Failed - {str(e)}"}],
        }


def deployment_config_node(state: AppGraphState) -> Dict[str, Any]:
    generator = create_generator(state)
    
    spec_dict = state.get("app_spec", {})
    spec = AppSpec(
        app_name=spec_dict.get("app_name", "Generated App"),
        description=spec_dict.get("description", ""),
        pages=spec_dict.get("pages", []),
        data_sources=spec_dict.get("data_sources", []),
        features=spec_dict.get("features", []),
    )
    
    try:
        deployment_files = generator.generate_deployment_config(spec)
        
        return {
            "deployment_files": deployment_files,
            "current_state": AppGeneratorState.TEST_AND_VALIDATE.value,
            "messages": [{"role": "system", "content": f"DEPLOYMENT_CONFIG_GENERATION: Generated {len(deployment_files)} deployment files"}],
        }
    except Exception as e:
        return {
            "current_state": AppGeneratorState.FAILED.value,
            "errors": [f"DEPLOYMENT_CONFIG_GENERATION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"DEPLOYMENT_CONFIG_GENERATION: Failed - {str(e)}"}],
        }


def test_and_validate_node(state: AppGraphState) -> Dict[str, Any]:
    validation_results = {
        "react": {"status": "passed", "checks": []},
        "backend": {"status": "passed", "checks": []},
        "deployment": {"status": "passed", "checks": []},
    }
    
    react_files = state.get("react_files", {})
    if "App.tsx" not in react_files:
        validation_results["react"]["status"] = "warning"
        validation_results["react"]["checks"].append("Missing App.tsx entry point")
    else:
        validation_results["react"]["checks"].append("App.tsx present")
    
    backend_files = state.get("backend_files", {})
    if "main.py" not in backend_files:
        validation_results["backend"]["status"] = "warning"
        validation_results["backend"]["checks"].append("Missing main.py entry point")
    else:
        validation_results["backend"]["checks"].append("main.py present")
    
    for name, content in backend_files.items():
        if "import " in content and "fastapi" in content.lower():
            validation_results["backend"]["checks"].append(f"{name}: FastAPI imports valid")
    
    deployment_files = state.get("deployment_files", {})
    if "Dockerfile" in deployment_files:
        validation_results["deployment"]["checks"].append("Dockerfile present")
    if "requirements.txt" in deployment_files:
        validation_results["deployment"]["checks"].append("requirements.txt present")
    if "spcs_service.yaml" in deployment_files:
        validation_results["deployment"]["checks"].append("SPCS spec present")
    
    output_dir = state.get("output_dir", "./generated_app")
    all_files = {}
    all_files.update({f"frontend/src/{k}": v for k, v in react_files.items()})
    all_files.update({f"backend/{k}": v for k, v in backend_files.items()})
    all_files.update(deployment_files)
    
    try:
        for rel_path, content in all_files.items():
            full_path = os.path.join(output_dir, rel_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write(content)
        validation_results["files_written"] = len(all_files)
    except Exception as e:
        validation_results["write_error"] = str(e)
    
    return {
        "validation_results": validation_results,
        "current_state": AppGeneratorState.COMPLETE.value,
        "messages": [{"role": "system", "content": f"TEST_AND_VALIDATE: Validation complete - {len(all_files)} files generated"}],
    }


def should_continue(state: AppGraphState) -> Literal["react_code", "fastapi_code", "deployment_config", "test_validate", "end"]:
    current = state.get("current_state", "")
    
    if current == AppGeneratorState.FAILED.value:
        return "end"
    elif current == AppGeneratorState.REACT_CODE_GENERATION.value:
        return "react_code"
    elif current == AppGeneratorState.FASTAPI_CODE_GENERATION.value:
        return "fastapi_code"
    elif current == AppGeneratorState.DEPLOYMENT_CONFIG_GENERATION.value:
        return "deployment_config"
    elif current == AppGeneratorState.TEST_AND_VALIDATE.value:
        return "test_validate"
    else:
        return "end"


def build_app_graph(checkpointer: Optional[BaseCheckpointSaver] = None) -> StateGraph:
    graph = StateGraph(AppGraphState)
    
    graph.add_node("app_spec", app_spec_node)
    graph.add_node("react_code", react_code_node)
    graph.add_node("fastapi_code", fastapi_code_node)
    graph.add_node("deployment_config", deployment_config_node)
    graph.add_node("test_validate", test_and_validate_node)
    
    graph.set_entry_point("app_spec")
    
    graph.add_conditional_edges(
        "app_spec",
        should_continue,
        {
            "react_code": "react_code",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "react_code",
        should_continue,
        {
            "fastapi_code": "fastapi_code",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "fastapi_code",
        should_continue,
        {
            "deployment_config": "deployment_config",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "deployment_config",
        should_continue,
        {
            "test_validate": "test_validate",
            "end": END,
        }
    )
    graph.add_edge("test_validate", END)
    
    return graph.compile(checkpointer=checkpointer)


def run_app_pipeline(
    use_case: str,
    tables: List[str],
    models: Optional[List[str]] = None,
    search_services: Optional[List[str]] = None,
    semantic_models: Optional[List[str]] = None,
    output_dir: str = "./generated_app",
    llm_model: str = "mistral-large2",
    checkpointer: Optional[BaseCheckpointSaver] = None,
    thread_id: str = "app-default",
) -> Dict[str, Any]:
    graph = build_app_graph(checkpointer)
    
    initial_state: AppGraphState = {
        "use_case": use_case,
        "tables": tables,
        "models": models,
        "search_services": search_services,
        "semantic_models": semantic_models,
        "output_dir": output_dir,
        "llm_model": llm_model,
        "app_spec": None,
        "react_files": {},
        "backend_files": {},
        "deployment_files": {},
        "validation_results": {},
        "current_state": AppGeneratorState.APP_SPEC_GENERATION.value,
        "errors": [],
        "messages": [],
    }
    
    config = {"configurable": {"thread_id": thread_id}}
    result = graph.invoke(initial_state, config)
    
    return result
