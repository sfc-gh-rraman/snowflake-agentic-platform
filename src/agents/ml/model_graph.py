"""LangGraph state machine for ML Model Builder agent."""

from typing import Any, Dict, List, Literal, Optional, TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import BaseCheckpointSaver
import operator

from .model_builder import MLModelBuilder, MLModelBuilderState, TaskType, ModelMetrics


class MLGraphState(TypedDict):
    source_table: str
    target_column: str
    model_name: str
    version: str
    database: str
    schema: str
    exclude_columns: List[str]
    task_type: Optional[str]
    feature_columns: List[str]
    pipeline: Optional[Any]
    train_df: Optional[Any]
    test_df: Optional[Any]
    metrics: Optional[Dict[str, Any]]
    model_reference: Optional[str]
    explanation: Optional[Dict[str, Any]]
    current_state: str
    errors: Annotated[List[str], operator.add]
    messages: Annotated[List[Dict[str, str]], operator.add]


def create_builder(state: MLGraphState) -> MLModelBuilder:
    return MLModelBuilder(
        database=state.get("database", "AGENTIC_PLATFORM"),
        schema=state.get("schema", "ML"),
    )


def task_classification_node(state: MLGraphState) -> Dict[str, Any]:
    builder = create_builder(state)
    
    try:
        task_type = builder.classify_task(
            state["source_table"],
            state["target_column"],
        )
        return {
            "task_type": task_type.value,
            "current_state": MLModelBuilderState.FEATURE_SELECTION.value,
            "messages": [{"role": "system", "content": f"TASK_CLASSIFICATION: Detected {task_type.value} task"}],
        }
    except Exception as e:
        return {
            "current_state": MLModelBuilderState.FAILED.value,
            "errors": [f"TASK_CLASSIFICATION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"TASK_CLASSIFICATION: Failed - {str(e)}"}],
        }


def feature_selection_node(state: MLGraphState) -> Dict[str, Any]:
    builder = create_builder(state)
    
    try:
        features = builder.select_features(
            state["source_table"],
            state["target_column"],
            state.get("exclude_columns", []),
        )
        
        if not features:
            return {
                "current_state": MLModelBuilderState.FAILED.value,
                "errors": ["No suitable numeric features found"],
                "messages": [{"role": "system", "content": "FEATURE_SELECTION: No features found"}],
            }
        
        return {
            "feature_columns": features,
            "current_state": MLModelBuilderState.TRAINING.value,
            "messages": [{"role": "system", "content": f"FEATURE_SELECTION: Selected {len(features)} features"}],
        }
    except Exception as e:
        return {
            "current_state": MLModelBuilderState.FAILED.value,
            "errors": [f"FEATURE_SELECTION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"FEATURE_SELECTION: Failed - {str(e)}"}],
        }


def training_node(state: MLGraphState) -> Dict[str, Any]:
    builder = create_builder(state)
    
    task_type = TaskType(state["task_type"])
    
    try:
        train_result = builder.train(
            state["source_table"],
            state["target_column"],
            state["feature_columns"],
            task_type,
        )
        
        return {
            "pipeline": train_result["pipeline"],
            "train_df": train_result["train_df"],
            "test_df": train_result["test_df"],
            "current_state": MLModelBuilderState.EVALUATION.value,
            "messages": [{"role": "system", "content": "TRAINING: Model training complete"}],
        }
    except Exception as e:
        return {
            "current_state": MLModelBuilderState.FAILED.value,
            "errors": [f"TRAINING failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"TRAINING: Failed - {str(e)}"}],
        }


def evaluation_node(state: MLGraphState) -> Dict[str, Any]:
    builder = create_builder(state)
    
    task_type = TaskType(state["task_type"])
    
    try:
        metrics = builder.evaluate(
            state["pipeline"],
            state["test_df"],
            state["target_column"],
            task_type,
        )
        
        return {
            "metrics": metrics.to_dict(),
            "current_state": MLModelBuilderState.REGISTRATION.value,
            "messages": [{"role": "system", "content": f"EVALUATION: Metrics computed - {metrics.metrics}"}],
        }
    except Exception as e:
        return {
            "current_state": MLModelBuilderState.FAILED.value,
            "errors": [f"EVALUATION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"EVALUATION: Failed - {str(e)}"}],
        }


def registration_node(state: MLGraphState) -> Dict[str, Any]:
    builder = create_builder(state)
    
    task_type = TaskType(state["task_type"])
    metrics = ModelMetrics(
        task_type=task_type,
        metrics=state["metrics"]["metrics"],
    )
    
    try:
        model_ref = builder.register(
            state["pipeline"],
            state["model_name"],
            state["version"],
            metrics,
            state["train_df"].limit(100),
        )
        
        return {
            "model_reference": model_ref,
            "current_state": MLModelBuilderState.EXPLAINABILITY.value,
            "messages": [{"role": "system", "content": f"REGISTRATION: Model registered as {model_ref}"}],
        }
    except Exception as e:
        return {
            "current_state": MLModelBuilderState.FAILED.value,
            "errors": [f"REGISTRATION failed: {str(e)}"],
            "messages": [{"role": "system", "content": f"REGISTRATION: Failed - {str(e)}"}],
        }


def explainability_node(state: MLGraphState) -> Dict[str, Any]:
    builder = create_builder(state)
    
    try:
        explanation = builder.explain(
            state["pipeline"],
            state["test_df"],
            state["feature_columns"],
        )
        
        return {
            "explanation": explanation,
            "current_state": MLModelBuilderState.COMPLETE.value,
            "messages": [{"role": "system", "content": f"EXPLAINABILITY: Generated explanations using {explanation.get('method', 'unknown')}"}],
        }
    except Exception as e:
        return {
            "explanation": {"method": "unavailable", "error": str(e)},
            "current_state": MLModelBuilderState.COMPLETE.value,
            "messages": [{"role": "system", "content": f"EXPLAINABILITY: Skipped - {str(e)}"}],
        }


def should_continue(state: MLGraphState) -> Literal["feature_selection", "training", "evaluation", "registration", "explainability", "end"]:
    current = state.get("current_state", "")
    
    if current == MLModelBuilderState.FAILED.value:
        return "end"
    elif current == MLModelBuilderState.FEATURE_SELECTION.value:
        return "feature_selection"
    elif current == MLModelBuilderState.TRAINING.value:
        return "training"
    elif current == MLModelBuilderState.EVALUATION.value:
        return "evaluation"
    elif current == MLModelBuilderState.REGISTRATION.value:
        return "registration"
    elif current == MLModelBuilderState.EXPLAINABILITY.value:
        return "explainability"
    else:
        return "end"


def build_ml_graph(checkpointer: Optional[BaseCheckpointSaver] = None) -> StateGraph:
    graph = StateGraph(MLGraphState)
    
    graph.add_node("task_classification", task_classification_node)
    graph.add_node("feature_selection", feature_selection_node)
    graph.add_node("training", training_node)
    graph.add_node("evaluation", evaluation_node)
    graph.add_node("registration", registration_node)
    graph.add_node("explainability", explainability_node)
    
    graph.set_entry_point("task_classification")
    
    graph.add_conditional_edges(
        "task_classification",
        should_continue,
        {
            "feature_selection": "feature_selection",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "feature_selection",
        should_continue,
        {
            "training": "training",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "training",
        should_continue,
        {
            "evaluation": "evaluation",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "evaluation",
        should_continue,
        {
            "registration": "registration",
            "end": END,
        }
    )
    graph.add_conditional_edges(
        "registration",
        should_continue,
        {
            "explainability": "explainability",
            "end": END,
        }
    )
    graph.add_edge("explainability", END)
    
    return graph.compile(checkpointer=checkpointer)


def run_ml_pipeline(
    source_table: str,
    target_column: str,
    model_name: str,
    version: str = "v1",
    database: str = "AGENTIC_PLATFORM",
    schema: str = "ML",
    exclude_columns: Optional[List[str]] = None,
    checkpointer: Optional[BaseCheckpointSaver] = None,
    thread_id: str = "ml-default",
) -> Dict[str, Any]:
    graph = build_ml_graph(checkpointer)
    
    initial_state: MLGraphState = {
        "source_table": source_table,
        "target_column": target_column,
        "model_name": model_name,
        "version": version,
        "database": database,
        "schema": schema,
        "exclude_columns": exclude_columns or [],
        "task_type": None,
        "feature_columns": [],
        "pipeline": None,
        "train_df": None,
        "test_df": None,
        "metrics": None,
        "model_reference": None,
        "explanation": None,
        "current_state": MLModelBuilderState.TASK_CLASSIFICATION.value,
        "errors": [],
        "messages": [],
    }
    
    config = {"configurable": {"thread_id": thread_id}}
    result = graph.invoke(initial_state, config)
    
    return result
