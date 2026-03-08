"""LangGraph definition for Meta-Agent."""

from typing import Any, Dict, Optional

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import BaseCheckpointSaver

from .state import MetaAgentState
from .tools import parse_use_case, scan_data, query_registry, generate_plan
from .approval import human_approval, should_wait_for_approval, execute_plan


def create_meta_agent_graph(
    checkpointer: Optional[BaseCheckpointSaver] = None,
) -> StateGraph:
    """Create the Meta-Agent LangGraph.
    
    Flow:
    START → parse_use_case → scan_data → query_registry → generate_plan 
          → human_approval → execute_plan → END
    
    Args:
        checkpointer: Optional checkpoint saver for persistence
        
    Returns:
        Compiled LangGraph
    """
    workflow = StateGraph(MetaAgentState)

    workflow.add_node("parse_use_case", parse_use_case)
    workflow.add_node("scan_data", scan_data)
    workflow.add_node("query_registry", query_registry)
    workflow.add_node("generate_plan", generate_plan)
    workflow.add_node("human_approval", human_approval)
    workflow.add_node("execute_plan", execute_plan)

    workflow.set_entry_point("parse_use_case")

    workflow.add_edge("parse_use_case", "scan_data")
    workflow.add_edge("scan_data", "query_registry")
    workflow.add_edge("query_registry", "generate_plan")
    workflow.add_edge("generate_plan", "human_approval")

    workflow.add_conditional_edges(
        "human_approval",
        should_wait_for_approval,
        {
            "execute_plan": "execute_plan",
            "await_approval": END,
            "end": END,
        }
    )

    workflow.add_edge("execute_plan", END)

    return workflow.compile(checkpointer=checkpointer)


def run_meta_agent(
    use_case_description: str,
    data_locations: Optional[list] = None,
    checkpointer: Optional[BaseCheckpointSaver] = None,
    auto_approve: bool = False,
) -> Dict[str, Any]:
    """Run the meta-agent to generate an execution plan.
    
    Args:
        use_case_description: Natural language description of the use case
        data_locations: List of stage paths or table names to scan
        checkpointer: Optional checkpoint saver
        auto_approve: If True, automatically approve the plan
        
    Returns:
        Final state containing the execution plan
    """
    graph = create_meta_agent_graph(checkpointer=checkpointer)

    initial_state: MetaAgentState = {
        "use_case_description": use_case_description,
        "data_locations": data_locations or [],
        "data_assets": [],
        "parsed_requirements": None,
        "data_profile": None,
        "available_agents": [],
        "execution_plan": None,
        "approval_status": "approved" if auto_approve else "pending",
        "approval_feedback": None,
        "current_phase": "start",
        "error": None,
        "messages": [],
    }

    config = {"configurable": {"thread_id": "meta-agent-run"}}
    
    result = graph.invoke(initial_state, config)
    
    return result


if __name__ == "__main__":
    result = run_meta_agent(
        use_case_description="I need to analyze drilling reports for equipment failures and predict maintenance needs",
        data_locations=["@RAW.DATA_STAGE", "@RAW.DOCUMENTS_STAGE"],
        auto_approve=True,
    )
    
    print("Execution Plan:")
    if result.get("execution_plan"):
        import json
        print(json.dumps(result["execution_plan"], indent=2))
    else:
        print("No plan generated")
        print(f"Error: {result.get('error')}")
