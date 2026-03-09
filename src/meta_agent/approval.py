"""Human approval node for Meta-Agent."""

from typing import Any


def human_approval(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node for human-in-the-loop approval.

    This node is designed to pause execution and wait for human approval.
    In LangGraph, this is typically implemented using an interrupt.
    """
    if state.get("approval_status") == "approved":
        return {
            "current_phase": "execute_plan",
            "messages": state.get("messages", [])
            + [
                {
                    "role": "system",
                    "content": "Plan approved. Starting execution.",
                }
            ],
        }
    elif state.get("approval_status") == "rejected":
        return {
            "current_phase": "end",
            "error": state.get("approval_feedback", "Plan rejected by user"),
            "messages": state.get("messages", [])
            + [
                {
                    "role": "system",
                    "content": f"Plan rejected: {state.get('approval_feedback', 'No feedback')}",
                }
            ],
        }
    else:
        return {
            "current_phase": "awaiting_approval",
            "messages": state.get("messages", [])
            + [
                {
                    "role": "system",
                    "content": "Awaiting human approval for execution plan.",
                }
            ],
        }


def should_wait_for_approval(state: dict[str, Any]) -> str:
    """Routing function to determine if we should wait for approval."""
    status = state.get("approval_status", "pending")

    if status == "approved":
        return "execute_plan"
    elif status == "rejected":
        return "end"
    else:
        return "await_approval"


def execute_plan(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node to execute the approved plan.

    This node transitions to the execution phase where sub-agents are invoked.
    """
    plan = state.get("execution_plan", {})
    phases = plan.get("phases", [])

    if not phases:
        return {
            "current_phase": "end",
            "error": "No phases in execution plan",
            "messages": state.get("messages", [])
            + [
                {
                    "role": "system",
                    "content": "Error: No phases found in execution plan",
                }
            ],
        }

    return {
        "current_phase": "executing",
        "messages": state.get("messages", [])
        + [
            {
                "role": "system",
                "content": f"Starting execution of {len(phases)} phases",
            }
        ],
    }
