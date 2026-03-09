"""Snowflake Agentic Platform - Core package.

This package provides a self-assembling agent system for building AI applications
on Snowflake. It includes:

- Configuration system for use case definition
- Meta-agent orchestration with LangGraph
- Sub-agents for ML, search, document processing
- Observability with LangSmith, Langfuse, and Snowflake
- Code generation for React + FastAPI applications
- SPCS deployment automation

Imports are lazy to avoid requiring all dependencies upfront.
"""

__version__ = "0.1.0"


def get_meta_agent():
    """Get meta-agent components (requires langgraph)."""
    from .meta_agent import create_meta_agent_graph, MetaAgentState
    return create_meta_agent_graph, MetaAgentState


def get_state_manager():
    """Get state management components (requires snowflake)."""
    from .state import StateManager, SnowflakeCheckpointSaver
    return StateManager, SnowflakeCheckpointSaver


__all__ = [
    "__version__",
    "get_meta_agent",
    "get_state_manager",
]
