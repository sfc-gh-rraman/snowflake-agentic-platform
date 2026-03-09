"""Server entry point."""

from .api.websocket import get_manager
from .engine import WorkflowExecutor, set_executor
from .tasks import register_all_tasks

manager = get_manager()
executor = WorkflowExecutor(websocket_manager=manager)
register_all_tasks(executor)
set_executor(executor)
