"""Langfuse tracer for agentic platform observability.

Langfuse provides production-grade LLM observability with:
- Trace visualization
- Cost tracking
- Latency monitoring
- User feedback integration
- Prompt management
- A/B testing support

This module integrates Langfuse with our multi-agent architecture.
"""

import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Optional

try:
    from langfuse import Langfuse
    from langfuse.callback import CallbackHandler as LangfuseCallbackHandler

    LANGFUSE_AVAILABLE = True
except ImportError:
    LANGFUSE_AVAILABLE = False
    Langfuse = None
    LangfuseCallbackHandler = None


@dataclass
class LangfuseSpan:
    """Represents a span in Langfuse."""

    span_id: str
    trace_id: str
    name: str
    start_time: float
    end_time: float | None = None
    inputs: dict[str, Any] | None = None
    outputs: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    level: str = "DEFAULT"
    status_message: str | None = None


class LangfuseTracer:
    """Tracer that sends spans to Langfuse for LLM observability.

    Langfuse provides:
    - Full trace visualization with nested spans
    - Cost tracking per model/request
    - Latency percentiles
    - User feedback collection
    - Prompt versioning and management

    Usage:
        tracer = LangfuseTracer(
            public_key="pk-...",
            secret_key="sk-...",
            host="https://cloud.langfuse.com"
        )

        # Start a trace for a user session
        trace = tracer.start_trace(
            name="drilling_copilot_query",
            user_id="user-123",
            session_id="session-456",
            metadata={"well": "F-12"}
        )

        # Create spans within the trace
        span_id = tracer.start_span(
            trace_id=trace.id,
            name="cortex_search",
            inputs={"query": "stuck pipe incidents"}
        )

        # ... do work ...

        tracer.end_span(span_id, outputs={"results": [...]})
        tracer.end_trace(trace.id)
    """

    def __init__(
        self,
        public_key: str | None = None,
        secret_key: str | None = None,
        host: str | None = None,
        project_name: str = "agentic-platform",
        release: str | None = None,
        debug: bool = False,
    ):
        self.public_key = public_key or os.getenv("LANGFUSE_PUBLIC_KEY")
        self.secret_key = secret_key or os.getenv("LANGFUSE_SECRET_KEY")
        self.host = host or os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
        self.project_name = project_name
        self.release = release or os.getenv("LANGFUSE_RELEASE")
        self.debug = debug

        self._enabled = LANGFUSE_AVAILABLE and bool(self.public_key and self.secret_key)
        self._client: Langfuse | None = None
        self._traces: dict[str, Any] = {}
        self._spans: dict[str, LangfuseSpan] = {}
        self._generations: dict[str, Any] = {}

        if self._enabled:
            self._init_client()

    def _init_client(self) -> None:
        """Initialize the Langfuse client."""
        if not LANGFUSE_AVAILABLE:
            return

        try:
            self._client = Langfuse(
                public_key=self.public_key,
                secret_key=self.secret_key,
                host=self.host,
                release=self.release,
                debug=self.debug,
            )
        except Exception as e:
            if self.debug:
                print(f"Failed to initialize Langfuse client: {e}")
            self._enabled = False

    @property
    def enabled(self) -> bool:
        """Check if Langfuse tracing is enabled."""
        return self._enabled

    def get_callback_handler(
        self,
        trace_id: str | None = None,
        user_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ) -> Optional["LangfuseCallbackHandler"]:
        """Get a LangChain callback handler for automatic tracing.

        This integrates with LangChain/LangGraph for automatic span creation.

        Usage with LangGraph:
            handler = tracer.get_callback_handler(
                user_id="user-123",
                session_id="session-456"
            )

            result = graph.invoke(
                state,
                config={"callbacks": [handler]}
            )
        """
        if not self._enabled or not LANGFUSE_AVAILABLE:
            return None

        try:
            return LangfuseCallbackHandler(
                public_key=self.public_key,
                secret_key=self.secret_key,
                host=self.host,
                trace_id=trace_id,
                user_id=user_id,
                session_id=session_id,
                metadata=metadata,
                tags=tags,
                release=self.release,
            )
        except Exception as e:
            if self.debug:
                print(f"Failed to create callback handler: {e}")
            return None

    def start_trace(
        self,
        name: str,
        user_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        input_data: dict[str, Any] | None = None,
    ) -> str | None:
        """Start a new trace.

        A trace represents a complete user interaction or workflow execution.
        """
        if not self._enabled or not self._client:
            return None

        try:
            trace = self._client.trace(
                name=name,
                user_id=user_id,
                session_id=session_id,
                metadata=metadata or {},
                tags=tags or [],
                input=input_data,
            )
            self._traces[trace.id] = trace
            return trace.id
        except Exception as e:
            if self.debug:
                print(f"Failed to start trace: {e}")
            return None

    def end_trace(
        self,
        trace_id: str,
        output_data: dict[str, Any] | None = None,
        level: str = "DEFAULT",
        status_message: str | None = None,
    ) -> None:
        """End a trace with optional output data."""
        if not self._enabled or trace_id not in self._traces:
            return

        try:
            trace = self._traces[trace_id]
            trace.update(
                output=output_data,
                level=level,
                status_message=status_message,
            )
            del self._traces[trace_id]
        except Exception as e:
            if self.debug:
                print(f"Failed to end trace: {e}")

    def start_span(
        self,
        name: str,
        trace_id: str | None = None,
        parent_span_id: str | None = None,
        inputs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        level: str = "DEFAULT",
    ) -> str | None:
        """Start a span within a trace.

        Spans represent individual operations like agent calls, searches, etc.
        """
        if not self._enabled or not self._client:
            return None

        try:
            import uuid

            str(uuid.uuid4())

            if trace_id and trace_id in self._traces:
                trace = self._traces[trace_id]
                span = trace.span(
                    name=name,
                    input=inputs,
                    metadata=metadata or {},
                    level=level,
                )
            elif parent_span_id and parent_span_id in self._spans:
                parent = self._spans[parent_span_id]
                span = self._client.span(
                    name=name,
                    trace_id=parent.trace_id,
                    parent_observation_id=parent_span_id,
                    input=inputs,
                    metadata=metadata or {},
                    level=level,
                )
            else:
                span = self._client.span(
                    name=name,
                    input=inputs,
                    metadata=metadata or {},
                    level=level,
                )

            self._spans[span.id] = LangfuseSpan(
                span_id=span.id,
                trace_id=span.trace_id if hasattr(span, "trace_id") else "",
                name=name,
                start_time=time.time(),
                inputs=inputs,
                metadata=metadata or {},
                level=level,
            )

            return span.id
        except Exception as e:
            if self.debug:
                print(f"Failed to start span: {e}")
            return None

    def end_span(
        self,
        span_id: str,
        outputs: dict[str, Any] | None = None,
        level: str = "DEFAULT",
        status_message: str | None = None,
    ) -> None:
        """End a span with output data."""
        if not self._enabled or span_id not in self._spans:
            return

        try:
            span_data = self._spans[span_id]
            span_data.end_time = time.time()
            span_data.outputs = outputs
            span_data.level = level
            span_data.status_message = status_message

            self._client.span(
                id=span_id,
                output=outputs,
                level=level,
                status_message=status_message,
                end_time=span_data.end_time,
            )

            del self._spans[span_id]
        except Exception as e:
            if self.debug:
                print(f"Failed to end span: {e}")

    def log_generation(
        self,
        name: str,
        model: str,
        prompt: str,
        completion: str,
        trace_id: str | None = None,
        parent_span_id: str | None = None,
        model_parameters: dict[str, Any] | None = None,
        usage: dict[str, int] | None = None,
        metadata: dict[str, Any] | None = None,
        level: str = "DEFAULT",
        latency_ms: int | None = None,
    ) -> str | None:
        """Log an LLM generation (Cortex Complete call).

        This is specifically for logging LLM calls with token usage and cost tracking.

        Usage:
            tracer.log_generation(
                name="cortex_complete",
                model="mistral-large2",
                prompt="Analyze this sensor data...",
                completion="Based on the data...",
                usage={"prompt_tokens": 150, "completion_tokens": 200},
                latency_ms=450,
            )
        """
        if not self._enabled or not self._client:
            return None

        try:
            generation_params = {
                "name": name,
                "model": model,
                "input": prompt,
                "output": completion,
                "model_parameters": model_parameters or {},
                "metadata": metadata or {},
                "level": level,
            }

            if usage:
                generation_params["usage"] = usage

            if trace_id and trace_id in self._traces:
                trace = self._traces[trace_id]
                generation = trace.generation(**generation_params)
            else:
                generation = self._client.generation(**generation_params)

            if latency_ms:
                generation.update(metadata={**(metadata or {}), "latency_ms": latency_ms})

            self._generations[generation.id] = generation
            return generation.id
        except Exception as e:
            if self.debug:
                print(f"Failed to log generation: {e}")
            return None

    def log_cortex_search(
        self,
        query: str,
        results: list[dict[str, Any]],
        service_name: str,
        trace_id: str | None = None,
        latency_ms: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str | None:
        """Log a Cortex Search call.

        Usage:
            tracer.log_cortex_search(
                query="stuck pipe incidents F-11",
                results=[{"chunk": "...", "score": 0.95}],
                service_name="DDR_SEARCH_SERVICE",
                latency_ms=120,
            )
        """
        return self.start_span(
            name=f"cortex_search:{service_name}",
            trace_id=trace_id,
            inputs={"query": query, "service": service_name},
            metadata={
                "result_count": len(results),
                "latency_ms": latency_ms,
                **(metadata or {}),
            },
        )

    def log_cortex_analyst(
        self,
        question: str,
        generated_sql: str,
        results: list[dict[str, Any]] | None = None,
        semantic_model: str | None = None,
        trace_id: str | None = None,
        latency_ms: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str | None:
        """Log a Cortex Analyst call."""
        return self.start_span(
            name="cortex_analyst",
            trace_id=trace_id,
            inputs={"question": question, "semantic_model": semantic_model},
            metadata={
                "generated_sql": generated_sql,
                "result_count": len(results) if results else 0,
                "latency_ms": latency_ms,
                **(metadata or {}),
            },
        )

    def log_agent_action(
        self,
        agent_name: str,
        action: str,
        inputs: dict[str, Any] | None = None,
        outputs: dict[str, Any] | None = None,
        trace_id: str | None = None,
        latency_ms: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str | None:
        """Log an agent action (Watchdog alert, Advisor recommendation, etc.)."""
        span_id = self.start_span(
            name=f"agent:{agent_name}:{action}",
            trace_id=trace_id,
            inputs=inputs,
            metadata={
                "agent": agent_name,
                "action": action,
                "latency_ms": latency_ms,
                **(metadata or {}),
            },
        )

        if span_id and outputs:
            self.end_span(span_id, outputs=outputs)

        return span_id

    def score(
        self,
        trace_id: str,
        name: str,
        value: float,
        comment: str | None = None,
        data_type: str = "NUMERIC",
    ) -> None:
        """Add a score/feedback to a trace.

        Useful for tracking quality metrics, user feedback, etc.

        Usage:
            tracer.score(
                trace_id=trace_id,
                name="user_satisfaction",
                value=5.0,
                comment="Helpful response"
            )
        """
        if not self._enabled or not self._client:
            return

        try:
            self._client.score(
                trace_id=trace_id,
                name=name,
                value=value,
                comment=comment,
                data_type=data_type,
            )
        except Exception as e:
            if self.debug:
                print(f"Failed to add score: {e}")

    def flush(self) -> None:
        """Flush any pending traces to Langfuse."""
        if self._enabled and self._client:
            try:
                self._client.flush()
            except Exception as e:
                if self.debug:
                    print(f"Failed to flush: {e}")

    def shutdown(self) -> None:
        """Shutdown the Langfuse client."""
        if self._enabled and self._client:
            try:
                self._client.shutdown()
            except Exception:
                pass

    def trace(self, name: str) -> Callable:
        """Decorator for tracing a function.

        Usage:
            @tracer.trace("my_function")
            def my_function(x, y):
                return x + y
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                trace_id = self.start_trace(
                    name=name,
                    input_data={"args": str(args)[:500], "kwargs": str(kwargs)[:500]},
                )
                try:
                    result = func(*args, **kwargs)
                    if trace_id:
                        self.end_trace(trace_id, output_data={"result": str(result)[:500]})
                    return result
                except Exception as e:
                    if trace_id:
                        self.end_trace(trace_id, level="ERROR", status_message=str(e))
                    raise

            return wrapper

        return decorator

    def span(self, name: str) -> Callable:
        """Decorator for creating a span around a function.

        Usage:
            @tracer.span("search_ddrs")
            def search_ddrs(query):
                ...
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                span_id = self.start_span(
                    name=name,
                    inputs={"args": str(args)[:500], "kwargs": str(kwargs)[:500]},
                )
                try:
                    result = func(*args, **kwargs)
                    if span_id:
                        self.end_span(span_id, outputs={"result": str(result)[:500]})
                    return result
                except Exception as e:
                    if span_id:
                        self.end_span(span_id, level="ERROR", status_message=str(e))
                    raise

            return wrapper

        return decorator
