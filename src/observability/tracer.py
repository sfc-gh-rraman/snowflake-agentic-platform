"""Observability tracer - triple logging to LangSmith, Langfuse, and Snowflake.

This module provides unified observability across:
1. LangSmith - LangChain's native tracing platform
2. Langfuse - Open-source LLM observability with cost tracking
3. Snowflake - Persistent storage for compliance and analytics

The TripleLogger class coordinates all three for comprehensive coverage.
"""

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from functools import wraps

from .langfuse_tracer import LangfuseTracer


@dataclass
class TraceSpan:
    span_id: str
    name: str
    start_time: float
    end_time: Optional[float] = None
    inputs: Optional[Dict[str, Any]] = None
    outputs: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "span_id": self.span_id,
            "name": self.name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": int((self.end_time - self.start_time) * 1000) if self.end_time else None,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "metadata": self.metadata,
            "error": self.error,
        }


class LangSmithTracer:
    """Tracer that sends spans to LangSmith."""

    def __init__(
        self,
        project_name: str = "agentic-platform",
        api_key: Optional[str] = None,
    ):
        self.project_name = project_name
        self.api_key = api_key or os.getenv("LANGSMITH_API_KEY")
        self.endpoint = os.getenv("LANGSMITH_ENDPOINT", "https://api.smith.langchain.com")
        self._enabled = bool(self.api_key)
        self._spans: List[TraceSpan] = []

    def start_span(
        self,
        name: str,
        inputs: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        import uuid
        span_id = str(uuid.uuid4())
        
        span = TraceSpan(
            span_id=span_id,
            name=name,
            start_time=time.time(),
            inputs=inputs,
            metadata=metadata or {},
        )
        self._spans.append(span)
        
        return span_id

    def end_span(
        self,
        span_id: str,
        outputs: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        for span in self._spans:
            if span.span_id == span_id:
                span.end_time = time.time()
                span.outputs = outputs
                span.error = error
                
                if self._enabled:
                    self._send_to_langsmith(span)
                break

    def _send_to_langsmith(self, span: TraceSpan) -> None:
        if not self._enabled:
            return

        try:
            import requests
            
            payload = {
                "name": span.name,
                "run_type": "chain",
                "inputs": span.inputs or {},
                "outputs": span.outputs or {},
                "error": span.error,
                "start_time": span.start_time,
                "end_time": span.end_time,
                "extra": span.metadata,
            }

            headers = {
                "x-api-key": self.api_key,
                "Content-Type": "application/json",
            }

            requests.post(
                f"{self.endpoint}/runs",
                json=payload,
                headers=headers,
                timeout=5,
            )
        except Exception:
            pass

    def trace(self, name: str) -> Callable:
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                span_id = self.start_span(
                    name=name,
                    inputs={"args": str(args)[:500], "kwargs": str(kwargs)[:500]},
                )
                try:
                    result = func(*args, **kwargs)
                    self.end_span(span_id, outputs={"result": str(result)[:500]})
                    return result
                except Exception as e:
                    self.end_span(span_id, error=str(e))
                    raise
            return wrapper
        return decorator


class CortexCallLogger:
    """Logger that writes Cortex calls to Snowflake."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "STATE",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.schema = schema
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

    def _execute(self, sql: str) -> None:
        if hasattr(self.session, 'sql'):
            self.session.sql(sql).collect()
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()

    def _escape(self, text: str) -> str:
        if text is None:
            return "NULL"
        return f"'{str(text).replace(chr(39), chr(39)+chr(39))}'"

    def log_call(
        self,
        call_type: str,
        model_name: str,
        prompt_text: str,
        response_text: str,
        latency_ms: int,
        plan_id: Optional[str] = None,
        phase_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        prompt_tokens: Optional[int] = None,
        response_tokens: Optional[int] = None,
        status: str = "success",
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        import uuid
        log_id = str(uuid.uuid4())
        total_tokens = (prompt_tokens or 0) + (response_tokens or 0) if prompt_tokens or response_tokens else None

        metadata_json = json.dumps(metadata).replace("'", "''") if metadata else "{}"

        sql = f"""
            INSERT INTO {self.database}.{self.schema}.CORTEX_CALL_LOGS
            (log_id, plan_id, phase_id, execution_id, call_type, model_name,
             prompt_text, prompt_tokens, response_text, response_tokens, total_tokens,
             latency_ms, status, error_message, metadata)
            VALUES (
                '{log_id}',
                {self._escape(plan_id) if plan_id else 'NULL'},
                {self._escape(phase_id) if phase_id else 'NULL'},
                {self._escape(execution_id) if execution_id else 'NULL'},
                {self._escape(call_type)},
                {self._escape(model_name)},
                {self._escape(prompt_text[:10000] if prompt_text else None)},
                {prompt_tokens or 'NULL'},
                {self._escape(response_text[:10000] if response_text else None)},
                {response_tokens or 'NULL'},
                {total_tokens or 'NULL'},
                {latency_ms},
                {self._escape(status)},
                {self._escape(error_message)},
                PARSE_JSON('{metadata_json}')
            )
        """

        try:
            self._execute(sql)
        except Exception:
            pass

    def cortex_complete_with_logging(
        self,
        prompt: str,
        model: str = "mistral-large2",
        plan_id: Optional[str] = None,
    ) -> str:
        start_time = time.time()
        error = None
        response = ""

        try:
            escaped = prompt.replace("'", "''")
            sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{escaped}')"
            
            if hasattr(self.session, 'sql'):
                result = self.session.sql(sql).collect()
                response = result[0][0] if result else ""
            else:
                cursor = self.session.cursor()
                cursor.execute(sql)
                row = cursor.fetchone()
                response = row[0] if row else ""
                cursor.close()

        except Exception as e:
            error = str(e)
            raise
        finally:
            latency_ms = int((time.time() - start_time) * 1000)
            self.log_call(
                call_type="complete",
                model_name=model,
                prompt_text=prompt,
                response_text=response,
                latency_ms=latency_ms,
                plan_id=plan_id,
                status="success" if not error else "error",
                error_message=error,
            )

        return response


class DualLogger:
    """Dual logger that writes to both LangSmith and Snowflake.
    
    DEPRECATED: Use TripleLogger instead for full observability.
    """

    def __init__(
        self,
        langsmith_project: str = "agentic-platform",
        connection_name: Optional[str] = None,
    ):
        self.langsmith = LangSmithTracer(project_name=langsmith_project)
        self.snowflake = CortexCallLogger(connection_name=connection_name)

    def log_cortex_call(
        self,
        call_type: str,
        model_name: str,
        prompt_text: str,
        response_text: str,
        latency_ms: int,
        **kwargs,
    ) -> None:
        span_id = self.langsmith.start_span(
            name=f"cortex_{call_type}",
            inputs={"prompt": prompt_text[:500], "model": model_name},
            metadata={"call_type": call_type},
        )
        self.langsmith.end_span(
            span_id,
            outputs={"response": response_text[:500]},
        )

        self.snowflake.log_call(
            call_type=call_type,
            model_name=model_name,
            prompt_text=prompt_text,
            response_text=response_text,
            latency_ms=latency_ms,
            **kwargs,
        )


class TripleLogger:
    """Triple logger that writes to LangSmith, Langfuse, AND Snowflake.
    
    This provides comprehensive observability:
    - LangSmith: LangChain ecosystem integration, playground testing
    - Langfuse: Cost tracking, user feedback, prompt management
    - Snowflake: Persistent storage, compliance, custom analytics
    
    Usage:
        logger = TripleLogger(
            langsmith_project="drilling-copilot",
            langfuse_public_key="pk-...",
            langfuse_secret_key="sk-...",
            snowflake_database="DRILLING_OPS_DB",
        )
        
        # Start a trace for a user session
        trace_id = logger.start_trace(
            name="copilot_query",
            user_id="driller-123",
            session_id="shift-morning",
        )
        
        # Log a Cortex call
        logger.log_cortex_complete(
            prompt="Analyze torque trend...",
            response="Based on the data...",
            model="mistral-large2",
            latency_ms=450,
            trace_id=trace_id,
        )
        
        # Log a search call
        logger.log_cortex_search(
            query="stuck pipe incidents",
            results=[...],
            service_name="DDR_SEARCH_SERVICE",
            latency_ms=120,
            trace_id=trace_id,
        )
        
        # End trace
        logger.end_trace(trace_id)
    """

    def __init__(
        self,
        langsmith_project: str = "agentic-platform",
        langfuse_public_key: Optional[str] = None,
        langfuse_secret_key: Optional[str] = None,
        langfuse_host: Optional[str] = None,
        snowflake_connection_name: Optional[str] = None,
        snowflake_database: str = "AGENTIC_PLATFORM",
        snowflake_schema: str = "ORCHESTRATOR",
        debug: bool = False,
    ):
        self.langsmith = LangSmithTracer(project_name=langsmith_project)
        self.langfuse = LangfuseTracer(
            public_key=langfuse_public_key,
            secret_key=langfuse_secret_key,
            host=langfuse_host,
            project_name=langsmith_project,
            debug=debug,
        )
        self.snowflake = CortexCallLogger(
            connection_name=snowflake_connection_name,
            database=snowflake_database,
            schema=snowflake_schema,
        )
        self.debug = debug
        self._active_traces: Dict[str, Dict[str, Any]] = {}

    @property
    def langfuse_enabled(self) -> bool:
        """Check if Langfuse is enabled."""
        return self.langfuse.enabled

    @property
    def langsmith_enabled(self) -> bool:
        """Check if LangSmith is enabled."""
        return self.langsmith._enabled

    def get_langfuse_callback(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Get a LangChain callback handler for automatic tracing.
        
        Use this with LangGraph for automatic span creation:
        
            handler = logger.get_langfuse_callback(user_id="driller-123")
            result = graph.invoke(state, config={"callbacks": [handler]})
        """
        return self.langfuse.get_callback_handler(
            user_id=user_id,
            session_id=session_id,
            metadata=metadata,
        )

    def start_trace(
        self,
        name: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Start a new trace across all logging systems.
        
        Returns a trace_id that should be passed to subsequent log calls.
        """
        import uuid
        trace_id = str(uuid.uuid4())
        
        langsmith_span_id = self.langsmith.start_span(
            name=name,
            inputs=input_data,
            metadata={
                "user_id": user_id,
                "session_id": session_id,
                **(metadata or {}),
            },
        )
        
        langfuse_trace_id = self.langfuse.start_trace(
            name=name,
            user_id=user_id,
            session_id=session_id,
            metadata=metadata,
            input_data=input_data,
        )
        
        self._active_traces[trace_id] = {
            "langsmith_span_id": langsmith_span_id,
            "langfuse_trace_id": langfuse_trace_id,
            "name": name,
            "start_time": time.time(),
            "user_id": user_id,
            "session_id": session_id,
        }
        
        return trace_id

    def end_trace(
        self,
        trace_id: str,
        output_data: Optional[Dict[str, Any]] = None,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> None:
        """End a trace across all logging systems."""
        if trace_id not in self._active_traces:
            return
            
        trace_data = self._active_traces[trace_id]
        
        if trace_data.get("langsmith_span_id"):
            self.langsmith.end_span(
                trace_data["langsmith_span_id"],
                outputs=output_data,
                error=error_message,
            )
        
        if trace_data.get("langfuse_trace_id"):
            self.langfuse.end_trace(
                trace_data["langfuse_trace_id"],
                output_data=output_data,
                level="ERROR" if error_message else "DEFAULT",
                status_message=error_message,
            )
        
        del self._active_traces[trace_id]

    def log_cortex_complete(
        self,
        prompt: str,
        response: str,
        model: str = "mistral-large2",
        latency_ms: int = 0,
        trace_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        phase_id: Optional[str] = None,
        prompt_tokens: Optional[int] = None,
        response_tokens: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a Cortex Complete (LLM) call to all systems."""
        span_id = self.langsmith.start_span(
            name="cortex_complete",
            inputs={"prompt": prompt[:500], "model": model},
            metadata={"latency_ms": latency_ms, **(metadata or {})},
        )
        self.langsmith.end_span(
            span_id,
            outputs={"response": response[:500]},
        )
        
        langfuse_trace = None
        if trace_id and trace_id in self._active_traces:
            langfuse_trace = self._active_traces[trace_id].get("langfuse_trace_id")
        
        self.langfuse.log_generation(
            name="cortex_complete",
            model=model,
            prompt=prompt,
            completion=response,
            trace_id=langfuse_trace,
            usage={
                "prompt_tokens": prompt_tokens or 0,
                "completion_tokens": response_tokens or 0,
            } if prompt_tokens or response_tokens else None,
            latency_ms=latency_ms,
            metadata=metadata,
        )
        
        self.snowflake.log_call(
            call_type="complete",
            model_name=model,
            prompt_text=prompt,
            response_text=response,
            latency_ms=latency_ms,
            plan_id=plan_id,
            phase_id=phase_id,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            metadata=metadata,
        )

    def log_cortex_search(
        self,
        query: str,
        results: List[Dict[str, Any]],
        service_name: str,
        latency_ms: int = 0,
        trace_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a Cortex Search call to all systems."""
        span_id = self.langsmith.start_span(
            name=f"cortex_search:{service_name}",
            inputs={"query": query, "service": service_name},
            metadata={"result_count": len(results), "latency_ms": latency_ms},
        )
        self.langsmith.end_span(
            span_id,
            outputs={"result_count": len(results)},
        )
        
        langfuse_trace = None
        if trace_id and trace_id in self._active_traces:
            langfuse_trace = self._active_traces[trace_id].get("langfuse_trace_id")
        
        self.langfuse.log_cortex_search(
            query=query,
            results=results,
            service_name=service_name,
            trace_id=langfuse_trace,
            latency_ms=latency_ms,
            metadata=metadata,
        )
        
        self.snowflake.log_call(
            call_type="search",
            model_name=service_name,
            prompt_text=query,
            response_text=json.dumps(results[:3]) if results else "[]",
            latency_ms=latency_ms,
            plan_id=plan_id,
            metadata={
                "result_count": len(results),
                "service": service_name,
                **(metadata or {}),
            },
        )

    def log_cortex_analyst(
        self,
        question: str,
        generated_sql: str,
        results: Optional[List[Dict[str, Any]]] = None,
        semantic_model: Optional[str] = None,
        latency_ms: int = 0,
        trace_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a Cortex Analyst call to all systems."""
        span_id = self.langsmith.start_span(
            name="cortex_analyst",
            inputs={"question": question, "semantic_model": semantic_model},
            metadata={"latency_ms": latency_ms},
        )
        self.langsmith.end_span(
            span_id,
            outputs={"sql": generated_sql[:500], "result_count": len(results) if results else 0},
        )
        
        langfuse_trace = None
        if trace_id and trace_id in self._active_traces:
            langfuse_trace = self._active_traces[trace_id].get("langfuse_trace_id")
        
        self.langfuse.log_cortex_analyst(
            question=question,
            generated_sql=generated_sql,
            results=results,
            semantic_model=semantic_model,
            trace_id=langfuse_trace,
            latency_ms=latency_ms,
            metadata=metadata,
        )
        
        self.snowflake.log_call(
            call_type="analyst",
            model_name=semantic_model or "cortex_analyst",
            prompt_text=question,
            response_text=generated_sql,
            latency_ms=latency_ms,
            plan_id=plan_id,
            metadata={
                "result_count": len(results) if results else 0,
                "semantic_model": semantic_model,
                **(metadata or {}),
            },
        )

    def log_agent_action(
        self,
        agent_name: str,
        action: str,
        inputs: Optional[Dict[str, Any]] = None,
        outputs: Optional[Dict[str, Any]] = None,
        latency_ms: int = 0,
        trace_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log an agent action (Watchdog alert, Advisor recommendation, etc.)."""
        span_id = self.langsmith.start_span(
            name=f"agent:{agent_name}:{action}",
            inputs=inputs,
            metadata={"agent": agent_name, "action": action, "latency_ms": latency_ms},
        )
        self.langsmith.end_span(span_id, outputs=outputs)
        
        langfuse_trace = None
        if trace_id and trace_id in self._active_traces:
            langfuse_trace = self._active_traces[trace_id].get("langfuse_trace_id")
        
        self.langfuse.log_agent_action(
            agent_name=agent_name,
            action=action,
            inputs=inputs,
            outputs=outputs,
            trace_id=langfuse_trace,
            latency_ms=latency_ms,
            metadata=metadata,
        )
        
        self.snowflake.log_call(
            call_type="agent",
            model_name=f"{agent_name}:{action}",
            prompt_text=json.dumps(inputs) if inputs else "",
            response_text=json.dumps(outputs) if outputs else "",
            latency_ms=latency_ms,
            plan_id=plan_id,
            metadata={
                "agent": agent_name,
                "action": action,
                **(metadata or {}),
            },
        )

    def add_feedback(
        self,
        trace_id: str,
        score: float,
        name: str = "user_feedback",
        comment: Optional[str] = None,
    ) -> None:
        """Add user feedback/score to a trace (Langfuse only)."""
        if trace_id in self._active_traces:
            langfuse_trace = self._active_traces[trace_id].get("langfuse_trace_id")
            if langfuse_trace:
                self.langfuse.score(
                    trace_id=langfuse_trace,
                    name=name,
                    value=score,
                    comment=comment,
                )

    def flush(self) -> None:
        """Flush all pending data to logging systems."""
        self.langfuse.flush()

    def shutdown(self) -> None:
        """Shutdown all logging systems."""
        self.langfuse.shutdown()


def traceable(
    name: Optional[str] = None,
    run_type: str = "chain",
    metadata: Optional[Dict[str, Any]] = None,
) -> Callable:
    """Decorator to trace function execution with LangSmith-style tracing.
    
    Usage:
        @traceable(name="my_function")
        def my_function(arg1, arg2):
            return result
            
        @traceable(name="agent_step", run_type="llm")
        def call_llm(prompt):
            return response
    """
    def decorator(func: Callable) -> Callable:
        tracer = LangSmithTracer()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_name = name or func.__name__
            
            input_data = {}
            if args:
                input_data["args"] = str(args)[:1000]
            if kwargs:
                input_data["kwargs"] = str(kwargs)[:1000]
            
            span_id = tracer.start_span(
                name=func_name,
                inputs=input_data,
                metadata={
                    "run_type": run_type,
                    "function": func.__name__,
                    "module": func.__module__,
                    **(metadata or {}),
                },
            )
            
            try:
                result = func(*args, **kwargs)
                
                output_data = {}
                if result is not None:
                    if isinstance(result, dict):
                        output_data = {k: str(v)[:500] for k, v in list(result.items())[:10]}
                    else:
                        output_data = {"result": str(result)[:1000]}
                
                tracer.end_span(span_id, outputs=output_data)
                return result
                
            except Exception as e:
                tracer.end_span(span_id, error=str(e))
                raise
        
        return wrapper
    return decorator


def create_logger(
    database: str = "AGENTIC_PLATFORM",
    schema: str = "ORCHESTRATOR",
    connection_name: Optional[str] = None,
    debug: bool = False,
) -> TripleLogger:
    """Factory function to create a configured TripleLogger.
    
    Reads API keys from environment variables:
    - LANGSMITH_API_KEY
    - LANGFUSE_PUBLIC_KEY
    - LANGFUSE_SECRET_KEY
    - LANGFUSE_HOST (optional, defaults to cloud.langfuse.com)
    - SNOWFLAKE_CONNECTION_NAME (optional)
    """
    return TripleLogger(
        snowflake_database=database,
        snowflake_schema=schema,
        snowflake_connection_name=connection_name,
        debug=debug,
    )
