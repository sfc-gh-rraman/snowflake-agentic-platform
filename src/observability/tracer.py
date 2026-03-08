"""Observability tracer - dual logging to LangSmith and Snowflake."""

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from functools import wraps


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
    """Dual logger that writes to both LangSmith and Snowflake."""

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
