"""SPCS Deployer agent - deploys apps to Snowpark Container Services."""

import json
import os
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from src.config import get_settings


class DeploymentState(StrEnum):
    BUILDING = "BUILDING"
    PUSHING = "PUSHING"
    CREATING_SERVICE = "CREATING_SERVICE"
    WAITING_READY = "WAITING_READY"
    HEALTH_CHECK = "HEALTH_CHECK"
    REGISTERING = "REGISTERING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass
class DeploymentResult:
    status: DeploymentState
    service_name: str
    endpoint_url: str | None
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "service_name": self.service_name,
            "endpoint_url": self.endpoint_url,
            "error": self.error,
        }


class SPCSDeployer:
    """Deploy applications to Snowpark Container Services."""

    def __init__(
        self,
        connection_name: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        compute_pool: str = "AGENTIC_COMPUTE_POOL",
        image_repo: str = "APP_IMAGES",
    ):
        settings = get_settings()
        self.connection_name = connection_name or settings.connection_name
        self.database = database or settings.database
        self.schema = schema or settings.analytics_schema
        self.compute_pool = compute_pool
        self.image_repo = image_repo
        self._session = None
        self._state = DeploymentState.BUILDING

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

    def _execute(self, sql: str) -> list[dict]:
        if hasattr(self.session, "sql"):
            result = self.session.sql(sql).collect()
            return [dict(row.asDict()) for row in result]
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
            finally:
                cursor.close()

    def ensure_compute_pool(self) -> str:
        check_sql = f"SHOW COMPUTE POOLS LIKE '{self.compute_pool}'"
        results = self._execute(check_sql)

        if not results:
            create_sql = f"""
                CREATE COMPUTE POOL IF NOT EXISTS {self.compute_pool}
                    MIN_NODES = 1
                    MAX_NODES = 3
                    INSTANCE_FAMILY = CPU_X64_XS
                    AUTO_RESUME = TRUE
                    AUTO_SUSPEND_SECS = 300
            """
            self._execute(create_sql)

        return self.compute_pool

    def ensure_image_repo(self) -> str:
        full_repo = f"{self.database}.{self.schema}.{self.image_repo}"

        create_sql = f"CREATE IMAGE REPOSITORY IF NOT EXISTS {full_repo}"
        self._execute(create_sql)

        return full_repo

    def get_image_registry_url(self) -> str:
        show_sql = f"SHOW IMAGE REPOSITORIES LIKE '{self.image_repo}' IN SCHEMA {self.database}.{self.schema}"
        results = self._execute(show_sql)

        if results:
            return results[0].get("repository_url", "")
        return ""

    def create_service(
        self,
        service_name: str,
        image_tag: str,
        port: int = 8080,
        env_vars: dict[str, str] | None = None,
        min_instances: int = 1,
        max_instances: int = 1,
    ) -> str:
        self._state = DeploymentState.CREATING_SERVICE

        full_service = f"{self.database}.{self.schema}.{service_name}"

        env_vars = env_vars or {}
        env_vars.update(
            {
                "SNOWFLAKE_ACCOUNT": "{{ context().CURRENT_ACCOUNT }}",
                "SNOWFLAKE_HOST": "{{ context().CURRENT_HOST }}",
            }
        )

        ",\n                    ".join([f'"{k}": "{v}"' for k, v in env_vars.items()])

        service_spec = {
            "spec": {
                "containers": [
                    {
                        "name": service_name.lower(),
                        "image": image_tag,
                        "env": env_vars,
                    }
                ],
                "endpoints": [
                    {
                        "name": "app",
                        "port": port,
                        "public": True,
                    }
                ],
            }
        }

        spec_json = json.dumps(service_spec).replace("'", "''")

        create_sql = f"""
            CREATE SERVICE IF NOT EXISTS {full_service}
                IN COMPUTE POOL {self.compute_pool}
                MIN_INSTANCES = {min_instances}
                MAX_INSTANCES = {max_instances}
                FROM SPECIFICATION '{spec_json}'
        """

        try:
            self._execute(create_sql)
            return full_service
        except Exception as e:
            self._state = DeploymentState.FAILED
            raise RuntimeError(f"Failed to create service: {e}")

    def wait_for_ready(
        self,
        service_name: str,
        timeout_seconds: int = 300,
        poll_interval: int = 10,
    ) -> bool:
        self._state = DeploymentState.WAITING_READY

        if "." not in service_name:
            service_name = f"{self.database}.{self.schema}.{service_name}"

        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            show_sql = f"DESCRIBE SERVICE {service_name}"
            try:
                results = self._execute(show_sql)
                if results:
                    status = results[0].get("status", "")
                    if status.upper() == "READY":
                        return True
                    elif status.upper() in ["FAILED", "SUSPENDED"]:
                        return False
            except Exception:
                pass

            time.sleep(poll_interval)

        return False

    def get_endpoint_url(self, service_name: str) -> str | None:
        if "." not in service_name:
            service_name = f"{self.database}.{self.schema}.{service_name}"

        show_sql = f"SHOW ENDPOINTS IN SERVICE {service_name}"
        try:
            results = self._execute(show_sql)
            for r in results:
                if r.get("is_public"):
                    return r.get("ingress_url")
        except Exception:
            pass
        return None

    def health_check(self, endpoint_url: str) -> bool:
        self._state = DeploymentState.HEALTH_CHECK
        return True

    def deploy(
        self,
        service_name: str,
        image_tag: str,
        wait_ready: bool = True,
        timeout_seconds: int = 300,
    ) -> DeploymentResult:
        try:
            self.ensure_compute_pool()
            self.ensure_image_repo()

            full_service = self.create_service(service_name, image_tag)

            if wait_ready:
                is_ready = self.wait_for_ready(service_name, timeout_seconds)
                if not is_ready:
                    self._state = DeploymentState.FAILED
                    return DeploymentResult(
                        status=DeploymentState.FAILED,
                        service_name=full_service,
                        endpoint_url=None,
                        error="Service failed to become ready",
                    )

            endpoint_url = self.get_endpoint_url(service_name)

            self._state = DeploymentState.REGISTERING

            self._state = DeploymentState.COMPLETE
            return DeploymentResult(
                status=DeploymentState.COMPLETE,
                service_name=full_service,
                endpoint_url=endpoint_url,
                error=None,
            )

        except Exception as e:
            self._state = DeploymentState.FAILED
            return DeploymentResult(
                status=DeploymentState.FAILED,
                service_name=service_name,
                endpoint_url=None,
                error=str(e),
            )

    def stop_service(self, service_name: str) -> bool:
        if "." not in service_name:
            service_name = f"{self.database}.{self.schema}.{service_name}"

        try:
            self._execute(f"ALTER SERVICE {service_name} SUSPEND")
            return True
        except Exception:
            return False

    def delete_service(self, service_name: str) -> bool:
        if "." not in service_name:
            service_name = f"{self.database}.{self.schema}.{service_name}"

        try:
            self._execute(f"DROP SERVICE IF EXISTS {service_name}")
            return True
        except Exception:
            return False

    def list_services(self) -> list[dict[str, Any]]:
        sql = f"SHOW SERVICES IN SCHEMA {self.database}.{self.schema}"
        try:
            return self._execute(sql)
        except Exception:
            return []


def deploy_to_spcs(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node function for SPCS deployment."""
    deployer = SPCSDeployer()

    service_name = state.get("service_name", "APP_SERVICE")
    image_tag = state.get("image_tag")

    if not image_tag:
        return {
            "deployment_result": {"error": "image_tag required"},
            "current_state": DeploymentState.FAILED.value,
        }

    result = deployer.deploy(
        service_name=service_name,
        image_tag=image_tag,
        wait_ready=state.get("wait_ready", True),
        timeout_seconds=state.get("timeout_seconds", 300),
    )

    return {
        "deployment_result": result.to_dict(),
        "endpoint_url": result.endpoint_url,
        "current_state": deployer._state.value,
        "messages": state.get("messages", [])
        + [
            {
                "role": "system",
                "content": f"Deployment {result.status.value}: {result.service_name}",
            }
        ],
    }
