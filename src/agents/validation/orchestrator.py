"""Validation orchestrator - runs all validation suites with retry logic."""

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from .completeness import CompletenessValidator
from .schema_validator import SchemaValidator
from .quality import QualityValidator
from .semantic import SemanticValidator
from .ml_specific import MLValidator


class ValidationStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    RETRYING = "retrying"
    ESCALATED = "escalated"


@dataclass
class ValidationReport:
    status: ValidationStatus
    overall_score: float
    suite_results: Dict[str, Any]
    issues: List[str]
    recommendations: List[str]
    retry_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "overall_score": self.overall_score,
            "suite_results": self.suite_results,
            "issues": self.issues,
            "recommendations": self.recommendations,
            "retry_count": self.retry_count,
        }


class ValidationOrchestrator:
    """Orchestrate validation suites with retry logic."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        max_retries: int = 3,
        min_pass_score: float = 0.7,
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.max_retries = max_retries
        self.min_pass_score = min_pass_score
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

    def run_completeness(
        self,
        table_name: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        validator = CompletenessValidator(session=self.session)
        result = validator.validate(
            table_name=table_name,
            required_columns=config.get("required_columns"),
            min_rows=config.get("min_rows", 1),
            coverage_columns=config.get("coverage_columns"),
            min_coverage=config.get("min_coverage", 0.9),
        )
        return result.to_dict()

    def run_schema(
        self,
        table_name: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        validator = SchemaValidator(
            session=self.session,
            naming_convention=config.get("naming_convention", "SCREAMING_SNAKE"),
        )
        result = validator.validate(table_name=table_name)
        return result.to_dict()

    def run_quality(
        self,
        table_name: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        validator = QualityValidator(session=self.session)
        result = validator.validate(
            table_name=table_name,
            key_columns=config.get("key_columns"),
            numeric_columns=config.get("numeric_columns"),
            max_null_ratio=config.get("max_null_ratio", 0.3),
            max_duplicate_ratio=config.get("max_duplicate_ratio", 0.01),
        )
        return result.to_dict()

    def run_semantic(
        self,
        table_name: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        validator = SemanticValidator(connection_name=self.connection_name)
        result = validator.validate(
            table_name=table_name,
            business_context=config.get("business_context", "General validation"),
        )
        return result.to_dict()

    def run_ml_specific(
        self,
        table_name: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        validator = MLValidator(session=self.session)
        result = validator.validate(
            table_name=table_name,
            label_column=config.get("label_column"),
            feature_columns=config.get("feature_columns"),
            timestamp_column=config.get("timestamp_column"),
            id_column=config.get("id_column"),
        )
        return result.to_dict()

    def validate(
        self,
        table_name: str,
        suites: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> ValidationReport:
        suites = suites or ["completeness", "schema", "quality"]
        config = config or {}

        suite_results = {}
        all_issues = []
        all_recommendations = []
        scores = []
        retry_count = 0

        suite_runners = {
            "completeness": self.run_completeness,
            "schema": self.run_schema,
            "quality": self.run_quality,
            "semantic": self.run_semantic,
            "ml_specific": self.run_ml_specific,
        }

        for suite_name in suites:
            if suite_name not in suite_runners:
                continue

            suite_config = config.get(suite_name, {})
            success = False
            attempt = 0

            while not success and attempt < self.max_retries:
                try:
                    result = suite_runners[suite_name](table_name, suite_config)
                    suite_results[suite_name] = result
                    
                    if result.get("passed"):
                        success = True
                    else:
                        all_issues.extend(result.get("issues", []))
                        attempt += 1
                        retry_count += 1

                    scores.append(result.get("score", 0))

                    if "recommendations" in result:
                        all_recommendations.extend(result["recommendations"])

                except Exception as e:
                    attempt += 1
                    retry_count += 1
                    suite_results[suite_name] = {
                        "passed": False,
                        "error": str(e),
                        "score": 0,
                    }

        overall_score = sum(scores) / len(scores) if scores else 0
        all_passed = all(r.get("passed", False) for r in suite_results.values())

        if all_passed and overall_score >= self.min_pass_score:
            status = ValidationStatus.PASSED
        elif retry_count >= self.max_retries * len(suites):
            status = ValidationStatus.ESCALATED
        else:
            status = ValidationStatus.FAILED

        return ValidationReport(
            status=status,
            overall_score=overall_score,
            suite_results=suite_results,
            issues=list(set(all_issues)),
            recommendations=list(set(all_recommendations)),
            retry_count=retry_count,
        )


def validate_data(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function for data validation."""
    orchestrator = ValidationOrchestrator()
    
    tables = state.get("tables_to_validate", [])
    suites = state.get("validation_suites", ["completeness", "schema", "quality"])
    config = state.get("validation_config", {})

    validation_results = {}
    all_passed = True

    for table in tables:
        report = orchestrator.validate(table, suites, config)
        validation_results[table] = report.to_dict()
        
        if report.status != ValidationStatus.PASSED:
            all_passed = False

    return {
        "validation_results": validation_results,
        "validation_passed": all_passed,
        "current_state": "COMPLETE" if all_passed else "FAILED",
        "messages": state.get("messages", []) + [{
            "role": "system",
            "content": f"Validated {len(tables)} tables. All passed: {all_passed}",
        }],
    }
