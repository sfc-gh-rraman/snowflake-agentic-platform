"""ML Model Builder agent - 6-state pipeline for model training."""

import json
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class MLModelBuilderState(str, Enum):
    TASK_CLASSIFICATION = "TASK_CLASSIFICATION"
    FEATURE_SELECTION = "FEATURE_SELECTION"
    TRAINING = "TRAINING"
    EVALUATION = "EVALUATION"
    REGISTRATION = "REGISTRATION"
    EXPLAINABILITY = "EXPLAINABILITY"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class TaskType(str, Enum):
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    ANOMALY_DETECTION = "anomaly_detection"


@dataclass
class ModelMetrics:
    task_type: TaskType
    metrics: Dict[str, float]
    feature_importance: Optional[Dict[str, float]] = None
    confusion_matrix: Optional[List[List[int]]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_type": self.task_type.value,
            "metrics": self.metrics,
            "feature_importance": self.feature_importance,
            "confusion_matrix": self.confusion_matrix,
        }


class MLModelBuilder:
    """Build and register ML models using Snowpark ML."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "ML",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.schema = schema
        self._session = None
        self._state = MLModelBuilderState.TASK_CLASSIFICATION

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

    def _get_snowpark_session(self):
        if hasattr(self.session, 'sql'):
            return self.session
        raise RuntimeError("Snowpark session required for ML operations")

    def classify_task(
        self,
        table_name: str,
        target_column: str,
    ) -> TaskType:
        self._state = MLModelBuilderState.TASK_CLASSIFICATION

        session = self._get_snowpark_session()
        df = session.table(table_name)
        
        distinct_count = df.select(target_column).distinct().count()
        total_count = df.count()

        if distinct_count <= 20 and distinct_count < total_count * 0.1:
            return TaskType.CLASSIFICATION
        else:
            return TaskType.REGRESSION

    def select_features(
        self,
        table_name: str,
        target_column: str,
        exclude_columns: Optional[List[str]] = None,
    ) -> List[str]:
        self._state = MLModelBuilderState.FEATURE_SELECTION
        
        exclude_columns = exclude_columns or []
        exclude_set = {c.upper() for c in exclude_columns}
        exclude_set.add(target_column.upper())

        session = self._get_snowpark_session()
        df = session.table(table_name)

        features = []
        for field in df.schema.fields:
            col_name = field.name
            if col_name.upper() in exclude_set:
                continue
            
            dtype = str(field.datatype).upper()
            if any(t in dtype for t in ['NUMBER', 'FLOAT', 'INT', 'DOUBLE', 'DECIMAL', 'BOOLEAN']):
                features.append(col_name)

        return features

    def train(
        self,
        table_name: str,
        target_column: str,
        feature_columns: List[str],
        task_type: TaskType,
        test_size: float = 0.2,
    ) -> Any:
        self._state = MLModelBuilderState.TRAINING

        from snowflake.ml.modeling.preprocessing import StandardScaler
        from snowflake.ml.modeling.pipeline import Pipeline
        
        if task_type == TaskType.CLASSIFICATION:
            from snowflake.ml.modeling.xgboost import XGBClassifier
            model = XGBClassifier(
                input_cols=feature_columns,
                label_cols=[target_column],
                output_cols=["PREDICTION"],
            )
        else:
            from snowflake.ml.modeling.xgboost import XGBRegressor
            model = XGBRegressor(
                input_cols=feature_columns,
                label_cols=[target_column],
                output_cols=["PREDICTION"],
            )

        scaler = StandardScaler(
            input_cols=feature_columns,
            output_cols=feature_columns,
        )

        pipeline = Pipeline(steps=[
            ("scaler", scaler),
            ("model", model),
        ])

        session = self._get_snowpark_session()
        df = session.table(table_name)

        train_df, test_df = df.random_split([1 - test_size, test_size], seed=42)
        
        pipeline.fit(train_df)

        return {
            "pipeline": pipeline,
            "train_df": train_df,
            "test_df": test_df,
        }

    def evaluate(
        self,
        pipeline: Any,
        test_df: Any,
        target_column: str,
        task_type: TaskType,
    ) -> ModelMetrics:
        self._state = MLModelBuilderState.EVALUATION

        predictions_df = pipeline.predict(test_df)

        from snowflake.ml.modeling.metrics import (
            accuracy_score,
            precision_score,
            recall_score,
            f1_score,
            mean_squared_error,
            mean_absolute_error,
            r2_score,
        )

        if task_type == TaskType.CLASSIFICATION:
            y_true = predictions_df.select(target_column).to_pandas()[target_column]
            y_pred = predictions_df.select("PREDICTION").to_pandas()["PREDICTION"]

            metrics = {
                "accuracy": float(accuracy_score(y_true=y_true, y_pred=y_pred)),
                "precision": float(precision_score(y_true=y_true, y_pred=y_pred, average='weighted')),
                "recall": float(recall_score(y_true=y_true, y_pred=y_pred, average='weighted')),
                "f1": float(f1_score(y_true=y_true, y_pred=y_pred, average='weighted')),
            }
        else:
            y_true = predictions_df.select(target_column).to_pandas()[target_column]
            y_pred = predictions_df.select("PREDICTION").to_pandas()["PREDICTION"]

            metrics = {
                "mse": float(mean_squared_error(y_true=y_true, y_pred=y_pred)),
                "mae": float(mean_absolute_error(y_true=y_true, y_pred=y_pred)),
                "r2": float(r2_score(y_true=y_true, y_pred=y_pred)),
                "rmse": float(mean_squared_error(y_true=y_true, y_pred=y_pred) ** 0.5),
            }

        return ModelMetrics(
            task_type=task_type,
            metrics=metrics,
        )

    def register(
        self,
        pipeline: Any,
        model_name: str,
        version: str,
        metrics: ModelMetrics,
        sample_input: Any,
    ) -> str:
        self._state = MLModelBuilderState.REGISTRATION

        from snowflake.ml.registry import Registry
        
        session = self._get_snowpark_session()
        registry = Registry(
            session=session,
            database_name=self.database,
            schema_name=self.schema,
        )

        model_version = registry.log_model(
            model=pipeline,
            model_name=model_name,
            version_name=version,
            sample_input_data=sample_input,
            metrics=metrics.metrics,
            comment=f"Auto-generated {metrics.task_type.value} model",
        )

        return f"{self.database}.{self.schema}.{model_name}/{version}"

    def explain(
        self,
        pipeline: Any,
        sample_df: Any,
        feature_columns: List[str],
    ) -> Dict[str, Any]:
        self._state = MLModelBuilderState.EXPLAINABILITY

        try:
            import shap
            
            model_step = pipeline.steps[-1][1]
            if hasattr(model_step, 'to_sklearn'):
                sklearn_model = model_step.to_sklearn()
                
                sample_pd = sample_df.select(feature_columns).limit(100).to_pandas()
                
                explainer = shap.TreeExplainer(sklearn_model)
                shap_values = explainer.shap_values(sample_pd)

                feature_importance = {}
                if isinstance(shap_values, list):
                    importance = abs(shap_values[0]).mean(axis=0)
                else:
                    importance = abs(shap_values).mean(axis=0)

                for i, col in enumerate(feature_columns):
                    feature_importance[col] = float(importance[i])

                return {
                    "method": "SHAP",
                    "feature_importance": feature_importance,
                }
        except Exception as e:
            return {
                "method": "unavailable",
                "error": str(e),
            }

        return {"method": "unavailable"}

    def build(
        self,
        table_name: str,
        target_column: str,
        model_name: str,
        version: str = "v1",
        exclude_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        result = {
            "model_name": model_name,
            "version": version,
            "status": "started",
        }

        try:
            task_type = self.classify_task(table_name, target_column)
            result["task_type"] = task_type.value

            features = self.select_features(table_name, target_column, exclude_columns)
            result["features"] = features

            train_result = self.train(table_name, target_column, features, task_type)
            
            metrics = self.evaluate(
                train_result["pipeline"],
                train_result["test_df"],
                target_column,
                task_type,
            )
            result["metrics"] = metrics.to_dict()

            model_ref = self.register(
                train_result["pipeline"],
                model_name,
                version,
                metrics,
                train_result["train_df"].limit(100),
            )
            result["model_reference"] = model_ref

            explanation = self.explain(
                train_result["pipeline"],
                train_result["test_df"],
                features,
            )
            result["explanation"] = explanation

            self._state = MLModelBuilderState.COMPLETE
            result["status"] = "complete"

        except Exception as e:
            self._state = MLModelBuilderState.FAILED
            result["status"] = "failed"
            result["error"] = str(e)

        return result


def build_ml_model(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function for ML model building."""
    builder = MLModelBuilder()

    table_name = state.get("source_table")
    target_column = state.get("target_column")
    model_name = state.get("model_name", "AUTO_MODEL")
    version = state.get("version", "v1")

    if not table_name or not target_column:
        return {
            "ml_result": {"error": "source_table and target_column required"},
            "current_state": MLModelBuilderState.FAILED.value,
        }

    result = builder.build(
        table_name=table_name,
        target_column=target_column,
        model_name=model_name,
        version=version,
        exclude_columns=state.get("exclude_columns"),
    )

    return {
        "ml_result": result,
        "model_reference": result.get("model_reference"),
        "current_state": builder._state.value,
        "messages": state.get("messages", []) + [{
            "role": "system",
            "content": f"ML model build {result.get('status')}: {result.get('task_type', 'unknown')} task",
        }],
    }
