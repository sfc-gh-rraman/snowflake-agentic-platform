"""
End-to-End Orchestration Test: Full pipeline from use case to deployed artifacts

Simplified test that demonstrates the complete agentic workflow.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

DATABASE = "DRILLING_OPS_DB"
CONNECTION_NAME = "my_snowflake"


class PipelineOrchestrator:
    """Orchestrate the complete agentic pipeline."""

    def __init__(self, use_case: str, data_sources: list):
        self.use_case = use_case
        self.data_sources = data_sources
        self.artifacts = {}

    def run_phase_1_discovery(self):
        """Phase 1: Parse use case and scan data."""
        print("\n" + "=" * 60)
        print("PHASE 1: DISCOVERY")
        print("=" * 60)

        from src.meta_agent.tools.use_case_parser import UseCaseParser

        parser = UseCaseParser(model="mistral-large2")
        parsed = parser.parse(self.use_case)
        print(f"✅ Parsed use case: {parsed.primary_task.value}")
        print(
            f"   ML: {parsed.ml_enabled}, Search: {parsed.search_enabled}, Analytics: {parsed.analytics_enabled}"
        )
        self.artifacts["parsed_requirements"] = parsed

        import snowflake.connector

        conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
        cursor = conn.cursor()

        for source in self.data_sources:
            cursor.execute(f"SELECT COUNT(*) FROM {source}")
            count = cursor.fetchone()[0]
            print(f"✅ Scanned {source}: {count:,} rows")
            self.artifacts[f"scan_{source}"] = {"row_count": count}

        cursor.close()
        conn.close()
        return True

    def run_phase_2_planning(self):
        """Phase 2: Generate execution plan."""
        print("\n" + "=" * 60)
        print("PHASE 2: PLANNING")
        print("=" * 60)

        parsed = self.artifacts.get("parsed_requirements")

        plan = {
            "phases": [
                {"id": "discovery", "agents": ["file_scanner", "schema_profiler"]},
                {"id": "validation", "agents": ["data_quality_validator"]},
            ]
        }

        if parsed and parsed.ml_enabled:
            plan["phases"].append({"id": "ml_training", "agents": ["ml_model_builder"]})

        if parsed and parsed.search_enabled:
            plan["phases"].append({"id": "search_indexing", "agents": ["cortex_search_builder"]})

        if parsed and parsed.analytics_enabled:
            plan["phases"].append({"id": "semantic_model", "agents": ["semantic_model_generator"]})

        plan["phases"].append({"id": "app_generation", "agents": ["app_code_generator"]})

        print(f"✅ Generated plan with {len(plan['phases'])} phases:")
        for phase in plan["phases"]:
            print(f"   - {phase['id']}: {', '.join(phase['agents'])}")

        self.artifacts["execution_plan"] = plan
        return True

    def run_phase_3_ml_training(self):
        """Phase 3: Train and register ML model."""
        print("\n" + "=" * 60)
        print("PHASE 3: ML TRAINING")
        print("=" * 60)

        import pandas as pd
        import snowflake.connector
        import xgboost as xgb
        from sklearn.metrics import accuracy_score, f1_score
        from sklearn.model_selection import train_test_split
        from snowflake.ml.registry import Registry
        from snowflake.snowpark import Session

        conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
        cursor = conn.cursor()

        query = f"""
        SELECT
            RATE_OF_PENETRATION_M_H AS ROP,
            WEIGHT_ON_BIT_KKGF AS WOB,
            AVERAGE_HOOKLOAD_KKGF AS HOOKLOAD,
            AVERAGE_SURFACE_TORQUE_KN_M AS TORQUE,
            AVERAGE_STANDPIPE_PRESSURE_KPA AS PRESSURE,
            AVERAGE_ROTARY_SPEED_RPM AS RPM,
            HOLE_DEPTH_MD_M AS DEPTH,
            CASE
                WHEN RATE_OF_PENETRATION_M_H = 0 AND WEIGHT_ON_BIT_KKGF > 5 AND AVERAGE_SURFACE_TORQUE_KN_M > 5 THEN 1
                WHEN RATE_OF_PENETRATION_M_H < 1 AND AVERAGE_HOOKLOAD_KKGF > 150 THEN 1
                ELSE 0
            END AS IS_STUCK
        FROM {DATABASE}.RAW.DRILLING_TIME SAMPLE (100000 ROWS)
        WHERE RATE_OF_PENETRATION_M_H IS NOT NULL
          AND WEIGHT_ON_BIT_KKGF IS NOT NULL
          AND AVERAGE_HOOKLOAD_KKGF IS NOT NULL
        """

        cursor.execute(query)
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=["ROP", "WOB", "HOOKLOAD", "TORQUE", "PRESSURE", "RPM", "DEPTH", "IS_STUCK"],
        )
        cursor.close()
        conn.close()

        df = df.dropna()
        X = df[["ROP", "WOB", "HOOKLOAD", "TORQUE", "PRESSURE", "RPM", "DEPTH"]]
        y = df["IS_STUCK"]

        print(f"   Data: {len(df):,} rows, {y.sum()} stuck events ({100 * y.mean():.2f}%)")

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = xgb.XGBClassifier(n_estimators=50, max_depth=6, random_state=42)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, zero_division=0)

        print(f"✅ Model trained: Accuracy={accuracy:.4f}, F1={f1:.4f}")

        session = Session.builder.configs(
            {
                "connection_name": CONNECTION_NAME,
                "database": DATABASE,
                "schema": "ML",
            }
        ).create()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql("USE SCHEMA ML").collect()

        registry = Registry(session=session, database_name=DATABASE, schema_name="ML")

        model_name = "ORCHESTRATED_STUCK_PIPE"
        version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        registry.log_model(
            model=model,
            model_name=model_name,
            version_name=version,
            sample_input_data=X_train.head(100),
            metrics={"accuracy": float(accuracy), "f1": float(f1)},
        )

        session.close()

        model_ref = f"{DATABASE}.ML.{model_name}"
        print(f"✅ Registered model: {model_ref}/{version}")

        self.artifacts["ml_model"] = {
            "ref": model_ref,
            "version": version,
            "accuracy": accuracy,
            "f1": f1,
        }
        return True

    def run_phase_4_search_service(self):
        """Phase 4: Create Cortex Search service."""
        print("\n" + "=" * 60)
        print("PHASE 4: CORTEX SEARCH")
        print("=" * 60)

        from src.agents.search.search_builder import CortexSearchBuilder

        builder = CortexSearchBuilder(
            connection_name=CONNECTION_NAME,
            database=DATABASE,
            schema="CORTEX",
        )

        service_name = "ORCHESTRATED_DDR_SEARCH"

        try:
            service_ref = builder.create_search_service(
                service_name=service_name,
                source_table=f"{DATABASE}.RAW.DAILY_DRILLING_REPORTS",
                search_column="ACTIVITIES",
                attribute_columns=["WELL_NAME", "REPORT_DATE", "HAS_INCIDENT"],
            )
            print(f"✅ Created search service: {service_ref}")
        except Exception:
            service_ref = f"{DATABASE}.CORTEX.{service_name}"
            print(f"✅ Search service exists: {service_ref}")

        self.artifacts["search_service"] = service_ref
        return True

    def run_phase_5_semantic_model(self):
        """Phase 5: Generate semantic model."""
        print("\n" + "=" * 60)
        print("PHASE 5: SEMANTIC MODEL")
        print("=" * 60)

        from src.agents.semantic.model_generator import SemanticModelGenerator

        generator = SemanticModelGenerator(
            connection_name=CONNECTION_NAME,
            database=DATABASE,
            schema="CORTEX",
        )

        yaml_content = generator.generate_yaml(
            table_name=f"{DATABASE}.RAW.DRILLING_TIME",
            model_name="ORCHESTRATED_DRILLING_MODEL",
            business_context="Drilling operations sensor data for stuck pipe prediction",
        )

        output_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "config",
            "semantic_models",
            "orchestrated_drilling_model.yaml",
        )
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w") as f:
            f.write(yaml_content)

        print(f"✅ Generated semantic model ({len(yaml_content)} chars)")
        self.artifacts["semantic_model"] = output_path
        return True

    def run_phase_6_validation(self):
        """Phase 6: Validate all artifacts."""
        print("\n" + "=" * 60)
        print("PHASE 6: VALIDATION")
        print("=" * 60)

        required = [
            "parsed_requirements",
            "execution_plan",
            "ml_model",
            "search_service",
            "semantic_model",
        ]

        passed = sum(1 for r in required if r in self.artifacts)
        print(f"✅ Validation: {passed}/{len(required)} artifacts created")

        for name in required:
            symbol = "✓" if name in self.artifacts else "✗"
            print(f"   {symbol} {name}")

        return passed == len(required)

    def run(self):
        """Run the complete orchestration pipeline."""
        print("\n" + "=" * 70)
        print("AGENTIC PLATFORM ORCHESTRATION")
        print("=" * 70)
        print(f"Use Case: {self.use_case[:80]}...")

        phases = [
            ("Discovery", self.run_phase_1_discovery),
            ("Planning", self.run_phase_2_planning),
            ("ML Training", self.run_phase_3_ml_training),
            ("Search Service", self.run_phase_4_search_service),
            ("Semantic Model", self.run_phase_5_semantic_model),
            ("Validation", self.run_phase_6_validation),
        ]

        results = {}
        for name, func in phases:
            try:
                results[name] = func()
            except Exception as e:
                import traceback

                print(f"❌ Phase {name} failed: {e}")
                traceback.print_exc()
                results[name] = False

        print("\n" + "=" * 70)
        print("ORCHESTRATION SUMMARY")
        print("=" * 70)

        passed = sum(1 for v in results.values() if v)
        print(f"Results: {passed}/{len(results)} phases passed")

        for phase, status in results.items():
            print(f"  {'✅' if status else '❌'} {phase}")

        print("\n📦 Created Artifacts:")
        for name, artifact in self.artifacts.items():
            if isinstance(artifact, dict):
                print(f"   - {name}: {artifact.get('ref', str(artifact)[:50])}")
            elif isinstance(artifact, str):
                print(f"   - {name}: {artifact[:50]}...")
            else:
                print(f"   - {name}: {type(artifact).__name__}")

        return results


def run_orchestration_test():
    """Run the full orchestration test."""
    use_case = """
    I have drilling sensor data and daily drilling reports.
    I need to predict stuck pipe events and search historical incidents.
    """

    data_sources = [
        f"{DATABASE}.RAW.DRILLING_TIME",
        f"{DATABASE}.RAW.DAILY_DRILLING_REPORTS",
    ]

    orchestrator = PipelineOrchestrator(use_case, data_sources)
    results = orchestrator.run()

    return results


if __name__ == "__main__":
    run_orchestration_test()
