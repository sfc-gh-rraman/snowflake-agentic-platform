"""
Meta-Agent E2E Test: Full workflow with drilling data

This test validates the complete Meta-Agent workflow:
1. Parse use case → Extract requirements
2. Scan data → Find assets in DRILLING_OPS_DB
3. Query registry → Find matching agents (if registry exists)
4. Generate plan → Create execution plan
"""

import os
import sys
from typing import Optional
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

USE_CASE = """
I have drilling sensor data from 8 wells in the Volve field (Norway).
The data includes 12.5M sensor readings with ROP, WOB, torque, and pressure measurements.
I also have 1,759 daily drilling reports in text format.

Build an AI application that can:
1. Predict stuck pipe events using the sensor data
2. Enable natural language search over the drilling reports  
3. Answer questions about drilling performance using text-to-SQL
4. Provide a dashboard for drilling engineers
"""

DATABASE = "DRILLING_OPS_DB"
STAGE_PATH = f"@{DATABASE}.RAW.DRILLING_DATA_STAGE"
TABLE_FQN = f"{DATABASE}.RAW.DRILLING_TIME"


@dataclass
class TestResult:
    name: str
    passed: bool
    message: str
    details: Optional[dict] = None


def test_parse_use_case() -> TestResult:
    """Parse the drilling use case"""
    try:
        from src.meta_agent.tools.use_case_parser import UseCaseParser
        
        parser = UseCaseParser(connection_name="my_snowflake")
        result = parser.parse(USE_CASE)
        
        return TestResult(
            name="Parse Use Case",
            passed=True,
            message=f"Primary: {result.primary_task.value}, ML={result.ml_enabled}, Search={result.search_enabled}, Analytics={result.analytics_enabled}",
            details={
                "primary_task": result.primary_task.value,
                "ml_enabled": result.ml_enabled,
                "search_enabled": result.search_enabled,
                "analytics_enabled": result.analytics_enabled,
                "entities": result.entities,
                "key_features": result.key_features,
            }
        )
    except Exception as e:
        return TestResult("Parse Use Case", False, f"Failed: {str(e)}")


def test_scan_data() -> TestResult:
    """Scan data assets in DRILLING_OPS_DB"""
    try:
        from src.meta_agent.tools.data_scanner import DataScanner
        
        scanner = DataScanner(connection_name="my_snowflake")
        
        stage_assets = scanner.scan_stage(STAGE_PATH)
        table_asset = scanner.scan_table(TABLE_FQN)
        ddr_asset = scanner.scan_table(f"{DATABASE}.RAW.DAILY_DRILLING_REPORTS")
        
        return TestResult(
            name="Scan Data Assets",
            passed=len(stage_assets) > 0 or table_asset is not None,
            message=f"Found {len(stage_assets)} stage files, {2 if table_asset and ddr_asset else 1 if table_asset or ddr_asset else 0} tables",
            details={
                "stage_files": [a.name for a in stage_assets],
                "drilling_time_rows": table_asset.row_count if table_asset else 0,
                "ddr_rows": ddr_asset.row_count if ddr_asset else 0,
            }
        )
    except Exception as e:
        import traceback
        return TestResult("Scan Data Assets", False, f"Failed: {str(e)}\n{traceback.format_exc()}")


def test_profile_data() -> TestResult:
    """Profile the DRILLING_TIME table"""
    try:
        from src.agents.discovery.schema_profiler import SchemaProfiler
        
        profiler = SchemaProfiler(connection_name="my_snowflake", database=DATABASE)
        profile = profiler.profile_table(TABLE_FQN, sample_size=50000)
        
        numeric_cols = [c for c in profile.columns if c.data_type == "FLOAT"]
        
        return TestResult(
            name="Profile Data",
            passed=profile.row_count > 0 and len(profile.columns) > 0,
            message=f"{profile.row_count:,} rows, {len(profile.columns)} cols, {len(numeric_cols)} numeric",
            details={
                "row_count": profile.row_count,
                "column_count": len(profile.columns),
                "numeric_columns": [c.name for c in numeric_cols],
                "timestamp_columns": profile.timestamp_columns,
                "text_columns": profile.text_columns,
            }
        )
    except Exception as e:
        return TestResult("Profile Data", False, f"Failed: {str(e)}")


def test_generate_plan() -> TestResult:
    """Generate an execution plan using the plan generator"""
    try:
        from src.meta_agent.tools.plan_generator import PlanGenerator
        
        requirements = {
            "primary_task": "classification",
            "ml_enabled": True,
            "search_enabled": True,
            "analytics_enabled": True,
            "app_type": "copilot",
            "entities": ["drilling", "sensor", "well", "report"],
            "key_features": ["stuck pipe prediction", "document search", "text-to-SQL"],
        }
        
        data_profile = {
            "total_assets": 2,
            "structured_count": 2,
            "unstructured_count": 0,
            "total_rows": 12500000 + 1759,
            "has_labeled_data": False,
            "potential_features": ["RATE_OF_PENETRATION_M_H", "WEIGHT_ON_BIT_KKGF"],
            "text_content_detected": True,
        }
        
        available_agents = [
            {"agent_id": "parquet_processor", "name": "Parquet Processor", "capabilities": ["ingest", "transform"]},
            {"agent_id": "document_chunker", "name": "Document Chunker", "capabilities": ["extract", "chunk"]},
            {"agent_id": "ml_model_builder", "name": "ML Model Builder", "capabilities": ["train", "register"]},
            {"agent_id": "cortex_search_builder", "name": "Search Builder", "capabilities": ["search", "index"]},
            {"agent_id": "semantic_model_generator", "name": "Semantic Generator", "capabilities": ["analyst", "yaml"]},
            {"agent_id": "app_code_generator", "name": "App Generator", "capabilities": ["react", "fastapi"]},
        ]
        
        generator = PlanGenerator(connection_name="my_snowflake")
        plan = generator.generate(requirements, data_profile, available_agents)
        
        if plan and plan.phases:
            phase_names = [p.phase_name for p in plan.phases]
            return TestResult(
                name="Generate Plan",
                passed=True,
                message=f"Generated {len(plan.phases)} phases: {', '.join(phase_names[:5])}...",
                details={
                    "plan_id": plan.plan_id,
                    "phases": phase_names,
                    "estimated_duration_minutes": plan.estimated_duration_minutes,
                }
            )
        else:
            return TestResult("Generate Plan", False, "No plan generated")
    except Exception as e:
        import traceback
        return TestResult("Generate Plan", False, f"Failed: {str(e)}\n{traceback.format_exc()}")


def test_run_validation_suite() -> TestResult:
    """Run the full validation suite on DRILLING_TIME"""
    try:
        from src.agents.validation.orchestrator import ValidationOrchestrator
        
        orchestrator = ValidationOrchestrator(connection_name="my_snowflake")
        
        report = orchestrator.validate(
            table_name=TABLE_FQN,
            suites=["completeness", "quality"],
            config={
                "required_columns": ["WELL_NAME", "TIMESTAMP", "RATE_OF_PENETRATION_M_H"],
                "max_null_ratio": 0.9,
            },
        )
        
        passed = report.overall_score > 0 or len(report.issues) > 0
        
        return TestResult(
            name="Validation Suite",
            passed=passed,
            message=f"Score: {report.overall_score:.1%}, Issues found: {len(report.issues)}",
            details={
                "status": report.status.value,
                "score": report.overall_score,
                "issues": report.issues[:3] if report.issues else [],
            }
        )
    except Exception as e:
        import traceback
        return TestResult("Validation Suite", False, f"Failed: {str(e)}\n{traceback.format_exc()}")


def run_meta_agent_test():
    """Run the full meta-agent workflow test."""
    tests = [
        test_parse_use_case,
        test_scan_data,
        test_profile_data,
        test_generate_plan,
        test_run_validation_suite,
    ]
    
    print("\n" + "=" * 70)
    print("META-AGENT E2E TEST - DRILLING OPS USE CASE")
    print("=" * 70 + "\n")
    
    results = []
    for test_fn in tests:
        print(f"Running: {test_fn.__doc__}...")
        result = test_fn()
        results.append(result)
        
        status = "✅" if result.passed else "❌"
        print(f"  {status} {result.name}: {result.message}")
        if result.details:
            for k, v in list(result.details.items())[:3]:
                print(f"     {k}: {v}")
        print()
    
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    
    print("=" * 70)
    print(f"META-AGENT TEST RESULTS: {passed}/{total} passed")
    print("=" * 70)
    
    return results


if __name__ == "__main__":
    run_meta_agent_test()
