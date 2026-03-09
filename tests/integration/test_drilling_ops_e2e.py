"""
End-to-End Validation: Agentic Platform with Drilling Ops Data
"""

import os
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import snowflake.connector
from snowflake.connector import SnowflakeConnection

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

DATABASE = "DRILLING_OPS_DB"
RAW_SCHEMA = "RAW"
CONNECTION_NAME = "my_snowflake"
TABLE_FQN = f"{DATABASE}.{RAW_SCHEMA}.DRILLING_TIME"
STAGE_PATH = f"@{DATABASE}.{RAW_SCHEMA}.DRILLING_DATA_STAGE"

USE_CASE = """
Analyze drilling sensor data to predict stuck pipe events.
Enable natural language search over daily drilling reports.
Build a dashboard showing key drilling KPIs.
"""


@dataclass
class TestResult:
    name: str
    passed: bool
    message: str
    details: dict | None = None


def get_connection() -> SnowflakeConnection:
    return snowflake.connector.connect(connection_name=CONNECTION_NAME)


def test_1_connection_and_access() -> TestResult:
    """Test 1: Connection & Access"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.DRILLING_TIME")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if count > 0:
            return TestResult(
                "Connection & Access", True, f"Connected. DRILLING_TIME has {count:,} rows."
            )
        else:
            return TestResult("Connection & Access", False, "Table is empty")
    except Exception as e:
        return TestResult("Connection & Access", False, f"Failed: {str(e)}")


def test_2_file_scanner() -> TestResult:
    """Test 2: FileScanner"""
    try:
        from src.agents.discovery.file_scanner import FileScanner

        scanner = FileScanner(connection_name=CONNECTION_NAME, database=DATABASE)
        files = scanner.scan_stage(STAGE_PATH)

        if files:
            return TestResult(
                "FileScanner", True, f"Found {len(files)} files: {[f.name for f in files]}"
            )
        else:
            return TestResult("FileScanner", False, "No files found")
    except Exception as e:
        return TestResult("FileScanner", False, f"Failed: {str(e)}")


def test_3_schema_profiler() -> TestResult:
    """Test 3: SchemaProfiler"""
    try:
        from src.agents.discovery.schema_profiler import SchemaProfiler

        profiler = SchemaProfiler(connection_name=CONNECTION_NAME, database=DATABASE)
        profile = profiler.profile_table(TABLE_FQN, sample_size=10000)

        if profile and profile.columns:
            return TestResult(
                "SchemaProfiler",
                True,
                f"Profiled {len(profile.columns)} columns, {profile.row_count:,} rows",
            )
        else:
            return TestResult("SchemaProfiler", False, "No profile")
    except Exception as e:
        import traceback

        return TestResult("SchemaProfiler", False, f"Failed: {str(e)}\n{traceback.format_exc()}")


def test_4_use_case_parser() -> TestResult:
    """Test 4: UseCaseParser"""
    try:
        from src.meta_agent.tools.use_case_parser import UseCaseParser

        parser = UseCaseParser(connection_name=CONNECTION_NAME)
        result = parser.parse(USE_CASE)

        if result:
            return TestResult(
                "UseCaseParser",
                True,
                f"Task: {result.primary_task.value}, ML: {result.ml_enabled}, Search: {result.search_enabled}",
            )
        else:
            return TestResult("UseCaseParser", False, "Parse failed")
    except Exception as e:
        import traceback

        return TestResult("UseCaseParser", False, f"Failed: {str(e)}\n{traceback.format_exc()}")


def test_5_data_scanner() -> TestResult:
    """Test 5: DataScanner"""
    try:
        from src.meta_agent.tools.data_scanner import DataScanner

        scanner = DataScanner(connection_name=CONNECTION_NAME)
        stage_assets = scanner.scan_stage(STAGE_PATH)
        table_asset = scanner.scan_table(TABLE_FQN)

        total = len(stage_assets) + (1 if table_asset else 0)
        if total > 0:
            return TestResult(
                "DataScanner",
                True,
                f"Found {len(stage_assets)} stage files, {1 if table_asset else 0} tables",
            )
        else:
            return TestResult("DataScanner", False, "No assets")
    except Exception as e:
        import traceback

        return TestResult("DataScanner", False, f"Failed: {str(e)}\n{traceback.format_exc()}")


def test_6_completeness_validator() -> TestResult:
    """Test 6: CompletenessValidator"""
    try:
        from src.agents.validation.completeness import CompletenessValidator

        conn = get_connection()
        validator = CompletenessValidator(conn)

        result = validator.check_required_columns(
            TABLE_FQN, ["WELL_NAME", "TIMESTAMP", "RATE_OF_PENETRATION_M_H"]
        )
        conn.close()

        passed = result.get("passed", False)
        return TestResult(
            "CompletenessValidator",
            passed,
            f"Cols: {result.get('found', 0)}/{result.get('required', 0)}",
        )
    except Exception as e:
        return TestResult("CompletenessValidator", False, f"Failed: {str(e)}")


def test_7_quality_validator() -> TestResult:
    """Test 7: QualityValidator"""
    try:
        from src.agents.validation.quality import QualityValidator

        conn = get_connection()
        validator = QualityValidator(conn)

        result = validator.check_null_ratios(TABLE_FQN, max_null_ratio=0.5)
        conn.close()

        passed = result.get("passed", False)
        msg = result.get("message", "")
        return TestResult("QualityValidator", passed, f"Null check: {msg}")
    except Exception as e:
        return TestResult("QualityValidator", False, f"Failed: {str(e)}")


def test_8_semantic_validator() -> TestResult:
    """Test 8: SemanticValidator"""
    try:
        from src.agents.validation.semantic import SemanticValidator

        validator = SemanticValidator(connection_name=CONNECTION_NAME)
        result = validator.validate(TABLE_FQN, business_context="Oil and gas drilling operations")

        passed = result.score >= 0.3
        return TestResult("SemanticValidator", passed, f"Score: {result.score:.1%} (>30% = pass)")
    except Exception as e:
        import traceback

        return TestResult("SemanticValidator", False, f"Failed: {str(e)}\n{traceback.format_exc()}")


def run_all_tests():
    tests = [
        test_1_connection_and_access,
        test_2_file_scanner,
        test_3_schema_profiler,
        test_4_use_case_parser,
        test_5_data_scanner,
        test_6_completeness_validator,
        test_7_quality_validator,
        test_8_semantic_validator,
    ]

    print("\n" + "=" * 60)
    print("AGENTIC PLATFORM VALIDATION - DRILLING OPS DATA")
    print("=" * 60 + "\n")

    results = []
    for test_fn in tests:
        print(f"Running: {test_fn.__doc__}...")
        result = test_fn()
        results.append(result)

        status = "✅ PASS" if result.passed else "❌ FAIL"
        print(f"  {status}: {result.message}")
        print()

    passed = sum(1 for r in results if r.passed)
    total = len(results)

    print("=" * 60)
    print(f"RESULTS: {passed}/{total} tests passed")
    print("=" * 60)

    return results


if __name__ == "__main__":
    run_all_tests()
