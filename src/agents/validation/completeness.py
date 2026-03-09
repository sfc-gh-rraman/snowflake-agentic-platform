"""Completeness validator - checks for required data coverage."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class CompletenessResult:
    passed: bool
    checks: List[Dict[str, Any]]
    score: float
    issues: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "checks": self.checks,
            "score": self.score,
            "issues": self.issues,
        }


class CompletenessValidator:
    """Validate data completeness."""

    def __init__(self, session=None):
        self._session = session

    def _execute(self, sql: str) -> List[Dict]:
        if self._session is None:
            return []
        if hasattr(self._session, 'sql'):
            result = self._session.sql(sql).collect()
            return [dict(row.asDict()) for row in result]
        else:
            cursor = self._session.cursor()
            try:
                cursor.execute(sql)
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
            finally:
                cursor.close()

    def check_required_columns(
        self,
        table_name: str,
        required_columns: List[str],
    ) -> Dict[str, Any]:
        parts = table_name.split('.')
        if len(parts) == 3:
            db, schema, table = parts
        elif len(parts) == 2:
            db, schema, table = None, parts[0], parts[1]
        else:
            db, schema, table = None, None, parts[0]
        
        info_schema = f"{db}.INFORMATION_SCHEMA" if db else "INFORMATION_SCHEMA"
        sql = f"""
            SELECT COLUMN_NAME
            FROM {info_schema}.COLUMNS
            WHERE TABLE_NAME = '{table}'
            {f"AND TABLE_SCHEMA = '{schema}'" if schema else ""}
        """
        try:
            results = self._execute(sql)
            existing = {r.get("COLUMN_NAME", "").upper() for r in results}
            
            missing = [c for c in required_columns if c.upper() not in existing]
            
            return {
                "check": "required_columns",
                "passed": len(missing) == 0,
                "missing": missing,
                "found": [c for c in required_columns if c.upper() in existing],
            }
        except Exception as e:
            return {
                "check": "required_columns",
                "passed": False,
                "error": str(e),
            }

    def check_minimum_rows(
        self,
        table_name: str,
        min_rows: int,
    ) -> Dict[str, Any]:
        sql = f"SELECT COUNT(*) as cnt FROM {table_name}"
        try:
            results = self._execute(sql)
            count = results[0].get("CNT", 0) if results else 0
            
            return {
                "check": "minimum_rows",
                "passed": count >= min_rows,
                "actual_rows": count,
                "required_rows": min_rows,
            }
        except Exception as e:
            return {
                "check": "minimum_rows",
                "passed": False,
                "error": str(e),
            }

    def check_column_coverage(
        self,
        table_name: str,
        columns: List[str],
        min_coverage: float = 0.9,
    ) -> Dict[str, Any]:
        coverage_results = {}
        
        for col in columns:
            sql = f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN "{col}" IS NOT NULL THEN 1 ELSE 0 END) as non_null
                FROM {table_name}
            """
            try:
                results = self._execute(sql)
                if results:
                    total = results[0].get("TOTAL", 0)
                    non_null = results[0].get("NON_NULL", 0)
                    coverage = non_null / total if total > 0 else 0
                    coverage_results[col] = {
                        "coverage": coverage,
                        "passed": coverage >= min_coverage,
                    }
            except Exception:
                coverage_results[col] = {"coverage": 0, "passed": False, "error": True}

        all_passed = all(r.get("passed", False) for r in coverage_results.values())
        avg_coverage = sum(r.get("coverage", 0) for r in coverage_results.values()) / len(coverage_results) if coverage_results else 0

        return {
            "check": "column_coverage",
            "passed": all_passed,
            "columns": coverage_results,
            "average_coverage": avg_coverage,
            "threshold": min_coverage,
        }

    def validate(
        self,
        table_name: str,
        required_columns: Optional[List[str]] = None,
        min_rows: int = 1,
        coverage_columns: Optional[List[str]] = None,
        min_coverage: float = 0.9,
    ) -> CompletenessResult:
        checks = []
        issues = []

        if required_columns:
            col_check = self.check_required_columns(table_name, required_columns)
            checks.append(col_check)
            if not col_check.get("passed"):
                issues.append(f"Missing required columns: {col_check.get('missing', [])}")

        row_check = self.check_minimum_rows(table_name, min_rows)
        checks.append(row_check)
        if not row_check.get("passed"):
            issues.append(f"Insufficient rows: {row_check.get('actual_rows', 0)} < {min_rows}")

        if coverage_columns:
            cov_check = self.check_column_coverage(table_name, coverage_columns, min_coverage)
            checks.append(cov_check)
            if not cov_check.get("passed"):
                low_cov = [k for k, v in cov_check.get("columns", {}).items() if not v.get("passed")]
                issues.append(f"Low coverage columns: {low_cov}")

        passed_count = sum(1 for c in checks if c.get("passed"))
        score = passed_count / len(checks) if checks else 0

        return CompletenessResult(
            passed=len(issues) == 0,
            checks=checks,
            score=score,
            issues=issues,
        )
