"""Quality validator - checks for null ratios, outliers, and duplicates."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class QualityResult:
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


class QualityValidator:
    """Validate data quality metrics."""

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

    def check_null_ratios(
        self,
        table_name: str,
        max_null_ratio: float = 0.3,
    ) -> Dict[str, Any]:
        parts = table_name.split('.')
        if len(parts) == 3:
            db, schema, table = parts
        elif len(parts) == 2:
            db, schema, table = None, parts[0], parts[1]
        else:
            db, schema, table = None, None, parts[0]
        
        info_schema = f"{db}.INFORMATION_SCHEMA" if db else "INFORMATION_SCHEMA"
        col_sql = f"""
            SELECT COLUMN_NAME
            FROM {info_schema}.COLUMNS
            WHERE TABLE_NAME = '{table}'
            {f"AND TABLE_SCHEMA = '{schema}'" if schema else ""}
        """

        try:
            col_results = self._execute(col_sql)
            columns = [r.get("COLUMN_NAME") for r in col_results]

            count_sql = f"SELECT COUNT(*) as total FROM {table_name}"
            count_results = self._execute(count_sql)
            total_rows = count_results[0].get("TOTAL", 0) if count_results else 0

            if total_rows == 0:
                return {
                    "check": "null_ratios",
                    "passed": False,
                    "error": "Table has no rows",
                }

            violations = {}
            for col in columns[:20]:
                null_sql = f"""
                    SELECT COUNT(*) as null_count
                    FROM {table_name}
                    WHERE "{col}" IS NULL
                """
                null_results = self._execute(null_sql)
                null_count = null_results[0].get("NULL_COUNT", 0) if null_results else 0
                null_ratio = null_count / total_rows

                if null_ratio > max_null_ratio:
                    violations[col] = {
                        "null_ratio": round(null_ratio, 4),
                        "null_count": null_count,
                    }

            return {
                "check": "null_ratios",
                "passed": len(violations) == 0,
                "threshold": max_null_ratio,
                "violations": violations,
                "columns_checked": len(columns[:20]),
            }
        except Exception as e:
            return {
                "check": "null_ratios",
                "passed": False,
                "error": str(e),
            }

    def check_duplicates(
        self,
        table_name: str,
        key_columns: List[str],
        max_duplicate_ratio: float = 0.01,
    ) -> Dict[str, Any]:
        key_cols_str = ", ".join(f'"{c}"' for c in key_columns)

        sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT {key_cols_str}) as distinct_rows
            FROM {table_name}
        """

        try:
            results = self._execute(sql)
            if not results:
                return {"check": "duplicates", "passed": False, "error": "No results"}

            total = results[0].get("TOTAL_ROWS", 0)
            distinct = results[0].get("DISTINCT_ROWS", 0)
            
            duplicate_count = total - distinct
            duplicate_ratio = duplicate_count / total if total > 0 else 0

            return {
                "check": "duplicates",
                "passed": duplicate_ratio <= max_duplicate_ratio,
                "key_columns": key_columns,
                "total_rows": total,
                "distinct_rows": distinct,
                "duplicate_count": duplicate_count,
                "duplicate_ratio": round(duplicate_ratio, 6),
                "threshold": max_duplicate_ratio,
            }
        except Exception as e:
            return {
                "check": "duplicates",
                "passed": False,
                "error": str(e),
            }

    def check_outliers(
        self,
        table_name: str,
        numeric_columns: List[str],
        std_threshold: float = 3.0,
    ) -> Dict[str, Any]:
        outlier_results = {}

        for col in numeric_columns:
            sql = f"""
                WITH stats AS (
                    SELECT 
                        AVG("{col}") as mean_val,
                        STDDEV("{col}") as std_val
                    FROM {table_name}
                    WHERE "{col}" IS NOT NULL
                )
                SELECT 
                    COUNT(*) as outlier_count,
                    (SELECT COUNT(*) FROM {table_name} WHERE "{col}" IS NOT NULL) as total_count
                FROM {table_name}, stats
                WHERE "{col}" IS NOT NULL
                  AND ABS("{col}" - mean_val) > {std_threshold} * std_val
            """

            try:
                results = self._execute(sql)
                if results:
                    outlier_count = results[0].get("OUTLIER_COUNT", 0)
                    total_count = results[0].get("TOTAL_COUNT", 0)
                    outlier_ratio = outlier_count / total_count if total_count > 0 else 0
                    
                    outlier_results[col] = {
                        "outlier_count": outlier_count,
                        "total_count": total_count,
                        "outlier_ratio": round(outlier_ratio, 6),
                    }
            except Exception:
                outlier_results[col] = {"error": True}

        high_outlier_cols = [k for k, v in outlier_results.items() 
                            if v.get("outlier_ratio", 0) > 0.05]

        return {
            "check": "outliers",
            "passed": len(high_outlier_cols) == 0,
            "std_threshold": std_threshold,
            "columns": outlier_results,
            "high_outlier_columns": high_outlier_cols,
        }

    def validate(
        self,
        table_name: str,
        key_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        max_null_ratio: float = 0.3,
        max_duplicate_ratio: float = 0.01,
    ) -> QualityResult:
        checks = []
        issues = []

        null_check = self.check_null_ratios(table_name, max_null_ratio)
        checks.append(null_check)
        if not null_check.get("passed"):
            issues.append(f"High null ratio columns: {list(null_check.get('violations', {}).keys())}")

        if key_columns:
            dup_check = self.check_duplicates(table_name, key_columns, max_duplicate_ratio)
            checks.append(dup_check)
            if not dup_check.get("passed"):
                issues.append(f"Duplicate ratio {dup_check.get('duplicate_ratio', 0)} exceeds threshold")

        if numeric_columns:
            outlier_check = self.check_outliers(table_name, numeric_columns)
            checks.append(outlier_check)
            if not outlier_check.get("passed"):
                issues.append(f"High outlier columns: {outlier_check.get('high_outlier_columns', [])}")

        passed_count = sum(1 for c in checks if c.get("passed"))
        score = passed_count / len(checks) if checks else 0

        return QualityResult(
            passed=len(issues) == 0,
            checks=checks,
            score=score,
            issues=issues,
        )
