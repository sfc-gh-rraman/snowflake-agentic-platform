"""ML-specific validator - checks feature distributions and label balance."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class MLValidationResult:
    passed: bool
    checks: List[Dict[str, Any]]
    score: float
    issues: List[str]
    recommendations: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "checks": self.checks,
            "score": self.score,
            "issues": self.issues,
            "recommendations": self.recommendations,
        }


class MLValidator:
    """Validate data for ML readiness."""

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

    def check_label_balance(
        self,
        table_name: str,
        label_column: str,
        min_class_ratio: float = 0.1,
    ) -> Dict[str, Any]:
        sql = f"""
            SELECT 
                "{label_column}" as label,
                COUNT(*) as count
            FROM {table_name}
            WHERE "{label_column}" IS NOT NULL
            GROUP BY "{label_column}"
        """

        try:
            results = self._execute(sql)
            if not results:
                return {"check": "label_balance", "passed": False, "error": "No label data"}

            total = sum(r.get("COUNT", 0) for r in results)
            class_ratios = {}
            imbalanced_classes = []

            for r in results:
                label = str(r.get("LABEL"))
                count = r.get("COUNT", 0)
                ratio = count / total if total > 0 else 0
                class_ratios[label] = {
                    "count": count,
                    "ratio": round(ratio, 4),
                }
                if ratio < min_class_ratio:
                    imbalanced_classes.append(label)

            return {
                "check": "label_balance",
                "passed": len(imbalanced_classes) == 0,
                "label_column": label_column,
                "class_distribution": class_ratios,
                "imbalanced_classes": imbalanced_classes,
                "min_class_ratio": min_class_ratio,
            }
        except Exception as e:
            return {
                "check": "label_balance",
                "passed": False,
                "error": str(e),
            }

    def check_feature_variance(
        self,
        table_name: str,
        feature_columns: List[str],
        min_variance: float = 0.001,
    ) -> Dict[str, Any]:
        low_variance_cols = []
        variance_results = {}

        for col in feature_columns:
            sql = f"""
                SELECT VARIANCE("{col}") as var_val
                FROM {table_name}
                WHERE "{col}" IS NOT NULL
            """
            try:
                results = self._execute(sql)
                variance = results[0].get("VAR_VAL", 0) if results else 0
                variance = variance if variance else 0
                
                variance_results[col] = round(float(variance), 6)
                if variance < min_variance:
                    low_variance_cols.append(col)
            except Exception:
                variance_results[col] = None

        return {
            "check": "feature_variance",
            "passed": len(low_variance_cols) == 0,
            "variances": variance_results,
            "low_variance_columns": low_variance_cols,
            "min_variance": min_variance,
        }

    def check_feature_correlation(
        self,
        table_name: str,
        feature_columns: List[str],
        max_correlation: float = 0.95,
    ) -> Dict[str, Any]:
        high_correlations = []

        for i, col1 in enumerate(feature_columns[:10]):
            for col2 in feature_columns[i+1:10]:
                sql = f"""
                    SELECT CORR("{col1}", "{col2}") as corr_val
                    FROM {table_name}
                    WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL
                """
                try:
                    results = self._execute(sql)
                    corr = results[0].get("CORR_VAL", 0) if results else 0
                    corr = abs(corr) if corr else 0
                    
                    if corr > max_correlation:
                        high_correlations.append({
                            "col1": col1,
                            "col2": col2,
                            "correlation": round(float(corr), 4),
                        })
                except Exception:
                    pass

        return {
            "check": "feature_correlation",
            "passed": len(high_correlations) == 0,
            "high_correlations": high_correlations,
            "max_correlation": max_correlation,
        }

    def check_train_test_leakage_risk(
        self,
        table_name: str,
        timestamp_column: Optional[str] = None,
        id_column: Optional[str] = None,
    ) -> Dict[str, Any]:
        risks = []

        if not timestamp_column and not id_column:
            risks.append("No timestamp or ID column identified for temporal split")

        if timestamp_column:
            sql = f"""
                SELECT 
                    MIN("{timestamp_column}") as min_ts,
                    MAX("{timestamp_column}") as max_ts,
                    COUNT(DISTINCT DATE_TRUNC('day', "{timestamp_column}")) as distinct_days
                FROM {table_name}
                WHERE "{timestamp_column}" IS NOT NULL
            """
            try:
                results = self._execute(sql)
                if results:
                    distinct_days = results[0].get("DISTINCT_DAYS", 0)
                    if distinct_days and distinct_days < 7:
                        risks.append(f"Limited temporal range: only {distinct_days} distinct days")
            except Exception:
                pass

        return {
            "check": "leakage_risk",
            "passed": len(risks) == 0,
            "risks": risks,
            "timestamp_column": timestamp_column,
            "id_column": id_column,
        }

    def validate(
        self,
        table_name: str,
        label_column: Optional[str] = None,
        feature_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        id_column: Optional[str] = None,
    ) -> MLValidationResult:
        checks = []
        issues = []
        recommendations = []

        if label_column:
            balance_check = self.check_label_balance(table_name, label_column)
            checks.append(balance_check)
            if not balance_check.get("passed"):
                issues.append(f"Imbalanced classes: {balance_check.get('imbalanced_classes', [])}")
                recommendations.append("Consider using SMOTE or class weights for imbalanced data")

        if feature_columns:
            variance_check = self.check_feature_variance(table_name, feature_columns)
            checks.append(variance_check)
            if not variance_check.get("passed"):
                issues.append(f"Low variance features: {variance_check.get('low_variance_columns', [])}")
                recommendations.append("Remove or transform low-variance features")

            if len(feature_columns) > 1:
                corr_check = self.check_feature_correlation(table_name, feature_columns)
                checks.append(corr_check)
                if not corr_check.get("passed"):
                    issues.append(f"Highly correlated features detected: {len(corr_check.get('high_correlations', []))} pairs")
                    recommendations.append("Consider removing highly correlated features to reduce multicollinearity")

        leakage_check = self.check_train_test_leakage_risk(table_name, timestamp_column, id_column)
        checks.append(leakage_check)
        if not leakage_check.get("passed"):
            issues.extend(leakage_check.get("risks", []))
            recommendations.append("Ensure proper temporal train/test split to prevent data leakage")

        passed_count = sum(1 for c in checks if c.get("passed"))
        score = passed_count / len(checks) if checks else 0

        return MLValidationResult(
            passed=len(issues) == 0,
            checks=checks,
            score=score,
            issues=issues,
            recommendations=recommendations,
        )
