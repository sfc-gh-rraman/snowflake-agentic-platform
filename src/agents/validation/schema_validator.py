"""Schema validator - checks for type consistency and naming conventions."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import re


@dataclass
class SchemaValidationResult:
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


class SchemaValidator:
    """Validate schema consistency and naming conventions."""

    NAMING_PATTERNS = {
        "snake_case": r'^[a-z][a-z0-9_]*$',
        "SCREAMING_SNAKE": r'^[A-Z][A-Z0-9_]*$',
        "camelCase": r'^[a-z][a-zA-Z0-9]*$',
        "PascalCase": r'^[A-Z][a-zA-Z0-9]*$',
    }

    EXPECTED_TYPES = {
        'id': ['NUMBER', 'INTEGER', 'VARCHAR', 'STRING'],
        'name': ['VARCHAR', 'STRING', 'TEXT'],
        'date': ['DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ'],
        'amount': ['NUMBER', 'FLOAT', 'DOUBLE', 'DECIMAL'],
        'count': ['NUMBER', 'INTEGER'],
        'flag': ['BOOLEAN'],
        'description': ['VARCHAR', 'STRING', 'TEXT'],
    }

    def __init__(self, session=None, naming_convention: str = "SCREAMING_SNAKE"):
        self._session = session
        self.naming_convention = naming_convention

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

    def check_naming_convention(
        self,
        table_name: str,
    ) -> Dict[str, Any]:
        parts = table_name.split('.')
        if len(parts) >= 3:
            db, schema, table = parts[0], parts[1], parts[-1]
        else:
            table = parts[-1]
            db = schema = None

        sql = f"""
            SELECT COLUMN_NAME
            FROM {'.'.join(parts[:-1]) if len(parts) > 1 else parts[0]}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}'
        """
        
        pattern = self.NAMING_PATTERNS.get(self.naming_convention, self.NAMING_PATTERNS["SCREAMING_SNAKE"])
        violations = []

        try:
            results = self._execute(sql)
            for r in results:
                col_name = r.get("COLUMN_NAME", "")
                if not re.match(pattern, col_name):
                    violations.append(col_name)

            return {
                "check": "naming_convention",
                "convention": self.naming_convention,
                "passed": len(violations) == 0,
                "violations": violations,
                "total_columns": len(results),
            }
        except Exception as e:
            return {
                "check": "naming_convention",
                "passed": False,
                "error": str(e),
            }

    def check_type_consistency(
        self,
        table_name: str,
    ) -> Dict[str, Any]:
        parts = table_name.split('.')
        table = parts[-1]
        
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {'.'.join(parts[:-1]) if len(parts) > 1 else parts[0]}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}'
        """

        inconsistencies = []

        try:
            results = self._execute(sql)
            for r in results:
                col_name = r.get("COLUMN_NAME", "").lower()
                data_type = r.get("DATA_TYPE", "")

                for keyword, expected_types in self.EXPECTED_TYPES.items():
                    if keyword in col_name:
                        type_matches = any(exp_type in data_type.upper() for exp_type in expected_types)
                        if not type_matches:
                            inconsistencies.append({
                                "column": r.get("COLUMN_NAME"),
                                "actual_type": data_type,
                                "expected_types": expected_types,
                                "keyword": keyword,
                            })
                        break

            return {
                "check": "type_consistency",
                "passed": len(inconsistencies) == 0,
                "inconsistencies": inconsistencies,
            }
        except Exception as e:
            return {
                "check": "type_consistency",
                "passed": False,
                "error": str(e),
            }

    def check_reserved_words(
        self,
        table_name: str,
    ) -> Dict[str, Any]:
        reserved = {
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE',
            'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE',
            'INDEX', 'VIEW', 'DATABASE', 'SCHEMA', 'COLUMN', 'ROW', 'ORDER',
            'GROUP', 'BY', 'HAVING', 'LIMIT', 'OFFSET', 'JOIN', 'LEFT', 'RIGHT',
            'INNER', 'OUTER', 'FULL', 'CROSS', 'ON', 'AS', 'IN', 'EXISTS',
            'BETWEEN', 'LIKE', 'IS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        }

        parts = table_name.split('.')
        table = parts[-1]

        sql = f"""
            SELECT COLUMN_NAME
            FROM {'.'.join(parts[:-1]) if len(parts) > 1 else parts[0]}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}'
        """

        violations = []

        try:
            results = self._execute(sql)
            for r in results:
                col_name = r.get("COLUMN_NAME", "").upper()
                if col_name in reserved:
                    violations.append(col_name)

            return {
                "check": "reserved_words",
                "passed": len(violations) == 0,
                "violations": violations,
            }
        except Exception as e:
            return {
                "check": "reserved_words",
                "passed": False,
                "error": str(e),
            }

    def validate(
        self,
        table_name: str,
    ) -> SchemaValidationResult:
        checks = []
        issues = []

        naming_check = self.check_naming_convention(table_name)
        checks.append(naming_check)
        if not naming_check.get("passed"):
            issues.append(f"Naming convention violations: {naming_check.get('violations', [])}")

        type_check = self.check_type_consistency(table_name)
        checks.append(type_check)
        if not type_check.get("passed"):
            issues.append(f"Type inconsistencies found: {len(type_check.get('inconsistencies', []))}")

        reserved_check = self.check_reserved_words(table_name)
        checks.append(reserved_check)
        if not reserved_check.get("passed"):
            issues.append(f"Reserved word violations: {reserved_check.get('violations', [])}")

        passed_count = sum(1 for c in checks if c.get("passed"))
        score = passed_count / len(checks) if checks else 0

        return SchemaValidationResult(
            passed=len(issues) == 0,
            checks=checks,
            score=score,
            issues=issues,
        )
