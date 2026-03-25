#!/usr/bin/env python3
"""Health Sciences cross-cutting data validation.

Runs completeness, schema consistency, and semantic validation checks
against healthcare tables in Snowflake.

Usage:
    python scripts/validate.py completeness --table DB.SCHEMA.TABLE --domain fhir
    python scripts/validate.py schema       --table DB.SCHEMA.TABLE --domain fhir
    python scripts/validate.py semantic     --table DB.SCHEMA.TABLE --domain fhir
    python scripts/validate.py all          --table DB.SCHEMA.TABLE --domain fhir
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("PyYAML required: pip install pyyaml")

try:
    import snowflake.connector
except ImportError:
    sys.exit("snowflake-connector-python required: pip install snowflake-connector-python")

DOMAINS_DIR = Path(__file__).resolve().parent / "domains"

NULL_RATE_THRESHOLD = 0.05
MIN_ROW_COUNT = 1


def load_domain(domain: str) -> dict:
    path = DOMAINS_DIR / f"{domain}.yaml"
    if not path.exists():
        print(f"WARNING: No domain definition for '{domain}' at {path}")
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def get_connection(connection_name: str):
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or connection_name
    )


def get_table_columns(conn, table_fqn: str) -> list[dict]:
    parts = table_fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Table FQN must be DB.SCHEMA.TABLE, got: {table_fqn}")
    db, schema, table = parts
    cur = conn.cursor()
    cur.execute(f"DESCRIBE TABLE {table_fqn}")
    columns = []
    for row in cur.fetchall():
        columns.append({
            "name": row[0],
            "type": row[1],
            "nullable": row[3] == "Y",
        })
    cur.close()
    return columns


def get_row_count(conn, table_fqn: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_fqn}")
    count = cur.fetchone()[0]
    cur.close()
    return count


def get_null_rates(conn, table_fqn: str, columns: list[str]) -> dict:
    if not columns:
        return {}
    selects = [
        f"ROUND(SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 4) AS {c}_null_rate"
        for c in columns
    ]
    sql = f"SELECT {', '.join(selects)} FROM {table_fqn}"
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    return {columns[i]: row[i] for i in range(len(columns))}


def check_completeness(conn, table_fqn: str, domain_def: dict, table_name: str) -> dict:
    checks = []
    columns = get_table_columns(conn, table_fqn)
    col_names = [c["name"] for c in columns]
    row_count = get_row_count(conn, table_fqn)

    checks.append({
        "check": "row_count",
        "value": row_count,
        "status": "PASS" if row_count >= MIN_ROW_COUNT else "FAIL",
        "message": f"Table has {row_count} rows",
    })

    null_rates = get_null_rates(conn, table_fqn, col_names)

    table_def = (domain_def.get("tables") or {}).get(table_name, {})
    required_cols = table_def.get("required_columns", [])

    for col_name, rate in null_rates.items():
        is_required = col_name.upper() in [c.upper() for c in required_cols]
        threshold = 0.0 if is_required else NULL_RATE_THRESHOLD

        if rate <= threshold:
            status = "PASS"
        elif rate <= NULL_RATE_THRESHOLD * 2:
            status = "WARN"
        else:
            status = "FAIL" if is_required else "WARN"

        checks.append({
            "check": f"null_rate_{col_name}",
            "column": col_name,
            "null_rate": float(rate),
            "threshold": threshold,
            "required": is_required,
            "status": status,
        })

    passed = sum(1 for c in checks if c["status"] == "PASS")
    warnings = sum(1 for c in checks if c["status"] == "WARN")
    failures = sum(1 for c in checks if c["status"] == "FAIL")

    overall = "FAIL" if failures > 0 else ("WARN" if warnings > 0 else "PASS")

    return {
        "status": overall,
        "checks": len(checks),
        "passed": passed,
        "warnings": warnings,
        "failures": failures,
        "details": checks,
    }


def check_schema(conn, table_fqn: str, domain_def: dict, table_name: str) -> dict:
    checks = []
    columns = get_table_columns(conn, table_fqn)
    actual_cols = {c["name"].upper(): c for c in columns}

    table_def = (domain_def.get("tables") or {}).get(table_name, {})
    expected_cols = table_def.get("required_columns", [])
    type_expectations = table_def.get("type_expectations", {})
    pk_cols = table_def.get("primary_key", [])

    for exp_col in expected_cols:
        if exp_col.upper() in actual_cols:
            checks.append({
                "check": f"column_exists_{exp_col}",
                "status": "PASS",
                "message": f"Column {exp_col} exists",
            })
        else:
            checks.append({
                "check": f"column_exists_{exp_col}",
                "status": "FAIL",
                "message": f"Required column {exp_col} MISSING",
            })

    for col_name, expected_type in type_expectations.items():
        if col_name.upper() in actual_cols:
            actual_type = actual_cols[col_name.upper()]["type"]
            type_match = expected_type.upper() in actual_type.upper()
            checks.append({
                "check": f"type_{col_name}",
                "expected": expected_type,
                "actual": actual_type,
                "status": "PASS" if type_match else "WARN",
            })

    for pk_col in pk_cols:
        if pk_col.upper() in actual_cols:
            col_info = actual_cols[pk_col.upper()]
            checks.append({
                "check": f"pk_not_null_{pk_col}",
                "status": "PASS" if not col_info["nullable"] else "WARN",
                "message": f"PK column {pk_col} nullable={col_info['nullable']}",
            })

    passed = sum(1 for c in checks if c["status"] == "PASS")
    warnings = sum(1 for c in checks if c["status"] == "WARN")
    failures = sum(1 for c in checks if c["status"] == "FAIL")
    overall = "FAIL" if failures > 0 else ("WARN" if warnings > 0 else "PASS")

    return {
        "status": overall,
        "checks": len(checks),
        "passed": passed,
        "warnings": warnings,
        "failures": failures,
        "details": checks,
    }


def check_semantic(conn, table_fqn: str, domain_def: dict, table_name: str) -> dict:
    checks = []
    table_def = (domain_def.get("tables") or {}).get(table_name, {})
    semantic_checks = table_def.get("semantic_checks", [])

    for sc in semantic_checks:
        check_type = sc.get("type", "")
        column = sc.get("column", "")

        if check_type == "allowed_values":
            allowed = sc.get("values", [])
            quoted = ", ".join(f"'{v}'" for v in allowed)
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {table_fqn} "
                f"WHERE {column} IS NOT NULL AND {column} NOT IN ({quoted})"
            )
            invalid_count = cur.fetchone()[0]
            cur.close()
            checks.append({
                "check": f"allowed_values_{column}",
                "invalid_count": invalid_count,
                "allowed": allowed,
                "status": "PASS" if invalid_count == 0 else "WARN",
            })

        elif check_type == "reference_format":
            pattern = sc.get("pattern", "")
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {table_fqn} "
                f"WHERE {column} IS NOT NULL AND NOT RLIKE({column}, '{pattern}')"
            )
            invalid_count = cur.fetchone()[0]
            cur.close()
            checks.append({
                "check": f"reference_format_{column}",
                "pattern": pattern,
                "invalid_count": invalid_count,
                "status": "PASS" if invalid_count == 0 else "WARN",
            })

        elif check_type == "referential_integrity":
            ref_table = sc.get("ref_table", "")
            ref_column = sc.get("ref_column", "")
            if ref_table and ref_column:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {table_fqn} t "
                        f"WHERE t.{column} IS NOT NULL "
                        f"AND NOT EXISTS (SELECT 1 FROM {ref_table} r WHERE r.{ref_column} = t.{column})"
                    )
                    orphan_count = cur.fetchone()[0]
                    checks.append({
                        "check": f"ref_integrity_{column}",
                        "ref_table": ref_table,
                        "orphan_count": orphan_count,
                        "status": "PASS" if orphan_count == 0 else "FAIL",
                    })
                except Exception as e:
                    checks.append({
                        "check": f"ref_integrity_{column}",
                        "status": "WARN",
                        "message": f"Could not check: {str(e)[:100]}",
                    })
                finally:
                    cur.close()

    if not checks:
        checks.append({
            "check": "no_semantic_checks",
            "status": "PASS",
            "message": f"No semantic checks defined for {table_name}",
        })

    passed = sum(1 for c in checks if c["status"] == "PASS")
    warnings = sum(1 for c in checks if c["status"] == "WARN")
    failures = sum(1 for c in checks if c["status"] == "FAIL")
    overall = "FAIL" if failures > 0 else ("WARN" if warnings > 0 else "PASS")

    return {
        "status": overall,
        "checks": len(checks),
        "passed": passed,
        "warnings": warnings,
        "failures": failures,
        "details": checks,
    }


def run_validation(suite: str, table_fqn: str, domain: str, connection_name: str):
    conn = get_connection(connection_name)
    domain_def = load_domain(domain)
    table_name = table_fqn.split(".")[-1].lower()

    report = {
        "table": table_fqn,
        "domain": domain,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "suites": {},
    }

    suites_to_run = ["completeness", "schema", "semantic"] if suite == "all" else [suite]

    for s in suites_to_run:
        if s == "completeness":
            report["suites"]["completeness"] = check_completeness(conn, table_fqn, domain_def, table_name)
        elif s == "schema":
            report["suites"]["schema"] = check_schema(conn, table_fqn, domain_def, table_name)
        elif s == "semantic":
            report["suites"]["semantic"] = check_semantic(conn, table_fqn, domain_def, table_name)

    statuses = [v["status"] for v in report["suites"].values()]
    if "FAIL" in statuses:
        report["overall_status"] = "FAIL"
    elif "WARN" in statuses:
        report["overall_status"] = "WARN"
    else:
        report["overall_status"] = "PASS"

    conn.close()
    return report


def main():
    parser = argparse.ArgumentParser(description="Healthcare data validation")
    parser.add_argument("suite", choices=["completeness", "schema", "semantic", "all"])
    parser.add_argument("--table", required=True, help="Fully qualified table name (DB.SCHEMA.TABLE)")
    parser.add_argument("--domain", required=True, choices=["fhir", "omop", "dicom", "clinical_docs"],
                        help="Domain for validation rules")
    parser.add_argument("--connection", default="default", help="Snowflake connection name")
    parser.add_argument("--output", help="Output JSON file path (default: stdout)")
    args = parser.parse_args()

    report = run_validation(args.suite, args.table, args.domain, args.connection)
    output = json.dumps(report, indent=2, default=str)

    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Report written to {args.output}")
    else:
        print(output)

    sys.exit(0 if report["overall_status"] != "FAIL" else 1)


if __name__ == "__main__":
    main()
