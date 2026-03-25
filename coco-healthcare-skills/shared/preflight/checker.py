#!/usr/bin/env python3
"""Preflight checker — verifies Snowflake objects and Marketplace listings exist.

Skills include this in their setup workflow. The checker:
1. Tests each dependency (table, search service, Marketplace listing)
2. Reports status (READY / MISSING / ERROR)
3. Provides setup instructions for missing dependencies
4. Returns overall status: all-ready, partial, or none-ready

Usage:
    import os
    import snowflake.connector
    from shared.preflight.checker import PreflightChecker, Dependency

    conn = snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
    )
    checker = PreflightChecker(conn)
    checker.add(Dependency(
        name="DICOM Model Search Service",
        check_sql="SELECT 1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 0",
        probe_sql="SELECT COUNT(*) FROM SNOWFLAKE.CORTEX.SEARCH_PREVIEW("
                  "'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC',"
                  "'{\"query\":\"test\",\"columns\":[\"CONTENT\"],\"limit\":1}')",
        setup_instructions="Run: scripts/setup_dicom_model_knowledge_repo.sql",
        fallback="Skill will use hardcoded DICOM schema definitions instead of dynamic search.",
        required=False,
    ))
    results = checker.run()
    checker.print_report(results)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Status(Enum):
    READY = "READY"
    MISSING = "MISSING"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


@dataclass
class Dependency:
    name: str
    probe_sql: str
    setup_instructions: str
    fallback: str = ""
    required: bool = True
    category: str = "snowflake_object"


@dataclass
class CheckResult:
    dependency: Dependency
    status: Status
    message: str = ""
    error: str = ""


class PreflightChecker:
    def __init__(self, conn=None):
        self._conn = conn
        self._deps: list[Dependency] = []

    def add(self, dep: Dependency):
        self._deps.append(dep)

    def add_table(self, name: str, fqn: str, setup: str, fallback: str = "", required: bool = True):
        self._deps.append(Dependency(
            name=name,
            probe_sql=f"SELECT COUNT(*) FROM {fqn} LIMIT 1",
            setup_instructions=setup,
            fallback=fallback,
            required=required,
            category="table",
        ))

    def add_cortex_search(self, name: str, svc_fqn: str, setup: str, fallback: str = "", required: bool = True):
        self._deps.append(Dependency(
            name=name,
            probe_sql=f"SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('{svc_fqn}', "
                       f"'{{\"query\":\"test\",\"columns\":[\"CONTENT\"],\"limit\":1}}')",
            setup_instructions=setup,
            fallback=fallback,
            required=required,
            category="cortex_search",
        ))

    def add_marketplace_listing(self, name: str, db_name: str, test_table: str,
                                 listing_url: str, setup: str, fallback: str = "", required: bool = True):
        self._deps.append(Dependency(
            name=name,
            probe_sql=f"SELECT COUNT(*) FROM {db_name}.{test_table} LIMIT 1",
            setup_instructions=f"1. Install from Snowflake Marketplace: {listing_url}\n"
                               f"2. {setup}",
            fallback=fallback,
            required=required,
            category="marketplace_listing",
        ))

    def run(self) -> list[CheckResult]:
        results = []
        for dep in self._deps:
            if self._conn is None:
                results.append(CheckResult(
                    dependency=dep,
                    status=Status.SKIPPED,
                    message="No connection provided — skipping probe",
                ))
                continue
            try:
                cur = self._conn.cursor()
                cur.execute(dep.probe_sql)
                cur.fetchone()
                cur.close()
                results.append(CheckResult(
                    dependency=dep,
                    status=Status.READY,
                    message="Available",
                ))
            except Exception as e:
                err_str = str(e)
                if "does not exist" in err_str.lower() or "not found" in err_str.lower():
                    results.append(CheckResult(
                        dependency=dep,
                        status=Status.MISSING,
                        message="Not found in account",
                        error=err_str[:200],
                    ))
                else:
                    results.append(CheckResult(
                        dependency=dep,
                        status=Status.ERROR,
                        message="Check failed with error",
                        error=err_str[:200],
                    ))
        return results

    def print_report(self, results: list[CheckResult]):
        print("=" * 60)
        print("PREFLIGHT CHECK REPORT")
        print("=" * 60)

        ready = sum(1 for r in results if r.status == Status.READY)
        total = len(results)
        required_missing = [r for r in results if r.status != Status.READY and r.dependency.required]

        for r in results:
            icon = {"READY": "OK", "MISSING": "!!", "ERROR": "??", "SKIPPED": "--"}[r.status.value]
            print(f"  [{icon}] {r.dependency.name}: {r.status.value}")
            if r.status == Status.MISSING:
                print(f"       Setup: {r.dependency.setup_instructions}")
                if r.dependency.fallback:
                    print(f"       Fallback: {r.dependency.fallback}")
            elif r.status == Status.ERROR:
                print(f"       Error: {r.error}")

        print("-" * 60)
        print(f"  {ready}/{total} dependencies ready")

        if required_missing:
            print(f"  WARNING: {len(required_missing)} required dependencies missing!")
            print("  Skill may not function correctly without them.")
        elif ready < total:
            print("  Optional dependencies missing — skill will use fallback behavior.")
        else:
            print("  All dependencies satisfied. Skill is fully operational.")
        print("=" * 60)

    def all_ready(self, results: list[CheckResult]) -> bool:
        return all(r.status == Status.READY for r in results)

    def required_ready(self, results: list[CheckResult]) -> bool:
        return all(
            r.status == Status.READY
            for r in results
            if r.dependency.required
        )
