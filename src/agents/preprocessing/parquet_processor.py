"""Parquet processor agent - 6-state LangGraph processor."""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ParquetProcessorState(str, Enum):
    SCAN = "SCAN"
    SCHEMA_INFER = "SCHEMA_INFER"
    PROFILE = "PROFILE"
    QUALITY_CHECK = "QUALITY_CHECK"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass
class ParquetFile:
    path: str
    name: str
    size_bytes: int
    schema: Optional[Dict[str, str]] = None
    row_count: Optional[int] = None
    quality_issues: List[str] = field(default_factory=list)


class ParquetProcessor:
    """Process parquet files through a 6-state pipeline."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: str = "AGENTIC_PLATFORM",
        target_schema: str = "RAW",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.target_schema = target_schema
        self._session = None

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

    def _execute(self, sql: str) -> List[Dict]:
        if hasattr(self.session, 'sql'):
            result = self.session.sql(sql).collect()
            return [dict(row.asDict()) for row in result]
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
            finally:
                cursor.close()

    def scan(self, stage_path: str) -> List[ParquetFile]:
        if not stage_path.startswith('@'):
            stage_path = f"@{stage_path}"

        sql = f"LIST {stage_path} PATTERN='.*\\.parquet'"
        try:
            results = self._execute(sql)
        except Exception:
            return []

        files = []
        for row in results:
            name = row.get("name", "")
            if name:
                files.append(ParquetFile(
                    path=f"{stage_path}/{name}",
                    name=name.split('/')[-1],
                    size_bytes=row.get("size", 0),
                ))

        return files

    def infer_schema(self, parquet_file: ParquetFile) -> ParquetFile:
        if not hasattr(self.session, 'read'):
            parquet_file.schema = {"error": "Snowpark session required"}
            return parquet_file

        try:
            df = self.session.read.parquet(parquet_file.path)
            schema_dict = {}
            for field in df.schema.fields:
                schema_dict[field.name] = str(field.datatype)
            parquet_file.schema = schema_dict
            parquet_file.row_count = df.count()
        except Exception as e:
            parquet_file.quality_issues.append(f"Schema inference failed: {str(e)}")

        return parquet_file

    def profile(self, parquet_file: ParquetFile) -> Dict[str, Any]:
        if not parquet_file.schema:
            return {"error": "No schema available"}

        profile = {
            "file": parquet_file.name,
            "columns": len(parquet_file.schema),
            "row_count": parquet_file.row_count,
            "size_bytes": parquet_file.size_bytes,
            "schema": parquet_file.schema,
        }

        return profile

    def quality_check(self, parquet_file: ParquetFile) -> List[str]:
        issues = list(parquet_file.quality_issues)

        if not parquet_file.schema:
            issues.append("No schema detected")
            return issues

        if parquet_file.row_count == 0:
            issues.append("File contains no rows")

        for col_name, col_type in parquet_file.schema.items():
            if col_name.startswith('_'):
                issues.append(f"Column '{col_name}' has underscore prefix")
            if ' ' in col_name:
                issues.append(f"Column '{col_name}' contains spaces")

        return issues

    def transform(self, parquet_file: ParquetFile) -> Dict[str, str]:
        if not parquet_file.schema:
            return {}

        column_mapping = {}
        for col_name in parquet_file.schema.keys():
            new_name = col_name.upper().replace(' ', '_').replace('-', '_')
            if new_name.startswith('_'):
                new_name = new_name[1:]
            column_mapping[col_name] = new_name

        return column_mapping

    def load(
        self,
        parquet_file: ParquetFile,
        column_mapping: Optional[Dict[str, str]] = None,
        table_name: Optional[str] = None,
    ) -> str:
        if not hasattr(self.session, 'read'):
            raise RuntimeError("Snowpark session required for loading")

        if not table_name:
            base_name = parquet_file.name.replace('.parquet', '').upper()
            base_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in base_name)
            table_name = f"{self.database}.{self.target_schema}.{base_name}"

        df = self.session.read.parquet(parquet_file.path)

        if column_mapping:
            for old_name, new_name in column_mapping.items():
                if old_name != new_name:
                    df = df.with_column_renamed(old_name, new_name)

        df.write.mode("overwrite").save_as_table(table_name)

        return table_name

    def process(self, stage_path: str) -> Dict[str, Any]:
        result = {
            "stage_path": stage_path,
            "files_processed": 0,
            "tables_created": [],
            "errors": [],
        }

        files = self.scan(stage_path)
        
        for pf in files:
            try:
                pf = self.infer_schema(pf)
                
                issues = self.quality_check(pf)
                if issues:
                    pf.quality_issues.extend(issues)

                column_mapping = self.transform(pf)
                
                table_name = self.load(pf, column_mapping)
                
                result["tables_created"].append({
                    "source": pf.name,
                    "table": table_name,
                    "rows": pf.row_count,
                    "columns": len(pf.schema) if pf.schema else 0,
                    "issues": pf.quality_issues,
                })
                result["files_processed"] += 1

            except Exception as e:
                result["errors"].append({
                    "file": pf.name,
                    "error": str(e),
                })

        return result


def process_parquet(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function for parquet processing."""
    processor = ParquetProcessor(
        target_schema=state.get("target_schema", "RAW"),
    )

    stage_paths = state.get("stage_paths", ["@RAW.DATA_STAGE"])
    
    all_results = {
        "files_processed": 0,
        "tables_created": [],
        "errors": [],
    }

    for stage_path in stage_paths:
        result = processor.process(stage_path)
        all_results["files_processed"] += result["files_processed"]
        all_results["tables_created"].extend(result["tables_created"])
        all_results["errors"].extend(result["errors"])

    return {
        "parquet_results": all_results,
        "current_state": ParquetProcessorState.COMPLETE.value,
        "tables_created": [t["table"] for t in all_results["tables_created"]],
        "messages": state.get("messages", []) + [{
            "role": "system",
            "content": f"Processed {all_results['files_processed']} parquet files, created {len(all_results['tables_created'])} tables",
        }],
    }
