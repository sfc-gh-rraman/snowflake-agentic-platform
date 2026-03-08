"""Schema profiler agent - analyzes data schemas and statistics."""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ColumnProfile:
    name: str
    data_type: str
    nullable: bool
    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    null_percentage: Optional[float] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    avg_value: Optional[float] = None
    sample_values: List[Any] = field(default_factory=list)
    semantic_type: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "distinct_count": self.distinct_count,
            "null_count": self.null_count,
            "null_percentage": self.null_percentage,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "avg_value": self.avg_value,
            "sample_values": self.sample_values,
            "semantic_type": self.semantic_type,
        }


@dataclass 
class TableProfile:
    table_name: str
    database: str
    schema: str
    row_count: int
    column_count: int
    size_bytes: int
    columns: List[ColumnProfile]
    primary_key_candidates: List[str] = field(default_factory=list)
    timestamp_columns: List[str] = field(default_factory=list)
    text_columns: List[str] = field(default_factory=list)
    numeric_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "database": self.database,
            "schema": self.schema,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "size_bytes": self.size_bytes,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key_candidates": self.primary_key_candidates,
            "timestamp_columns": self.timestamp_columns,
            "text_columns": self.text_columns,
            "numeric_columns": self.numeric_columns,
        }


class SchemaProfiler:
    """Profile table schemas and column statistics."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: str = "AGENTIC_PLATFORM",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
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

    def _parse_table_name(self, table_name: str) -> tuple:
        parts = table_name.split('.')
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return self.database, parts[0], parts[1]
        else:
            return self.database, "RAW", parts[0]

    def _detect_semantic_type(self, col_name: str, data_type: str) -> Optional[str]:
        col_lower = col_name.lower()
        
        if any(x in col_lower for x in ['email', 'mail']):
            return "email"
        elif any(x in col_lower for x in ['phone', 'tel', 'mobile']):
            return "phone"
        elif any(x in col_lower for x in ['url', 'link', 'website']):
            return "url"
        elif any(x in col_lower for x in ['address', 'street', 'city', 'state', 'zip', 'country']):
            return "address"
        elif any(x in col_lower for x in ['date', 'time', 'timestamp', 'created', 'updated', 'modified']):
            return "datetime"
        elif any(x in col_lower for x in ['id', 'key', 'uuid', 'guid']):
            return "identifier"
        elif any(x in col_lower for x in ['price', 'cost', 'amount', 'total', 'revenue']):
            return "currency"
        elif any(x in col_lower for x in ['lat', 'lng', 'longitude', 'latitude', 'geo']):
            return "geographic"
        elif any(x in col_lower for x in ['name', 'title', 'description', 'comment', 'note', 'text']):
            return "text"
        elif any(x in col_lower for x in ['status', 'state', 'type', 'category', 'class']):
            return "category"
        
        return None

    def profile_table(self, table_name: str, sample_size: int = 1000) -> TableProfile:
        db, schema, table = self._parse_table_name(table_name)
        full_table = f"{db}.{schema}.{table}"

        meta_sql = f"""
            SELECT ROW_COUNT, BYTES
            FROM {db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        meta_results = self._execute(meta_sql)
        
        row_count = meta_results[0].get("ROW_COUNT", 0) if meta_results else 0
        size_bytes = meta_results[0].get("BYTES", 0) if meta_results else 0

        col_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
        col_results = self._execute(col_sql)

        columns = []
        pk_candidates = []
        timestamp_cols = []
        text_cols = []
        numeric_cols = []

        for col in col_results:
            col_name = col.get("COLUMN_NAME")
            data_type = col.get("DATA_TYPE")
            nullable = col.get("IS_NULLABLE", "YES") == "YES"

            profile = ColumnProfile(
                name=col_name,
                data_type=data_type,
                nullable=nullable,
                semantic_type=self._detect_semantic_type(col_name, data_type),
            )

            if row_count > 0:
                try:
                    stats_sql = f"""
                        SELECT 
                            COUNT(DISTINCT "{col_name}") as distinct_count,
                            SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) as null_count
                        FROM {full_table}
                        SAMPLE ({min(sample_size, row_count)} ROWS)
                    """
                    stats = self._execute(stats_sql)
                    if stats:
                        profile.distinct_count = stats[0].get("DISTINCT_COUNT", 0)
                        profile.null_count = stats[0].get("NULL_COUNT", 0)
                        profile.null_percentage = (profile.null_count / sample_size * 100) if sample_size > 0 else 0

                    sample_sql = f"""
                        SELECT DISTINCT "{col_name}"
                        FROM {full_table}
                        WHERE "{col_name}" IS NOT NULL
                        LIMIT 5
                    """
                    samples = self._execute(sample_sql)
                    profile.sample_values = [s.get(col_name) for s in samples][:5]

                except Exception:
                    pass

            columns.append(profile)

            if profile.semantic_type == "identifier" and profile.distinct_count and profile.distinct_count == row_count:
                pk_candidates.append(col_name)
            if profile.semantic_type == "datetime" or 'TIME' in data_type or 'DATE' in data_type:
                timestamp_cols.append(col_name)
            if 'VARCHAR' in data_type or 'TEXT' in data_type or 'STRING' in data_type:
                text_cols.append(col_name)
            if 'NUMBER' in data_type or 'INT' in data_type or 'FLOAT' in data_type or 'DOUBLE' in data_type:
                numeric_cols.append(col_name)

        return TableProfile(
            table_name=table,
            database=db,
            schema=schema,
            row_count=row_count,
            column_count=len(columns),
            size_bytes=size_bytes,
            columns=columns,
            primary_key_candidates=pk_candidates,
            timestamp_columns=timestamp_cols,
            text_columns=text_cols,
            numeric_columns=numeric_cols,
        )

    def profile_parquet(self, stage_path: str) -> Dict[str, Any]:
        if hasattr(self.session, 'read'):
            try:
                df = self.session.read.parquet(stage_path)
                schema = df.schema
                
                columns = []
                for field in schema.fields:
                    columns.append({
                        "name": field.name,
                        "data_type": str(field.datatype),
                        "nullable": field.nullable,
                    })

                return {
                    "path": stage_path,
                    "columns": columns,
                    "column_count": len(columns),
                }
            except Exception as e:
                return {"path": stage_path, "error": str(e)}
        
        return {"path": stage_path, "error": "Snowpark session required for parquet profiling"}


def profile_schema(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function for schema profiling."""
    profiler = SchemaProfiler()
    
    tables = state.get("tables_to_profile", [])
    parquet_files = state.get("parquet_files", [])
    
    table_profiles = {}
    for table in tables:
        profile = profiler.profile_table(table)
        table_profiles[table] = profile.to_dict()

    parquet_profiles = {}
    for path in parquet_files:
        profile = profiler.profile_parquet(path)
        parquet_profiles[path] = profile

    return {
        "table_profiles": table_profiles,
        "parquet_profiles": parquet_profiles,
        "current_state": "COMPLETE",
        "messages": state.get("messages", []) + [{
            "role": "system",
            "content": f"Profiled {len(table_profiles)} tables and {len(parquet_profiles)} parquet files",
        }],
    }
