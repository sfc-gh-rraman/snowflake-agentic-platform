"""Data scanner tool - discovers and profiles data assets."""

import os
from typing import Any, Dict, List, Optional

from ..state import DataAsset, DataProfile, DataType


class DataScanner:
    """Scan Snowflake stages and tables for data assets."""

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

    def _detect_file_type(self, filename: str) -> DataType:
        ext = filename.lower().split('.')[-1] if '.' in filename else ''
        type_map = {
            'parquet': DataType.PARQUET,
            'csv': DataType.CSV,
            'json': DataType.JSON,
            'pdf': DataType.PDF,
            'docx': DataType.DOCX,
            'txt': DataType.TXT,
        }
        return type_map.get(ext, DataType.UNKNOWN)

    def scan_stage(self, stage_path: str) -> List[DataAsset]:
        if not stage_path.startswith('@'):
            stage_path = f"@{stage_path}"

        sql = f"LIST {stage_path}"
        try:
            results = self._execute(sql)
        except Exception:
            return []

        assets = []
        for row in results:
            name = row.get("name", "")
            size = row.get("size", 0)
            
            file_type = self._detect_file_type(name)
            if file_type == DataType.UNKNOWN:
                continue

            assets.append(DataAsset(
                name=name.split('/')[-1],
                location=f"{stage_path}/{name}",
                data_type=file_type,
                size_bytes=size,
            ))

        return assets

    def scan_table(self, table_name: str) -> Optional[DataAsset]:
        parts = table_name.split('.')
        if len(parts) == 3:
            db, schema, table = parts
        elif len(parts) == 2:
            db = self.database
            schema, table = parts
        else:
            db = self.database
            schema = "RAW"
            table = parts[0]

        sql = f"""
            SELECT 
                ROW_COUNT,
                BYTES
            FROM {db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        try:
            results = self._execute(sql)
        except Exception:
            return None

        if not results:
            return None

        row = results[0]
        
        col_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
        col_results = self._execute(col_sql)
        schema_dict = {r.get("COLUMN_NAME"): r.get("DATA_TYPE") for r in col_results}

        return DataAsset(
            name=table,
            location=f"{db}.{schema}.{table}",
            data_type=DataType.TABLE,
            size_bytes=row.get("BYTES", 0),
            row_count=row.get("ROW_COUNT", 0),
            column_count=len(schema_dict),
            schema=schema_dict,
        )

    def profile_assets(self, assets: List[DataAsset]) -> DataProfile:
        structured_types = {DataType.PARQUET, DataType.CSV, DataType.JSON, DataType.TABLE}
        unstructured_types = {DataType.PDF, DataType.DOCX, DataType.TXT}

        structured_count = sum(1 for a in assets if a.data_type in structured_types)
        unstructured_count = sum(1 for a in assets if a.data_type in unstructured_types)
        total_rows = sum(a.row_count or 0 for a in assets)
        total_size = sum(a.size_bytes or 0 for a in assets)

        potential_targets = []
        potential_features = []
        for asset in assets:
            if asset.schema:
                for col, dtype in asset.schema.items():
                    col_lower = col.lower()
                    if any(t in col_lower for t in ['target', 'label', 'class', 'outcome', 'status']):
                        potential_targets.append(col)
                    elif dtype in ['NUMBER', 'FLOAT', 'INTEGER', 'DOUBLE']:
                        potential_features.append(col)

        return DataProfile(
            total_assets=len(assets),
            structured_count=structured_count,
            unstructured_count=unstructured_count,
            total_rows=total_rows,
            total_size_bytes=total_size,
            has_labeled_data=len(potential_targets) > 0,
            potential_target_columns=potential_targets[:5],
            potential_features=potential_features[:20],
            text_content_detected=unstructured_count > 0,
            profiles={a.name: {"type": a.data_type.value, "size": a.size_bytes} for a in assets},
        )


def scan_data(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function to scan data assets."""
    scanner = DataScanner()
    
    all_assets = []
    for location in state.get("data_locations", []):
        if location.startswith('@'):
            all_assets.extend(scanner.scan_stage(location))
        else:
            asset = scanner.scan_table(location)
            if asset:
                all_assets.append(asset)

    if not state.get("data_locations"):
        for stage in ["RAW.DATA_STAGE", "RAW.DOCUMENTS_STAGE"]:
            all_assets.extend(scanner.scan_stage(stage))

    profile = scanner.profile_assets(all_assets)

    return {
        "data_assets": [a.to_dict() for a in all_assets],
        "data_profile": profile.to_dict(),
        "current_phase": "query_registry",
        "messages": state.get("messages", []) + [{
            "role": "system",
            "content": f"Discovered {len(all_assets)} data assets: {profile.structured_count} structured, {profile.unstructured_count} unstructured",
        }],
    }
