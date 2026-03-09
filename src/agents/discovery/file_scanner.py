"""File scanner agent - discovers data files in Snowflake stages."""

import os
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class FileType(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    PDF = "pdf"
    DOCX = "docx"
    TXT = "txt"
    XML = "xml"
    AVRO = "avro"
    ORC = "orc"
    UNKNOWN = "unknown"


@dataclass
class FileInfo:
    name: str
    path: str
    size_bytes: int
    file_type: FileType
    last_modified: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "file_type": self.file_type.value,
            "last_modified": self.last_modified,
            "metadata": self.metadata,
        }


class FileScanner:
    """Scan Snowflake stages for data files."""

    def __init__(
        self,
        connection_name: str | None = None,
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

    def _execute(self, sql: str) -> list[dict]:
        if hasattr(self.session, "sql"):
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

    def _detect_file_type(self, filename: str) -> FileType:
        ext = filename.lower().split(".")[-1] if "." in filename else ""
        type_map = {
            "parquet": FileType.PARQUET,
            "csv": FileType.CSV,
            "json": FileType.JSON,
            "jsonl": FileType.JSON,
            "pdf": FileType.PDF,
            "docx": FileType.DOCX,
            "doc": FileType.DOCX,
            "txt": FileType.TXT,
            "xml": FileType.XML,
            "avro": FileType.AVRO,
            "orc": FileType.ORC,
        }
        return type_map.get(ext, FileType.UNKNOWN)

    def scan_stage(
        self,
        stage_path: str,
        pattern: str | None = None,
        include_subdirs: bool = True,
    ) -> list[FileInfo]:
        if not stage_path.startswith("@"):
            stage_path = f"@{stage_path}"

        sql = f"LIST {stage_path}"
        if pattern:
            sql += f" PATTERN='{pattern}'"

        try:
            results = self._execute(sql)
        except Exception:
            return []

        files = []
        for row in results:
            name = row.get("name", "")
            if not name:
                continue

            file_name = name.split("/")[-1]
            file_type = self._detect_file_type(file_name)

            files.append(
                FileInfo(
                    name=file_name,
                    path=f"{stage_path}/{name}",
                    size_bytes=row.get("size", 0),
                    file_type=file_type,
                    last_modified=str(row.get("last_modified", "")),
                    metadata={
                        "md5": row.get("md5"),
                        "etag": row.get("etag"),
                    },
                )
            )

        return files

    def scan_multiple_stages(self, stage_paths: list[str]) -> dict[str, list[FileInfo]]:
        results = {}
        for stage_path in stage_paths:
            results[stage_path] = self.scan_stage(stage_path)
        return results

    def get_file_inventory(self, stage_path: str) -> dict[str, Any]:
        files = self.scan_stage(stage_path)

        by_type = {}
        total_size = 0

        for f in files:
            ft = f.file_type.value
            if ft not in by_type:
                by_type[ft] = {"count": 0, "size_bytes": 0, "files": []}
            by_type[ft]["count"] += 1
            by_type[ft]["size_bytes"] += f.size_bytes
            by_type[ft]["files"].append(f.name)
            total_size += f.size_bytes

        return {
            "stage_path": stage_path,
            "total_files": len(files),
            "total_size_bytes": total_size,
            "by_type": by_type,
            "files": [f.to_dict() for f in files],
        }


def scan_files(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node function for file scanning."""
    scanner = FileScanner()

    stage_paths = state.get("stage_paths", ["@RAW.DATA_STAGE", "@RAW.DOCUMENTS_STAGE"])

    all_files = []
    inventories = {}

    for stage_path in stage_paths:
        inventory = scanner.get_file_inventory(stage_path)
        inventories[stage_path] = inventory
        all_files.extend(inventory.get("files", []))

    return {
        "discovered_files": all_files,
        "file_inventories": inventories,
        "current_state": "COMPLETE",
        "messages": state.get("messages", [])
        + [
            {
                "role": "system",
                "content": f"Discovered {len(all_files)} files across {len(stage_paths)} stages",
            }
        ],
    }
