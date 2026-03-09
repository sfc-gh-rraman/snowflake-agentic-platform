from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import os
import snowflake.connector

router = APIRouter(prefix="/search", tags=["Search"])


def get_connection():
    if os.path.exists("/snowflake/session/token"):
        with open("/snowflake/session/token", "r") as f:
            token = f.read()
        return snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            authenticator="oauth",
            token=token,
        )
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    )


class QueryRequest(BaseModel):
    query: str
    params: Optional[Dict[str, Any]] = None


class DataResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int



class SearchRequest(BaseModel):
    query: str
    limit: int = 10


@router.post("/")
async def search(request: SearchRequest) -> DataResponse:
    import json
    conn = get_connection()
    cursor = conn.cursor()
    try:
        search_spec = json.dumps({
            "query": request.query,
            "columns": [],
            "limit": request.limit,
        }).replace("'", "''")
        cursor.execute(f"""
            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'AGENTIC_PLATFORM.ANALYTICS.DOCUMENT_SEARCH',
                '{search_spec}'
            )
        """)
        result = cursor.fetchone()
        if result:
            data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
            return DataResponse(data=data.get("results", []), count=len(data.get("results", [])))
        return DataResponse(data=[], count=0)
    finally:
        cursor.close()
        conn.close()

