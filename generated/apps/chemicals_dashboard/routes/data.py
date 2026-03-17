from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import os
import snowflake.connector

router = APIRouter(prefix="/data", tags=["Data"])


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



@router.get("/")
async def list_data() -> DataResponse:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM AGENTIC_PLATFORM.ANALYTICS.DATA LIMIT 100")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return DataResponse(data=rows, count=len(rows))
    finally:
        cursor.close()
        conn.close()


@router.post("/query")
async def execute_query(request: QueryRequest) -> DataResponse:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(request.query)
        columns = [col[0] for col in cursor.description] if cursor.description else []
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()] if columns else []
        return DataResponse(data=rows, count=len(rows))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()

