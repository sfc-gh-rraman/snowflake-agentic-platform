from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import os
import snowflake.connector

router = APIRouter(prefix="/ml", tags=["ML"])


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



class PredictRequest(BaseModel):
    model_name: str
    features: Dict[str, Any]


@router.post("/predict")
async def predict(request: PredictRequest) -> Dict[str, Any]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT * FROM {request.model_name}!PREDICT(
                {request.features}
            )
        """)
        result = cursor.fetchone()
        return {"prediction": result[0] if result else None}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()

