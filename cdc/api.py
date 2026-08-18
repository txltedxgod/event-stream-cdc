from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any
from cdc.engine import CDCEngine

app = FastAPI(title="Event Stream CDC", version="0.1.0")
cdc = CDCEngine()

class MutationReq(BaseModel):
    table: str
    operation: str
    data: Dict[str, Any]

@app.post("/api/v1/capture")
def capture(req: MutationReq):
    evt_id = cdc.capture_mutation(req.table, req.operation, req.data)
    flushed = cdc.flush_outbox()
    return {"event_id": evt_id, "flushed_count": flushed}

@app.get("/api/v1/stream")
def get_stream():
    return {"events": cdc.stream}
