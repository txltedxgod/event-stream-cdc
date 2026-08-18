from typing import Dict, Any, List
import time
import uuid

class CDCEngine:
    def __init__(self):
        self.outbox: List[Dict[str, Any]] = []
        self.stream: List[Dict[str, Any]] = []

    def capture_mutation(self, table: str, op: str, data: Dict[str, Any]) -> str:
        evt_id = str(uuid.uuid4())
        event = {
            "event_id": evt_id,
            "table": table,
            "operation": op,
            "payload": data,
            "timestamp": time.time(),
            "status": "PENDING"
        }
        self.outbox.append(event)
        return evt_id

    def flush_outbox(self) -> int:
        count = 0
        for evt in self.outbox:
            if evt["status"] == "PENDING":
                evt["status"] = "PUBLISHED"
                self.stream.append(evt)
                count += 1
        return count
