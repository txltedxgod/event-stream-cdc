from typing import Dict, Any, List, Optional
import time
import uuid
from cdc.dlq_router import DLQRouter, RoutingDecision

class CDCEngine:
    def __init__(self, dlq_router: Optional[DLQRouter] = None):
        self.outbox: List[Dict[str, Any]] = []
        self.stream: List[Dict[str, Any]] = []
        self.dead_letter_queue: List[Dict[str, Any]] = []
        self.dlq_router = dlq_router or DLQRouter()

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

    def route_failure(
        self, original_topic: str, event_id: str, attempt: int, error_msg: str
    ) -> RoutingDecision:
        decision = self.dlq_router.route_failure(original_topic, attempt, error_msg)
        record = {
            "event_id": event_id,
            "original_topic": original_topic,
            "target_topic": decision.target_topic,
            "is_terminal_dlq": decision.is_terminal_dlq,
            "headers": decision.headers,
            "timestamp": time.time(),
        }
        if decision.is_terminal_dlq:
            self.dead_letter_queue.append(record)
        return decision
