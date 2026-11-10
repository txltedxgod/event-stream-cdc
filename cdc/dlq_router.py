"""
Dead Letter Queue (DLQ) & Retry Router for CDC Event Streaming.
Implements progressive retry routing and terminal DLQ publishing with error tracing headers.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class RoutingDecision:
    target_topic: str
    is_terminal_dlq: bool
    headers: Dict[str, str]


class DLQRouter:
    """
    Routes failed stream events to progressive retry topics or terminal DLQ.
    Appends audit and diagnostic headers for end-to-end telemetry.
    """

    def __init__(self, dlq_suffix: str = ".DLQ", max_retries: int = 3):
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        self.dlq_suffix = dlq_suffix
        self.max_retries = max_retries

    def get_target_topic(self, original_topic: str, attempt: int) -> str:
        """Determine target topic based on retry attempt index."""
        if attempt >= self.max_retries:
            return f"{original_topic}{self.dlq_suffix}"
        safe_attempt = max(1, attempt)
        return f"{original_topic}.retry-{safe_attempt}"

    def build_headers(
        self, error_msg: str, attempt: int, extra_metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """Construct standard retry/DLQ trace headers."""
        headers = {
            "x-retry-attempt": str(attempt),
            "x-error-message": str(error_msg),
            "x-timestamp": str(int(time.time())),
        }
        if extra_metadata:
            headers.update(extra_metadata)
        return headers

    def route_failure(
        self, original_topic: str, attempt: int, error_msg: str
    ) -> RoutingDecision:
        """Compute complete routing decision including headers and DLQ status."""
        target = self.get_target_topic(original_topic, attempt)
        is_dlq = attempt >= self.max_retries
        headers = self.build_headers(error_msg, attempt)
        if is_dlq:
            headers["x-terminal-dlq"] = "true"
        return RoutingDecision(
            target_topic=target,
            is_terminal_dlq=is_dlq,
            headers=headers,
        )
