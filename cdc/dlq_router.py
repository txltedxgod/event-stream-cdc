import json
import time

class DLQRouter:
    def __init__(self, dlq_suffix: str = ".DLQ", max_retries: int = 3):
        self.dlq_suffix = dlq_suffix
        self.max_retries = max_retries

    def get_target_topic(self, original_topic: str, attempt: int) -> str:
        if attempt >= self.max_retries:
            return f"{original_topic}{self.dlq_suffix}"
        return f"{original_topic}.retry-{attempt}"

    def build_headers(self, error_msg: str, attempt: int) -> dict[str, str]:
        return {
            "x-retry-attempt": str(attempt),
            "x-error-message": error_msg,
            "x-timestamp": str(int(time.time()))
        }
