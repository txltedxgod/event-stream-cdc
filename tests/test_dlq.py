import pytest
from cdc.dlq_router import DLQRouter, RoutingDecision
from cdc.engine import CDCEngine


def test_dlq_router_progressive_retries():
    router = DLQRouter(max_retries=3)
    assert router.get_target_topic("orders.events", 1) == "orders.events.retry-1"
    assert router.get_target_topic("orders.events", 2) == "orders.events.retry-2"


def test_dlq_router_terminal_dlq_boundary():
    router = DLQRouter(dlq_suffix=".DLQ", max_retries=3)
    # At attempt 3 (max_retries), route to terminal DLQ
    assert router.get_target_topic("orders.events", 3) == "orders.events.DLQ"
    # Greater than max_retries also routes to terminal DLQ
    assert router.get_target_topic("orders.events", 4) == "orders.events.DLQ"
    assert router.get_target_topic("orders.events", 10) == "orders.events.DLQ"


def test_dlq_custom_suffix_and_max_retries():
    router = DLQRouter(dlq_suffix="-dead-letter", max_retries=5)
    assert router.get_target_topic("payments", 4) == "payments.retry-4"
    assert router.get_target_topic("payments", 5) == "payments-dead-letter"


def test_dlq_invalid_max_retries():
    with pytest.raises(ValueError, match="max_retries must be at least 1"):
        DLQRouter(max_retries=0)


def test_dlq_headers_generation():
    router = DLQRouter()
    headers = router.build_headers("Connection timeout to broker", 2)
    assert headers["x-retry-attempt"] == "2"
    assert headers["x-error-message"] == "Connection timeout to broker"
    assert "x-timestamp" in headers
    assert int(headers["x-timestamp"]) > 0


def test_dlq_route_failure_decision():
    router = DLQRouter(max_retries=3)
    
    # Retry decision
    dec1 = router.route_failure("inventory", 1, "Temporary 503")
    assert dec1.target_topic == "inventory.retry-1"
    assert dec1.is_terminal_dlq is False
    assert "x-terminal-dlq" not in dec1.headers

    # Terminal DLQ decision
    dec2 = router.route_failure("inventory", 3, "Schema validation failed")
    assert dec2.target_topic == "inventory.DLQ"
    assert dec2.is_terminal_dlq is True
    assert dec2.headers["x-terminal-dlq"] == "true"


def test_cdc_engine_dlq_integration():
    engine = CDCEngine()
    evt_id = engine.capture_mutation("accounts", "UPDATE", {"balance": 100})
    
    # Simulate a failed attempt 1
    d1 = engine.route_failure("cdc.accounts", evt_id, 1, "Database lock timeout")
    assert d1.target_topic == "cdc.accounts.retry-1"
    assert len(engine.dead_letter_queue) == 0

    # Simulate final failed attempt 3
    d2 = engine.route_failure("cdc.accounts", evt_id, 3, "Permanent deserialization error")
    assert d2.target_topic == "cdc.accounts.DLQ"
    assert d2.is_terminal_dlq is True
    assert len(engine.dead_letter_queue) == 1
    assert engine.dead_letter_queue[0]["event_id"] == evt_id
    assert engine.dead_letter_queue[0]["headers"]["x-terminal-dlq"] == "true"
