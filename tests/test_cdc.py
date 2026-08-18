from cdc.engine import CDCEngine

def test_cdc_outbox_flush():
    c = CDCEngine()
    e1 = c.capture_mutation("users", "INSERT", {"name": "Alice"})
    assert len(c.outbox) == 1
    assert c.flush_outbox() == 1
    assert len(c.stream) == 1
    assert c.stream[0]["payload"]["name"] == "Alice"
