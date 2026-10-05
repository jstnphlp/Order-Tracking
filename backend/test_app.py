"""API contract checks without a running Cassandra cluster."""

from collections import namedtuple
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from cassandra import OperationTimedOut
from cassandra.cluster import NoHostAvailable
from fastapi import HTTPException
from fastapi.testclient import TestClient
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

import app as api_module
from app import app, get_session


class FakeSession:
    def __init__(self):
        self.rows = []
        self.calls = []
        self.error = None

    def execute(self, query, parameters=None):
        self.calls.append((query, parameters))
        if self.error:
            raise self.error
        return self.rows


@pytest.fixture
def api():
    db = FakeSession()
    app.dependency_overrides[get_session] = lambda: db
    with TestClient(app) as client:
        yield client, db
    app.dependency_overrides.clear()


def test_stats_total_and_timestamp(api):
    client, db = api
    row = namedtuple("Stat", "stat_key stat_value updated_at")
    now = datetime(2026, 10, 5, tzinfo=timezone.utc)
    db.rows = [row("count_shipped", 3, now), row("count_delivered", 7, now)]
    result = client.get("/api/stats")
    assert result.status_code == 200
    assert result.json()["total_orders"] == 10
    assert result.json()["updated_at"].startswith("2026-10-05")
    assert result.json()["items"][0]["stat_key"] == "count_delivered"


def test_empty_stats(api):
    client, _ = api
    assert client.get("/api/stats").json() == {"items": [], "total_orders": 0, "updated_at": None}


def test_orders_bounded_and_sorted(api):
    client, db = api
    row = namedtuple("Order", "order_id updated_at")
    db.rows = [row("old", None), row("new", datetime(2026, 10, 5, tzinfo=timezone.utc))]
    result = client.get("/api/orders?limit=20").json()
    assert [item["order_id"] for item in result["items"]] == ["new", "old"]
    assert result["scope"] == "sample"
    assert db.calls[0][1] == (20,)
    assert client.get("/api/orders?limit=201").status_code == 422


def test_order_not_found_and_parameterized(api):
    client, db = api
    assert client.get("/api/orders/ORD-1001").status_code == 404
    assert db.calls[0][1] == ("ORD-1001",)


def test_history_chronological_and_bounded(api):
    client, db = api
    row = namedtuple("Event", "event_time status note")
    db.rows = [row(None, "SHIPPED", "On the way"), row(None, "CREATED", None)]
    result = client.get("/api/orders/ORD-1001/history").json()
    assert [item["status"] for item in result["items"]] == ["CREATED", "SHIPPED"]
    assert "LIMIT 100" in db.calls[0][0]
    assert db.calls[0][1] == ("ORD-1001",)


def test_database_failure_is_service_unavailable(api):
    client, db = api
    db.error = OperationTimedOut("unavailable")
    result = client.get("/api/stats")
    assert result.status_code == 503
    assert "unavailable" in result.json()["detail"]


def test_failed_connection_is_closed_and_can_retry(monkeypatch):
    class Candidate:
        closed = False
        fail = True

        def connect(self, keyspace):
            if self.fail:
                raise NoHostAvailable("not ready", {})
            return SimpleNamespace(default_timeout=None)

        def shutdown(self):
            self.closed = True

    candidate = Candidate()
    monkeypatch.setattr(api_module, "Cluster", lambda *args, **kwargs: candidate)
    monkeypatch.setattr(api_module, "session", None)
    monkeypatch.setattr(api_module, "cluster", None)
    with pytest.raises(HTTPException) as failure:
        get_session()
    assert failure.value.status_code == 503
    assert candidate.closed
    assert api_module.session is None
    candidate.fail = False
    connected = get_session()
    assert connected.default_timeout == 8
    assert get_session() is connected


STAMP = datetime(2026, 10, 5, 9, tzinfo=timezone.utc)


@pytest.fixture
def event_api(api, monkeypatch):
    client, db = api
    fields = {
        "order_id": "ORD-1001", "customer_id": "CUST-001",
        "product_name": "Wireless Headphones", "seller_id": "SELLER-101",
        "quantity": 2, "total_amount": 4998.0, "status": "SHIPPED",
        "updated_at": STAMP.replace(tzinfo=None),
    }
    db.rows = [namedtuple("Order", fields.keys())(**fields)]

    class Publisher:
        calls = None
        error = None

        def __init__(self):
            self.calls = []

        def send(self, topic, key, value):
            self.calls.append((topic, key, value))
            return self

        def get(self, timeout):
            if self.error:
                raise self.error

    publisher = Publisher()
    monkeypatch.setattr(api_module, "get_producer", lambda: publisher)
    monkeypatch.setenv("KAFKA_TOPIC", "order-events")
    return client, db, publisher


def status_payload(status="DELIVERED", expected_status="SHIPPED"):
    return {"status": status, "expected_status": expected_status,
            "expected_updated_at": STAMP.isoformat(), "note": "  Delivered at reception  "}


@pytest.mark.parametrize("before,after", [
    ("CREATED", "CONFIRMED"), ("CONFIRMED", "PROCESSING"),
    ("PROCESSING", "SHIPPED"), ("SHIPPED", "DELIVERED"),
])
def test_delivery_actions_publish_events_without_writing_cassandra(event_api, before, after):
    client, db, publisher = event_api
    db.rows = [db.rows[0]._replace(status=before)]
    result = client.post("/api/orders/ORD-1001/status", json=status_payload(after, before))
    assert result.status_code == 202
    topic, key, event = publisher.calls[0]
    assert topic == "order-events"
    assert key == b"ORD-1001"
    assert event["status"] == after
    assert event["customer_id"] == "CUST-001"
    assert event["seller_id"] == "SELLER-101"
    assert event["product_name"] == "Wireless Headphones"
    assert event["quantity"] == 2
    assert event["total_amount"] == 4998.0
    assert event["note"] == "Delivered at reception"
    assert datetime.fromisoformat(event["timestamp"]) > STAMP
    assert result.json()["timestamp"] == event["timestamp"]
    assert len(db.calls) == 1
    assert db.calls[0][0].startswith("SELECT")


@pytest.mark.parametrize("field,value", [
    ("expected_status", "PROCESSING"), ("expected_updated_at", "2026-10-05T08:00:00Z"),
])
def test_stale_status_update_does_not_publish(event_api, field, value):
    client, _, publisher = event_api
    payload = {**status_payload(), field: value}
    assert client.post("/api/orders/ORD-1001/status", json=payload).status_code == 409
    assert not publisher.calls


@pytest.mark.parametrize("before,after", [
    ("CREATED", "DELIVERED"), ("SHIPPED", "PROCESSING"),
    ("DELIVERED", "SHIPPED"), ("CANCELLED", "DELIVERED"),
    ("RETURN_IN_TRANSIT", "DELIVERED"),
])
def test_invalid_delivery_transitions_do_not_publish(event_api, before, after):
    client, db, publisher = event_api
    db.rows = [db.rows[0]._replace(status=before)]
    assert client.post("/api/orders/ORD-1001/status", json=status_payload(after, before)).status_code == 409
    assert not publisher.calls


def test_missing_order_cannot_publish(event_api):
    client, db, publisher = event_api
    db.rows = []
    assert client.post("/api/orders/ORD-missing/status", json=status_payload()).status_code == 404
    assert not publisher.calls


def test_note_length_is_bounded(event_api):
    client, _, publisher = event_api
    payload = {**status_payload(), "note": "x" * 501}
    assert client.post("/api/orders/ORD-1001/status", json=payload).status_code == 422
    assert not publisher.calls


def test_kafka_failure_does_not_report_acceptance(event_api):
    client, _, publisher = event_api
    publisher.error = KafkaTimeoutError("no acknowledgement")
    result = client.post("/api/orders/ORD-1001/status", json=status_payload())
    assert result.status_code == 503
    assert "Could not confirm Kafka receipt" in result.json()["detail"]


def test_order_actions_and_utc_timestamp(event_api):
    client, _, _ = event_api
    order = client.get("/api/orders/ORD-1001").json()
    assert order["allowed_statuses"] == ["DELIVERED"]
    assert order["updated_at"].endswith("Z") or order["updated_at"].endswith("+00:00")


def test_blank_note_uses_delivery_description(event_api):
    client, _, publisher = event_api
    payload = {**status_payload(), "note": " "}
    assert client.post("/api/orders/ORD-1001/status", json=payload).status_code == 202
    assert publisher.calls[0][2]["note"] == "Marked delivered by operations"


def test_sample_creation_publishes_unique_complete_created_events(event_api):
    client, db, publisher = event_api
    result = client.post("/api/orders/sample", json={})
    assert result.status_code == 202
    ids = [item["order_id"] for item in result.json()["items"]]
    assert len(ids) == len(set(ids)) == 5
    assert len(publisher.calls) == 5
    assert not db.calls
    for (topic, key, event), order_id in zip(publisher.calls, ids):
        assert topic == "order-events"
        assert key.decode() == order_id == event["order_id"]
        assert event["status"] == "CREATED"
        assert event["customer_id"] and event["product_name"] and event["seller_id"]
        assert event["quantity"] > 0 and event["total_amount"] > 0
        assert datetime.fromisoformat(event["timestamp"]).tzinfo is not None
        assert event["note"] == "Sample order added from dashboard"
    again = client.post("/api/orders/sample", json={"count": 2})
    assert again.status_code == 202
    assert len(again.json()["items"]) == 2
    assert set(ids).isdisjoint(item["order_id"] for item in again.json()["items"])


@pytest.mark.parametrize("count", [0, 11, 1.5, "5", True])
def test_sample_batch_size_is_bounded(event_api, count):
    client, _, publisher = event_api
    assert client.post("/api/orders/sample", json={"count": count}).status_code == 422
    assert not publisher.calls


def test_sample_timeout_reports_submitted_ids_without_claiming_success(event_api):
    client, _, publisher = event_api
    publisher.error = KafkaTimeoutError("unacknowledged")
    result = client.post("/api/orders/sample", json={"count": 3})
    assert result.status_code == 503
    submitted = result.json()["detail"]["submitted_order_ids"]
    assert submitted == [event["order_id"] for _, _, event in publisher.calls]
    assert len(submitted) == 3


def test_sample_kafka_connection_failure_has_no_submitted_ids(event_api, monkeypatch):
    client, _, publisher = event_api

    def unavailable():
        raise NoBrokersAvailable()

    monkeypatch.setattr(api_module, "get_producer", unavailable)
    result = client.post("/api/orders/sample", json={})
    assert result.status_code == 503
    assert result.json()["detail"]["submitted_order_ids"] == []
    assert not publisher.calls


def test_tracked_orders_are_loaded_outside_the_sample(api):
    client, db = api
    row = namedtuple("Order", "order_id status updated_at")
    old = row("OLD", "DELIVERED", STAMP)
    new = row("NEW", "CREATED", STAMP)

    def execute(query, parameters=None):
        db.calls.append((query, parameters))
        return [new] if " IN " in query else [old]

    db.execute = execute
    result = client.get("/api/orders?limit=1&tracked=NEW&tracked=NEW&tracked=OLD")
    assert result.status_code == 200
    assert {order["order_id"] for order in result.json()["items"]} == {"OLD", "NEW"}
    assert db.calls[1][1] == (("NEW",),)
    assert next(order for order in result.json()["items"] if order["order_id"] == "NEW")["allowed_statuses"] == ["CONFIRMED"]


def test_tracked_partition_reads_are_bounded(api):
    client, db = api
    query = "&".join(f"tracked=ORD-{index}" for index in range(51))
    assert client.get(f"/api/orders?{query}").status_code == 422
    assert not db.calls
