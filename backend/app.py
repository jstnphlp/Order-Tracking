"""Read order data from Cassandra and send status events through Kafka."""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from threading import Lock
from time import monotonic
from typing import Literal
from uuid import uuid4

from cassandra import DriverException
from cassandra.cluster import Cluster, NoHostAvailable
from fastapi import Depends, FastAPI, HTTPException, Query
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
connection_lock = Lock()
cluster = None
session = None
producer = None
producer_lock = Lock()

DELIVERY_TRANSITIONS = {
    "CREATED": "CONFIRMED",
    "CONFIRMED": "PROCESSING",
    "PROCESSING": "SHIPPED",
    "SHIPPED": "DELIVERED",
}
STATUS_NOTES = {
    "CONFIRMED": "Payment confirmed by operations",
    "PROCESSING": "Order preparation started by operations",
    "SHIPPED": "Handed to courier by operations",
    "DELIVERED": "Marked delivered by operations",
}
SAMPLE_PRODUCTS = [
    ("Wireless Headphones", 2999.0, "SELLER-101"),
    ("Mechanical Keyboard", 1899.0, "SELLER-102"),
    ("Canvas Backpack", 1499.0, "SELLER-104"),
    ("Running Shoes", 2499.0, "SELLER-103"),
    ("LED Desk Lamp", 899.0, "SELLER-105"),
]


class SampleOrdersRequest(BaseModel):
    count: int = Field(default=5, ge=1, le=10, strict=True)


class StatusUpdate(BaseModel):
    status: Literal["CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED"]
    expected_status: str = Field(min_length=1, max_length=64)
    expected_updated_at: datetime | None
    note: str = Field(default="", max_length=500)


@asynccontextmanager
async def lifespan(app):
    yield
    if cluster is not None:
        cluster.shutdown()
    if producer is not None:
        producer.close(timeout=5)


app = FastAPI(title="Order Tracking API", lifespan=lifespan)


def get_session():
    """Connect lazily so the API can start before Cassandra is ready."""
    global cluster, session
    with connection_lock:
        if session is None:
            candidate = Cluster(
                [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
                port=int(os.getenv("CASSANDRA_PORT", "9042")),
                connect_timeout=5,
            )
            try:
                session = candidate.connect(os.getenv("CASSANDRA_KEYSPACE", "ecommerce"))
                session.default_timeout = 8
                cluster = candidate
            except (DriverException, NoHostAvailable) as exc:
                candidate.shutdown()
                logger.warning("Cassandra connection unavailable: %s", exc)
                raise HTTPException(503, "Order data is unavailable. Check Cassandra and its schema.") from exc
    return session


def read_rows(db, query, parameters=None):
    try:
        return [
            {key: utc(value) if isinstance(value, datetime) else value
             for key, value in row._asdict().items()}
            for row in db.execute(query, parameters)
        ]
    except (DriverException, NoHostAvailable) as exc:
        logger.warning("Cassandra query failed: %s", exc)
        raise HTTPException(503, "Order data is unavailable. Check Cassandra and its schema.") from exc


def get_producer():
    global producer
    with producer_lock:
        if producer is None:
            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BROKER", "127.0.0.1:9092"),
                value_serializer=lambda event: json.dumps(event).encode("utf-8"),
                acks="all",
                retries=0,
                max_block_ms=5000,
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=3000,
            )
    return producer


def publish_event(event):
    try:
        get_producer().send(
            os.getenv("KAFKA_TOPIC", "order-events"),
            key=event["order_id"].encode("utf-8"),
            value=event,
        ).get(timeout=10)
    except KafkaError as exc:
        logger.warning("Kafka status event was not acknowledged: %s", exc)
        raise HTTPException(
            503, "Could not confirm Kafka receipt. Refresh the order before retrying."
        ) from exc


def publish_sample_events(events):
    submitted = []
    try:
        deadline = monotonic() + 10
        publisher = get_producer()
        futures = []
        for event in events:
            if monotonic() >= deadline:
                raise KafkaTimeoutError("Sample batch timed out")
            future = publisher.send(
                os.getenv("KAFKA_TOPIC", "order-events"),
                key=event["order_id"].encode("utf-8"), value=event,
            )
            submitted.append(event["order_id"])
            futures.append(future)
        for future in futures:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise KafkaTimeoutError("Sample batch timed out")
            future.get(timeout=remaining)
    except KafkaError as exc:
        logger.warning("Sample order batch was not fully acknowledged: %s", exc)
        raise HTTPException(503, detail={
            "message": "Could not confirm the sample batch in Kafka. "
                       "Some orders may still appear; check before adding another batch.",
            "submitted_order_ids": submitted,
        }) from exc


def utc(value):
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def with_actions(row):
    next_status = DELIVERY_TRANSITIONS.get(row.get("status"))
    return {**row, "allowed_statuses": [next_status] if next_status else []}


@app.get("/api/health")
def health(db=Depends(get_session)):
    read_rows(db, "SELECT release_version FROM system.local")
    return {"status": "ok"}


@app.get("/api/stats")
def stats(db=Depends(get_session)):
    rows = read_rows(db, "SELECT stat_key, stat_value, updated_at FROM order_stats")
    items = sorted(rows, key=lambda row: row["stat_key"])
    timestamps = [row["updated_at"] for row in items if row["updated_at"] is not None]
    return {
        "items": items,
        "total_orders": sum(row["stat_value"] or 0 for row in items if row["stat_key"].startswith("count_")),
        "updated_at": max(timestamps) if timestamps else None,
    }


@app.get("/api/orders")
def orders(limit: int = Query(100, ge=1, le=200),
           tracked: list[str] = Query(default=[], max_length=50), db=Depends(get_session)):
    # Cassandra partitions orders by ID. LIMIT bounds the read; it does not
    # select the globally newest orders. Sort only this loaded sample.
    rows = read_rows(db, "SELECT * FROM orders LIMIT %s", (limit,))
    # Newly created sample orders may fall outside Cassandra's arbitrary LIMIT
    # sample. Fetch their known partition IDs explicitly so they remain usable.
    loaded = {row["order_id"] for row in rows}
    missing = tuple(dict.fromkeys(order_id for order_id in tracked if order_id not in loaded))
    if missing:
        rows.extend(read_rows(
            db, "SELECT * FROM orders WHERE order_id IN %s", (missing,),
        ))
    rows.sort(key=lambda row: row["updated_at"].isoformat() if row["updated_at"] else "", reverse=True)
    return {"items": [with_actions(row) for row in rows], "limit": limit, "scope": "sample"}


@app.post("/api/orders/sample", status_code=202)
def create_sample_orders(request: SampleOrdersRequest):
    now = datetime.now(timezone.utc)
    events = []
    for index in range(request.count):
        product, price, seller = SAMPLE_PRODUCTS[index % len(SAMPLE_PRODUCTS)]
        quantity = index % 3 + 1
        events.append({
            "order_id": f"ORD-SIM-{uuid4().hex[:12].upper()}",
            "customer_id": f"CUST-{uuid4().hex[:8].upper()}",
            "product_name": product, "seller_id": seller, "quantity": quantity,
            "total_amount": round(price * quantity, 2), "status": "CREATED",
            "timestamp": (now + timedelta(milliseconds=index)).isoformat(),
            "note": "Sample order added from dashboard",
        })
    publish_sample_events(events)
    return {
        "items": [{"order_id": event["order_id"], "status": "CREATED",
                   "timestamp": event["timestamp"]} for event in events],
        "message": "Sample events accepted by Kafka. Waiting for Spark to create the orders.",
    }


@app.get("/api/orders/{order_id}")
def order(order_id: str, db=Depends(get_session)):
    rows = read_rows(db, "SELECT * FROM orders WHERE order_id = %s", (order_id,))
    if not rows:
        raise HTTPException(404, "Order not found.")
    return with_actions(rows[0])


@app.post("/api/orders/{order_id}/status", status_code=202)
def update_status(order_id: str, update: StatusUpdate, db=Depends(get_session)):
    rows = read_rows(db, "SELECT * FROM orders WHERE order_id = %s", (order_id,))
    if not rows:
        raise HTTPException(404, "Order not found.")
    current = rows[0]
    if (current["status"] != update.expected_status
            or utc(current["updated_at"]) != utc(update.expected_updated_at)):
        raise HTTPException(409, "This order changed. Refresh it before updating its status.")
    if DELIVERY_TRANSITIONS.get(current["status"]) != update.status:
        raise HTTPException(409, "This delivery transition is not available for the current status.")

    now = datetime.now(timezone.utc)
    if current["updated_at"] is not None:
        now = max(now, utc(current["updated_at"]) + timedelta(milliseconds=1))
    event = {
        field: current.get(field)
        for field in ("order_id", "customer_id", "product_name", "seller_id", "quantity", "total_amount")
    }
    event.update(status=update.status, timestamp=now.isoformat(),
                 note=update.note.strip() or STATUS_NOTES[update.status])
    publish_event(event)
    return {
        "order_id": order_id,
        "status": update.status,
        "timestamp": event["timestamp"],
        "message": "Event accepted by Kafka. Waiting for Spark to update the order.",
    }


@app.get("/api/orders/{order_id}/history")
def history(order_id: str, db=Depends(get_session)):
    rows = read_rows(
        db,
        "SELECT event_time, status, note FROM order_history WHERE order_id = %s "
        "ORDER BY event_time DESC LIMIT 100",
        (order_id,),
    )
    return {"items": list(reversed(rows))}
