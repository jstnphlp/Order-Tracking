"""
Order Event Producer
--------------------
Sends a single manual order event to Kafka.
Used for testing individual events.
For automated load, use the simulator instead.
"""

import json
import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "order-events")


def get_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[Producer] Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"[Producer] Kafka not ready (attempt {attempt}/{retries}). Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple attempts.")


def send_event(producer: KafkaProducer, event: dict) -> None:
    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()
    print(f"[Producer] Sent → {event}")


def make_event(status: str, note: str) -> dict:
    """Build one event for the fixed test order ORD-1001 (timestamp = now)."""
    return {
        "order_id":     "ORD-1001",
        "customer_id":  "CUST-001",
        "product_name": "Wireless Headphones",
        "seller_id":    "SELLER-101",
        "quantity":     1,
        "total_amount": 2999.00,
        "status":       status,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "note":         note,
    }


if __name__ == "__main__":
    producer = get_producer()

    # A fixed test order: delivered, then returned to the seller and refunded.
    sample_sequence = [
        ("CREATED",           "Order placed by customer"),
        ("CONFIRMED",         "Payment confirmed"),
        ("PROCESSING",        "Order being prepared"),
        ("SHIPPED",           "Handed to courier"),
        ("DELIVERED",         "Delivered to customer"),
        ("RETURN_REQUESTED",  "Buyer requested a return: Item damaged"),
        ("RETURN_APPROVED",   "Seller approved the return"),
        ("RETURN_IN_TRANSIT", "Item on its way to the seller"),
        ("RETURN_RECEIVED",   "Seller received the item and is inspecting it"),
        ("REFUNDED",          "Return accepted, payment refunded to buyer"),
    ]

    for status, note in sample_sequence:
        send_event(producer, make_event(status, note))   # timestamp taken at send time
        time.sleep(2)   # 2-second gap between status transitions

    producer.close()
    print("[Producer] Done.")
