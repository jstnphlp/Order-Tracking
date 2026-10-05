"""
Order Simulator
---------------
Continuously generates realistic random order events and
publishes them to Kafka to simulate a live e-commerce platform.

Every order goes through the normal delivery pipeline:

  CREATED → CONFIRMED → PROCESSING → SHIPPED → DELIVERED

Along the way, two things can happen:

  1. CANCELLATION (5% chance at CREATED or CONFIRMED)
       - Cancelled at CREATED   : nothing was paid, order simply ends (CANCELLED).
       - Cancelled at CONFIRMED : payment was already taken, so the order
                                  continues CANCELLED → REFUNDED.

  2. RETURN TO SELLER (10% of delivered orders, RETURN_PROBABILITY)
       RETURN_REQUESTED → RETURN_APPROVED → RETURN_IN_TRANSIT → RETURN_RECEIVED
       then the seller inspects the item and either:
         - accepts (80%, REFUND_PROBABILITY): → REFUNDED
         - rejects (20%): → RETURN_REJECTED → RETURNING_TO_BUYER → RETURNED_TO_BUYER
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC",  "order-events")
NEW_ORDER_INTERVAL = float(os.getenv("NEW_ORDER_INTERVAL", "5"))   # seconds between new orders
STATUS_DELAY       = float(os.getenv("STATUS_DELAY", "3"))         # seconds between status transitions
CANCEL_PROBABILITY = float(os.getenv("CANCEL_PROBABILITY", "0.05"))  # chance an order is cancelled
RETURN_PROBABILITY = float(os.getenv("RETURN_PROBABILITY", "0.10"))  # chance a delivered order is returned
REFUND_PROBABILITY = float(os.getenv("REFUND_PROBABILITY", "0.80"))  # chance a returned item is accepted (else rejected)

# (product name, price, seller that sells it)
PRODUCTS = [
    ("Wireless Headphones",    2999.00, "SELLER-101"),
    ("Mechanical Keyboard",    3500.00, "SELLER-102"),
    ("USB-C Hub",              1299.00, "SELLER-103"),
    ("Laptop Stand",           1800.00, "SELLER-103"),
    ("Webcam HD 1080p",        2500.00, "SELLER-102"),
    ("Noise-Cancelling Buds",  4999.00, "SELLER-101"),
    ("Smart Watch",            7999.00, "SELLER-104"),
    ("Portable SSD 1TB",       3200.00, "SELLER-104"),
    ("LED Desk Lamp",           899.00, "SELLER-105"),
    ("Phone Case",              299.00, "SELLER-105"),
]

STATUS_PIPELINE = [
    ("CREATED",     "Order placed by customer"),
    ("CONFIRMED",   "Payment confirmed"),
    ("PROCESSING",  "Order being prepared"),
    ("SHIPPED",     "Handed to courier"),
    ("DELIVERED",   "Delivered to customer"),
]

CANCEL_STATUSES = {"CREATED", "CONFIRMED"}   # stages where the customer can still cancel
PAID_STATUSES   = {"CONFIRMED"}              # cancelling here means money must be refunded

RETURN_REASONS = [
    "Item damaged",
    "Wrong item received",
    "Item not as described",
    "Item defective",
    "Changed my mind",
]


# ── Kafka setup ───────────────────────────────────────────────────────────────
def get_producer(retries: int = 15, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[Simulator] Connected to Kafka at {KAFKA_BROKER}")
            return p
        except NoBrokersAvailable:
            print(f"[Simulator] Kafka not ready ({attempt}/{retries}). Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka.")


# ── Event builder ─────────────────────────────────────────────────────────────
def build_event(order: dict, status: str, note: str) -> dict:
    """Build one event for `order` (a dict holding the fixed order details)."""
    return {
        "order_id":     order["order_id"],
        "customer_id":  order["customer_id"],
        "product_name": order["product_name"],
        "seller_id":    order["seller_id"],
        "quantity":     order["quantity"],
        "total_amount": order["total_amount"],
        "status":       status,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "note":         note,
    }


def emit(producer: KafkaProducer, order: dict, status: str, note: str) -> None:
    producer.send(KAFKA_TOPIC, value=build_event(order, status, note))
    producer.flush()
    print(f"  [{order['order_id']}] {status}")


def pause() -> None:
    time.sleep(STATUS_DELAY + random.uniform(0, 2))


# ── Order lifecycle ───────────────────────────────────────────────────────────
def simulate_return(producer: KafkaProducer, order: dict) -> None:
    """Return-to-seller flow, run after an order has been DELIVERED."""
    reason = random.choice(RETURN_REASONS)

    pause()
    emit(producer, order, "RETURN_REQUESTED", f"Buyer requested a return: {reason}")
    pause()
    emit(producer, order, "RETURN_APPROVED", "Seller approved the return")
    pause()
    emit(producer, order, "RETURN_IN_TRANSIT", "Item on its way to the seller")
    pause()
    emit(producer, order, "RETURN_RECEIVED", "Seller received the item and is inspecting it")
    pause()

    if random.random() < REFUND_PROBABILITY:
        emit(producer, order, "REFUNDED", "Return accepted, payment refunded to buyer")
    else:
        emit(producer, order, "RETURN_REJECTED", "Return rejected: item failed inspection")
        pause()
        emit(producer, order, "RETURNING_TO_BUYER", "Item on its way back to the buyer")
        pause()
        emit(producer, order, "RETURNED_TO_BUYER", "Item returned to the buyer")


def simulate_order(producer: KafkaProducer) -> None:
    product, base_price, seller_id = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    order = {
        "order_id":     f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id":  f"CUST-{random.randint(1000, 9999)}",
        "product_name": product,
        "seller_id":    seller_id,
        "quantity":     quantity,
        "total_amount": round(base_price * quantity, 2),
    }

    print(f"\n[Simulator] New order → {order['order_id']} | {product} x{quantity} | {seller_id}")

    for i, (status, note) in enumerate(STATUS_PIPELINE):
        emit(producer, order, status, note)

        # Random cancellation
        if status in CANCEL_STATUSES and random.random() < CANCEL_PROBABILITY:
            pause()
            emit(producer, order, "CANCELLED", "Order cancelled by customer")
            if status in PAID_STATUSES:
                pause()
                emit(producer, order, "REFUNDED", "Order cancelled after payment, payment refunded to buyer")
            return

        if i < len(STATUS_PIPELINE) - 1:
            pause()

    # Order was delivered. A few of them get sent back to the seller.
    if random.random() < RETURN_PROBABILITY:
        simulate_return(producer, order)


# ── Main loop ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    producer = get_producer()
    print(f"[Simulator] Starting. New order every ~{NEW_ORDER_INTERVAL}s.\n")

    order_count = 0
    while True:
        try:
            simulate_order(producer)
            order_count += 1
            print(f"[Simulator] Completed orders so far: {order_count}")
        except Exception as e:
            print(f"[Simulator] Error: {e}")
        time.sleep(NEW_ORDER_INTERVAL)
