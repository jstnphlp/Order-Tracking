"""
Query Results Viewer
--------------------
Run this locally (outside Docker) to inspect what Spark
has written to Cassandra in real time.

Requirements:
    pip install cassandra-driver

Usage:
    python query_results.py
"""

import time
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = "ecommerce"

def connect():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)
    print(f"[Query] Connected to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}\n")
    return session

def print_orders(session):
    rows = session.execute("SELECT order_id, customer_id, product_name, seller_id, status, updated_at FROM orders LIMIT 20")
    print("=" * 100)
    print(f"{'ORDER ID':<13} {'CUSTOMER':<11} {'PRODUCT':<22} {'SELLER':<11} {'STATUS':<19} UPDATED")
    print("-" * 100)
    for r in rows:
        print(f"{r.order_id:<13} {r.customer_id:<11} {r.product_name:<22} {str(r.seller_id):<11} {r.status:<19} {r.updated_at}")
    print()

def print_customer_orders(session, customer_id: str):
    rows = session.execute(
        "SELECT order_id, product_name, status, updated_at FROM orders_by_customer WHERE customer_id = %s",
        [customer_id]
    )
    print(f"── All orders of {customer_id} ──")
    for r in rows:
        print(f"  {r.order_id:<13} {r.product_name:<22} {r.status:<19} {r.updated_at}")
    print()

def print_history(session, order_id: str):
    rows = session.execute(
        "SELECT event_time, status, note FROM order_history WHERE order_id = %s",
        [order_id]
    )
    print(f"\n── History for {order_id} ──")
    for r in rows:
        print(f"  {r.event_time}  [{r.status}]  {r.note}")
    print()

def print_stats(session):
    rows = sorted(session.execute("SELECT stat_key, stat_value FROM order_stats"),
                  key=lambda r: r.stat_key)
    print("── Orders currently in each status ──")
    for r in rows:
        print(f"  {r.stat_key:<25} {r.stat_value}")
    print()

if __name__ == "__main__":
    session = connect()

    while True:
        try:
            print_orders(session)
            print_stats(session)

            # Show history for the first order in the table
            first = session.execute("SELECT order_id, customer_id FROM orders LIMIT 1").one()
            if first:
                print_history(session, first.order_id)
                print_customer_orders(session, first.customer_id)

        except Exception as e:
            print(f"[Query] Error: {e}")

        time.sleep(5)
        print("\n" + "=" * 70 + "\n")
