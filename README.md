# Real-Time E-Commerce Order Tracking System
**Group Members:** Ferrer, David Gabriel C. · Martinez, Justin Philip S. · Soriano, Darrelle Yuri B. · Ticzon, Niccolo Earl R.

**Stack:** Apache Kafka · Apache Spark (PySpark) · Apache Cassandra · Docker

---

## Architecture

```
[Order Simulator]
      │  JSON events
      ▼
[Apache Kafka]  ←──────────── topic: order-events
      │
      ▼
[Apache Spark]  ←──────────── Structured Streaming (micro-batch)
      │
      ├──► orders              (Cassandra) — current order state
      ├──► orders_by_customer  (Cassandra) — same, searchable by customer
      ├──► order_history       (Cassandra) — full event log
      └──► order_stats         (Cassandra) — orders currently in each status
```

---

## Order Statuses

**Normal delivery:** `CREATED` → `CONFIRMED` → `PROCESSING` → `SHIPPED` → `DELIVERED`

**Cancellation** (5% of orders, at `CREATED` or `CONFIRMED`):
- Cancelled at `CREATED` (nothing paid yet): ends at `CANCELLED`.
- Cancelled at `CONFIRMED` (payment already taken): `CANCELLED` → `REFUNDED`.

**Return to seller** (10% of delivered orders):
`RETURN_REQUESTED` → `RETURN_APPROVED` → `RETURN_IN_TRANSIT` → `RETURN_RECEIVED`, then the seller inspects the item:
- Accepted (80%): `REFUNDED`
- Rejected (20%): `RETURN_REJECTED` → `RETURNING_TO_BUYER` → `RETURNED_TO_BUYER`

The chances can be changed with the environment variables `CANCEL_PROBABILITY`,
`RETURN_PROBABILITY` and `REFUND_PROBABILITY` on the `simulator` service.

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop |
| Docker Compose | Included with Docker Desktop | — |
| Python | 3.10+ (for local query viewer) | https://python.org |

> Make sure Docker Desktop is **running** before you start.

---

## Project Structure

```
order-tracking/
├── docker-compose.yml          # All services (Kafka, Spark, Cassandra, etc.)
├── README.md                   # This file
│
├── cassandra/
│   └── init.cql               # Keyspace + table definitions
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py            # Manual single-order event sender
│
├── simulator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── simulator.py           # Continuous random order generator
│
└── spark/
    ├── spark_processor.py     # PySpark Structured Streaming job
    ├── query_results.py       # Local Cassandra viewer
    └── submit.sh              # spark-submit helper
```

---

## Step-by-Step Setup

### Step 1 — Start all services

Open a terminal in the project folder and run:

```bash
docker-compose up -d
```

This starts: Zookeeper, Kafka, Cassandra, Cassandra-init (runs the CQL schema),
Spark master, Spark worker, the order producer, and the simulator.

> **First run takes 3–5 minutes** — Docker needs to pull the images (~2 GB total).

> **Upgrading from an older version?** The Cassandra tables changed (new `seller_id`
> column and the new `orders_by_customer` table). Reset the stored data once with
> `docker-compose down -v`, then start again with `docker-compose up -d`.

Check everything is running:

```bash
docker-compose ps
```

All services should show `Up`. If `cassandra` shows `starting`, wait another minute and check again.

---

### Step 2 — Verify Cassandra schema was created

```bash
docker exec -it cassandra cqlsh
```

Inside cqlsh:

```sql
USE ecommerce;
DESCRIBE TABLES;
```

You should see: `orders`, `orders_by_customer`, `order_history`, `order_stats`

Type `exit` to leave cqlsh.

---

### Step 3 — Submit the Spark Streaming job

```bash
docker exec spark bash /opt/spark-apps/submit.sh
```

This starts the PySpark job that reads from Kafka and writes to Cassandra.

You will see output like:
```
[Spark] Session started.
[Spark] Streaming query started. Waiting for events...
```

> Keep this terminal open. This is your streaming processor running live.

---

### Step 4 — The simulator is already running

The `order-simulator` container started automatically in Step 1.
It continuously generates random orders and sends them to Kafka.

To watch its output:

```bash
docker logs -f order-simulator
```

Example output:
```
[Simulator] New order → ORD-A3F2C1B0 | Wireless Headphones x1 | SELLER-101
  [ORD-A3F2C1B0] CREATED
  [ORD-A3F2C1B0] CONFIRMED
  [ORD-A3F2C1B0] PROCESSING
  [ORD-A3F2C1B0] SHIPPED
  [ORD-A3F2C1B0] DELIVERED
  [ORD-A3F2C1B0] RETURN_REQUESTED     ← only some orders are returned
  ...
```

---

### Step 5 — View results in Cassandra

**Option A: Query viewer script (recommended)**

Install the driver locally:
```bash
pip install cassandra-driver
```

Run the viewer:
```bash
python spark/query_results.py
```

It refreshes every 5 seconds showing current orders (with seller), the number of
orders in each status, the history of one order, and all orders of that order's customer.

---

**Option B: Manual cqlsh queries**

```bash
docker exec -it cassandra cqlsh
USE ecommerce;
```

Current orders:
```sql
SELECT order_id, product_name, seller_id, status, updated_at FROM orders;
```

All orders of one customer:
```sql
SELECT * FROM orders_by_customer WHERE customer_id = 'CUST-1234';
```

Full history for a specific order:
```sql
SELECT * FROM order_history WHERE order_id = 'ORD-XXXXXXXX';
```

Order statistics (how many orders are in each status **right now**):
```sql
SELECT * FROM order_stats;
```
Every status always has a row (`0` when no order is in it). An order that moves from
`SHIPPED` to `DELIVERED` is subtracted from `count_shipped` and added to `count_delivered`.
Orders cancelled before payment stay at `CANCELLED`; orders cancelled after payment end
at `REFUNDED`, together with returned orders that were refunded.

---

### Step 6 — Send a manual test order (optional)

```bash
docker exec -it order-producer python producer.py
```

This sends a fixed sequence for order `ORD-1001`: normal delivery, then a return to
the seller that ends in a refund.

---

## Monitoring UIs

| Service | URL |
|---------|-----|
| Spark Master UI | http://localhost:8080 |

---

## Stopping the project

```bash
docker-compose down
```

To also delete all stored Cassandra data:

```bash
docker-compose down -v
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `cassandra-init` exits with error | Cassandra wasn't ready yet. Run `docker-compose restart cassandra-init` |
| Spark job can't connect to Kafka | Wait 30s for Kafka to fully start, then resubmit |
| `NoBrokersAvailable` in simulator logs | Normal on first start — it retries automatically |
| Port 9092 already in use | Another Kafka instance is running. Stop it or change the port in `docker-compose.yml` |
| Cassandra takes too long to start | Normal — Cassandra takes 60–90 seconds on first boot |

---

## How It Works (Summary)

1. **Simulator** generates random e-commerce order events (delivery, cancellations with refunds, and returns to the seller, see *Order Statuses*) and publishes them as JSON to the `order-events` Kafka topic. Each order belongs to a seller (`seller_id`).

2. **Kafka** buffers and streams these events in real time, decoupling the producers from consumers.

3. **Spark Structured Streaming** reads from the Kafka topic in micro-batches every 5 seconds. For each batch it:
   - Skips invalid messages (bad JSON, or missing order id, customer id, status or timestamp) and logs how many
   - Appends every event to the `order_history` table
   - Upserts the latest order status into the `orders` and `orders_by_customer` tables
   - Recounts how many orders are in each status and refreshes the `order_stats` table

4. **Cassandra** stores all data with low-latency reads, enabling fast order lookups by `order_id`.
