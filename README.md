# Real-Time E-Commerce Order Tracking System Using Apache Kafka, Spark, and Cassandra
**Group Members:** Ferrer, David Gabriel C. · Martinez, Justin Philip S. · Soriano, Darrelle Yuri B. · Ticzon, Niccolo Earl R.

**Stack:** Apache Kafka · Apache Spark (PySpark) · Apache Cassandra · FastAPI · React + Vite · Docker

## Web dashboard

The **Parcel** dashboard uses React + Vite (JavaScript), with a vector logistics
theme, status totals, searchable orders, CSV export, and a selected order's event
timeline. FastAPI reads processed results from Cassandra and publishes delivery
status changes to Kafka. Spark processes those events and updates the stored
order state, history, and statistics.

### Run with Docker

```bash
docker compose up -d --build
docker exec spark bash /opt/spark-apps/submit.sh
```

Keep the Spark submission terminal open. Open **http://localhost:3000** for the
dashboard. API documentation is at **http://localhost:8000/docs**.

**Live** mode refreshes orders, statistics, and the selected order's history every
five seconds. **Demo** mode is explicitly labeled and works without the backend;
use it to preview the design when the infrastructure is unavailable. A connection
failure in Live mode displays an error and retains any previously loaded snapshot.

### Add sample orders

Click **Add sample orders** to create five orders with unique IDs, products,
customers, sellers, quantities, and amounts. Each starts at **CREATED**, ready
for you to confirm, process, ship, and deliver using the status controls.

In **Live**, the button publishes creation events to Kafka. Keep Spark running;
the dashboard waits until the orders appear in Cassandra and selects the first
new order. These orders are advanced manually, so the automatic simulator can
remain paused. The last 50 orders added in this browser session are fetched by
ID alongside the normal table sample, keeping them visible even when the
database contains more than 100 orders. If a Kafka batch fails partway through,
the dashboard reports the error and continues checking any submitted IDs.

In **Demo**, the button immediately adds five sample orders and updates the
local timeline and statistics. Reloading the page resets demo data.

### Advance an order from the dashboard

Select an order in **Order activity**. In **Parcel journey**, use the available
action: **Confirm order**, **Start processing**, **Ship order**, or **Mark delivered**.
An optional event note is saved in the order history. Actions follow the sequence
`CREATED → CONFIRMED → PROCESSING → SHIPPED → DELIVERED`; skipping stages,
moving backward, and advancing cancelled or returned orders are rejected.

In **Live**, start Spark before submitting an update. The API returns HTTP 202
after Kafka acknowledges the event. The dashboard then waits for Spark's result
to appear in Cassandra; it does not display a queued event as a completed update.
An outdated order snapshot is rejected with HTTP 409 so you can refresh it.

The simulator also advances its orders automatically. For deliberate manual
testing, pause it with `docker compose stop simulator` while there are active
orders. Resume it with `docker compose start simulator` when finished. Snapshot
validation applies at submission time; it cannot prevent a concurrent simulator
event from advancing the order afterward.

In **Demo**, the same controls update the sample order, timeline, and statistics
locally. Changes last until the page reloads and never send events to Kafka.

### Run the frontend and API locally

Keep Cassandra, Kafka, and the Spark processor running in Docker. In PowerShell,
from the project root, create the backend environment and start the API:

If the full Docker stack is already running, first run
`docker compose stop frontend backend` to free port 8000 for the local API.

```powershell
python -m venv backend/.venv
backend/.venv/Scripts/python.exe -m pip install -r backend/requirements.txt
backend/.venv/Scripts/python.exe -m uvicorn app:app --app-dir backend --reload
```

In a second terminal:

```powershell
cd frontend
npm install
npm run dev
```

Open the URL printed by Vite (normally **http://localhost:5173**). Vite proxies
`/api` to `http://127.0.0.1:8000`; set `API_PROXY_TARGET` before starting Vite to
use a different API host. The production Docker frontend serves built assets
through Nginx and proxies `/api` to the backend.

The backend accepts `CASSANDRA_HOST` (default `127.0.0.1`), `CASSANDRA_PORT`
(default `9042`), and `CASSANDRA_KEYSPACE` (default `ecommerce`). Kafka publishing
uses `KAFKA_BROKER` (default `localhost:9092`) and `KAFKA_TOPIC` (default
`order-events`). Docker configures the internal broker address automatically.
The API connects lazily
and returns HTTP 503 when Cassandra or the schema is unavailable.

### Data scope

- Status totals come from Spark's `order_stats` snapshot across all orders.
- The orders API loads at most 200 orders, with 100 loaded by the dashboard.
  Cassandra partitions this table by order ID, so this is a sample, **not the
  globally newest orders**. Sorting, searching, status filters, and CSV export
  apply only to that loaded sample.
- The optional repeated `tracked` query parameter includes up to 50 additional
  order IDs. The dashboard uses it for sample orders added in this session.
- The timeline shows the latest 100 events for the selected order, displayed in
  chronological order. Return, cancellation, and refund events are preserved.
- The API exposes `GET /api/health`, `/api/stats`, `/api/orders?limit=100`,
  `/api/orders/{order_id}`, and `/api/orders/{order_id}/history`.
- Delivery actions use `POST /api/orders/{order_id}/status` with `status`,
  `expected_status`, `expected_updated_at`, and an optional `note`.
- Sample creation uses `POST /api/orders/sample` with `count` (default 5,
  between 1 and 10). HTTP 202 indicates Kafka acknowledged the batch;
  Spark creates the Cassandra records afterward.

### Targeted checks

```powershell
cd frontend
npm run lint
npm test
npm run build
```

From the project root, API contract tests run without Cassandra:

```powershell
backend/.venv/Scripts/python.exe -m pip install pytest httpx
backend/.venv/Scripts/python.exe -m pytest backend/test_app.py -q
```

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
| Node.js | 22 LTS (for local frontend development) | https://nodejs.org |
| Python | 3.10+ (for local API or query viewer) | https://python.org |

Node.js and Python are only needed on the host for local development or the
optional query viewer. Docker builds include the required runtimes.

> Make sure Docker Desktop is **running** before you start.

---

## Project Structure

```
order-tracking/
├── frontend/                   # React + Vite dashboard and production Nginx
├── backend/                    # FastAPI data access, Kafka status events, API tests
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
docker compose up -d --build
```

This starts: Zookeeper, Kafka, Cassandra, Cassandra-init (runs the CQL schema),
Spark master, Spark worker, the order producer, the simulator, the FastAPI backend,
and the React frontend. The Spark streaming job still requires Step 3.

> **First run takes 3–5 minutes** — Docker needs to pull the images (~2 GB total).

> **Upgrading from an older version?** `CREATE TABLE IF NOT EXISTS` does not add
> columns to existing tables. Inspect the current schema with
> `docker exec cassandra cqlsh -e "DESCRIBE TABLE ecommerce.orders;"`. If
> `seller_id` is missing, apply the one-time migration:
> `docker exec cassandra cqlsh -e "ALTER TABLE ecommerce.orders ADD seller_id TEXT;"`.
> The migration is also saved in `cassandra/migrations/001_add_orders_seller_id.cql`.
> Run `docker compose run --rm cassandra-init` to create any missing tables,
> then submit the Spark job again.

Check everything is running:

```bash
docker compose ps
```

Long-running services should be running. `cassandra-init` exits with code 0 after
creating the schema; the manual producer also exits after sending its events.
If Cassandra is still starting, wait another minute and check again.

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

### Step 5 — View the dashboard and Cassandra results

**Option A: Web dashboard (recommended)**

Open **http://localhost:3000** and select **Live**. The dashboard refreshes every
five seconds with status totals, order activity, and the selected order's timeline.
Use **Demo** to preview the interface with sample orders. The API documentation
is available at **http://localhost:8000/docs**.

**Option B: Query viewer script**

Install the driver locally:
```bash
pip install cassandra-driver
```

On Python 3.12 or newer, also run `pip install pyasyncore` for driver compatibility.

Run the viewer:
```bash
python spark/query_results.py
```

It refreshes every 5 seconds showing current orders (with seller), the number of
orders in each status, the history of one order, and all orders of that order's customer.

---

**Option C: Manual cqlsh queries**

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
| Parcel dashboard | http://localhost:3000 |
| API documentation | http://localhost:8000/docs |
| Spark Master UI | http://localhost:8080 |

---

## Stopping the project

```bash
docker compose down
```

To also delete all stored Cassandra data:

```bash
docker compose down -v
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `cassandra-init` exits with error | Cassandra wasn't ready yet. Run `docker compose restart cassandra-init` |
| Spark job can't connect to Kafka | Wait 30s for Kafka to fully start, then resubmit |
| `NoBrokersAvailable` in simulator logs | Normal on first start — it retries automatically |
| Port 9092 already in use | Another Kafka instance is running. Stop it or change the port in `docker-compose.yml` |
| Cassandra takes too long to start | Normal — Cassandra takes 60–90 seconds on first boot |
| `POST /api/orders/sample` returns 405 | The running API is outdated. Run `docker compose up -d --no-deps --build --force-recreate backend`. With Podman, use `podman-compose up -d --no-deps --build --force-recreate backend`. |
| Spark fails with `Columns not found in table ecommerce.orders: seller_id` | Apply the one-time schema migration described in Step 1, then resubmit Spark. |

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
