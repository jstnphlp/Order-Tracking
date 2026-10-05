-- ============================================================
-- Keyspace
-- ============================================================
CREATE KEYSPACE IF NOT EXISTS ecommerce
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE ecommerce;

-- ============================================================
-- Current order state (latest status only)
-- ============================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id     TEXT,
    customer_id  TEXT,
    product_name TEXT,
    seller_id    TEXT,
    quantity     INT,
    total_amount DOUBLE,
    status       TEXT,
    updated_at   TIMESTAMP,
    PRIMARY KEY (order_id)
);

-- ============================================================
-- Orders looked up by customer ("show all orders of CUST-1234")
-- Same latest-status data as `orders`, but partitioned by customer.
-- ============================================================
CREATE TABLE IF NOT EXISTS orders_by_customer (
    customer_id  TEXT,
    order_id     TEXT,
    product_name TEXT,
    seller_id    TEXT,
    total_amount DOUBLE,
    status       TEXT,
    updated_at   TIMESTAMP,
    PRIMARY KEY ((customer_id), order_id)
);

-- ============================================================
-- Full order event history (all status transitions)
-- ============================================================
CREATE TABLE IF NOT EXISTS order_history (
    order_id    TEXT,
    event_time  TIMESTAMP,
    status      TEXT,
    note        TEXT,
    PRIMARY KEY (order_id, event_time)
) WITH CLUSTERING ORDER BY (event_time ASC);

-- ============================================================
-- Aggregated stats (updated by Spark)
-- Snapshot of how many orders are in each status RIGHT NOW,
-- e.g. count_shipped = 8. Rewritten every cycle.
-- ============================================================
CREATE TABLE IF NOT EXISTS order_stats (
    stat_key    TEXT,
    stat_value  BIGINT,
    updated_at  TIMESTAMP,
    PRIMARY KEY (stat_key)
);
