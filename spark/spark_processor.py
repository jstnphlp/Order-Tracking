"""
PySpark Structured Streaming — Order Processor
-----------------------------------------------
Consumes order events from Kafka, processes them in real time,
and writes results to Apache Cassandra:

  • orders             → current state of each order (upsert)
  • orders_by_customer → same current state, searchable by customer
  • order_history      → full event log per order
  • order_stats        → how many orders are in each status right now

Run inside the Spark container (see submit.sh):
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    --conf spark.cassandra.connection.host=cassandra \
    /opt/spark-apps/spark_processor.py
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, desc, from_json, row_number, to_timestamp, trim, upper,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

# ── Schema of the JSON event coming from Kafka ────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",     StringType(),  True),
    StructField("customer_id",  StringType(),  True),
    StructField("product_name", StringType(),  True),
    StructField("seller_id",    StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("total_amount", DoubleType(),  True),
    StructField("status",       StringType(),  True),
    StructField("timestamp",    StringType(),  True),
    StructField("note",         StringType(),  True),
])

# Schema of the rows written to order_stats
STATS_SCHEMA = StructType([
    StructField("stat_key",   StringType(),    False),
    StructField("stat_value", LongType(),      False),
    StructField("updated_at", TimestampType(), False),
])

# Every status the system knows about. order_stats always gets a row for each
# of these (0 when no order is in that status), so no number ever goes stale.
KNOWN_STATUSES = [
    # normal delivery
    "CREATED", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED",
    # cancellation
    "CANCELLED",
    # return to seller
    "RETURN_REQUESTED", "RETURN_APPROVED", "RETURN_IN_TRANSIT", "RETURN_RECEIVED",
    "REFUNDED",
    "RETURN_REJECTED", "RETURNING_TO_BUYER", "RETURNED_TO_BUYER",
]

KAFKA_BROKER       = "kafka:29092"
KAFKA_TOPIC        = "order-events"
CASSANDRA_HOST     = "cassandra"
CASSANDRA_KEYSPACE = "ecommerce"
CASSANDRA_FORMAT   = "org.apache.spark.sql.cassandra"
CHECKPOINT_LOCATION = "/tmp/checkpoints/order-processor"


@contextmanager
def checkpoint_lock():
    """Allow only one processor to use this checkpoint in the Linux container."""
    import fcntl

    checkpoint = Path(CHECKPOINT_LOCATION)
    checkpoint.mkdir(parents=True, exist_ok=True)
    # Keep this file in place: closing the descriptor releases the lock, even
    # after a crash. Unlinking it could let another process lock a different inode.
    with (checkpoint / ".processor.lock").open("a") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(
                "[Spark] Another order processor is already using "
                f"{CHECKPOINT_LOCATION}. Stop the existing processor before "
                "submitting again."
            ) from None
        yield


# ── Pure transformation helpers (no Kafka/Cassandra needed, easy to test) ─────
def parse_events(raw_df):
    """Turn raw Kafka messages (binary) into typed event columns.

    A message that is not valid JSON produces a row where every column is NULL;
    those rows are removed later by `valid_events_filter`.
    """
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), ORDER_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )


def valid_events_filter():
    """A usable event needs every field that is part of a Cassandra primary key:
    order_id (orders, order_history), customer_id (orders_by_customer) and
    event_time (order_history). It also needs a status."""
    return (
        col("order_id").isNotNull() & (trim(col("order_id")) != "")
        & col("customer_id").isNotNull() & (trim(col("customer_id")) != "")
        & col("status").isNotNull() & (col("status") != "")
        & col("event_time").isNotNull()
    )


def latest_per_order(events_df):
    """Keep only the most recent event of each order."""
    w = Window.partitionBy("order_id").orderBy(desc("event_time"))
    return (
        events_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def build_stats_df(spark, orders_df):
    """Count how many orders are in each status right now.

    Every known status is included (0 if nobody is in it). A status we have
    never heard of still gets its own row rather than being dropped.
    """
    counts = {
        r["status"]: r["count"]
        for r in orders_df.groupBy("status").count().collect()
        if r["status"] is not None
    }
    unknown = sorted(s for s in counts if s not in KNOWN_STATUSES)
    now = datetime.now(timezone.utc)
    rows = [
        (f"count_{status.lower()}", int(counts.get(status, 0)), now)
        for status in KNOWN_STATUSES + unknown
    ]
    return spark.createDataFrame(rows, STATS_SCHEMA)


def write_cassandra(df, table):
    (
        df.write
        .format(CASSANDRA_FORMAT)
        .mode("append")
        .options(table=table, keyspace=CASSANDRA_KEYSPACE)
        .save()
    )


# ── Micro-batch handler ───────────────────────────────────────────────────────
def make_batch_handler(spark):
    def process_batch(batch_df, batch_id):
        """Runs once per micro-batch. Doing all writes in one place, in this
        order, means order_stats is always computed AFTER `orders` is updated."""
        batch_df = batch_df.persist()
        try:
            total = batch_df.count()
            if total == 0:
                return

            # Problem messages are skipped instead of crashing the whole stream.
            valid = batch_df.filter(valid_events_filter()).persist()
            try:
                valid_count = valid.count()
                skipped = total - valid_count
                if skipped:
                    print(f"[Spark] Batch {batch_id}: skipped {skipped} invalid "
                          f"message(s) (bad JSON or missing order_id/customer_id/"
                          f"status/timestamp).")
                if valid_count == 0:
                    return

                # 1) order_history: every event
                write_cassandra(
                    valid.select("order_id", "event_time", "status", "note"),
                    "order_history",
                )

                # 2) orders + orders_by_customer: latest status of each order
                latest = latest_per_order(valid).persist()
                try:
                    write_cassandra(
                        latest.select(
                            "order_id", "customer_id", "product_name", "seller_id",
                            "quantity", "total_amount", "status",
                            col("event_time").alias("updated_at"),
                        ),
                        "orders",
                    )
                    write_cassandra(
                        latest.select(
                            "customer_id", "order_id", "product_name", "seller_id",
                            "total_amount", "status",
                            col("event_time").alias("updated_at"),
                        ),
                        "orders_by_customer",
                    )
                finally:
                    latest.unpersist()

                # 3) order_stats: orders per status right now (read back from `orders`)
                current_orders = (
                    spark.read.format(CASSANDRA_FORMAT)
                    .options(table="orders", keyspace=CASSANDRA_KEYSPACE)
                    .load()
                    .select("status")
                )
                stats_df = build_stats_df(spark, current_orders)
                write_cassandra(stats_df, "order_stats")

                print(f"[Spark] Batch {batch_id}: {valid_count} event(s) processed, "
                      f"{skipped} skipped. Stats refreshed.")
            finally:
                valid.unpersist()
        finally:
            batch_df.unpersist()

    return process_batch


# ── Main ──────────────────────────────────────────────────────────────────────
def run_processor():
    spark = (
        SparkSession.builder
        .appName("OrderTrackingProcessor")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.shuffle.partitions", "2")   # keep low for local dev
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[Spark] Session started.")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        parse_events(raw_stream)
        .writeStream
        .foreachBatch(make_batch_handler(spark))
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("[Spark] Streaming query started. Waiting for events...")
    query.awaitTermination()


def main():
    with checkpoint_lock():
        run_processor()


if __name__ == "__main__":
    main()
