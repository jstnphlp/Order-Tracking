#!/bin/bash

export HOME=/tmp
export SPARK_SUBMIT_OPTS="-Duser.home=/tmp"

mkdir -p /tmp/.ivy2/cache
mkdir -p /tmp/.ivy2/jars

/opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.sql.shuffle.partitions=2 \
  /opt/spark-apps/spark_processor.py
