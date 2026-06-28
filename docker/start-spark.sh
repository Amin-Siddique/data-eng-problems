#!/bin/bash
set -e

echo "Starting Spark with Delta Lake..."

# Start Spark Connect server (for SQL editor and remote connections)
/opt/spark/sbin/start-connect-server.sh \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Start Thrift server (for dbt/JDBC connections)
/opt/spark/sbin/start-thriftserver.sh \
    --hiveconf hive.server2.thrift.port=10000

echo "Spark is ready!"
echo "  - Spark UI: http://localhost:4040"
echo "  - Spark Connect: localhost:15002"
echo "  - Thrift Server: localhost:10000"

# Keep container running
tail -f /opt/spark/logs/*
