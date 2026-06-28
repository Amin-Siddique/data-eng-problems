"""Load sample datasets for Lakehouse Local."""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

SPARK_HOST = os.environ.get("SPARK_HOST", "spark")
SPARK_PORT = os.environ.get("SPARK_PORT", "15002")


def get_spark():
    """Create Spark session with Delta Lake."""
    return (
        SparkSession.builder
        .appName("LakehouseLocal-DataLoader")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/opt/spark/warehouse")
        .getOrCreate()
    )


def create_tpch_sample(spark):
    """Create sample TPC-H-like data."""
    print("Creating TPC-H sample data...")

    spark.sql("CREATE CATALOG IF NOT EXISTS samples")
    spark.sql("CREATE SCHEMA IF NOT EXISTS samples.tpch")

    # Lineitem table (simplified)
    lineitem_data = []
    for i in range(100000):
        lineitem_data.append((
            i,  # orderkey
            i % 10000,  # partkey
            i % 1000,  # suppkey
            (i % 7) + 1,  # linenumber
            float((i % 100) + 1),  # quantity
            float((i % 10000) + 100),  # extendedprice
            float(i % 10) / 100,  # discount
            float(i % 8) / 100,  # tax
            ['A', 'R', 'F'][i % 3],  # returnflag
            ['O', 'F'][i % 2],  # linestatus
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",  # shipdate
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",  # commitdate
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",  # receiptdate
            ['DELIVER IN PERSON', 'COLLECT COD', 'TAKE BACK RETURN'][i % 3],  # shipinstruct
            ['AIR', 'TRUCK', 'SHIP', 'RAIL', 'MAIL'][i % 5],  # shipmode
        ))

    lineitem_schema = StructType([
        StructField("l_orderkey", LongType()),
        StructField("l_partkey", LongType()),
        StructField("l_suppkey", LongType()),
        StructField("l_linenumber", IntegerType()),
        StructField("l_quantity", DoubleType()),
        StructField("l_extendedprice", DoubleType()),
        StructField("l_discount", DoubleType()),
        StructField("l_tax", DoubleType()),
        StructField("l_returnflag", StringType()),
        StructField("l_linestatus", StringType()),
        StructField("l_shipdate", StringType()),
        StructField("l_commitdate", StringType()),
        StructField("l_receiptdate", StringType()),
        StructField("l_shipinstruct", StringType()),
        StructField("l_shipmode", StringType()),
    ])

    lineitem_df = spark.createDataFrame(lineitem_data, lineitem_schema)
    lineitem_df.write.format("delta").mode("overwrite").saveAsTable("samples.tpch.lineitem")
    print(f"  Created samples.tpch.lineitem with {lineitem_df.count()} rows")


def create_interview_data(spark):
    """Create data for interview problems."""
    print("Creating interview problem data...")

    spark.sql("CREATE CATALOG IF NOT EXISTS interview")

    # Problem 001: Skewed Join
    spark.sql("CREATE SCHEMA IF NOT EXISTS interview.skew")

    # Create skewed orders (top 1% of customers have 50% of orders)
    orders_data = []
    for i in range(100000):
        # Skew: customer_ids 1-10 have way more orders
        if i < 50000:
            customer_id = (i % 10) + 1  # Heavy hitters
        else:
            customer_id = (i % 990) + 11  # Long tail

        orders_data.append((
            f"order_{i}",
            customer_id,
            float((i % 1000) + 10),
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
        ))

    orders_schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", LongType()),
        StructField("amount", DoubleType()),
        StructField("order_date", StringType()),
    ])

    orders_df = spark.createDataFrame(orders_data, orders_schema)
    orders_df.write.format("delta").mode("overwrite").saveAsTable("interview.skew.orders")

    # Customers table
    customers_data = [(i, f"Customer {i}", ["Bronze", "Silver", "Gold"][i % 3]) for i in range(1, 1001)]
    customers_schema = StructType([
        StructField("customer_id", LongType()),
        StructField("customer_name", StringType()),
        StructField("segment", StringType()),
    ])

    customers_df = spark.createDataFrame(customers_data, customers_schema)
    customers_df.write.format("delta").mode("overwrite").saveAsTable("interview.skew.customers")

    print(f"  Created interview.skew.orders with {orders_df.count()} rows (skewed)")
    print(f"  Created interview.skew.customers with {customers_df.count()} rows")

    # Problem 002: Incremental Load
    spark.sql("CREATE SCHEMA IF NOT EXISTS interview.incremental")

    events_data = []
    from datetime import datetime, timedelta
    import random

    base_date = datetime(2024, 1, 1)
    for i in range(50000):
        event_time = base_date + timedelta(hours=i % (24 * 30))
        # Some late-arriving data
        ingest_lag = random.randint(0, 48) if random.random() < 0.1 else 0
        ingest_time = event_time + timedelta(hours=ingest_lag)

        events_data.append((
            f"evt_{i}",
            f"user_{i % 1000}",
            ["click", "view", "purchase", "signup"][i % 4],
            event_time.isoformat(),
            ingest_time.isoformat(),
        ))

    events_schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("_ingested_at", StringType()),
    ])

    events_df = spark.createDataFrame(events_data, events_schema)
    events_df.write.format("delta").mode("overwrite").saveAsTable("interview.incremental.events")
    print(f"  Created interview.incremental.events with {events_df.count()} rows")

    # Problem 003: SCD Type 2
    spark.sql("CREATE SCHEMA IF NOT EXISTS interview.scd")

    # Source customers (current state)
    source_data = [
        ("C001", "Alice Smith", "alice@email.com", "123 Main St", "Gold", "2024-01-15T10:00:00"),
        ("C002", "Bob Jones", "bob@email.com", "456 Oak Ave", "Silver", "2024-01-15T10:00:00"),
        ("C003", "Carol White", "carol@email.com", "789 Pine Rd", "Bronze", "2024-01-15T10:00:00"),
    ]

    source_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("address", StringType()),
        StructField("segment", StringType()),
        StructField("updated_at", StringType()),
    ])

    source_df = spark.createDataFrame(source_data, source_schema)
    source_df.write.format("delta").mode("overwrite").saveAsTable("interview.scd.customers_source")

    # Dimension table (with history)
    dim_data = [
        (1, "C001", "Alice Smith", "alice@email.com", "123 Main St", "Silver", "2023-01-01T00:00:00", "2024-01-14T23:59:59", False, "2024-01-15T00:00:00"),
        (2, "C001", "Alice Smith", "alice@email.com", "123 Main St", "Gold", "2024-01-15T00:00:00", None, True, "2024-01-15T00:00:00"),
        (3, "C002", "Bob Jones", "bob@email.com", "456 Oak Ave", "Silver", "2023-06-01T00:00:00", None, True, "2024-01-15T00:00:00"),
    ]

    dim_schema = StructType([
        StructField("customer_sk", LongType()),
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("address", StringType()),
        StructField("segment", StringType()),
        StructField("effective_from", StringType()),
        StructField("effective_to", StringType()),
        StructField("is_current", BooleanType()),
        StructField("_loaded_at", StringType()),
    ])

    dim_df = spark.createDataFrame(dim_data, dim_schema)
    dim_df.write.format("delta").mode("overwrite").saveAsTable("interview.scd.customers_dim")

    print(f"  Created interview.scd.customers_source with {source_df.count()} rows")
    print(f"  Created interview.scd.customers_dim with {dim_df.count()} rows")


def main():
    print("=" * 50)
    print("Lakehouse Local - Loading Sample Data")
    print("=" * 50)

    spark = get_spark()

    create_tpch_sample(spark)
    create_interview_data(spark)

    print("=" * 50)
    print("Data loading complete!")
    print("=" * 50)

    # List all tables
    print("\nAvailable tables:")
    for catalog in ["samples", "interview"]:
        try:
            schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
            for schema in schemas:
                tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema[0]}").collect()
                for table in tables:
                    print(f"  - {catalog}.{schema[0]}.{table.tableName}")
        except Exception as e:
            print(f"  Error listing {catalog}: {e}")


if __name__ == "__main__":
    main()
