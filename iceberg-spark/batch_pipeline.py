"""
Spark 4.1 Declarative Pipeline: Batch Example with Apache Iceberg
==================================================================

This file defines a batch Spark Declarative Pipeline that models a simple
e-commerce analytics scenario using Apache Iceberg tables.

Pipeline graph
--------------

  orders            (@materialized_view – inline sample data as source)
      └── validated_orders  (@materialized_view – filter & enrich)
              ├── daily_sales   (@materialized_view – daily totals per category)
              └── customer_ltv  (@materialized_view – lifetime value per customer)

All materialized views are written to Iceberg tables in the ``local`` catalog
(a local HadoopCatalog backed by the filesystem – no external service needed).

How to run
----------

Install PySpark with pipeline support::

    pip install "pyspark[pipelines]==4.1.*"

To validate the checked-in module layout with Bazel from the repository root::

    bazel test //iceberg-spark:spark_examples_smoke_test

Build the Iceberg Spark 4.1 runtime JAR from source (no Maven Central release
exists yet)::

    git clone https://github.com/apache/iceberg.git
    cd iceberg
    ./gradlew :iceberg-spark:iceberg-spark-runtime-4.1_2.13:shadowJar
    # JAR: iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar

Run the pipeline::

    spark-pipelines run \\
        --pipeline-file batch_pipeline.py \\
        --pipeline-conf spark.jars=/path/to/iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar \\
        --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \\
        --pipeline-conf spark.sql.catalog.local.type=hadoop \\
        --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \\
        --pipeline-conf spark.sql.defaultCatalog=local

The pipeline runner injects a ``spark`` variable (SparkSession) automatically;
there is no need to create one manually in pipeline files.
"""

from pyspark import pipelines as dp  # noqa: E402  (imported at module level)
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Schema definition shared across pipeline datasets
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), nullable=False),
        StructField("customer_id", LongType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Step 1 – Source: sample orders table
# ---------------------------------------------------------------------------


@dp.materialized_view(
    comment="Sample e-commerce orders (inline seed data for the pipeline demo).",
    table_properties={"format-version": "2"},
)
def orders() -> DataFrame:
    """Seed the pipeline with sample order rows.

    In production this function would read from an external Iceberg table::

        return spark.read.table("raw_catalog.sales.orders")

    For this self-contained demo, inline data is created with
    ``spark.createDataFrame``.  The pipeline runner injects ``spark``
    automatically.
    """
    from datetime import datetime  # local import avoids module-level side-effects

    data = [
        (1, 101, "electronics", 299.99, datetime(2024, 1, 15, 10, 0, 0)),
        (2, 102, "clothing", 49.99, datetime(2024, 1, 15, 11, 30, 0)),
        (3, 101, "electronics", 89.99, datetime(2024, 1, 15, 14, 0, 0)),
        (4, 103, "books", 19.99, datetime(2024, 1, 16, 9, 0, 0)),
        (5, 102, "clothing", 79.99, datetime(2024, 1, 16, 10, 15, 0)),
        (6, 104, "electronics", 499.99, datetime(2024, 1, 16, 12, 0, 0)),
        (7, 103, "books", 14.99, datetime(2024, 1, 17, 8, 0, 0)),
        (8, 101, "home", 129.99, datetime(2024, 1, 17, 15, 0, 0)),
        (9, 105, "electronics", 349.99, datetime(2024, 1, 17, 16, 30, 0)),
        (10, 104, "clothing", 59.99, datetime(2024, 1, 17, 17, 0, 0)),
        # Row with a null customer_id – should be filtered out downstream
        (11, None, "books", 9.99, datetime(2024, 1, 17, 18, 0, 0)),
        # Row with a negative amount – should be filtered out downstream
        (12, 106, "home", -5.00, datetime(2024, 1, 17, 19, 0, 0)),
    ]
    return spark.createDataFrame(data, ORDERS_SCHEMA)  # noqa: F821


# ---------------------------------------------------------------------------
# Step 2 – Cleaning: validated_orders
# ---------------------------------------------------------------------------


@dp.materialized_view(
    comment=(
        "Cleaned orders: rows with null customer_id or non-positive amounts are "
        "removed; an order_date column derived from event_time is added."
    ),
    partition_cols=["order_date"],
    table_properties={"format-version": "2"},
)
def validated_orders() -> DataFrame:
    """Filter and enrich the raw orders.

    Reads the ``orders`` dataset defined above.  The pipeline runner resolves
    short dataset names (without a catalog prefix) to datasets produced in the
    same pipeline run.
    """
    return (
        spark.read.table("orders")  # noqa: F821
        .filter(F.col("amount") > 0)
        .filter(F.col("customer_id").isNotNull())
        .withColumn("order_date", F.to_date(F.col("event_time")))
        .select("order_id", "customer_id", "category", "amount", "order_date")
    )


# ---------------------------------------------------------------------------
# Step 3a – Aggregation: daily_sales
# ---------------------------------------------------------------------------


@dp.materialized_view(
    comment="Total revenue and order count per day and product category.",
    partition_cols=["order_date"],
    table_properties={"format-version": "2"},
)
def daily_sales() -> DataFrame:
    """Aggregate validated orders into a daily sales summary by category."""
    return (
        spark.read.table("validated_orders")  # noqa: F821
        .groupBy("order_date", "category")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("order_count"),
            F.avg("amount").alias("avg_order_value"),
        )
        .orderBy("order_date", "category")
    )


# ---------------------------------------------------------------------------
# Step 3b – Aggregation: customer_ltv
# ---------------------------------------------------------------------------


@dp.materialized_view(
    comment="Lifetime value and order frequency per customer.",
    table_properties={"format-version": "2"},
)
def customer_ltv() -> DataFrame:
    """Compute lifetime purchase value and order frequency per customer."""
    return (
        spark.read.table("validated_orders")  # noqa: F821
        .groupBy("customer_id")
        .agg(
            F.sum("amount").alias("lifetime_value"),
            F.count("*").alias("total_orders"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .orderBy(F.col("lifetime_value").desc())
    )
