"""
Spark 4.1 Declarative Pipeline: Streaming Example with Apache Iceberg
======================================================================

This file defines a streaming Spark Declarative Pipeline that models an IoT
sensor-monitoring scenario using Apache Iceberg tables.

Pipeline graph
--------------

  temperature_readings  (@flow → sensor_events streaming table)
  humidity_readings     (@flow → sensor_events streaming table)
      └── sensor_events        (@table – append-only Iceberg streaming table)
              └── sensor_hourly_stats  (@materialized_view – batch aggregation)

Two separate ``@dp.flow`` functions feed into the same ``sensor_events``
streaming table, demonstrating how multiple sources can be merged into a
single Iceberg table.  The ``sensor_hourly_stats`` materialized view is
refreshed as a batch job over the current snapshot of the streaming table.

The built-in ``rate`` source is used so the pipeline runs without any
external infrastructure (no Kafka cluster required).

How to run
----------

Install PySpark with pipeline support::

    pip install "pyspark[pipelines]==4.1.*"

Build the Iceberg Spark 4.1 runtime JAR from source (no Maven Central release
exists yet)::

    git clone https://github.com/apache/iceberg.git
    cd iceberg
    ./gradlew :iceberg-spark:iceberg-spark-runtime-4.1_2.12:shadowJar
    # JAR: iceberg-spark/iceberg-spark-runtime-4.1_2.12/build/libs/iceberg-spark-runtime-4.1_2.12-*-SNAPSHOT.jar

Run the pipeline::

    spark-pipelines run \\
        --pipeline-file streaming_pipeline.py \\
        --pipeline-conf spark.jars=/path/to/iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.12/build/libs/iceberg-spark-runtime-4.1_2.12-*-SNAPSHOT.jar \\
        --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \\
        --pipeline-conf spark.sql.catalog.local.type=hadoop \\
        --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \\
        --pipeline-conf spark.sql.defaultCatalog=local \\
        --pipeline-conf spark.sql.streaming.checkpointLocation=/tmp/iceberg-spark-checkpoints

In production, replace the ``rate`` source in the flow functions with a
Kafka ``readStream``::

    spark.readStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "broker:9092") \\
        .option("subscribe", "sensor-events") \\
        .load()

The pipeline runner injects a ``spark`` variable (SparkSession) automatically;
there is no need to create one manually in pipeline files.
"""

from pyspark import pipelines as dp  # noqa: E402  (imported at module level)
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Schema for events written into the sensor_events streaming table
# ---------------------------------------------------------------------------

SENSOR_EVENT_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# Step 1 – Streaming table target
# ---------------------------------------------------------------------------


@dp.table(
    comment=(
        "Append-only Iceberg table that collects raw sensor events from all "
        "measurement flows.  Partitioned by day for efficient time-range queries."
    ),
    partition_cols=["event_date"],
    table_properties={
        "format-version": "2",
        # Enable Iceberg merge-on-read for efficient small-file streaming writes
        "write.merge.mode": "merge-on-read",
        "write.update.mode": "merge-on-read",
        "write.delete.mode": "merge-on-read",
    },
)
def sensor_events() -> None:
    """Streaming table populated by the temperature and humidity flows below.

    Returning ``None`` (or omitting a ``return`` statement) signals that this
    table has no default flow.  All data arrives through the ``@dp.flow``
    functions declared below.
    """


# ---------------------------------------------------------------------------
# Step 2a – Flow: temperature readings
# ---------------------------------------------------------------------------


@dp.flow(
    target="sensor_events",
    comment="Simulated temperature readings from a rate source (5 events/s).",
)
def temperature_readings() -> DataFrame:
    """Produce synthetic temperature events using Spark's built-in rate source.

    Each micro-batch generates rows with a monotonically increasing ``value``
    counter.  The counter is mapped to a sensor ID (0-4) and a temperature in
    the range [20 °C, 39 °C].

    In production, replace the rate source with a Kafka readStream::

        return (
            spark  # noqa: F821
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:9092")
            .option("subscribe", "temperature-events")
            .load()
            .select(from_json(col("value").cast("string"), SENSOR_EVENT_SCHEMA).alias("d"))
            .select("d.*")
        )
    """
    return (
        spark.readStream  # noqa: F821
        .format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .select(
            (F.col("value") % 5).cast("string").alias("sensor_id"),
            F.lit("temperature").alias("event_type"),
            (20.0 + (F.col("value") % 20).cast(DoubleType())).alias("value"),
            F.col("timestamp").alias("event_time"),
        )
        .withColumn("event_date", F.to_date("event_time"))
    )


# ---------------------------------------------------------------------------
# Step 2b – Flow: humidity readings
# ---------------------------------------------------------------------------


@dp.flow(
    target="sensor_events",
    comment="Simulated humidity readings from a rate source (3 events/s).",
)
def humidity_readings() -> DataFrame:
    """Produce synthetic humidity events using Spark's built-in rate source.

    Humidity values are in the range [50 %, 89 %].

    In production, replace the rate source with a Kafka readStream (see the
    ``temperature_readings`` flow above for the pattern).
    """
    return (
        spark.readStream  # noqa: F821
        .format("rate")
        .option("rowsPerSecond", 3)
        .load()
        .select(
            (F.col("value") % 5).cast("string").alias("sensor_id"),
            F.lit("humidity").alias("event_type"),
            (50.0 + (F.col("value") % 40).cast(DoubleType())).alias("value"),
            F.col("timestamp").alias("event_time"),
        )
        .withColumn("event_date", F.to_date("event_time"))
    )


# ---------------------------------------------------------------------------
# Step 3 – Materialized view: hourly statistics
# ---------------------------------------------------------------------------


@dp.materialized_view(
    comment=(
        "Hourly average and sample count per sensor and event type, computed "
        "as a batch refresh over the current snapshot of sensor_events."
    ),
    table_properties={"format-version": "2"},
)
def sensor_hourly_stats() -> DataFrame:
    """Batch aggregation over the current snapshot of the streaming table.

    The pipeline runner will periodically refresh this view by running a
    batch Spark job that reads the latest Iceberg snapshot of
    ``sensor_events`` and recomputes the aggregation.
    """
    return (
        spark.read.table("sensor_events")  # noqa: F821
        .withColumn("hour", F.date_trunc("hour", F.col("event_time")))
        .groupBy("sensor_id", "event_type", "hour")
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("sample_count"),
        )
        .orderBy("sensor_id", "event_type", "hour")
    )
