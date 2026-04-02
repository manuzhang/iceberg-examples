# Spark 4.1 Declarative Pipelines with Apache Iceberg

Examples demonstrating [Spark 4.1 Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
with [Apache Iceberg](https://iceberg.apache.org/) tables.

## Overview

Spark 4.1 introduces **Declarative Pipelines** (SDP), a programming model where
you describe *what* data products should exist and let Spark handle *how* to
produce them.  The key building blocks are:

| Construct | Decorator / SQL | Purpose |
|-----------|-----------------|---------|
| **Materialized view** | `@dp.materialized_view` / `CREATE OR REFRESH MATERIALIZED VIEW` | Precomputed batch table, refreshed on demand |
| **Streaming table** | `@dp.table` / `CREATE STREAMING TABLE` | Append-only table continuously updated from a stream |
| **Flow** | `@dp.flow` / `CREATE FLOW` | Named data-movement step that writes into a streaming table |

Iceberg tables are used as the storage layer for all datasets, giving you ACID
transactions, schema evolution, and time-travel queries.

## Prerequisites

- **Python 3.11** or higher
- **Apache Spark 4.1** with pipeline support

Install the PySpark package (includes the `spark-pipelines` CLI)::

```bash
pip install "pyspark[pipelines]==4.1.*"
```

Build the Iceberg Spark 4.1 runtime JAR from source (no Maven Central release exists yet):

```bash
git clone https://github.com/apache/iceberg.git
cd iceberg
./gradlew :iceberg-spark:iceberg-spark-runtime-4.1_2.13:shadowJar
```

The built JAR will be at:

```
iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar
```

> **Note** — A released Iceberg runtime for Spark 4.1 will be published to Maven Central once
> the Iceberg project cuts a release with Spark 4.1 support.  Track progress at
> [apache/iceberg](https://github.com/apache/iceberg).

## Examples Included

### 1. Batch Pipeline (`batch_pipeline.py`)

Models an e-commerce analytics scenario.

**Pipeline graph:**

```
orders               (inline sample data as source)
  └── validated_orders   (filter & enrich)
          ├── daily_sales     (daily totals per category)
          └── customer_ltv    (lifetime value per customer)
```

**Concepts demonstrated:**

- `@dp.materialized_view` decorator with `comment`, `partition_cols`, and `table_properties`
- Using `spark.createDataFrame()` inside a materialized view as a self-contained seed source
- Building a multi-step pipeline where each view reads from the previous one
- Iceberg table properties: format version 2

**Run:**

```bash
spark-pipelines run \
    --pipeline-file batch_pipeline.py \
    --pipeline-conf spark.jars=/path/to/iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar \
    --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --pipeline-conf spark.sql.catalog.local.type=hadoop \
    --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \
    --pipeline-conf spark.sql.defaultCatalog=local
```

After a successful run the following Iceberg tables are created under
`/tmp/iceberg-spark-example/default/`:

| Table | Description |
|-------|-------------|
| `orders` | Raw seed data (12 sample rows) |
| `validated_orders` | Filtered orders (10 rows – nulls and negatives removed) |
| `daily_sales` | Revenue and order count per day and category |
| `customer_ltv` | Lifetime value and order frequency per customer |

---

### 2. Streaming Pipeline (`streaming_pipeline.py`)

Models an IoT sensor-monitoring scenario.

**Pipeline graph:**

```
temperature_readings  (@flow)  ─┐
                                ├─→  sensor_events  (@table)
humidity_readings     (@flow)  ─┘        └── sensor_hourly_stats  (@materialized_view)
```

**Concepts demonstrated:**

- `@dp.table` to define an append-only streaming table with Iceberg merge-on-read
- `@dp.flow` to define multiple independent streams that feed the same table
- `@dp.materialized_view` for batch aggregation over a streaming table's snapshot
- Spark's built-in `rate` source for a self-contained demo (no Kafka required)
- How to swap the `rate` source for Kafka in production

**Run:**

```bash
spark-pipelines run \
    --pipeline-file streaming_pipeline.py \
    --pipeline-conf spark.jars=/path/to/iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar \
    --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --pipeline-conf spark.sql.catalog.local.type=hadoop \
    --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \
    --pipeline-conf spark.sql.defaultCatalog=local \
    --pipeline-conf spark.sql.streaming.checkpointLocation=/tmp/iceberg-spark-checkpoints
```

After startup, two micro-batch streams write into the `sensor_events` Iceberg
table.  Stop the pipeline with **Ctrl-C**; the `sensor_hourly_stats`
materialized view is computed on the next refresh cycle.

---

### 3. SQL Pipeline (`sql_pipeline.sql`)

SQL equivalents of both pipelines above, using the declarative SQL syntax:

- `CREATE OR REFRESH MATERIALIZED VIEW … AS SELECT …`
- `CREATE STREAMING TABLE …`
- `CREATE FLOW … AS INSERT INTO … SELECT …`

**Run:**

```bash
spark-pipelines run \
    --pipeline-file sql_pipeline.sql \
    --pipeline-conf spark.jars=/path/to/iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar \
    --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --pipeline-conf spark.sql.catalog.local.type=hadoop \
    --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \
    --pipeline-conf spark.sql.defaultCatalog=local \
    --pipeline-conf spark.sql.streaming.checkpointLocation=/tmp/iceberg-spark-checkpoints
```

## Project Structure

```
iceberg-spark/
├── README.md                # This file
├── batch_pipeline.py        # Batch pipeline: @materialized_view (Python)
├── streaming_pipeline.py    # Streaming pipeline: @table + @flow (Python)
└── sql_pipeline.sql         # SQL equivalents for both pipelines
```

## Key Dependencies

| Package / JAR | Purpose |
|---------------|---------|
| `pyspark[pipelines]==4.1.*` | Spark engine + `spark-pipelines` CLI |
| `iceberg-spark-runtime-4.1_2.13` | Iceberg catalog and table format integration (build from [apache/iceberg](https://github.com/apache/iceberg) main) |

## Python vs SQL

Both styles produce identical Iceberg tables and support the same decorator
options.  Choose Python when you need:

- Complex DataFrame transformations (UDFs, ML feature engineering, etc.)
- Conditional logic or loops in the pipeline definition
- Reuse of helper functions across multiple datasets

Choose SQL when you want:

- Concise, readable pipeline definitions for standard ETL transforms
- Easier sharing with analysts who are not Python developers

## Learning Resources

- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
- [PySpark Pipelines API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pipelines.html)
- [Apache Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Iceberg Table Properties Reference](https://iceberg.apache.org/docs/latest/configuration/)
