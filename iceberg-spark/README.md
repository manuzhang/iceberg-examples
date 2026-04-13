# Spark 4.1 Declarative Pipelines with Apache Iceberg

Examples demonstrating [Spark 4.1 Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
with [Apache Iceberg](https://iceberg.apache.org/) tables.

## Overview

Spark 4.1 introduces Declarative Pipelines, a programming model where you describe what data
products should exist and let Spark handle how to produce them. These examples cover:

- Batch materialized views
- Streaming tables and flows
- SQL and Python pipeline definitions
- Iceberg-backed storage for all datasets

## Prerequisites

- Python 3.11
- Apache Spark 4.1 with pipeline support
- Bazel 8.4.0 or higher, or Bazelisk, for repository build/test tasks

## Validate with Bazel

From the repository root:

```bash
bazel test //iceberg-spark:spark_examples_smoke_test
```

This Bazel target resolves the checked-in Python dependency graph and verifies that the pipeline
modules import successfully.

## Running the Pipelines

To execute the pipelines themselves you still need a local Spark environment that provides the
`spark-pipelines` CLI:

```bash
pip install "pyspark[pipelines]==4.1.*"
```

Build the Iceberg Spark 4.1 runtime JAR from source:

```bash
git clone https://github.com/apache/iceberg.git
cd iceberg
./gradlew :iceberg-spark:iceberg-spark-runtime-4.1_2.13:shadowJar
```

The built JAR will be at:

```text
iceberg/iceberg-spark/iceberg-spark-runtime-4.1_2.13/build/libs/iceberg-spark-runtime-4.1_2.13-*-SNAPSHOT.jar
```

### Batch Pipeline

```bash
spark-pipelines run \
    --pipeline-file batch_pipeline.py \
    --pipeline-conf spark.jars=/path/to/iceberg-spark-runtime-4.1_2.13.jar \
    --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --pipeline-conf spark.sql.catalog.local.type=hadoop \
    --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \
    --pipeline-conf spark.sql.defaultCatalog=local
```

### Streaming Pipeline

```bash
spark-pipelines run \
    --pipeline-file streaming_pipeline.py \
    --pipeline-conf spark.jars=/path/to/iceberg-spark-runtime-4.1_2.13.jar \
    --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --pipeline-conf spark.sql.catalog.local.type=hadoop \
    --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \
    --pipeline-conf spark.sql.defaultCatalog=local \
    --pipeline-conf spark.sql.streaming.checkpointLocation=/tmp/iceberg-spark-checkpoints
```

### SQL Pipeline

```bash
spark-pipelines run \
    --pipeline-file sql_pipeline.sql \
    --pipeline-conf spark.jars=/path/to/iceberg-spark-runtime-4.1_2.13.jar \
    --pipeline-conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --pipeline-conf spark.sql.catalog.local.type=hadoop \
    --pipeline-conf spark.sql.catalog.local.warehouse=/tmp/iceberg-spark-example \
    --pipeline-conf spark.sql.defaultCatalog=local \
    --pipeline-conf spark.sql.streaming.checkpointLocation=/tmp/iceberg-spark-checkpoints
```

## Project Structure

```text
iceberg-spark/
├── BUILD.bazel              # Bazel targets for validating the Python examples
├── README.md
├── smoke_test.py            # Import smoke test used by Bazel CI
├── batch_pipeline.py
├── streaming_pipeline.py
└── sql_pipeline.sql
```

## Key Dependencies

| Package / JAR | Purpose |
|---------------|---------|
| `pyspark[pipelines]==4.1.*` | Spark engine plus `spark-pipelines` |
| `iceberg-spark-runtime-4.1_2.13` | Iceberg catalog and table format integration |

## Learning Resources

- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
- [PySpark Pipelines API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pipelines.html)
- [Apache Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Iceberg Table Properties Reference](https://iceberg.apache.org/docs/latest/configuration/)
