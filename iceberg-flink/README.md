# Iceberg Flink SQL Example

A minimal Apache Flink SQL example that uses an Apache Iceberg catalog and table.

## What this example shows

- Configuring an Iceberg catalog in Flink SQL
- Creating an Iceberg table with watermark metadata
- Inserting sample rows
- Running a simple aggregation query

The SQL script is in [`sql_pipeline.sql`](./sql_pipeline.sql).

## Prerequisites

- Java 17+
- A Flink distribution compatible with your selected Iceberg runtime JAR
- Iceberg Flink runtime JAR on the SQL client classpath

For example, for Flink 1.19 use a matching artifact such as:

- `org.apache.iceberg:iceberg-flink-runtime-1.19:<iceberg_version>`

## Run with Flink SQL Client

From your Flink installation directory:

```bash
./bin/sql-client.sh embedded -j /path/to/iceberg-flink-runtime.jar
```

Inside the SQL client:

```sql
SOURCE '/absolute/path/to/iceberg-examples/iceberg-flink/sql_pipeline.sql';
```

## Validate in Bazel

This repository includes a lightweight smoke test that verifies the SQL example shape:

```bash
bazel test //iceberg-flink:flink_sql_smoke_test
```
