# Spark + Iceberg Batch Pipeline: Scala Example

Example demonstrating [Apache Spark](https://spark.apache.org/) with
[Apache Iceberg](https://iceberg.apache.org/) tables, written in Scala.

## Overview

`BatchPipeline.scala` models a four-step e-commerce analytics pipeline that
mirrors `batch_pipeline.py` from the neighbouring `iceberg-spark/` project,
showing how the same ideas translate to the Spark Scala API.

**Pipeline graph:**

```
orders              (inline seed data → Iceberg table)
    └── validated_orders  (filter & enrich)
            ├── daily_sales     (daily totals per category)
            └── customer_ltv    (lifetime value per customer)
```

**Iceberg features demonstrated:**

- Format version 2 tables created via `CREATE OR REPLACE TABLE … USING iceberg`
- Day-level partitioning (`PARTITIONED BY (order_date)`)
- Snapshot history inspection via the `*.snapshots` metadata table
- Time-travel reads by snapshot ID

## Prerequisites

- **Java 17** or higher
- **Scala 2.12**
- **SBT 1.10** or higher — download from <https://www.scala-sbt.org/>

## Getting Started

### Build

```bash
cd iceberg-scala
sbt compile
```

### Run

```bash
sbt run
```

By default, Iceberg tables are written to `/tmp/iceberg-scala-example`.  Pass a
custom warehouse path as the first argument:

```bash
sbt "run /path/to/my/warehouse"
```

Expected output:

```
[1/4] orders:           12 rows
[2/4] validated_orders: 10 rows
[3a/4] daily_sales:      6 rows
[3b/4] customer_ltv:     6 rows

=== Daily Sales ===
...

=== Customer Lifetime Value ===
...

=== Iceberg Snapshot History (orders) ===
...

=== Time Travel: orders at snapshot <id> ===
...
```

## Project Structure

```
iceberg-scala/
├── build.sbt                                       # SBT build (Spark 3.5 + Iceberg 1.8.1)
├── project/
│   └── build.properties                            # SBT version pin
└── src/main/scala/io/github/manuzhang/iceberg/examples/
    └── BatchPipeline.scala                         # End-to-end batch ETL example
```

## Key Dependencies

| Artifact | Version | Purpose |
|----------|---------|---------|
| `spark-core` / `spark-sql` | 3.5.4 | Spark engine |
| `iceberg-spark-runtime-3.5_2.12` | 1.8.1 | Iceberg catalog and table format integration |
| `hadoop-common` | 3.4.3 | Filesystem support for the local HadoopCatalog |

## Learning Resources

- [Apache Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Iceberg Spark Writes](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Iceberg Table Properties Reference](https://iceberg.apache.org/docs/latest/configuration/)
- [Spark DataFrameWriterV2 API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameWriterV2.html)
