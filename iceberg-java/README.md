# Iceberg Examples

A collection of examples demonstrating the Apache Iceberg Java API for table format operations,
schema evolution, and data management.

## Overview

Apache Iceberg is an open table format for huge analytic datasets. These examples cover:

- Schema definition and inspection
- Data records and type handling
- Schema evolution concepts
- Table format v3 features
- Row-level upserts with v3 deletion vectors
- Third-party changelog planning for v3 deletion vectors
- Apache Beam integration with Iceberg tables
- Apache Beam CDC reads from Iceberg snapshots

## Prerequisites

- Java 17 or 21
- Bazel 8.4.0 or higher, or Bazelisk

## Getting Started

### Build

```bash
bazel build //iceberg-java:lib
```

Build with the JDK 21 Bazel toolchain:

```bash
bazel build --config=jdk21 //iceberg-java:lib
```

### Run Examples

```bash
bazel run //iceberg-java:iceberg_examples
bazel run //iceberg-java:data_operations_example
bazel run //iceberg-java:schema_evolution_example
bazel run //iceberg-java:table_format_v3_example
bazel run //iceberg-java:row_level_upsert_example
bazel run //iceberg-java:deletion_vector_changelog_example
bazel run //iceberg-java:beam_iceberg_example
bazel run //iceberg-java:beam_iceberg_cdc_example
```

The Beam examples use Beam Managed Iceberg IO with Iceberg's
`org.apache.iceberg.rest.RESTCatalog` implementation. By default it starts Iceberg's
`RESTCatalogServer` backed by SQLite metadata and a shared in-memory `FileIO`, so this command is
self-contained and does not require a local Hadoop warehouse:

```bash
bazel run //iceberg-java:beam_iceberg_example
bazel run //iceberg-java:beam_iceberg_cdc_example
```

To use an external REST catalog service, provide the catalog URI and warehouse location:

```bash
ICEBERG_REST_URI=http://localhost:8181 \
ICEBERG_WAREHOUSE=file:///tmp/iceberg-beam-warehouse \
bazel run //iceberg-java:beam_iceberg_example

ICEBERG_REST_URI=http://localhost:8181 \
ICEBERG_WAREHOUSE=file:///tmp/iceberg-beam-warehouse \
ICEBERG_TABLE=default.customers \
bazel run //iceberg-java:beam_iceberg_cdc_example
```

You can also pass `catalog_uri`, `warehouse`, and `table` as positional arguments:

```bash
bazel run //iceberg-java:beam_iceberg_example -- \
  http://localhost:8181 \
  file:///tmp/iceberg-beam-warehouse \
  default.users

bazel run //iceberg-java:beam_iceberg_cdc_example -- \
  http://localhost:8181 \
  file:///tmp/iceberg-beam-warehouse \
  default.customers
```

### Run Tests

```bash
bazel test //iceberg-java:all
```

Run the Java targets with JDK 21:

```bash
bazel test --config=jdk21 //iceberg-java:all
```

## Examples Included

### 1. Basic Iceberg Operations (`IcebergExamples.java`)

- Schema definition and creation
- Iceberg data type examples
- Schema field inspection
- Understanding schema structure and properties

### 2. Data Operations (`DataOperationsExample.java`)

- Creating sample records
- Working with `GenericRecord`
- Understanding data writing concepts
- Record structure inspection

### 3. Schema Evolution (`SchemaEvolutionExample.java`)

- Adding new columns
- Type promotion rules
- Safe and unsafe schema changes
- Field ID stability

### 4. Table Format V3 (`TableFormatV3Example.java`)

- Nanosecond timestamp types
- Variant type support
- Geospatial types
- Default column values

### 5. Row-Level Upsert (`RowLevelUpsertExample.java`)

- Creating an Iceberg v3 table through a REST catalog
- Starting an embedded local `RESTCatalogServer` for a self-contained run
- Backing the REST catalog with SQLite metadata and a shared in-memory `FileIO`
- Writing a Puffin-backed deletion vector with `PartitioningDVWriter`
- Committing a row-level upsert with `RowDelta`
- Verifying that readers only see the replacement row after the upsert

### 6. Deletion Vector Changelog (`DeletionVectorChangelogExample.java`)

- Demonstrating the boundary for a third-party changelog planner
- Reading snapshot file-change metadata through Iceberg's public APIs
- Emitting the full source row schema followed by Iceberg's CDC metadata columns:
  `_change_type`, `_change_ordinal`, and `_commit_snapshot_id`
- Producing raw changelog events and net changes that remove opposite
  `INSERT`/`DELETE` pairs for identical source rows
- Handling rows from added data files, Puffin deletion vectors, and removed data files
- Rejecting equality deletes and non-DV position deletes
- Verifying the planned changelog against a v3 DV-backed row-level upsert

### 7. Apache Beam + Iceberg (`BeamIcebergExample.java`)

- Defining a Beam schema that maps to an Iceberg table
- Writing rows with `Managed.write("iceberg")`
- Reading rows back with `Managed.read("iceberg")`
- Configuring Iceberg's `RESTCatalog`
- Running Iceberg's embedded `RESTCatalogServer` with SQLite metadata for the default example
- Running pipelines locally with the DirectRunner

### 8. Apache Beam + Iceberg CDC (`BeamIcebergCdcExample.java`)

- Writing ordinary customer rows to Iceberg across two snapshots
- Capturing Iceberg snapshot IDs after each Beam write
- Reading only rows added by the second snapshot with `IcebergIO.readRows(...).withCdc()`
- Running the CDC reader locally with the DirectRunner and embedded `RESTCatalogServer`

## Key Dependencies

- Apache Iceberg API / Core / Data
- Apache Beam SDK, Managed IO, and IcebergIO
- SLF4J
- JUnit 4

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API Quickstart](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Apache Beam IcebergIO Documentation](https://beam.apache.org/documentation/io/built-in/iceberg/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## License

This project is licensed under the same terms as the Apache Iceberg project.
