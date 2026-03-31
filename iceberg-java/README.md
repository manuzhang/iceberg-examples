# Iceberg Examples

A collection of examples demonstrating the Apache Iceberg Java API for table format operations, schema evolution, and data management.

## Overview

Apache Iceberg is an open table format for huge analytic datasets. This project provides practical examples of using Iceberg's Java API to:

- Create and manage table catalogs
- Define and evolve table schemas
- Perform data operations
- Understand Iceberg's core concepts

## Prerequisites

- Java 11 or higher
- Gradle 8.5 or higher (included via wrapper)

## Getting Started

### Build the Project

```bash
./gradlew build
```

### Run Examples

Run the main examples class:
```bash
./gradlew run
```

Run specific example classes:
```bash
# Data operations example
./gradlew runDataOperations

# Schema evolution example
./gradlew runSchemaEvolution

# Table format v3 example
./gradlew runTableFormatV3

# Write an Iceberg table to the local filesystem (for cross-language reading)
./gradlew runCrossLanguageWrite
```

### Run Tests

```bash
./gradlew test
```

## Examples Included

### 1. Basic Iceberg Operations (`IcebergExamples.java`)
- Schema definition and creation
- Iceberg data type examples
- Schema field inspection
- Understanding schema structure and properties

### 2. Data Operations (`DataOperationsExample.java`)
- Creating sample records
- Working with GenericRecord API
- Understanding data writing concepts
- Table information display

### 3. Schema Evolution (`SchemaEvolutionExample.java`)
- Adding new columns
- Renaming existing columns
- Updating column types (safe operations)
- Deleting columns
- Schema compatibility rules

### 4. Table Format V3 (`TableFormatV3Example.java`)
- Nanosecond-precision timestamp types (`timestamptz_ns`, `timestamp_ns`)
- Variant type for semi-structured / schema-less data
- Geospatial types: Geometry and Geography with CRS support
- Default column values (initial default and write default)
- Overview of Iceberg format version 3 additions

### 5. Cross-Language Write (`CrossLanguageWriteExample.java`)
- Writes a `users` table to the local filesystem as a real Iceberg table with Parquet data files
- Uses `HadoopTables` (catalog-less) and `GenericParquetWriter` for local filesystem I/O
- Produces a table at `/tmp/iceberg-java-rust-example` that the Rust example can read
- Demonstrates Iceberg's cross-language interoperability (write in Java, read in Rust)

## Key Dependencies

- **Apache Iceberg Core**: Table format and core API functionality
- **Apache Iceberg API**: Public API interfaces and types
- **Apache Iceberg Parquet**: Parquet file format integration
- **Apache Parquet Column**: Parquet column I/O (needed at compile time for `GenericParquetWriter`)
- **Apache Hadoop Common**: Local filesystem support via `HadoopTables`
- **SLF4J**: Logging framework
- **JUnit 5**: Testing framework

Note: Most examples focus on demonstrating Iceberg's schema and data type APIs. The
`CrossLanguageWriteExample` goes further: it uses Hadoop-backed local filesystem I/O and Parquet to
write a real Iceberg table that can be read by any Iceberg implementation, including the Rust
example in `iceberg-rust/`.

## Next Steps

These examples demonstrate Iceberg's core schema and data APIs. For complete table operations, consider:

### Adding Catalog Support
- **Hadoop Catalog**: For HDFS or local filesystem
- **Hive Metastore**: For integration with existing Hive setups
- **REST Catalog**: For modern cloud-native deployments
- **JDBC Catalog**: For SQL-based metadata storage

### Adding File Format Support
- **Parquet**: Most common format for analytics workloads
- **ORC**: Alternative columnar format
- **Avro**: For schema evolution scenarios

### Storage Integration
- **HDFS**: For on-premises Hadoop clusters
- **Amazon S3**: For AWS environments
- **Azure Data Lake Storage**: For Azure environments
- **Google Cloud Storage**: For GCP environments

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API Quickstart](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## Important Notes

**Scope of Examples**: These examples focus on demonstrating Iceberg's schema and data type APIs without requiring external infrastructure. They are educational examples showing:
- How to define and work with schemas
- Iceberg's rich type system
- Schema evolution concepts and rules
- Record creation and manipulation

**For Production Use**: To build complete Iceberg applications, you'll need to add:
- Catalog implementations for metadata storage
- File format dependencies (Parquet, ORC, etc.)
- Storage system integration (HDFS, S3, etc.)
- Compute engine integration (Spark, Flink, Trino)

**Data Operations**: The data examples show record creation and structure but don't perform actual table I/O. For production data operations, use:
- Iceberg's `AppendFiles` API for direct writes
- Compute engines like Apache Spark, Apache Flink, or Trino
- Proper transaction and commit handling

## License

This project is licensed under the same terms as the Apache Iceberg project.
