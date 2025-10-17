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
```

### Run Tests

```bash
./gradlew test
```

## Examples Included

### 1. Basic Iceberg Operations (`IcebergExamples.java`)
- Setting up Hadoop catalog
- Creating tables with schemas
- Basic table metadata operations
- Schema inspection

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

## Project Structure

```
src/
├── main/
│   └── java/
│       └── com/
│           └── example/
│               └── iceberg/
│                   ├── IcebergExamples.java           # Main examples class
│                   ├── DataOperationsExample.java     # Data operations
│                   └── SchemaEvolutionExample.java    # Schema evolution
└── test/
    └── java/
        └── com/
            └── example/
                └── iceberg/
                    └── IcebergExamplesTest.java        # Unit tests
```

## Key Dependencies

- **Apache Iceberg Core**: Table format and API
- **Apache Iceberg Hadoop**: Hadoop catalog implementation  
- **Apache Iceberg Parquet**: Parquet file format support
- **Hadoop Client**: For filesystem operations
- **SLF4J**: Logging framework
- **JUnit 5**: Testing framework

## Warehouse Location

Examples use a local filesystem warehouse at `/tmp/iceberg-warehouse`. In production environments, you would typically use:
- HDFS
- Amazon S3
- Azure Data Lake Storage
- Google Cloud Storage

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API Quickstart](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## Notes

The data writing examples in this project are conceptual demonstrations of the Iceberg API structure. For production data writing, use:
- Iceberg's `AppendFiles` API for direct writes
- Compute engines like Apache Spark, Apache Flink, or Trino
- Proper transaction and commit handling

## License

This project is licensed under the same terms as the Apache Iceberg project.