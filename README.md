# Iceberg Examples

A collection of examples demonstrating the Apache Iceberg API across multiple languages: Java and Rust.

## Overview

Apache Iceberg is an open table format for huge analytic datasets. This project provides practical examples in both Java and Rust covering:

- Create and manage table catalogs
- Define and evolve table schemas
- Perform data operations
- Understand Iceberg's core concepts

## Languages

| Language | Directory | Description |
|----------|-----------|-------------|
| Java | (root) | Java API examples using `iceberg-api` and `iceberg-core` |
| Rust | [`iceberg-rust/`](iceberg-rust/) | Rust API examples using the [`iceberg`](https://crates.io/crates/iceberg) crate |

## Prerequisites

### Java Examples

- Java 11 or higher
- Gradle 8.5 or higher (included via wrapper)

### Rust Examples

See [`iceberg-rust/README.md`](iceberg-rust/README.md) for Rust prerequisites and setup instructions.

## Getting Started

### Java Examples

#### Build the Project

```bash
./gradlew build
```

#### Run Examples

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

#### Run Tests

```bash
./gradlew test
```

### Rust Examples

See [`iceberg-rust/README.md`](iceberg-rust/README.md) for build, run, and test instructions.

## Examples Included

### Java Examples

#### 1. Basic Iceberg Operations (`IcebergExamples.java`)
- Schema definition and creation
- Iceberg data type examples
- Schema field inspection
- Understanding schema structure and properties

#### 2. Data Operations (`DataOperationsExample.java`)
- Creating sample records
- Working with GenericRecord API
- Understanding data writing concepts
- Table information display

#### 3. Schema Evolution (`SchemaEvolutionExample.java`)
- Adding new columns
- Renaming existing columns
- Updating column types (safe operations)
- Deleting columns
- Schema compatibility rules

### Rust Examples

See [`iceberg-rust/README.md`](iceberg-rust/README.md) for a full description of each example.

## Project Structure

```
├── src/                              # Java source
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── example/
│   │               └── iceberg/
│   │                   ├── IcebergExamples.java           # Main examples class
│   │                   ├── DataOperationsExample.java     # Data operations
│   │                   └── SchemaEvolutionExample.java    # Schema evolution
│   └── test/
│       └── java/
│           └── com/
│               └── example/
│                   └── iceberg/
│                       └── IcebergExamplesTest.java        # Unit tests
└── iceberg-rust/                     # Rust examples (see iceberg-rust/README.md)
```

## Key Dependencies

### Java

- **Apache Iceberg Core**: Table format and core API functionality
- **Apache Iceberg API**: Public API interfaces and types
- **SLF4J**: Logging framework
- **JUnit 5**: Testing framework

### Rust

See [`iceberg-rust/README.md`](iceberg-rust/README.md).

Note: These examples focus on demonstrating Iceberg's schema and data type APIs. For production table operations, you would typically add catalog implementations (Hadoop, Hive, REST, etc.) and file format dependencies (Parquet, ORC, etc.).

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
