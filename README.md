# Iceberg Examples

A collection of examples demonstrating the [Apache Iceberg](https://iceberg.apache.org/) API across
multiple languages.

## Sub-projects

| Directory | Language | Description |
|-----------|----------|-------------|
| [`iceberg-java/`](iceberg-java/) | Java | Examples using the Apache Iceberg Java API |
| [`iceberg-rust/`](iceberg-rust/) | Rust | Examples using the Apache Iceberg Rust API |

## iceberg-java

Examples demonstrating the Apache Iceberg Java API for table format operations, schema evolution,
and data management. See [iceberg-java/README.md](iceberg-java/README.md) for details.

### Prerequisites

- Java 11 or higher
- Gradle 8.5 or higher (included via wrapper)

### Quick Start

```bash
cd iceberg-java
./gradlew build
./gradlew run
```

## iceberg-rust

Examples demonstrating the [Apache Iceberg Rust API](https://crates.io/crates/iceberg) for schema
definition, data types, schema evolution, and catalog management. See
[iceberg-rust/README.md](iceberg-rust/README.md) for details.

### Prerequisites

- Rust 1.92 or higher

### Quick Start

```bash
cd iceberg-rust
cargo build
cargo run
```

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API Quickstart](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Apache Iceberg Rust Documentation](https://rust.iceberg.apache.org/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## License

This project is licensed under the same terms as the Apache Iceberg project.
