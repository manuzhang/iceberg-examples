# Iceberg Examples

A collection of examples demonstrating the [Apache Iceberg](https://iceberg.apache.org/) API in multiple languages.

## Overview

Apache Iceberg is an open table format for huge analytic datasets. This repository provides practical examples using:

- **[iceberg-java](./iceberg-java/README.md)** — Examples using the Iceberg Java API
- **[iceberg-rust](./iceberg-rust/README.md)** — Examples using the Iceberg Rust API

## iceberg-java

Examples demonstrating the Apache Iceberg Java API for table format operations, schema evolution, and data management.

### Prerequisites

- Java 11 or higher
- Gradle 8.5 or higher (included via wrapper)

### Getting Started

```bash
cd iceberg-java
./gradlew build
./gradlew run
./gradlew test
```

See [iceberg-java/README.md](./iceberg-java/README.md) for full details.

## iceberg-rust

Examples demonstrating the [Apache Iceberg Rust API](https://crates.io/crates/iceberg) for table format operations, schema definition, data types, and catalog management.

### Prerequisites

- Rust 1.92 or higher

### Getting Started

```bash
cd iceberg-rust
cargo build
cargo run
cargo test
```

See [iceberg-rust/README.md](./iceberg-rust/README.md) for full details.

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API Quickstart](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Iceberg Rust Documentation](https://rust.iceberg.apache.org/)
- [Apache Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## License

This project is licensed under the same terms as the Apache Iceberg project.
