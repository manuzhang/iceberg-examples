# Apache Iceberg Rust Examples

A collection of examples demonstrating the [Apache Iceberg Rust API](https://crates.io/crates/iceberg)
for table format operations, schema definition, data types, and catalog management.

## Overview

The [`iceberg`](https://crates.io/crates/iceberg) crate is the official native Rust implementation
of Apache Iceberg. These examples cover:

- Defining and inspecting table schemas
- Working with Iceberg's rich type system
- Understanding schema evolution concepts
- Creating namespaces and tables using an in-memory catalog

## Prerequisites

- Rust 1.92 or higher (required by `iceberg` 0.9.0)
- Bazel 8.4.0 or higher, or Bazelisk

## Getting Started

### Build

```bash
bazel build //iceberg-rust:iceberg_rust_examples
```

### Run Examples

```bash
bazel run //iceberg-rust:iceberg_rust_examples
```

### Run Tests

```bash
bazel test //iceberg-rust:iceberg_rust_test
```

## Examples Included

### 1. Schema Operations (`schema_examples.rs`)

- Building schemas using `Schema::builder()`
- Defining required and optional fields with `NestedField`
- Inspecting fields by ID and by position
- Counting required vs optional fields

### 2. Data Types (`data_types.rs`)

- All 14 primitive types
- Nested types: `list`, `map`, and `struct`
- Deeply nested types such as `list<struct<...>>`

### 3. Schema Evolution (`schema_evolution.rs`)

- Adding optional columns while keeping existing data readable
- Safe type promotions
- Field ID stability across schema versions
- Schema evolution rules

### 4. Catalog Operations (`catalog_examples.rs`)

- Building an in-memory catalog backed by `MemoryStorageFactory`
- Creating and listing namespaces
- Creating tables with schemas, partition specs, and sort orders
- Listing tables in a namespace
- Checking table existence

## Project Structure

```text
iceberg-rust/
├── BUILD.bazel
└── src/
    ├── lib.rs                # Shared library used by tests and the runnable binary
    ├── main.rs               # Entry point, runs all examples
    ├── schema_examples.rs    # Schema creation and field inspection
    ├── data_types.rs         # Primitive and nested type demonstrations
    ├── schema_evolution.rs   # Schema evolution concepts
    └── catalog_examples.rs   # In-memory catalog and table operations
```

## Key Dependencies

These dependencies are resolved through Bazel's Rust crate integration:

| Crate | Version | Purpose |
|-------|---------|---------|
| [`iceberg`](https://crates.io/crates/iceberg) | 0.9.0 | Apache Iceberg Rust implementation |
| [`tokio`](https://crates.io/crates/tokio) | 1 | Async runtime for catalog operations |

## Learning Resources

- [Apache Iceberg Rust Documentation](https://rust.iceberg.apache.org/)
- [iceberg crate on crates.io](https://crates.io/crates/iceberg)
- [iceberg-rust GitHub repository](https://github.com/apache/iceberg-rust)
- [Apache Iceberg Table Format Specification](https://iceberg.apache.org/spec/)
