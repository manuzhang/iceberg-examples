# Apache Iceberg Rust Examples

A collection of examples demonstrating the [Apache Iceberg Rust API](https://crates.io/crates/iceberg) for
table format operations, schema definition, data types, catalog management, and DataFusion integration.

## Overview

The [`iceberg`](https://crates.io/crates/iceberg) crate is the official native Rust implementation of
Apache Iceberg. These examples cover:

- Defining and inspecting table schemas
- Working with Iceberg's rich type system
- Understanding schema evolution concepts
- Creating namespaces and tables using an in-memory catalog
- Querying Iceberg tables with Apache DataFusion (the engine that powers [Apache DataFusion Comet](https://datafusion.apache.org/comet/))

## Prerequisites

- Rust 1.92 or higher (required by `iceberg` 0.9.0)

Install Rust via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Getting Started

### Build

```bash
cd iceberg-rust
cargo build
```

### Run Examples

```bash
cargo run
```

### Run Tests

```bash
cargo test
```

## Examples Included

### 1. Schema Operations (`schema_examples.rs`)

- Building schemas using `Schema::builder()`
- Defining required and optional fields with `NestedField`
- Inspecting fields by ID and by position
- Counting required vs optional fields

### 2. Data Types (`data_types.rs`)

- All 14 primitive types: `boolean`, `int`, `long`, `float`, `double`,
  `decimal(P,S)`, `date`, `time`, `timestamp`, `timestamptz`, `string`,
  `uuid`, `fixed(N)`, `binary`
- Nested types: `list`, `map`, and `struct`
- Deeply nested types (e.g. `list<struct<…>>`)

### 3. Schema Evolution (`schema_evolution.rs`)

- Adding optional columns while keeping existing data readable
- Safe type promotions (`int → long`, `float → double`)
- Field ID stability across schema versions
- Schema evolution rules: what is and isn't allowed

### 4. Catalog Operations (`catalog_examples.rs`)

- Building an in-memory catalog backed by `MemoryStorageFactory`
- Creating and listing namespaces
- Creating tables with schemas, partition specs, and sort orders
- Listing tables in a namespace
- Checking table existence

### 5. DataFusion Integration (`datafusion_examples.rs`)

Demonstrates how to combine the `iceberg` and `iceberg-datafusion` crates to run
DataFusion SQL queries against Iceberg tables — the same query engine used by
[Apache DataFusion Comet](https://datafusion.apache.org/comet/).

- Creating an Iceberg catalog backed by the **local filesystem**
  (`LocalFsStorageFactory`) so data is persisted as real Parquet files
- Creating a partitioned `orders` table (partitioned by `category` using
  the identity transform)
- Inserting rows with DataFusion `INSERT INTO` SQL
- Running a plain `SELECT` to read all rows back
- Applying a `WHERE` predicate to demonstrate filter/predicate pushdown
- Running an aggregation query (`COUNT`, `SUM`, `GROUP BY`)

## Project Structure

```
iceberg-rust/
├── Cargo.toml
└── src/
    ├── main.rs                  # Entry point, runs all examples
    ├── schema_examples.rs       # Schema creation and field inspection
    ├── data_types.rs            # Primitive and nested type demonstrations
    ├── schema_evolution.rs      # Schema evolution concepts
    ├── catalog_examples.rs      # In-memory catalog and table operations
    └── datafusion_examples.rs   # DataFusion (Comet) SQL integration
```

## Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| [`iceberg`](https://crates.io/crates/iceberg) | 0.9.0 | Apache Iceberg Rust implementation |
| [`iceberg-datafusion`](https://crates.io/crates/iceberg-datafusion) | 0.9.0 | DataFusion table/catalog providers for Iceberg |
| [`datafusion`](https://crates.io/crates/datafusion) | 52 | Apache Arrow DataFusion query engine |
| [`tokio`](https://crates.io/crates/tokio) | 1 | Async runtime for catalog and DataFusion operations |
| [`tempfile`](https://crates.io/crates/tempfile) | 3 | Temporary directories for local-filesystem examples |

## Learning Resources

- [Apache Iceberg Rust Documentation](https://rust.iceberg.apache.org/)
- [iceberg crate on crates.io](https://crates.io/crates/iceberg)
- [iceberg-rust GitHub repository](https://github.com/apache/iceberg-rust)
- [Apache DataFusion Comet](https://datafusion.apache.org/comet/)
- [Apache Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## Notes

The catalog and DataFusion examples use a **local-filesystem catalog**
(`MemoryCatalogBuilder` + `LocalFsStorageFactory`) which requires no external
infrastructure. For production use you would add:

- A persistent catalog: REST, Hive Metastore, AWS Glue, or SQL-backed
- A cloud storage backend: S3, GCS, Azure ADLS
  (via [`iceberg-storage-opendal`](https://crates.io/crates/iceberg-storage-opendal))
- Comet acceleration inside a Spark cluster using the
  [DataFusion Comet plugin](https://datafusion.apache.org/comet/)
