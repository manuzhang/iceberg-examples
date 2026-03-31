# Apache Iceberg Rust Examples

A collection of examples demonstrating the [Apache Iceberg Rust API](https://crates.io/crates/iceberg) for
table format operations, schema definition, data types, and catalog management.

## Overview

The [`iceberg`](https://crates.io/crates/iceberg) crate is the official native Rust implementation of
Apache Iceberg. These examples cover:

- Defining and inspecting table schemas
- Working with Iceberg's rich type system
- Understanding schema evolution concepts
- Creating namespaces and tables using an in-memory catalog

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

To exercise the cross-language read example, run the Java write example first:

```bash
# From the iceberg-java/ directory
./gradlew runCrossLanguageWrite

# Then, from iceberg-rust/
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

### 5. Cross-Language Read (`cross_language_read.rs`)

- Reads the Iceberg table written by the Java `CrossLanguageWriteExample`
- Uses `StaticTable::from_metadata_file` and `FileIO::new_with_fs()` for local-filesystem access
- Scans data with `TableScan::to_arrow()` and collects Arrow `RecordBatch`es
- Pretty-prints the table contents using `arrow_cast::pretty::pretty_format_batches`
- Demonstrates Iceberg's cross-language interoperability (write in Java, read in Rust)

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
    └── cross_language_read.rs   # Read Iceberg table written by the Java example
```

## Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| [`iceberg`](https://crates.io/crates/iceberg) | 0.9.0 | Apache Iceberg Rust implementation |
| [`tokio`](https://crates.io/crates/tokio) | 1 | Async runtime for catalog and scan operations |
| [`arrow-array`](https://crates.io/crates/arrow-array) | 57 | Arrow columnar data (RecordBatch) |
| [`arrow-cast`](https://crates.io/crates/arrow-cast) | 57 | Pretty-print Arrow record batches |
| [`futures`](https://crates.io/crates/futures) | 0.3 | Async stream utilities (`TryStreamExt`) |

## Learning Resources

- [Apache Iceberg Rust Documentation](https://rust.iceberg.apache.org/)
- [iceberg crate on crates.io](https://crates.io/crates/iceberg)
- [iceberg-rust GitHub repository](https://github.com/apache/iceberg-rust)
- [Apache Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## Notes

These examples use an **in-memory catalog** (`MemoryCatalogBuilder` + `MemoryStorageFactory`) which
requires no external infrastructure. For production use you would add:

- A persistent catalog: REST, Hive Metastore, AWS Glue, or SQL-backed
- A storage backend: S3, GCS, Azure ADLS, or local filesystem
  (via [`iceberg-storage-opendal`](https://crates.io/crates/iceberg-storage-opendal))
- A compute integration: Apache DataFusion
  (via [`iceberg-datafusion`](https://crates.io/crates/iceberg-datafusion))
