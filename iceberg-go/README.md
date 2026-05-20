# Apache Iceberg Go Example

A small example using the official [`github.com/apache/iceberg-go`](https://github.com/apache/iceberg-go)
module to model Iceberg schema and partition metadata in Go.

## What this example shows

- Defines an Iceberg table schema with field IDs and identifier fields
- Builds a partition spec against the schema using `day` and `bucket` transforms
- Prints the derived Iceberg partition struct type

## Build

```bash
bazel build //iceberg-go:iceberg_go_example
```

## Run

```bash
bazel run //iceberg-go:iceberg_go_example
```

## Test

```bash
bazel test //iceberg-go:iceberg_go_test
```

## Why this is metadata-focused

The Go client supports catalog, scan, and write workflows, but those examples require a REST
catalog, warehouse location, or object store. This repository example stays self-contained so it
can run in the shared Bazel workspace without external services.
