# Apache Iceberg Go Example

A small example using the official [`github.com/apache/iceberg-go`](https://github.com/apache/iceberg-go)
module to model Iceberg schema and partition metadata in Go, plus an optional REST catalog
workflow for writing and reading table rows.

## What this example shows

- Defines an Iceberg table schema with field IDs and identifier fields
- Builds a partition spec against the schema using `day` and `bucket` transforms
- Prints the derived Iceberg partition struct type
- Connects to an Iceberg REST catalog, creates or loads a table, appends Arrow rows, and scans
  rows back into an Arrow table

## Build

```bash
bazel build //iceberg-go:iceberg_go_example
```

## Run

```bash
bazel run //iceberg-go:iceberg_go_example
```

### REST Catalog Read/Write

The REST catalog workflow requires a running Iceberg REST catalog and a writable warehouse.
For a local REST catalog backed by the local filesystem, set:

```bash
export ICEBERG_REST_URI=http://localhost:8181
export ICEBERG_REST_WAREHOUSE=file:///tmp/iceberg-warehouse
export ICEBERG_REST_NAMESPACE=default
export ICEBERG_REST_TABLE=go_rest_orders

bazel run //iceberg-go:iceberg_go_example -- rest-catalog
```

Optional environment variables:

- `ICEBERG_REST_TOKEN`: bearer token for REST catalog authentication
- `ICEBERG_REST_CREDENTIAL`: OAuth credential for catalogs that exchange credentials for a token
- `ICEBERG_REST_PREFIX`: REST catalog prefix
- `ICEBERG_REST_TABLE_LOCATION`: explicit table location

## Test

```bash
bazel test //iceberg-go:iceberg_go_test
```

## Why the default example is metadata-focused

The default command stays self-contained so it can run in the shared Bazel workspace without
external services. The `rest-catalog` mode exercises the catalog, writer, and scanner APIs when a
compatible REST catalog is available.
