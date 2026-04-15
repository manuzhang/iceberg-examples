# Apache Iceberg C++ Example

A minimal C++ example for the `iceberg-examples` workspace, focused on a small in-memory workflow.

## What this example shows

- Defines an Iceberg-style table schema in native C++
- Creates an in-memory table and appends sample records
- Reads records back and prints rows alongside schema information

## Build

```bash
bazel build //iceberg-cpp:iceberg_cpp_example
```

## Run

```bash
bazel run //iceberg-cpp:iceberg_cpp_example
```

## Upstream reference

For a fuller end-to-end native example (catalog registration and scanning), see
[`apache/iceberg-cpp/example/demo_example.cc`](https://github.com/apache/iceberg-cpp/blob/main/example/demo_example.cc).

## Why this is minimal

This example intentionally stays in-memory and does not set up a catalog or file
I/O pipeline. For end-to-end integrations, follow the upstream C++ demo and
engine-specific examples from the Apache Iceberg project.
