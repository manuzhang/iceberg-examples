# Apache Iceberg C++ Example

A minimal C++ example for the `iceberg-examples` workspace, aligned with the
Apache `iceberg-cpp` 0.3.0 demo while staying focused on a small in-memory
workflow.

## What this example shows

- Defines an Iceberg-style table schema in native C++
- Creates an in-memory table and appends sample records
- Reads records back and prints rows alongside schema information
- Tracks the upstream Apache `iceberg-cpp` 0.3.0 demo as the version reference

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
[`apache/iceberg-cpp/example/demo_example.cc`](https://github.com/apache/iceberg-cpp/blob/v0.3.0/example/demo_example.cc)
from the Apache `iceberg-cpp` 0.3.0 release.

## Why this is minimal

This example intentionally stays in-memory and does not set up a catalog or file
I/O pipeline. The upstream Apache `iceberg-cpp` 0.3.0 project is distributed as
a CMake/Meson project rather than a Bazel module, so this repository keeps a
lightweight Bazel-native example and links to the versioned upstream demo for
end-to-end integrations.
