# Iceberg Examples

A collection of examples demonstrating the [Apache Iceberg](https://iceberg.apache.org/) API across
multiple languages, built and tested from a single Bazel workspace.

## Build and Test

Use Bazel at the repository root for every sub-project:

```bash
bazel build //...
bazel test //...
```

The default Java toolchain is JDK 17. JDK 21 is also supported through an explicit Bazel config:

```bash
bazel build --config=jdk21 //...
bazel test --config=jdk21 //...
```

Common runnable targets:

```bash
bazel run //iceberg-java:iceberg_examples
bazel run //iceberg-java:data_operations_example
bazel run //iceberg-java:schema_evolution_example
bazel run //iceberg-java:table_format_v3_example
bazel run //iceberg-java:beam_iceberg_example
bazel run //iceberg-rust:iceberg_rust_examples
```

The Spark examples are validated through Bazel as Python modules:

```bash
bazel test //iceberg-spark:spark_examples_smoke_test
```

Running the Spark pipelines themselves still requires a Spark 4.1 environment plus an Iceberg
runtime JAR. See [`iceberg-spark/README.md`](iceberg-spark/README.md) for the runtime-specific
steps.

## Sub-projects

| Directory | Language | Description |
|-----------|----------|-------------|
| [`iceberg-java/`](iceberg-java/) | Java | Examples using the Apache Iceberg Java API, including Apache Beam |
| [`iceberg-rust/`](iceberg-rust/) | Rust | Examples using the Apache Iceberg Rust API |
| [`iceberg-spark/`](iceberg-spark/) | Python / SQL | Spark 4.1 Declarative Pipelines with Apache Iceberg |

## Repository Layout

- `MODULE.bazel` defines the shared Maven, Rust crate, and PyPI dependencies.
- `iceberg-java/BUILD.bazel` exposes Java libraries, runnable examples, and tests.
- `iceberg-rust/BUILD.bazel` exposes the Rust example binary and unit tests.
- `iceberg-spark/BUILD.bazel` validates the Spark Python examples and exports the SQL pipeline.

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Java API Quickstart](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Apache Iceberg Rust Documentation](https://rust.iceberg.apache.org/)
- [Apache Beam IcebergIO Documentation](https://beam.apache.org/documentation/io/built-in/iceberg/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)
- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
- [Apache Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)

## License

This project is licensed under the same terms as the Apache Iceberg project.
