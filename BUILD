# Root BUILD file for the iceberg-examples Bazel workspace.
#
# Running `bazel build //...` builds all three sub-projects:
#   //iceberg-java/...  — Apache Beam + Iceberg (Java)
#   //iceberg-rust/...  — Iceberg Rust crate examples
#   //iceberg-spark/... — PySpark Declarative Pipelines

# Convenience filegroup that aggregates build artifacts from all sub-projects.
filegroup(
    name = "all",
    srcs = [
        "//iceberg-java:all_examples",
        "//iceberg-rust:iceberg_rust_examples",
        "//iceberg-spark:all_pipelines",
    ],
    visibility = ["//visibility:public"],
)
