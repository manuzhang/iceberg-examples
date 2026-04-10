"""
Bazel BUILD file for iceberg-rust.

The crate dependency graph is derived from Cargo.toml/Cargo.lock via
the `crate_index` repository generated in MODULE.bazel.

Targets
-------
  :iceberg_rust_examples — rust_binary wrapping src/main.rs and its modules
"""

load("@rules_rust//rust:defs.bzl", "rust_binary")

rust_binary(
    name = "iceberg_rust_examples",
    srcs = glob(["src/**/*.rs"]),
    edition = "2021",
    visibility = ["//visibility:public"],
    deps = [
        "@crate_index//:iceberg",
        "@crate_index//:tokio",
    ],
)
