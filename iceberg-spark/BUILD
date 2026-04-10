"""
Bazel BUILD file for iceberg-spark.

PySpark dependencies are resolved via the `iceberg_spark_deps` pip hub
declared in MODULE.bazel.  Generate or update the lock file with:

  pip-compile iceberg-spark/requirements.txt \\
      --output-file iceberg-spark/requirements_lock.txt \\
      --generate-hashes

NOTE: The Iceberg Spark runtime JAR is not available as a pip package and
must be supplied at Spark job submission time.  See README.md for details.

Targets
-------
  :batch_pipeline     — py_binary for batch_pipeline.py
  :streaming_pipeline — py_binary for streaming_pipeline.py
  :all_pipelines      — filegroup collecting both binaries
"""

load("@iceberg_spark_deps//:requirements.bzl", "requirement")

py_binary(
    name = "batch_pipeline",
    srcs = ["batch_pipeline.py"],
    main = "batch_pipeline.py",
    python_version = "PY3",
    visibility = ["//visibility:public"],
    deps = [requirement("pyspark")],
)

py_binary(
    name = "streaming_pipeline",
    srcs = ["streaming_pipeline.py"],
    main = "streaming_pipeline.py",
    python_version = "PY3",
    visibility = ["//visibility:public"],
    deps = [requirement("pyspark")],
)

# Convenience target: build both pipelines at once.
filegroup(
    name = "all_pipelines",
    srcs = [
        ":batch_pipeline",
        ":streaming_pipeline",
    ],
    visibility = ["//visibility:public"],
)
