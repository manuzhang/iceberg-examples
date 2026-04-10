"""
Bazel BUILD file for iceberg-spark.

The PySpark runtime is provided by the Spark cluster at job submission time and
is therefore NOT declared as a Bazel build-time dependency.  The py_binary
targets below package the pipeline source so that `bazel build //iceberg-spark/...`
validates Python syntax and produces runnable wrappers.

To submit a pipeline to a Spark cluster, supply pyspark via:
  --packages org.apache.iceberg:iceberg-spark-runtime-<version>
or pass PYSPARK_PYTHON / spark-submit --py-files as appropriate.

Targets
-------
  :batch_pipeline     — py_binary for batch_pipeline.py
  :streaming_pipeline — py_binary for streaming_pipeline.py
  :all_pipelines      — filegroup collecting both binaries
"""

py_binary(
    name = "batch_pipeline",
    srcs = ["batch_pipeline.py"],
    main = "batch_pipeline.py",
    python_version = "PY3",
    visibility = ["//visibility:public"],
)

py_binary(
    name = "streaming_pipeline",
    srcs = ["streaming_pipeline.py"],
    main = "streaming_pipeline.py",
    python_version = "PY3",
    visibility = ["//visibility:public"],
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
