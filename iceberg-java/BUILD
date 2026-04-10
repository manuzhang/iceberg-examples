"""
Bazel BUILD file for iceberg-java.

Targets
-------
  :iceberg_java_lib        — java_library with all main-source classes
  :iceberg_examples        — runnable entry-point (IcebergExamples)
  :data_operations         — runnable example (DataOperationsExample)
  :schema_evolution        — runnable example (SchemaEvolutionExample)
  :table_format_v3         — runnable example (TableFormatV3Example)
  :beam_iceberg            — runnable example (BeamIcebergExample)
  :all_examples            — filegroup collecting all binaries
  :IcebergExamplesTest     — JUnit 5 test suite
  :BeamIcebergExampleTest  — JUnit 5 test suite
"""

load("@contrib_rules_jvm//java:defs.bzl", "java_junit5_test")

# ---------------------------------------------------------------------------
# Library — shared across all binaries and tests
# ---------------------------------------------------------------------------

java_library(
    name = "iceberg_java_lib",
    srcs = glob(["src/main/java/**/*.java"]),
    visibility = ["//visibility:private"],
    deps = [
        "@maven//:org_apache_iceberg_iceberg_api",
        "@maven//:org_apache_iceberg_iceberg_core",
        "@maven//:org_apache_beam_beam_sdks_java_core",
        "@maven//:org_apache_beam_beam_sdks_java_io_iceberg",
        "@maven//:org_apache_hadoop_hadoop_common",
        "@maven//:org_slf4j_slf4j_simple",
    ],
    runtime_deps = [
        # DirectRunner: no cluster required for local execution
        "@maven//:org_apache_beam_beam_runners_direct_java",
    ],
)

# ---------------------------------------------------------------------------
# Runnable binaries (replaces Gradle `task runXxx` targets)
# ---------------------------------------------------------------------------

java_binary(
    name = "iceberg_examples",
    main_class = "io.github.manuzhang.iceberg.examples.IcebergExamples",
    runtime_deps = [":iceberg_java_lib"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "data_operations",
    main_class = "io.github.manuzhang.iceberg.examples.DataOperationsExample",
    runtime_deps = [":iceberg_java_lib"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "schema_evolution",
    main_class = "io.github.manuzhang.iceberg.examples.SchemaEvolutionExample",
    runtime_deps = [":iceberg_java_lib"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "table_format_v3",
    main_class = "io.github.manuzhang.iceberg.examples.TableFormatV3Example",
    runtime_deps = [":iceberg_java_lib"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "beam_iceberg",
    main_class = "io.github.manuzhang.iceberg.examples.BeamIcebergExample",
    runtime_deps = [":iceberg_java_lib"],
    visibility = ["//visibility:public"],
)

# Convenience target: build all example binaries at once (`bazel build //iceberg-java:all_examples`).
filegroup(
    name = "all_examples",
    srcs = [
        ":iceberg_examples",
        ":data_operations",
        ":schema_evolution",
        ":table_format_v3",
        ":beam_iceberg",
    ],
    visibility = ["//visibility:public"],
)

# ---------------------------------------------------------------------------
# Tests (JUnit 5 via contrib_rules_jvm)
# ---------------------------------------------------------------------------

_TEST_DEPS = [
    ":iceberg_java_lib",
    "@maven//:org_junit_jupiter_junit_jupiter",
    "@maven//:org_junit_platform_junit_platform_launcher",
]

java_junit5_test(
    name = "IcebergExamplesTest",
    size = "small",
    srcs = ["src/test/java/io/github/manuzhang/iceberg/examples/IcebergExamplesTest.java"],
    test_class = "io.github.manuzhang.iceberg.examples.IcebergExamplesTest",
    deps = _TEST_DEPS,
)

java_junit5_test(
    name = "BeamIcebergExampleTest",
    size = "medium",
    srcs = ["src/test/java/io/github/manuzhang/iceberg/examples/BeamIcebergExampleTest.java"],
    test_class = "io.github.manuzhang.iceberg.examples.BeamIcebergExampleTest",
    # BeamIcebergExample.runExample() needs the DirectRunner at test runtime.
    runtime_deps = ["@maven//:org_apache_beam_beam_runners_direct_java"],
    deps = _TEST_DEPS,
)
