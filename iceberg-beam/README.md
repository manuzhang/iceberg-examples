# Apache Beam + Apache Iceberg Examples

Examples demonstrating how to use [Apache Beam](https://beam.apache.org/) with
[Apache Iceberg](https://iceberg.apache.org/) tables via Beam's built-in
[IcebergIO](https://beam.apache.org/documentation/io/built-in/iceberg/) connector.

## Requirements

- Java 17 or higher

## What the example covers

[`BeamIcebergExample.java`](src/main/java/io/github/manuzhang/iceberg/beam/BeamIcebergExample.java)
demonstrates:

- Defining a Beam `Schema` whose fields map to an Iceberg table
- **Writing** rows to an Iceberg table with `IcebergIO.writeRows()`
- **Reading** rows back from an Iceberg table with `IcebergIO.readRows()`
- Configuring a local `HadoopCatalog` (no external catalog service required)
- Running pipelines locally with the `DirectRunner` (no cluster required)

## Running the example

```bash
./gradlew run
```

## Running the tests

```bash
./gradlew test
```

## Key dependencies

| Dependency | Version | Purpose |
|---|---|---|
| `beam-sdks-java-core` | 2.61.0 | Apache Beam Java SDK |
| `beam-sdks-java-io-iceberg` | 2.61.0 | Beam IcebergIO connector |
| `beam-runners-direct-java` | 2.61.0 | Local DirectRunner |
| `iceberg-api` | 1.10.0 | Apache Iceberg API |
| `hadoop-common` | 3.3.6 | Hadoop filesystem support for HadoopCatalog |

## Learning resources

- [Apache Beam IcebergIO documentation](https://beam.apache.org/documentation/io/built-in/iceberg/)
- [Apache Iceberg documentation](https://iceberg.apache.org/)
- [Apache Beam programming guide](https://beam.apache.org/documentation/programming-guide/)
