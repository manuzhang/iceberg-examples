package io.github.manuzhang.iceberg.beam;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating Apache Iceberg integration with Apache Beam. Shows how to write and read
 * data from Iceberg tables using Beam's IcebergIO connector with the DirectRunner (no cluster
 * required).
 *
 * <p>This example:
 *
 * <ul>
 *   <li>Defines a Beam schema for a users table
 *   <li>Writes sample rows to an Iceberg table using {@link IcebergIO#writeRows}
 *   <li>Reads the rows back from the Iceberg table using {@link IcebergIO#readRows}
 *   <li>Uses a local HadoopCatalog backed by the filesystem (no external catalog service required)
 * </ul>
 */
public class BeamIcebergExample {

  private static final Logger LOG = LoggerFactory.getLogger(BeamIcebergExample.class);

  /** Beam schema for the example users table. */
  static final Schema BEAM_SCHEMA =
      Schema.builder()
          .addInt64Field("id")
          .addStringField("name")
          .addStringField("email")
          .addInt32Field("age")
          .build();

  public static void main(String[] args) throws IOException {
    LOG.info("Starting Apache Beam + Apache Iceberg Example...");

    Path warehouseDir = Files.createTempDirectory("iceberg-beam-example");

    try {
      runExample(warehouseDir.toAbsolutePath().toString());
      LOG.info("Example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error running example: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  /**
   * Runs the Beam + Iceberg write and read pipelines using the given warehouse path.
   *
   * @param warehousePath path to the local Iceberg warehouse directory
   */
  static void runExample(String warehousePath) {
    LOG.info("=== Apache Beam + Apache Iceberg Example ===");
    LOG.info("Warehouse path: {}", warehousePath);

    TableIdentifier tableId = TableIdentifier.of("default", "users");

    // Configure the Iceberg catalog (HadoopCatalog backed by the local filesystem).
    // "type" -> "hadoop" selects HadoopCatalog; "warehouse" sets the storage location.
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("example_catalog")
            .setCatalogProperties(Map.of("type", "hadoop", "warehouse", warehousePath))
            .build();

    List<Row> rows =
        Arrays.asList(
            Row.withSchema(BEAM_SCHEMA).addValues(1L, "Alice", "alice@example.com", 30).build(),
            Row.withSchema(BEAM_SCHEMA).addValues(2L, "Bob", "bob@example.com", 25).build(),
            Row.withSchema(BEAM_SCHEMA)
                .addValues(3L, "Charlie", "charlie@example.com", 35)
                .build());

    // Step 1: write rows to the Iceberg table
    LOG.info("--- Writing {} rows to Iceberg table '{}' ---", rows.size(), tableId);
    writePipeline(rows, tableId, catalogConfig);

    // Step 2: read rows back from the Iceberg table
    LOG.info("--- Reading rows from Iceberg table '{}' ---", tableId);
    readPipeline(tableId, catalogConfig);
  }

  /**
   * Beam pipeline that writes a list of {@link Row}s to an Iceberg table using {@link
   * IcebergIO#writeRows}.
   *
   * @param rows rows to write
   * @param tableId target Iceberg table identifier
   * @param catalogConfig Iceberg catalog configuration
   */
  static void writePipeline(
      List<Row> rows, TableIdentifier tableId, IcebergCatalogConfig catalogConfig) {
    Pipeline p = Pipeline.create();

    p.apply("Create rows", Create.of(rows).withRowSchema(BEAM_SCHEMA))
        .apply("Write to Iceberg", IcebergIO.writeRows(catalogConfig).to(tableId));

    p.run().waitUntilFinish();
    LOG.info("Successfully wrote {} rows to table '{}'", rows.size(), tableId);
  }

  /**
   * Beam pipeline that reads {@link Row}s from an Iceberg table using {@link IcebergIO#readRows}.
   *
   * @param tableId source Iceberg table identifier
   * @param catalogConfig Iceberg catalog configuration
   */
  static void readPipeline(TableIdentifier tableId, IcebergCatalogConfig catalogConfig) {
    Pipeline p = Pipeline.create();

    p.apply("Read from Iceberg", IcebergIO.readRows(catalogConfig).from(tableId))
        .apply(
            "Print rows",
            ParDo.of(
                new DoFn<Row, Void>() {
                  @ProcessElement
                  public void processElement(@Element Row row) {
                    LOG.info(
                        "  Row: id={}, name={}, email={}, age={}",
                        row.getInt64("id"),
                        row.getString("name"),
                        row.getString("email"),
                        row.getInt32("age"));
                  }
                }));

    p.run().waitUntilFinish();
    LOG.info("Successfully read rows from table '{}'", tableId);
  }
}
