package io.github.manuzhang.iceberg.examples;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.EmbeddedRestCatalogServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating Apache Iceberg integration with Apache Beam. Shows how to write and read
 * data from Iceberg tables using Beam's Managed Iceberg transform with the DirectRunner.
 *
 * <p>This example:
 *
 * <ul>
 *   <li>Defines a Beam schema for a users table
 *   <li>Writes sample rows to an Iceberg table using {@link Managed#write(String)}
 *   <li>Reads the rows back from the Iceberg table using {@link Managed#read(String)}
 *   <li>Uses an Iceberg REST catalog configured through Managed IO catalog properties
 * </ul>
 */
public class BeamIcebergExample {

  private static final Logger LOG = LoggerFactory.getLogger(BeamIcebergExample.class);
  static final String DEFAULT_CATALOG_NAME = "rest";
  static final String DEFAULT_REST_CATALOG_URI = "http://localhost:8181";
  static final String DEFAULT_TABLE = "default.users";
  static final String DEFAULT_WAREHOUSE = "file:///tmp/iceberg-beam-warehouse";
  static final String ICEBERG_REST_CATALOG_IMPL = "org.apache.iceberg.rest.RESTCatalog";

  /** Beam schema for the example users table. */
  static final Schema BEAM_SCHEMA =
      Schema.builder()
          .addInt64Field("id")
          .addStringField("name")
          .addStringField("email")
          .addInt32Field("age")
          .build();

  public static void main(String[] args) {
    LOG.info("Starting Apache Beam + Apache Iceberg Example...");

    String catalogUri =
        configuredValue(args, 0, "ICEBERG_REST_URI", DEFAULT_REST_CATALOG_URI);
    String table = configuredValue(args, 2, "ICEBERG_TABLE", DEFAULT_TABLE);

    try {
      String warehouse = configuredWarehouse(args);
      try (EmbeddedRestCatalogServer ignored =
          maybeStartLocalRestCatalog(catalogUri, warehouse)) {
        runExample(catalogUri, warehouse, table);
      }
      LOG.info("Example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error running example: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  /**
   * Runs the Beam + Iceberg write and read pipelines using a REST catalog.
   *
   * @param catalogUri Iceberg REST catalog URI
   * @param warehousePath Iceberg warehouse location known to the REST catalog
   * @param table fully qualified table identifier, for example {@code default.users}
   */
  static void runExample(String catalogUri, String warehousePath, String table) {
    LOG.info("=== Apache Beam + Apache Iceberg Example ===");
    LOG.info("REST catalog URI: {}", catalogUri);
    LOG.info("Warehouse path: {}", warehousePath);
    LOG.info("Table: {}", table);

    Map<String, Object> config = managedIcebergConfig(table, catalogUri, warehousePath);

    List<Row> rows =
        Arrays.asList(
            Row.withSchema(BEAM_SCHEMA).addValues(1L, "Alice", "alice@example.com", 30).build(),
            Row.withSchema(BEAM_SCHEMA).addValues(2L, "Bob", "bob@example.com", 25).build(),
            Row.withSchema(BEAM_SCHEMA)
                .addValues(3L, "Charlie", "charlie@example.com", 35)
                .build());

    // Step 1: write rows to the Iceberg table
    LOG.info("--- Writing {} rows to Iceberg table '{}' ---", rows.size(), table);
    writePipeline(rows, config);

    // Step 2: read rows back from the Iceberg table
    LOG.info("--- Reading rows from Iceberg table '{}' ---", table);
    readPipeline(config);
  }

  /**
   * Beam pipeline that writes a list of {@link Row}s to an Iceberg table using Managed Iceberg IO.
   *
   * @param rows rows to write
   * @param config Managed Iceberg configuration
   */
  static void writePipeline(List<Row> rows, Map<String, Object> config) {
    Pipeline p = Pipeline.create();

    p.apply("Create rows", Create.of(rows).withRowSchema(BEAM_SCHEMA))
        .apply("Write to Iceberg", Managed.write(Managed.ICEBERG).withConfig(config));

    p.run().waitUntilFinish();
    LOG.info("Successfully wrote {} rows to table '{}'", rows.size(), config.get("table"));
  }

  /**
   * Beam pipeline that reads {@link Row}s from an Iceberg table using Managed Iceberg IO.
   *
   * @param config Managed Iceberg configuration
   */
  static void readPipeline(Map<String, Object> config) {
    Pipeline p = Pipeline.create();

    PCollection<Row> rows =
        p.apply("Read from Iceberg", Managed.read(Managed.ICEBERG).withConfig(config))
            .getSinglePCollection();

    rows.apply(
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
    LOG.info("Successfully read rows from table '{}'", config.get("table"));
  }

  static Map<String, Object> managedIcebergConfig(
      String table, String catalogUri, String warehousePath) {
    return Map.<String, Object>of(
        "table",
        table,
        "catalog_name",
        DEFAULT_CATALOG_NAME,
        "catalog_properties",
        restCatalogProperties(catalogUri, warehousePath));
  }

  static Map<String, String> restCatalogProperties(String catalogUri, String warehousePath) {
    Map<String, String> properties = new HashMap<>();
    properties.put("catalog-impl", ICEBERG_REST_CATALOG_IMPL);
    properties.put("uri", catalogUri);
    properties.put("warehouse", warehousePath);
    if (usesInMemoryWarehouse(warehousePath)) {
      properties.put(CatalogProperties.FILE_IO_IMPL, SharedInMemoryFileIO.class.getName());
    }
    return Map.copyOf(properties);
  }

  private static String configuredValue(
      String[] args, int index, String environmentVariable, String defaultValue) {
    if (args.length > index && !args[index].isBlank()) {
      return args[index];
    }

    String value = System.getenv(environmentVariable);
    if (value != null && !value.isBlank()) {
      return value;
    }

    return defaultValue;
  }

  private static String configuredWarehouse(String[] args) {
    String configuredWarehouse = configuredValue(args, 1, "ICEBERG_WAREHOUSE", null);
    if (configuredWarehouse != null) {
      return configuredWarehouse;
    }

    return newInMemoryWarehousePath("iceberg-beam-warehouse");
  }

  private static EmbeddedRestCatalogServer maybeStartLocalRestCatalog(
      String catalogUri, String warehousePath) throws Exception {
    if (!DEFAULT_REST_CATALOG_URI.equals(catalogUri)) {
      return EmbeddedRestCatalogServer.noop();
    }

    if (EmbeddedRestCatalogServer.isReachable(catalogUri)) {
      return EmbeddedRestCatalogServer.noop();
    }

    LOG.info("No REST catalog found at {}; starting an embedded local catalog", catalogUri);
    return EmbeddedRestCatalogServer.start(catalogUri, warehousePath);
  }

  static String newInMemoryWarehousePath(String warehouseName) {
    return "in-memory://" + warehouseName + "-" + Long.toUnsignedString(System.nanoTime());
  }

  static boolean usesInMemoryWarehouse(String warehousePath) {
    return warehousePath.startsWith("in-memory://");
  }
}
