package io.github.manuzhang.iceberg.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.EmbeddedRestCatalogServer;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating a bounded CDC read from an Iceberg table using Beam IcebergIO's CDC
 * interface.
 *
 * <p>Beam's Iceberg CDC reader uses Iceberg's incremental append scan. This example writes ordinary
 * customer rows into Iceberg, captures snapshot IDs, and then reads only the rows added by the
 * second snapshot through {@link IcebergIO#readRows(IcebergCatalogConfig)} with {@link
 * IcebergIO.ReadRows#withCdc()}.
 */
public class BeamIcebergCdcExample {

  private static final Logger LOG = LoggerFactory.getLogger(BeamIcebergCdcExample.class);
  static final String DEFAULT_TABLE = "default.customers";

  static final Schema CUSTOMER_SCHEMA =
      Schema.builder()
          .addInt64Field("customer_id")
          .addStringField("name")
          .addStringField("email")
          .addStringField("signup_time")
          .build();

  public static void main(String[] args) {
    LOG.info("Starting Apache Beam + Apache Iceberg CDC Example...");

    String catalogUri =
        configuredValue(args, 0, "ICEBERG_REST_URI", BeamIcebergExample.DEFAULT_REST_CATALOG_URI);
    String table = configuredValue(args, 2, "ICEBERG_TABLE", DEFAULT_TABLE);

    try {
      String warehouse = configuredWarehouse(args);
      try (EmbeddedRestCatalogServer ignored =
          maybeStartLocalRestCatalog(catalogUri, warehouse)) {
        runExample(catalogUri, warehouse, table);
      }
      LOG.info("CDC example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error running CDC example: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  static void runExample(String catalogUri, String warehousePath, String table) throws IOException {
    LOG.info("=== Apache Beam + Apache Iceberg CDC Example ===");
    LOG.info("REST catalog URI: {}", catalogUri);
    LOG.info("Warehouse path: {}", warehousePath);
    LOG.info("Customer table: {}", table);

    Map<String, Object> writeConfig =
        BeamIcebergExample.managedIcebergConfig(table, catalogUri, warehousePath);

    List<Row> baselineCustomers =
        List.of(
            customer(1001L, "Alice", "alice@example.com", "2026-05-02T09:00:00Z"),
            customer(1002L, "Bob", "bob@example.com", "2026-05-02T09:01:00Z"));

    LOG.info("--- Writing {} baseline customers to '{}' ---", baselineCustomers.size(), table);
    writePipeline("Write baseline customers", baselineCustomers, writeConfig);
    long baselineSnapshot = currentSnapshotId(catalogUri, warehousePath, table);
    LOG.info("Baseline snapshot id: {}", baselineSnapshot);

    List<Row> newCustomers =
        List.of(
            customer(1003L, "Charlie", "charlie@example.com", "2026-05-02T09:05:00Z"),
            customer(1004L, "Dana", "dana@example.com", "2026-05-02T09:06:00Z"),
            customer(1005L, "Eve", "eve@example.com", "2026-05-02T09:07:00Z"));

    LOG.info("--- Writing {} new customers to '{}' ---", newCustomers.size(), table);
    writePipeline("Write new customers", newCustomers, writeConfig);
    long newCustomerSnapshot = currentSnapshotId(catalogUri, warehousePath, table);
    LOG.info("New customer snapshot id: {}", newCustomerSnapshot);

    LOG.info("--- Reading customers added by snapshot {} ---", newCustomerSnapshot);
    // Beam's Iceberg CDC read treats from_snapshot as inclusive. Using the second snapshot for
    // both bounds returns only the rows appended by that snapshot.
    readCdcPipeline(table, catalogUri, warehousePath, newCustomerSnapshot, newCustomerSnapshot);
  }

  static Row customer(long customerId, String name, String email, String signupTime) {
    return Row.withSchema(CUSTOMER_SCHEMA).addValues(customerId, name, email, signupTime).build();
  }

  static IcebergCatalogConfig icebergCatalogConfig(String catalogUri, String warehousePath) {
    return IcebergCatalogConfig.builder()
        .setCatalogName(BeamIcebergExample.DEFAULT_CATALOG_NAME)
        .setCatalogProperties(BeamIcebergExample.restCatalogProperties(catalogUri, warehousePath))
        .setConfigProperties(Map.of())
        .build();
  }

  static void writePipeline(String name, List<Row> rows, Map<String, Object> config) {
    Pipeline p = Pipeline.create();

    p.apply("Create " + name, Create.of(rows).withRowSchema(CUSTOMER_SCHEMA))
        .apply(name, Managed.write(Managed.ICEBERG).withConfig(config));

    p.run().waitUntilFinish();
    LOG.info("Successfully wrote {} rows to table '{}'", rows.size(), config.get("table"));
  }

  static void readCdcPipeline(
      String table, String catalogUri, String warehousePath, long fromSnapshot, long toSnapshot) {
    Pipeline p = Pipeline.create();

    PCollection<Row> rows =
        p.apply(
            "Read rows added by snapshot",
            IcebergIO.readRows(icebergCatalogConfig(catalogUri, warehousePath))
                .withCdc()
                .from(TableIdentifier.parse(table))
                .fromSnapshot(fromSnapshot)
                .toSnapshot(toSnapshot)
                .streaming(false));

    rows.apply(
        "Print added customer rows",
        ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void processElement(@Element Row row) {
                LOG.info(
                    "  Added customer: customer_id={}, name={}, email={}, signup_time={}",
                    row.getInt64("customer_id"),
                    row.getString("name"),
                    row.getString("email"),
                    row.getString("signup_time"));
              }
            }));

    p.run().waitUntilFinish();
    LOG.info("Successfully read rows added to table '{}'", table);
  }

  static long currentSnapshotId(String catalogUri, String warehousePath, String table)
      throws IOException {
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize(
        BeamIcebergExample.DEFAULT_CATALOG_NAME,
        BeamIcebergExample.restCatalogProperties(catalogUri, warehousePath));

    try {
      Table icebergTable = catalog.loadTable(TableIdentifier.parse(table));
      Snapshot snapshot = icebergTable.currentSnapshot();
      if (snapshot == null) {
        throw new IllegalStateException("Table has no snapshots: " + table);
      }

      return snapshot.snapshotId();
    } finally {
      catalog.close();
    }
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

  private static String configuredWarehouse(String[] args) throws IOException {
    String configuredWarehouse =
        configuredValue(args, 1, "ICEBERG_WAREHOUSE", null);
    if (configuredWarehouse != null) {
      return configuredWarehouse;
    }

    return Files.createTempDirectory("iceberg-beam-cdc-warehouse").toUri().toString();
  }

  private static EmbeddedRestCatalogServer maybeStartLocalRestCatalog(
      String catalogUri, String warehousePath) throws Exception {
    if (!BeamIcebergExample.DEFAULT_REST_CATALOG_URI.equals(catalogUri)) {
      return EmbeddedRestCatalogServer.noop();
    }

    if (EmbeddedRestCatalogServer.isReachable(catalogUri)) {
      return EmbeddedRestCatalogServer.noop();
    }

    LOG.info("No REST catalog found at {}; starting an embedded local catalog", catalogUri);
    return EmbeddedRestCatalogServer.start(catalogUri, warehousePath);
  }
}
