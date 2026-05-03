package io.github.manuzhang.iceberg.examples;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.rest.EmbeddedRestCatalogServer;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating a row-level upsert against an Iceberg v3 table using a deletion vector.
 *
 * <p>The example writes an initial data file, marks one row deleted in that original file through a
 * Puffin-backed deletion vector, then appends replacement rows in the same {@code RowDelta}
 * commit.
 */
public class RowLevelUpsertExample {

  private static final Logger LOG = LoggerFactory.getLogger(RowLevelUpsertExample.class);
  private static final String DEFAULT_CATALOG_NAME = "rest";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.parse("default.customers");

  static final Schema CUSTOMER_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "customer_id", Types.LongType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "loyalty_tier", Types.StringType.get()));
  private static final long UPSERTED_CUSTOMER_ID = 2L;

  public static void main(String[] args) {
    Path warehouseDir = null;

    try {
      warehouseDir = Files.createTempDirectory("iceberg-v3-dv-upsert");
      RowLevelUpsertExample example = new RowLevelUpsertExample();
      UpsertResult result = example.demonstrateRowLevelUpsert(warehouseDir);

      LOG.info("Created temporary warehouse at {}", warehouseDir);
      LOG.info("Visible rows after the DV-backed upsert:");
      for (Record row : result.visibleRows()) {
        LOG.info(
            "  customer_id={}, name={}, loyalty_tier={}",
            row.getField("customer_id"),
            row.getField("name"),
            row.getField("loyalty_tier"));
      }

      LOG.info(
          "Deletion vector file: {} (format={}, referenced_data_file={}, deleted_rows={})",
          result.deletionVectorFile().location(),
          result.deletionVectorFile().format(),
          result.deletionVectorFile().referencedDataFile(),
          result.deletionVectorFile().recordCount());
      LOG.info(
          "Snapshot summary: added_files={}, added_delete_files={}, added_dvs={}, added_pos_deletes={}",
          result.snapshotSummary().get(SnapshotSummary.ADDED_FILES_PROP),
          result.snapshotSummary().get(SnapshotSummary.ADDED_DELETE_FILES_PROP),
          result.snapshotSummary().get(SnapshotSummary.ADDED_DVS_PROP),
          result.snapshotSummary().get(SnapshotSummary.ADDED_POS_DELETES_PROP));
      LOG.info("Row-level upsert example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error running row-level upsert example: {}", e.getMessage(), e);
      System.exit(1);
    } finally {
      if (warehouseDir != null) {
        deleteRecursively(warehouseDir);
      }
    }
  }

  public UpsertResult demonstrateRowLevelUpsert(Path warehouseDir) throws Exception {
    String warehousePath = warehouseDir.toUri().toString();
    String catalogUri = localCatalogUri();

    try (EmbeddedRestCatalogServer ignored =
            EmbeddedRestCatalogServer.start(catalogUri, warehousePath);
        RESTCatalog catalog = restCatalog(catalogUri, warehousePath)) {
      LOG.info("Using REST catalog {} backed by warehouse {}", catalogUri, warehousePath);
      Table table = createV3Table(catalog);

      DataFile initialDataFile =
          writeDataFile(
              table,
              List.of(
                  new Customer(2L, "Bob Smith", "bronze"),
                  new Customer(1L, "Alice Johnson", "silver")),
              "initial-load",
              1L);
      table.newAppend().appendFile(initialDataFile).commit();
      table.refresh();

      ExistingCustomerRow existingRow = findCustomerRow(table, UPSERTED_CUSTOMER_ID);
      if (!initialDataFile.location().equals(existingRow.filePath())) {
        throw new IllegalStateException(
            "Expected customer_id="
                + UPSERTED_CUSTOMER_ID
                + " to be located in "
                + initialDataFile.location()
                + " but found "
                + existingRow.filePath());
      }
      DeleteFile deletionVectorFile =
          writeDeletionVector(
              table, initialDataFile, existingRow.rowPosition(), "row-level-upsert-dv", 2L);
      DataFile upsertDataFile =
          writeDataFile(
              table,
              List.of(
                  new Customer(2L, "Bob Smith", "gold"),
                  new Customer(3L, "Carol Lee", "bronze")),
              "row-level-upsert-data",
              3L);

      table.newRowDelta().addDeletes(deletionVectorFile).addRows(upsertDataFile).commit();
      table.refresh();

      Snapshot snapshot = table.currentSnapshot();
      return new UpsertResult(
          readVisibleRows(table),
          new LinkedHashMap<>(snapshot.summary()),
          initialDataFile,
          upsertDataFile,
          deletionVectorFile);
    }
  }

  private Table createV3Table(RESTCatalog catalog) {
    ensureNamespace(catalog, TABLE_IDENTIFIER.namespace());

    return catalog
        .buildTable(TABLE_IDENTIFIER, CUSTOMER_SCHEMA)
        .withPartitionSpec(PartitionSpec.unpartitioned())
        .withProperty(TableProperties.FORMAT_VERSION, "3")
        .create();
  }

  private RESTCatalog restCatalog(String catalogUri, String warehousePath) {
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize(DEFAULT_CATALOG_NAME, restCatalogProperties(catalogUri, warehousePath));
    return catalog;
  }

  private void ensureNamespace(RESTCatalog catalog, Namespace namespace) {
    try {
      catalog.createNamespace(namespace);
    } catch (AlreadyExistsException ignored) {
      // The example may be pointed at an existing REST catalog namespace.
    }
  }

  private DataFile writeDataFile(
      Table table, List<Customer> customers, String operationId, long taskId) throws IOException {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table, table.schema(), table.spec(), null, null, null, null);
    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, 0, taskId)
            .format(FileFormat.PARQUET)
            .operationId(operationId)
            .build();
    DataWriter<Record> writer =
        appenderFactory.newDataWriter(outputFileFactory.newOutputFile(), FileFormat.PARQUET, null);

    try (writer) {
      for (Customer customer : customers) {
        writer.write(toIcebergRecord(customer));
      }
    }

    return writer.toDataFile();
  }

  private DeleteFile writeDeletionVector(
      Table table, DataFile dataFile, long rowPosition, String operationId, long taskId)
      throws IOException {
    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, 0, taskId)
            .format(FileFormat.PUFFIN)
            .operationId(operationId)
            .build();
    PartitioningDVWriter<Record> writer = new PartitioningDVWriter<>(outputFileFactory, path -> null);
    PositionDelete<Record> positionDelete =
        PositionDelete.<Record>create().set(dataFile.location(), rowPosition);

    try (writer) {
      writer.write(positionDelete, table.spec(), dataFile.partition());
    }

    DeleteWriteResult result = writer.result();
    return result.deleteFiles().get(0);
  }

  private ExistingCustomerRow findCustomerRow(Table table, long customerId) throws IOException {
    Schema lookupSchema =
        new Schema(
            CUSTOMER_SCHEMA.findField("customer_id"),
            MetadataColumns.FILE_PATH,
            MetadataColumns.ROW_POSITION);

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(table)
            .where(Expressions.equal("customer_id", customerId))
            .project(lookupSchema)
            .build()) {
      ExistingCustomerRow match = null;

      for (Record row : iterable) {
        ExistingCustomerRow candidate =
            new ExistingCustomerRow(
                ((Number) row.getField("customer_id")).longValue(),
                row.getField(MetadataColumns.FILE_PATH.name()).toString(),
                ((Number) row.getField(MetadataColumns.ROW_POSITION.name())).longValue());

        if (match != null) {
          throw new IllegalStateException("Expected exactly one row for customer_id=" + customerId);
        }

        match = candidate;
      }

      if (match == null) {
        throw new IllegalStateException("Could not find existing row for customer_id=" + customerId);
      }

      return match;
    }
  }

  private List<Record> readVisibleRows(Table table) throws IOException {
    List<Record> rows = new ArrayList<>();

    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      for (Record row : iterable) {
        rows.add(row);
      }
    }

    rows.sort(Comparator.comparingLong(row -> (Long) row.getField("customer_id")));
    return rows;
  }

  private static Record toIcebergRecord(Customer customer) {
    Record record = GenericRecord.create(CUSTOMER_SCHEMA);
    record.setField("customer_id", customer.customerId());
    record.setField("name", customer.name());
    record.setField("loyalty_tier", customer.loyaltyTier());
    return record;
  }

  private static Map<String, String> restCatalogProperties(String catalogUri, String warehousePath) {
    return Map.of(
        CatalogProperties.URI, catalogUri,
        CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
  }

  private static String localCatalogUri() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return "http://localhost:" + socket.getLocalPort();
    }
  }

  static void deleteRecursively(Path root) {
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    } catch (IOException e) {
      LOG.warn("Failed to delete temporary directory {}", root, e);
    } catch (UncheckedIOException e) {
      LOG.warn("Failed to delete temporary directory {}", root, e.getCause());
    }
  }

  private record Customer(long customerId, String name, String loyaltyTier) {}

  private record ExistingCustomerRow(long customerId, String filePath, long rowPosition) {}

  public record UpsertResult(
      List<Record> visibleRows,
      Map<String, String> snapshotSummary,
      DataFile originalDataFile,
      DataFile upsertDataFile,
      DeleteFile deletionVectorFile) {}
}
