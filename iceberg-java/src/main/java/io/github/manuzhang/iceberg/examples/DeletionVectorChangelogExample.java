package io.github.manuzhang.iceberg.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.rest.EmbeddedRestCatalogServer;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of a third-party, metadata-level changelog planner for Iceberg v3 deletion vectors.
 *
 * <p>This intentionally does not plug into Iceberg's {@code IncrementalChangelogScan}. Instead it
 * demonstrates the part an external library can own: read public snapshot file-change metadata,
 * accept Puffin-backed deletion vectors on v3 tables, and surface metadata events for added rows
 * and rows deleted by a deletion vector. Equality deletes and non-DV position deletes are rejected.
 */
public class DeletionVectorChangelogExample {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletionVectorChangelogExample.class);
  private static final String DEFAULT_CATALOG_NAME = "rest";
  private static final FileFormat DATA_FILE_FORMAT = FileFormat.AVRO;
  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.parse("default.dv_changelog_customers");
  private static final long UPDATED_CUSTOMER_ID = 2L;
  private static final String UNSUPPORTED_DELETE_MESSAGE =
      "Only deletion vectors in v3 tables are supported by this changelog planner";

  public static void main(String[] args) {
    Path warehouseDir = null;

    try {
      warehouseDir = Files.createTempDirectory("iceberg-v3-dv-changelog");
      DeletionVectorChangelogExample example = new DeletionVectorChangelogExample();
      ChangelogResult result = example.demonstrateDeletionVectorChangelog(warehouseDir);

      LOG.info(
          "Planned changes from snapshot {} to {}:",
          result.fromSnapshotId(),
          result.toSnapshotId());
      for (ChangelogEvent event : result.events()) {
        LOG.info("  {}", event);
      }

      LOG.info("Visible rows after applying the DV-backed upsert:");
      for (Record row : result.visibleRows()) {
        LOG.info(
            "  customer_id={}, name={}, loyalty_tier={}",
            row.getField("customer_id"),
            row.getField("name"),
            row.getField("loyalty_tier"));
      }

      LOG.info("Deletion vector changelog example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error running deletion vector changelog example: {}", e.getMessage(), e);
      System.exit(1);
    } finally {
      if (warehouseDir != null) {
        RowLevelUpsertExample.deleteRecursively(warehouseDir);
      }
    }
  }

  public ChangelogResult demonstrateDeletionVectorChangelog(Path warehouseDir) throws Exception {
    String warehousePath = inMemoryWarehousePath(warehouseDir);
    String jdbcUri = sqliteCatalogUri(warehouseDir);

    try (EmbeddedRestCatalogServer server =
            EmbeddedRestCatalogServer.startJdbcSqliteInMemoryFileIO(
                localCatalogUri(), jdbcUri, warehousePath);
        RESTCatalog catalog = restCatalog(server.catalogUri(), warehousePath)) {
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
      long fromSnapshotId = table.currentSnapshot().snapshotId();

      ExistingCustomerRow existingRow = findCustomerRow(table, UPDATED_CUSTOMER_ID);
      if (!initialDataFile.location().equals(existingRow.filePath())) {
        throw new IllegalStateException(
            "Expected customer_id="
                + UPDATED_CUSTOMER_ID
                + " to be located in "
                + initialDataFile.location()
                + " but found "
                + existingRow.filePath());
      }
      DeleteFile deletionVectorFile =
          writeDeletionVector(
              table, initialDataFile, existingRow.rowPosition(), "dv-changelog-delete", 2L);
      DataFile upsertDataFile =
          writeDataFile(
              table,
              List.of(
                  new Customer(2L, "Bob Smith", "gold"),
                  new Customer(3L, "Carol Lee", "bronze")),
              "dv-changelog-add",
              3L);

      table.newRowDelta().addDeletes(deletionVectorFile).addRows(upsertDataFile).commit();
      table.refresh();
      long toSnapshotId = table.currentSnapshot().snapshotId();

      DvOnlyChangelogPlanner planner = new DvOnlyChangelogPlanner();
      return new ChangelogResult(
          planner.plan(table, fromSnapshotId, toSnapshotId),
          readVisibleRows(table),
          fromSnapshotId,
          toSnapshotId,
          initialDataFile,
          upsertDataFile,
          deletionVectorFile);
    }
  }

  private Table createV3Table(RESTCatalog catalog) {
    ensureNamespace(catalog, TABLE_IDENTIFIER.namespace());

    return catalog
        .buildTable(TABLE_IDENTIFIER, RowLevelUpsertExample.CUSTOMER_SCHEMA)
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
            .format(DATA_FILE_FORMAT)
            .operationId(operationId)
            .build();
    DataWriter<Record> writer =
        appenderFactory.newDataWriter(outputFileFactory.newOutputFile(), DATA_FILE_FORMAT, null);

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
    PartitioningDVWriter<Record> writer =
        new PartitioningDVWriter<>(outputFileFactory, path -> null);
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
            RowLevelUpsertExample.CUSTOMER_SCHEMA.findField("customer_id"),
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
        throw new IllegalStateException(
            "Could not find existing row for customer_id=" + customerId);
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
    Record record = GenericRecord.create(RowLevelUpsertExample.CUSTOMER_SCHEMA);
    record.setField("customer_id", customer.customerId());
    record.setField("name", customer.name());
    record.setField("loyalty_tier", customer.loyaltyTier());
    return record;
  }

  private static Map<String, String> restCatalogProperties(
      String catalogUri, String warehousePath) {
    return Map.of(
        CatalogProperties.URI, catalogUri,
        CatalogProperties.FILE_IO_IMPL, SharedInMemoryFileIO.class.getName(),
        CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
  }

  private static String localCatalogUri() {
    return "http://localhost:0";
  }

  private static String sqliteCatalogUri(Path warehouseDir) {
    return "jdbc:sqlite:" + warehouseDir.resolve("catalog.db").toAbsolutePath();
  }

  private static String inMemoryWarehousePath(Path warehouseDir) {
    return "in-memory://" + warehouseDir.getFileName();
  }

  private record Customer(long customerId, String name, String loyaltyTier) {}

  private record ExistingCustomerRow(long customerId, String filePath, long rowPosition) {}

  public record ChangelogResult(
      List<ChangelogEvent> events,
      List<Record> visibleRows,
      long fromSnapshotId,
      long toSnapshotId,
      DataFile originalDataFile,
      DataFile upsertDataFile,
      DeleteFile deletionVectorFile) {}

  public enum ChangelogEventType {
    ADDED_ROWS,
    DELETED_ROWS_BY_DV
  }

  public record ChangelogEvent(
      ChangelogEventType type,
      long snapshotId,
      String dataFileLocation,
      long recordCount,
      String deletionVectorLocation,
      Long deletionVectorOffset,
      Long deletionVectorSize) {}

  public static class DvOnlyChangelogPlanner {

    public List<ChangelogEvent> plan(
        Table table, long fromSnapshotExclusive, long toSnapshotInclusive)
        throws IOException {
      if (formatVersion(table) < 3) {
        throw new UnsupportedOperationException(UNSUPPORTED_DELETE_MESSAGE);
      }

      List<ChangelogEvent> events = new ArrayList<>();
      for (Snapshot snapshot :
          snapshotsBetween(table, fromSnapshotExclusive, toSnapshotInclusive)) {
        if (!shouldPlanSnapshotOperation(snapshot.operation())) {
          continue;
        }

        events.addAll(addedRows(table, snapshot));
        events.addAll(addedDeletionVectors(table, snapshot));
      }

      events.sort(
          Comparator.comparingLong(ChangelogEvent::snapshotId)
              .thenComparing(event -> event.type().ordinal())
              .thenComparing(ChangelogEvent::dataFileLocation));
      return Collections.unmodifiableList(events);
    }

    static boolean shouldPlanSnapshotOperation(String operation) {
      return !DataOperations.REPLACE.equals(operation);
    }

    static void requireSupportedDeleteFile(DeleteFile deleteFile) {
      if (!isDeletionVector(deleteFile)) {
        throw new UnsupportedOperationException(UNSUPPORTED_DELETE_MESSAGE);
      }
    }

    static boolean isDeletionVector(DeleteFile deleteFile) {
      return deleteFile.content() == FileContent.POSITION_DELETES
          && deleteFile.format() == FileFormat.PUFFIN
          && deleteFile.referencedDataFile() != null
          && deleteFile.contentOffset() != null
          && deleteFile.contentSizeInBytes() != null;
    }

    private List<ChangelogEvent> addedRows(Table table, Snapshot snapshot) {
      List<ChangelogEvent> events = new ArrayList<>();

      for (DataFile dataFile : snapshot.addedDataFiles(table.io())) {
        events.add(
            new ChangelogEvent(
                ChangelogEventType.ADDED_ROWS,
                snapshot.snapshotId(),
                dataFile.location(),
                dataFile.recordCount(),
                null,
                null,
                null));
      }

      return events;
    }

    private List<ChangelogEvent> addedDeletionVectors(Table table, Snapshot snapshot) {
      List<ChangelogEvent> events = new ArrayList<>();

      for (DeleteFile deleteFile : snapshot.addedDeleteFiles(table.io())) {
        requireSupportedDeleteFile(deleteFile);
        events.add(
            new ChangelogEvent(
                ChangelogEventType.DELETED_ROWS_BY_DV,
                snapshot.snapshotId(),
                deleteFile.referencedDataFile(),
                deleteFile.recordCount(),
                deleteFile.location(),
                deleteFile.contentOffset(),
                deleteFile.contentSizeInBytes()));
      }

      return events;
    }

    private List<Snapshot> snapshotsBetween(
        Table table, long fromSnapshotExclusive, long toSnapshotInclusive) {
      List<Snapshot> snapshots = new ArrayList<>();
      Snapshot snapshot = table.snapshot(toSnapshotInclusive);

      while (snapshot != null && snapshot.snapshotId() != fromSnapshotExclusive) {
        snapshots.add(snapshot);
        Long parentId = snapshot.parentId();
        snapshot = parentId != null ? table.snapshot(parentId) : null;
      }

      if (snapshot == null) {
        throw new IllegalArgumentException(
            "Snapshot "
                + fromSnapshotExclusive
                + " is not an ancestor of snapshot "
                + toSnapshotInclusive);
      }

      Collections.reverse(snapshots);
      return snapshots;
    }

    private static int formatVersion(Table table) {
      return TableUtil.formatVersion(table);
    }
  }
}
