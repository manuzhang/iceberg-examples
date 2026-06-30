package io.github.manuzhang.iceberg.examples;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.ChangelogOperation;
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
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.rest.EmbeddedRestCatalogServer;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of a third-party changelog planner for Iceberg v3 deletion vectors.
 *
 * <p>This intentionally does not plug into Iceberg's {@code IncrementalChangelogScan}. Instead it
 * demonstrates the part an external library can own: read public snapshot file-change metadata,
 * accept Puffin-backed deletion vectors on v3 tables, and surface row-shaped CDC events for added
 * rows and rows deleted by a deletion vector or removed data file. The planner can also collapse
 * raw events into net changes by removing opposite INSERT/DELETE pairs for identical source rows.
 * Equality deletes and non-DV position deletes are rejected.
 */
public class DeletionVectorChangelogExample {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletionVectorChangelogExample.class);
  private static final String DEFAULT_CATALOG_NAME = "rest";
  private static final FileFormat DATA_FILE_FORMAT = FileFormat.AVRO;
  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.parse("default.dv_changelog_customers");
  private static final Schema ROW_LOCATION_METADATA_SCHEMA =
      new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION);
  private static final long UPDATED_CUSTOMER_ID = 2L;
  private static final long TRANSIENT_CUSTOMER_ID = 5L;
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

      LOG.info(
          "Net changes from snapshot {} to {}:",
          result.fromSnapshotId(),
          result.toSnapshotId());
      for (ChangelogEvent event : result.netChangeEvents()) {
        LOG.info("  {}", event);
      }

      LOG.info("Visible rows after applying the DV-backed upsert and removed data file:");
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
      DataFile removableDataFile =
          writeDataFile(
              table,
              List.of(new Customer(4L, "Dave Kim", "silver")),
              "initial-removable",
              4L);
      table.newAppend().appendFile(initialDataFile).appendFile(removableDataFile).commit();
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
      DataFile transientDataFile =
          writeDataFile(
              table,
              List.of(new Customer(TRANSIENT_CUSTOMER_ID, "Eve Stone", "trial")),
              "dv-changelog-transient",
              5L);

      table
          .newRowDelta()
          .addDeletes(deletionVectorFile)
          .addRows(upsertDataFile)
          .addRows(transientDataFile)
          .commit();
      table.refresh();

      table.newDelete().deleteFile(removableDataFile).deleteFile(transientDataFile).commit();
      table.refresh();
      long toSnapshotId = table.currentSnapshot().snapshotId();

      DvOnlyChangelogPlanner planner = new DvOnlyChangelogPlanner();
      List<ChangelogEvent> events = planner.plan(table, fromSnapshotId, toSnapshotId);
      return new ChangelogResult(
          planner.changelogSchema(table),
          events,
          planner.netChanges(events),
          readVisibleRows(table),
          fromSnapshotId,
          toSnapshotId,
          initialDataFile,
          upsertDataFile,
          removableDataFile,
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

  private static Schema rowReadSchema(Table table) {
    return TypeUtil.join(table.schema(), ROW_LOCATION_METADATA_SCHEMA);
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

  private record PositionedCustomerRow(
      long customerId, String name, String loyaltyTier, long rowPosition) {}

  public record ChangelogResult(
      Schema changelogSchema,
      List<ChangelogEvent> events,
      List<ChangelogEvent> netChangeEvents,
      List<Record> visibleRows,
      long fromSnapshotId,
      long toSnapshotId,
      DataFile originalDataFile,
      DataFile upsertDataFile,
      DataFile removedDataFile,
      DeleteFile deletionVectorFile) {}

  public record ChangelogEvent(
      long customer_id,
      String name,
      String loyalty_tier,
      ChangelogOperation _change_type,
      int _change_ordinal,
      long _commit_snapshot_id) {}

  private record SourceRowKey(long customerId, String name, String loyaltyTier) {}

  public static class DvOnlyChangelogPlanner {

    private static final Comparator<String> NULL_SAFE_STRING_ORDER =
        Comparator.nullsFirst(String::compareTo);
    private static final Comparator<ChangelogEvent> EVENT_ORDER =
        Comparator.comparingInt(ChangelogEvent::_change_ordinal)
            .thenComparingLong(ChangelogEvent::customer_id)
            .thenComparingInt(event -> event._change_type().ordinal())
            .thenComparing(ChangelogEvent::name, NULL_SAFE_STRING_ORDER)
            .thenComparing(ChangelogEvent::loyalty_tier, NULL_SAFE_STRING_ORDER);

    public Schema changelogSchema(Table table) {
      return ChangelogUtil.changelogSchema(table.schema());
    }

    public List<ChangelogEvent> plan(
        Table table, long fromSnapshotExclusive, long toSnapshotInclusive)
        throws IOException {
      if (formatVersion(table) < 3) {
        throw new UnsupportedOperationException(UNSUPPORTED_DELETE_MESSAGE);
      }

      List<ChangelogEvent> events = new ArrayList<>();
      List<Snapshot> snapshots =
          snapshotsBetween(table, fromSnapshotExclusive, toSnapshotInclusive);

      int changeOrdinal = 0;
      for (Snapshot snapshot : snapshots) {
        if (!shouldPlanSnapshotOperation(snapshot.operation())) {
          continue;
        }

        events.addAll(addedRows(table, snapshot, changeOrdinal));
        events.addAll(addedDeletionVectors(table, snapshot, changeOrdinal));
        events.addAll(removedDataFiles(table, snapshot, changeOrdinal));
        changeOrdinal++;
      }

      events.sort(EVENT_ORDER);
      return Collections.unmodifiableList(events);
    }

    public List<ChangelogEvent> planNetChanges(
        Table table, long fromSnapshotExclusive, long toSnapshotInclusive)
        throws IOException {
      return netChanges(plan(table, fromSnapshotExclusive, toSnapshotInclusive));
    }

    public List<ChangelogEvent> netChanges(List<ChangelogEvent> events) {
      Map<SourceRowKey, Deque<ChangelogEvent>> netEventsByRow = new LinkedHashMap<>();
      List<ChangelogEvent> orderedEvents = new ArrayList<>(events);
      orderedEvents.sort(EVENT_ORDER);

      for (ChangelogEvent event : orderedEvents) {
        Deque<ChangelogEvent> rowEvents =
            netEventsByRow.computeIfAbsent(sourceRowKey(event), ignored -> new ArrayDeque<>());

        if (!rowEvents.isEmpty() && oppositeChanges(rowEvents.peekLast(), event)) {
          rowEvents.removeLast();
        } else {
          rowEvents.addLast(event);
        }
      }

      List<ChangelogEvent> netEvents = new ArrayList<>();
      for (Deque<ChangelogEvent> rowEvents : netEventsByRow.values()) {
        netEvents.addAll(rowEvents);
      }

      netEvents.sort(EVENT_ORDER);
      return Collections.unmodifiableList(netEvents);
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

    private List<ChangelogEvent> addedRows(
        Table table, Snapshot snapshot, int changeOrdinal) throws IOException {
      List<ChangelogEvent> events = new ArrayList<>();

      for (DataFile dataFile : snapshot.addedDataFiles(table.io())) {
        for (PositionedCustomerRow row :
            rowsFromDataFile(table, snapshot.snapshotId(), dataFile.location())) {
          events.add(toChangelogEvent(row, ChangelogOperation.INSERT, changeOrdinal, snapshot));
        }
      }

      return events;
    }

    private List<ChangelogEvent> addedDeletionVectors(
        Table table, Snapshot snapshot, int changeOrdinal) throws IOException {
      List<ChangelogEvent> events = new ArrayList<>();
      Long parentSnapshotId = snapshot.parentId();
      if (parentSnapshotId == null) {
        throw new IllegalStateException(
            "Cannot plan deleted rows without a parent snapshot: " + snapshot.snapshotId());
      }

      for (DeleteFile deleteFile : snapshot.addedDeleteFiles(table.io())) {
        requireSupportedDeleteFile(deleteFile);
        PositionDeleteIndex deletedPositions = deletionVectorIndex(table, deleteFile);
        for (PositionedCustomerRow row :
            rowsFromDataFile(table, parentSnapshotId, deleteFile.referencedDataFile())) {
          if (deletedPositions.isDeleted(row.rowPosition())) {
            events.add(toChangelogEvent(row, ChangelogOperation.DELETE, changeOrdinal, snapshot));
          }
        }
      }

      return events;
    }

    private List<ChangelogEvent> removedDataFiles(
        Table table, Snapshot snapshot, int changeOrdinal) throws IOException {
      List<ChangelogEvent> events = new ArrayList<>();
      requireSupportedRemovedDeleteFiles(table, snapshot);
      Long parentSnapshotId = snapshot.parentId();
      if (parentSnapshotId == null) {
        throw new IllegalStateException(
            "Cannot plan removed data file rows without a parent snapshot: "
                + snapshot.snapshotId());
      }

      for (DataFile dataFile : snapshot.removedDataFiles(table.io())) {
        for (PositionedCustomerRow row :
            rowsFromDataFile(table, parentSnapshotId, dataFile.location())) {
          events.add(toChangelogEvent(row, ChangelogOperation.DELETE, changeOrdinal, snapshot));
        }
      }

      return events;
    }

    private void requireSupportedRemovedDeleteFiles(Table table, Snapshot snapshot) {
      for (DeleteFile deleteFile : snapshot.removedDeleteFiles(table.io())) {
        requireSupportedDeleteFile(deleteFile);
      }
    }

    private List<PositionedCustomerRow> rowsFromDataFile(
        Table table, long snapshotId, String dataFileLocation) throws IOException {
      List<PositionedCustomerRow> rows = new ArrayList<>();

      try (CloseableIterable<Record> iterable =
          IcebergGenerics.read(table)
              .useSnapshot(snapshotId)
              .project(rowReadSchema(table))
              .build()) {
        for (Record row : iterable) {
          if (dataFileLocation.equals(row.getField(MetadataColumns.FILE_PATH.name()).toString())) {
            rows.add(
                new PositionedCustomerRow(
                    ((Number) row.getField("customer_id")).longValue(),
                    stringField(row, "name"),
                    stringField(row, "loyalty_tier"),
                    ((Number) row.getField(MetadataColumns.ROW_POSITION.name())).longValue()));
          }
        }
      }

      rows.sort(Comparator.comparingLong(PositionedCustomerRow::rowPosition));
      return rows;
    }

    private String stringField(Record row, String fieldName) {
      Object value = row.getField(fieldName);
      return value != null ? value.toString() : null;
    }

    private ChangelogEvent toChangelogEvent(
        PositionedCustomerRow row,
        ChangelogOperation operation,
        int changeOrdinal,
        Snapshot snapshot) {
      return new ChangelogEvent(
          row.customerId(),
          row.name(),
          row.loyaltyTier(),
          operation,
          changeOrdinal,
          snapshot.snapshotId());
    }

    private SourceRowKey sourceRowKey(ChangelogEvent event) {
      return new SourceRowKey(event.customer_id(), event.name(), event.loyalty_tier());
    }

    private boolean oppositeChanges(ChangelogEvent previous, ChangelogEvent current) {
      return isInsert(previous) && isDelete(current) || isDelete(previous) && isInsert(current);
    }

    private boolean isInsert(ChangelogEvent event) {
      return event._change_type() == ChangelogOperation.INSERT;
    }

    private boolean isDelete(ChangelogEvent event) {
      return event._change_type() == ChangelogOperation.DELETE;
    }

    private PositionDeleteIndex deletionVectorIndex(Table table, DeleteFile deleteFile)
        throws IOException {
      byte[] bytes = new byte[Math.toIntExact(deleteFile.contentSizeInBytes())];

      try (SeekableInputStream input =
          table.io().newInputFile(deleteFile.location()).newStream()) {
        input.seek(deleteFile.contentOffset());
        int bytesRead = 0;
        while (bytesRead < bytes.length) {
          int read = input.read(bytes, bytesRead, bytes.length - bytesRead);
          if (read < 0) {
            throw new EOFException("Could not read deletion vector: " + deleteFile.location());
          }

          bytesRead += read;
        }
      }

      return PositionDeleteIndex.deserialize(bytes, deleteFile);
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
