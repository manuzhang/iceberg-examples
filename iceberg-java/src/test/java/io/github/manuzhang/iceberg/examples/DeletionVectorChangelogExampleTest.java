package io.github.manuzhang.iceberg.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.Test;

/** Unit tests for the deletion-vector-only changelog planner example. */
public class DeletionVectorChangelogExampleTest {

  @Test
  public void testPlansDeletionVectorChangelog() throws Exception {
    Path warehouseDir = Files.createTempDirectory("dv-changelog-example-test");

    try {
      DeletionVectorChangelogExample example = new DeletionVectorChangelogExample();
      DeletionVectorChangelogExample.ChangelogResult result =
          example.demonstrateDeletionVectorChangelog(warehouseDir);

      List<DeletionVectorChangelogExample.ChangelogEvent> events = result.events();
      assertChangelogSchema(result.changelogSchema());
      assertEquals(4, events.size());

      DeletionVectorChangelogExample.ChangelogEvent addedRows = events.get(0);
      assertEvent(
          addedRows,
          2L,
          "Bob Smith",
          "gold",
          ChangelogOperation.INSERT,
          0,
          addedRows._commit_snapshot_id());

      DeletionVectorChangelogExample.ChangelogEvent deletedRows = events.get(1);
      assertEvent(
          deletedRows,
          2L,
          "Bob Smith",
          "bronze",
          ChangelogOperation.DELETE,
          0,
          addedRows._commit_snapshot_id());

      DeletionVectorChangelogExample.ChangelogEvent addedCarol = events.get(2);
      assertEvent(
          addedCarol,
          3L,
          "Carol Lee",
          "bronze",
          ChangelogOperation.INSERT,
          0,
          addedRows._commit_snapshot_id());

      DeletionVectorChangelogExample.ChangelogEvent removedDataFile = events.get(3);
      assertEvent(
          removedDataFile,
          4L,
          "Dave Kim",
          "silver",
          ChangelogOperation.DELETE,
          1,
          result.toSnapshotId());

      List<Record> rows = result.visibleRows();
      assertEquals(3, rows.size());
      assertCustomer(rows.get(0), 1L, "Alice Johnson", "silver");
      assertCustomer(rows.get(1), 2L, "Bob Smith", "gold");
      assertCustomer(rows.get(2), 3L, "Carol Lee", "bronze");
    } finally {
      RowLevelUpsertExample.deleteRecursively(warehouseDir);
    }
  }

  @Test
  public void testRejectsEqualityAndNonDvPositionDeletes() {
    assertUnsupported(equalityDeleteFile());
    assertUnsupported(positionDeleteFile());
  }

  @Test
  public void testSkipsReplaceOperations() {
    assertFalse(
        DeletionVectorChangelogExample.DvOnlyChangelogPlanner.shouldPlanSnapshotOperation(
            DataOperations.REPLACE));
    assertTrue(
        DeletionVectorChangelogExample.DvOnlyChangelogPlanner.shouldPlanSnapshotOperation(
            DataOperations.APPEND));
  }

  private static void assertUnsupported(DeleteFile deleteFile) {
    try {
      DeletionVectorChangelogExample.DvOnlyChangelogPlanner.requireSupportedDeleteFile(deleteFile);
      fail("Expected non-DV delete file to be rejected");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("Only deletion vectors in v3 tables are supported"));
    }
  }

  private static DeleteFile equalityDeleteFile() {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofEqualityDeletes(1)
        .withPath("in-memory://warehouse/equality-delete.avro")
        .withFormat(FileFormat.AVRO)
        .withRecordCount(1)
        .withFileSizeInBytes(100)
        .build();
  }

  private static DeleteFile positionDeleteFile() {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes()
        .withPath("in-memory://warehouse/position-delete.avro")
        .withFormat(FileFormat.AVRO)
        .withRecordCount(1)
        .withFileSizeInBytes(100)
        .build();
  }

  private static void assertCustomer(Record row, long id, String name, String loyaltyTier) {
    assertEquals(Long.valueOf(id), row.getField("customer_id"));
    assertEquals(name, row.getField("name"));
    assertEquals(loyaltyTier, row.getField("loyalty_tier"));
  }

  private static void assertChangelogSchema(Schema schema) {
    List<Types.NestedField> columns = schema.columns();
    assertEquals(6, columns.size());
    assertEquals(RowLevelUpsertExample.CUSTOMER_SCHEMA.columns(), columns.subList(0, 3));
    assertField(MetadataColumns.CHANGE_TYPE, columns.get(3));
    assertField(MetadataColumns.CHANGE_ORDINAL, columns.get(4));
    assertField(MetadataColumns.COMMIT_SNAPSHOT_ID, columns.get(5));
  }

  private static void assertField(Types.NestedField expected, Types.NestedField actual) {
    assertEquals(expected.fieldId(), actual.fieldId());
    assertEquals(expected.name(), actual.name());
    assertEquals(expected.type(), actual.type());
    assertEquals(expected.isOptional(), actual.isOptional());
  }

  private static void assertEvent(
      DeletionVectorChangelogExample.ChangelogEvent event,
      long id,
      String name,
      String loyaltyTier,
      ChangelogOperation changeType,
      int changeOrdinal,
      long commitSnapshotId) {
    assertEquals(id, event.customer_id());
    assertEquals(name, event.name());
    assertEquals(loyaltyTier, event.loyalty_tier());
    assertEquals(changeType, event._change_type());
    assertEquals(changeOrdinal, event._change_ordinal());
    assertEquals(commitSnapshotId, event._commit_snapshot_id());
  }
}
