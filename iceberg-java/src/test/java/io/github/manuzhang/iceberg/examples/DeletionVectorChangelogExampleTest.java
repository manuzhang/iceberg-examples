package io.github.manuzhang.iceberg.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.Record;
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
      assertEquals(3, events.size());

      DeletionVectorChangelogExample.ChangelogEvent addedRows = events.get(0);
      assertEquals(ChangelogOperation.INSERT, addedRows.type());
      assertEquals(result.upsertDataFile().location(), addedRows.dataFileLocation());
      assertEquals(2L, addedRows.recordCount());

      DeletionVectorChangelogExample.ChangelogEvent deletedRows = events.get(1);
      assertEquals(ChangelogOperation.DELETE, deletedRows.type());
      assertEquals(addedRows.snapshotId(), deletedRows.snapshotId());
      assertEquals(result.originalDataFile().location(), deletedRows.dataFileLocation());
      assertEquals(result.deletionVectorFile().location(), deletedRows.deletionVectorLocation());
      assertEquals(result.deletionVectorFile().contentOffset(), deletedRows.deletionVectorOffset());
      assertEquals(
          result.deletionVectorFile().contentSizeInBytes(), deletedRows.deletionVectorSize());
      assertEquals(1L, deletedRows.recordCount());

      DeletionVectorChangelogExample.ChangelogEvent removedDataFile = events.get(2);
      assertEquals(ChangelogOperation.DELETE, removedDataFile.type());
      assertEquals(result.toSnapshotId(), removedDataFile.snapshotId());
      assertEquals(result.removedDataFile().location(), removedDataFile.dataFileLocation());
      assertEquals(1L, removedDataFile.recordCount());

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

  @Test
  public void testRemovedDataFileEventCarriesRemovedDeletionVector() {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("in-memory://warehouse/data-file.avro")
            .withFormat(FileFormat.AVRO)
            .withRecordCount(3)
            .withFileSizeInBytes(100)
            .build();
    DeleteFile deletionVector =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("in-memory://warehouse/data-file-dv.puffin")
            .withFormat(FileFormat.PUFFIN)
            .withRecordCount(1)
            .withFileSizeInBytes(50)
            .withReferencedDataFile(dataFile.location())
            .withContentOffset(10)
            .withContentSizeInBytes(20)
            .build();

    DeletionVectorChangelogExample.ChangelogEvent event =
        DeletionVectorChangelogExample.DvOnlyChangelogPlanner.removedDataFileEvent(
            12L, dataFile, deletionVector);

    assertEquals(ChangelogOperation.DELETE, event.type());
    assertEquals(12L, event.snapshotId());
    assertEquals(dataFile.location(), event.dataFileLocation());
    assertEquals(2L, event.recordCount());
    assertEquals(deletionVector.location(), event.deletionVectorLocation());
    assertEquals(deletionVector.contentOffset(), event.deletionVectorOffset());
    assertEquals(deletionVector.contentSizeInBytes(), event.deletionVectorSize());
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
}
