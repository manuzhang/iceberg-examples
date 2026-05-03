package io.github.manuzhang.iceberg.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.data.Record;
import org.junit.Test;

/** Unit tests for the row-level upsert example backed by a deletion vector. */
public class RowLevelUpsertExampleTest {

  @Test
  public void testV3RowLevelUpsertWithDeletionVector() throws Exception {
    Path warehouseDir = Files.createTempDirectory("row-level-upsert-example-test");

    try {
      RowLevelUpsertExample example = new RowLevelUpsertExample();
      RowLevelUpsertExample.UpsertResult result = example.demonstrateRowLevelUpsert(warehouseDir);

      List<Record> rows = result.visibleRows();
      assertEquals(3, rows.size());

      assertCustomer(rows.get(0), 1L, "Alice Johnson", "silver");
      assertCustomer(rows.get(1), 2L, "Bob Smith", "gold");
      assertCustomer(rows.get(2), 3L, "Carol Lee", "bronze");

      assertEquals(FileFormat.PUFFIN, result.deletionVectorFile().format());
      assertEquals(result.originalDataFile().location(), result.deletionVectorFile().referencedDataFile());
      assertEquals(1L, result.deletionVectorFile().recordCount());
      assertNotNull(result.deletionVectorFile().contentOffset());
      assertNotNull(result.deletionVectorFile().contentSizeInBytes());

      Map<String, String> summary = result.snapshotSummary();
      assertEquals("1", summary.get(SnapshotSummary.ADDED_FILES_PROP));
      assertEquals("1", summary.get(SnapshotSummary.ADDED_DELETE_FILES_PROP));
      assertEquals("1", summary.get(SnapshotSummary.ADDED_DVS_PROP));
      assertEquals("1", summary.get(SnapshotSummary.ADDED_POS_DELETES_PROP));
    } finally {
      RowLevelUpsertExample.deleteRecursively(warehouseDir);
    }
  }

  private static void assertCustomer(Record row, long id, String name, String loyaltyTier) {
    assertEquals(Long.valueOf(id), row.getField("customer_id"));
    assertEquals(name, row.getField("name"));
    assertEquals(loyaltyTier, row.getField("loyalty_tier"));
  }
}
