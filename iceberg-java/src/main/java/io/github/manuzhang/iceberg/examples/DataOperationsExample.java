package io.github.manuzhang.iceberg.examples;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating data write operations with Apache Iceberg. Shows how to write records to
 * Iceberg tables using the GenericRecord API.
 */
public class DataOperationsExample {

  private static final Logger LOG = LoggerFactory.getLogger(DataOperationsExample.class);

  public static void main(String[] args) {
    LOG.info("Starting Data Operations Example...");

    DataOperationsExample example = new DataOperationsExample();

    try {
      example.demonstrateDataOperations();
      LOG.info("Data operations example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error in data operations: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  /** Demonstrates creating records and working with Iceberg data structures. */
  public void demonstrateDataOperations() {
    LOG.info("=== Data Operations Example ===");

    // Create table schema
    Schema schema = createUserSchema();
    LOG.info("Created schema with {} fields", schema.columns().size());

    // Create sample records
    List<Record> sampleRecords = createSampleRecords(schema);

    // Display initial record information
    displayRecordInformation(sampleRecords, schema, "Initial Records");

    // Demonstrate row-level upsert semantics
    List<Record> upsertedRecords = applyRowLevelUpsert(sampleRecords, createUpsertRecords(schema));
    displayRecordInformation(upsertedRecords, schema, "After Row-Level Upsert");

    // Show how the same operation maps to Iceberg RowDelta API usage
    demonstrateRowDeltaApiUsage();
  }

  /** Creates a user schema for the example table. */
  private Schema createUserSchema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "email", Types.StringType.get()),
        Types.NestedField.optional(4, "age", Types.IntegerType.get()),
        Types.NestedField.required(5, "created_at", Types.TimestampType.withZone()),
        Types.NestedField.optional(6, "active", Types.BooleanType.get()));
  }

  /** Creates sample records for demonstration. */
  private List<Record> createSampleRecords(Schema schema) {
    LOG.info("Creating sample records...");

    long currentTime = OffsetDateTime.now().toInstant().toEpochMilli() * 1000; // microseconds

    Record record1 = GenericRecord.create(schema);
    record1.setField("id", 1L);
    record1.setField("name", "Alice Johnson");
    record1.setField("email", "alice@example.com");
    record1.setField("age", 28);
    record1.setField("created_at", currentTime);
    record1.setField("active", true);

    Record record2 = GenericRecord.create(schema);
    record2.setField("id", 2L);
    record2.setField("name", "Bob Smith");
    record2.setField("email", "bob@example.com");
    record2.setField("age", 35);
    record2.setField("created_at", currentTime + 1000000); // 1 second later
    record2.setField("active", true);

    Record record3 = GenericRecord.create(schema);
    record3.setField("id", 3L);
    record3.setField("name", "Charlie Brown");
    record3.setField("email", null); // Optional field
    record3.setField("age", null); // Optional field
    record3.setField("created_at", currentTime + 2000000); // 2 seconds later
    record3.setField("active", false);

    List<Record> records = Arrays.asList(record1, record2, record3);
    LOG.info("Created {} sample records", records.size());

    return records;
  }

  /** Creates records that update existing rows and insert new rows. */
  private List<Record> createUpsertRecords(Schema schema) {
    LOG.info("Creating upsert records...");

    long currentTime = OffsetDateTime.now().toInstant().toEpochMilli() * 1000;

    Record updatedBob = GenericRecord.create(schema);
    updatedBob.setField("id", 2L);
    updatedBob.setField("name", "Bob Smith");
    updatedBob.setField("email", "bob.smith@example.com");
    updatedBob.setField("age", 36);
    updatedBob.setField("created_at", currentTime + 5000000);
    updatedBob.setField("active", true);

    Record newDana = GenericRecord.create(schema);
    newDana.setField("id", 4L);
    newDana.setField("name", "Dana Lee");
    newDana.setField("email", "dana@example.com");
    newDana.setField("age", 24);
    newDana.setField("created_at", currentTime + 6000000);
    newDana.setField("active", true);

    return Arrays.asList(updatedBob, newDana);
  }

  /**
   * Applies row-level upsert semantics using id as the primary key and created_at as sequence
   * number.
   */
  public List<Record> applyRowLevelUpsert(List<Record> baseRecords, List<Record> changeRecords) {
    Map<Long, Record> mergedById = new LinkedHashMap<>();

    for (Record record : baseRecords) {
      mergedById.put((Long) record.getField("id"), record);
    }

    for (Record changeRecord : changeRecords) {
      Long id = (Long) changeRecord.getField("id");
      Record existingRecord = mergedById.get(id);

      if (existingRecord == null
          || (Long) changeRecord.getField("created_at") >= (Long) existingRecord.getField("created_at")) {
        mergedById.put(id, changeRecord);
      }
    }

    List<Record> upsertedRecords = new ArrayList<>(mergedById.values());
    upsertedRecords.sort(Comparator.comparing(record -> (Long) record.getField("id")));
    return upsertedRecords;
  }

  /**
   * Demonstrates how upsert-style changes are committed through Iceberg's RowDelta API.
   *
   * <p>This method constructs representative DataFile/DeleteFile metadata and shows the
   * table.newRowDelta() flow. The files/paths are illustrative.
   */
  public void demonstrateRowDeltaApiUsage() {
    LOG.info("=== RowDelta API Mapping ===");

    PartitionSpec spec = PartitionSpec.unpartitioned();

    DataFile insertDataFile =
        DataFiles.builder(spec)
            .withPath("file:///tmp/iceberg-examples/data/insert-file.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(1)
            .build();

    DeleteFile equalityDeleteFile =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes(1)
            .withPath("file:///tmp/iceberg-examples/delete/equality-delete-file.parquet")
            .withFileSizeInBytes(256)
            .withRecordCount(1)
            .withFormat(org.apache.iceberg.FileFormat.PARQUET)
            .build();

    LOG.info("RowDelta pattern for row-level upsert:");
    LOG.info("  1) table.newRowDelta()");
    LOG.info("  2) addDeletes(equalityDeleteFile) for rows to replace");
    LOG.info("  3) addRows(insertDataFile) for new row versions");
    LOG.info("  4) commit() atomically");

    LOG.info("Illustrative files: insert={}, delete={}", insertDataFile.path(), equalityDeleteFile.path());
    LOG.info("Use applyRowDelta(table, insertDataFile, equalityDeleteFile) when a Table handle is available.");
  }

  /** Applies a row-level change set to a table using table.newRowDelta(). */
  public void applyRowDelta(Table table, DataFile insertDataFile, DeleteFile deleteFile) {
    RowDelta rowDelta = table.newRowDelta();
    rowDelta.addDeletes(deleteFile).addRows(insertDataFile).commit();
  }

  /** Displays information about the created records. */
  private void displayRecordInformation(List<Record> records, Schema schema, String sectionTitle) {
    LOG.info("=== {} ===", sectionTitle);
    LOG.info("Schema: {}", schema);
    LOG.info("Created {} records:", records.size());

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      LOG.info(
          "  Record {}: ID={}, Name={}, Email={}, Age={}, Active={}",
          i + 1,
          record.getField("id"),
          record.getField("name"),
          record.getField("email"),
          record.getField("age"),
          record.getField("active"));
    }

    LOG.info("Row-level upsert demonstration:");
    LOG.info("- Records are created using GenericRecord.create(schema)");
    LOG.info("- Fields are set using record.setField(fieldName, value)");
    LOG.info("- Optional fields can be set to null");
    LOG.info("- Required fields must have non-null values");
    LOG.info("- Upsert uses id as row key and created_at as ordering field");

    LOG.info("Note: For actual table operations (create, read, write, merge),");
    LOG.info("use catalog implementations and Iceberg's table APIs or");
    LOG.info("compute engines like Spark, Flink, or Trino.");
  }
}
