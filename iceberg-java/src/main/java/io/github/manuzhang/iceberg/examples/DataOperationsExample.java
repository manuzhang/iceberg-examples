package io.github.manuzhang.iceberg.examples;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.Schema;
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

    // Display record information
    displayRecordInformation(sampleRecords, schema);
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

  /** Displays information about the created records. */
  private void displayRecordInformation(List<Record> records, Schema schema) {
    LOG.info("=== Record Information ===");
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

    LOG.info("Record structure demonstration:");
    LOG.info("- Records are created using GenericRecord.create(schema)");
    LOG.info("- Fields are set using record.setField(fieldName, value)");
    LOG.info("- Optional fields can be set to null");
    LOG.info("- Required fields must have non-null values");

    LOG.info("Note: For actual table operations (create, read, write),");
    LOG.info("use catalog implementations and Iceberg's table APIs or");
    LOG.info("compute engines like Spark, Flink, or Trino.");
  }
}
