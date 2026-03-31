package io.github.manuzhang.iceberg.examples;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating how to write an Iceberg table in Java that can be read by Rust (or any
 * other language with Iceberg support).
 *
 * <p>This example creates a simple {@code users} table on the local filesystem using {@link
 * HadoopTables} and writes three sample records as a Parquet data file. The resulting table can
 * then be read from the {@code iceberg-rust} module via {@code cargo run}.
 *
 * <p>Run order:
 *
 * <ol>
 *   <li>Java write: {@code ./gradlew runCrossLanguageWrite} (from {@code iceberg-java/})
 *   <li>Rust read: {@code cargo run} (from {@code iceberg-rust/})
 * </ol>
 */
public class CrossLanguageWriteExample {

  private static final Logger LOG = LoggerFactory.getLogger(CrossLanguageWriteExample.class);

  /** Default location on the local filesystem where the Iceberg table is written. */
  public static final String DEFAULT_TABLE_LOCATION = "/tmp/iceberg-java-rust-example";

  public static void main(String[] args) throws Exception {
    String location = args.length > 0 ? args[0] : DEFAULT_TABLE_LOCATION;
    CrossLanguageWriteExample example = new CrossLanguageWriteExample();
    try {
      example.writeTable(location);
      LOG.info("Cross-language write example completed successfully!");
    } catch (Exception e) {
      LOG.error("Error in cross-language write example: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  /**
   * Writes a sample {@code users} Iceberg table with three records to the given location.
   *
   * <p>The table is always recreated from scratch so the example is idempotent.
   *
   * @param location directory on the local filesystem where the table will be written
   * @throws IOException if the table cannot be written
   */
  public void writeTable(String location) throws IOException {
    LOG.info("=== Cross-Language Write Example ===");

    Schema schema = createSchema();
    LOG.info("Schema: {}", schema);

    // Delete existing table directory so the example is idempotent.
    deleteDirectory(new File(location));

    // Create an unpartitioned table using the local-filesystem-backed HadoopTables catalog.
    HadoopTables tables = new HadoopTables(new Configuration());
    Table table = tables.create(schema, PartitionSpec.unpartitioned(), location);
    LOG.info("Created Iceberg table at: {}", location);

    // Write sample records to a Parquet data file and commit the snapshot.
    List<Record> records = createSampleRecords(schema);
    writeRecords(table, records);

    LOG.info("Wrote {} records", records.size());
    LOG.info("Metadata location: {}/metadata/v2.metadata.json", location);
    LOG.info("To read this table with Rust: cd ../iceberg-rust && cargo run");
  }

  /** Returns the schema used for the example {@code users} table. */
  Schema createSchema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "age", Types.IntegerType.get()));
  }

  /** Returns three sample user records. */
  List<Record> createSampleRecords(Schema schema) {
    Record alice = GenericRecord.create(schema);
    alice.setField("id", 1L);
    alice.setField("name", "Alice");
    alice.setField("age", 30);

    Record bob = GenericRecord.create(schema);
    bob.setField("id", 2L);
    bob.setField("name", "Bob");
    bob.setField("age", 25);

    // Charlie has no age (optional field left as null).
    Record charlie = GenericRecord.create(schema);
    charlie.setField("id", 3L);
    charlie.setField("name", "Charlie");
    charlie.setField("age", null);

    return Arrays.asList(alice, bob, charlie);
  }

  /**
   * Writes the given records to a new Parquet data file and commits the file to the table as a new
   * snapshot.
   */
  @SuppressWarnings("unchecked")
  private void writeRecords(Table table, List<Record> records) throws IOException {
    String dataFilePath = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
    OutputFile outputFile = table.io().newOutputFile(dataFilePath);

    FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::create)
            .build();

    for (Record record : records) {
      appender.add(record);
    }
    appender.close();

    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withInputFile(outputFile.toInputFile())
            .withMetrics(appender.metrics())
            .withFormat(FileFormat.PARQUET)
            .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  /** Recursively deletes {@code dir} and all its contents if it exists. */
  private static void deleteDirectory(File dir) throws IOException {
    if (!dir.exists()) {
      return;
    }
    Path dirPath = dir.toPath();
    Files.walk(dirPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
  }
}
