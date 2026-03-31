package io.github.manuzhang.iceberg.examples;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for Iceberg examples. These tests demonstrate basic functionality without requiring
 * external dependencies.
 */
class IcebergExamplesTest {

  private IcebergExamples examples;

  @BeforeEach
  void setUp() {
    examples = new IcebergExamples();
  }

  @Test
  @DisplayName("Test basic schema creation")
  void testSchemaCreation() {
    // Create a test schema
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()));

    // Verify schema properties
    assertNotNull(schema);
    assertEquals(3, schema.columns().size());

    // Verify field properties
    assertTrue(schema.findField("id").isRequired());
    assertTrue(schema.findField("name").isRequired());
    assertFalse(schema.findField("age").isRequired());
  }

  @Test
  @DisplayName("Test schema field types")
  void testSchemaFieldTypes() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "active", Types.BooleanType.get()),
            Types.NestedField.optional(5, "timestamp", Types.TimestampType.withZone()));

    // Test field types
    assertEquals(Types.LongType.get(), schema.findType("id"));
    assertEquals(Types.StringType.get(), schema.findType("name"));
    assertEquals(Types.IntegerType.get(), schema.findType("age"));
    assertEquals(Types.BooleanType.get(), schema.findType("active"));
    assertEquals(Types.TimestampType.withZone(), schema.findType("timestamp"));
  }

  @Test
  @DisplayName("Test schema evolution concepts")
  void testSchemaEvolution() {
    // Initial schema
    Schema v1Schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    // Evolved schema (conceptual - in practice this would be done through table
    // operations)
    Schema v2Schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get()),
            Types.NestedField.optional(4, "created_at", Types.TimestampType.withZone()));

    // Verify evolution
    assertEquals(2, v1Schema.columns().size());
    assertEquals(4, v2Schema.columns().size());

    // Verify backward compatibility - all v1 fields exist in v2
    assertNotNull(v2Schema.findField("id"));
    assertNotNull(v2Schema.findField("name"));

    // Verify new fields are optional
    assertFalse(v2Schema.findField("email").isRequired());
    assertFalse(v2Schema.findField("created_at").isRequired());
  }

  @Test
  @DisplayName("Test field ID uniqueness")
  void testFieldIdUniqueness() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()));

    // Verify all field IDs are unique
    long distinctIds =
        schema.columns().stream().mapToInt(Types.NestedField::fieldId).distinct().count();

    assertEquals(schema.columns().size(), distinctIds);
  }

  @Test
  @DisplayName("Test examples class instantiation")
  void testExamplesInstantiation() {
    assertNotNull(examples);
    assertTrue(examples instanceof IcebergExamples);
  }

  @Test
  @DisplayName("Test v3 nanosecond timestamp types")
  void testTimestampNanoTypes() {
    Types.TimestampNanoType tsWithZone = Types.TimestampNanoType.withZone();
    Types.TimestampNanoType tsWithoutZone = Types.TimestampNanoType.withoutZone();

    assertNotNull(tsWithZone);
    assertNotNull(tsWithoutZone);
    assertTrue(tsWithZone.shouldAdjustToUTC());
    assertFalse(tsWithoutZone.shouldAdjustToUTC());
    assertNotEquals(tsWithZone, tsWithoutZone);
  }

  @Test
  @DisplayName("Test v3 variant type")
  void testVariantType() {
    Types.VariantType variantType = Types.VariantType.get();

    assertNotNull(variantType);
    assertTrue(variantType.isVariantType());
    assertEquals(variantType, Types.VariantType.get()); // singleton
  }

  @Test
  @DisplayName("Test v3 geospatial types")
  void testGeospatialTypes() {
    Types.GeometryType geometryCrs84 = Types.GeometryType.crs84();
    Types.GeometryType geometryCustom = Types.GeometryType.of("EPSG:4326");
    Types.GeographyType geographyCrs84 = Types.GeographyType.crs84();
    Types.GeographyType geographySpherical =
        Types.GeographyType.of("OGC:CRS84", EdgeAlgorithm.SPHERICAL);

    assertNotNull(geometryCrs84);
    assertNotNull(geometryCustom);
    assertNotNull(geographyCrs84);
    assertNotNull(geographySpherical);

    // crs84() stores null internally (the default CRS); custom CRS stores the given string
    assertNull(geometryCrs84.crs());
    assertEquals("EPSG:4326", geometryCustom.crs());
    // crs84() stores null internally for both crs and algorithm
    assertNull(geographyCrs84.crs());
    assertNull(geographyCrs84.algorithm());
    assertEquals(EdgeAlgorithm.SPHERICAL, geographySpherical.algorithm());
  }

  @Test
  @DisplayName("Test v3 default column values")
  void testDefaultColumnValues() {
    Types.NestedField fieldWithDefaults =
        Types.NestedField.optional("status")
            .withId(1)
            .ofType(Types.StringType.get())
            .withInitialDefault("active")
            .withWriteDefault("active")
            .build();

    assertNotNull(fieldWithDefaults);
    assertEquals("active", fieldWithDefaults.initialDefault());
    assertEquals("active", fieldWithDefaults.writeDefault());
    assertFalse(fieldWithDefaults.isRequired());
  }

  @Test
  @DisplayName("Test v3 schema with new types and default values")
  void testV3Schema() {
    Schema v3Schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "event_time", Types.TimestampNanoType.withZone()),
            Types.NestedField.optional(3, "payload", Types.VariantType.get()),
            Types.NestedField.optional(4, "location", Types.GeographyType.crs84()),
            Types.NestedField.optional("status")
                .withId(5)
                .ofType(Types.StringType.get())
                .withInitialDefault("active")
                .withWriteDefault("active")
                .build());

    assertEquals(5, v3Schema.columns().size());
    assertEquals(Types.TimestampNanoType.withZone(), v3Schema.findType("event_time"));
    assertTrue(v3Schema.findType("payload").isVariantType());
    assertEquals("active", v3Schema.findField("status").initialDefault());
  }

  @Test
  @DisplayName("Test TableFormatV3Example class can be instantiated and run")
  void testTableFormatV3Example() {
    TableFormatV3Example v3Example = new TableFormatV3Example();
    assertNotNull(v3Example);
    // Verify each method runs without exception
    assertDoesNotThrow(v3Example::demonstrateV3DataTypes);
    assertDoesNotThrow(v3Example::demonstrateDefaultValues);
    assertDoesNotThrow(v3Example::demonstrateV3Schema);
  }

  @Test
  @DisplayName("Test CrossLanguageWriteExample creates a valid Iceberg table on disk")
  void testCrossLanguageWrite(@TempDir Path tempDir) throws Exception {
    CrossLanguageWriteExample example = new CrossLanguageWriteExample();
    example.writeTable(tempDir.toString());

    // Verify the metadata directory and at least two metadata files were created
    // (v1 = table creation, v2 = after the first append).
    File metadataDir = new File(tempDir.toFile(), "metadata");
    assertTrue(metadataDir.exists(), "metadata directory should exist");
    assertTrue(metadataDir.isDirectory(), "metadata should be a directory");

    File[] metadataFiles = metadataDir.listFiles(f -> f.getName().endsWith(".metadata.json"));
    assertNotNull(metadataFiles);
    assertTrue(metadataFiles.length >= 2, "expected at least two metadata files (v1 and v2)");

    // Verify the data directory and a Parquet file were created.
    File dataDir = new File(tempDir.toFile(), "data");
    assertTrue(dataDir.exists(), "data directory should exist");

    File[] parquetFiles = dataDir.listFiles(f -> f.getName().endsWith(".parquet"));
    assertNotNull(parquetFiles);
    assertEquals(1, parquetFiles.length, "expected exactly one Parquet data file");
  }

  @Test
  @DisplayName("Test CrossLanguageWriteExample schema has expected fields")
  void testCrossLanguageWriteSchema() {
    CrossLanguageWriteExample example = new CrossLanguageWriteExample();
    Schema schema = example.createSchema();

    assertNotNull(schema);
    assertEquals(3, schema.columns().size());
    assertTrue(schema.findField("id").isRequired());
    assertTrue(schema.findField("name").isRequired());
    assertFalse(schema.findField("age").isRequired());
  }

  @Test
  @DisplayName("Test CrossLanguageWriteExample sample records")
  void testCrossLanguageWriteSampleRecords() {
    CrossLanguageWriteExample example = new CrossLanguageWriteExample();
    Schema schema = example.createSchema();

    var records = example.createSampleRecords(schema);
    assertEquals(3, records.size());
    assertEquals(1L, records.get(0).getField("id"));
    assertEquals("Alice", records.get(0).getField("name"));
    assertEquals(30, records.get(0).getField("age"));
    assertNull(records.get(2).getField("age"), "Charlie's age should be null");
  }
}
