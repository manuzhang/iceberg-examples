package io.github.manuzhang.iceberg.examples;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the Apache Beam + Apache Iceberg example. Tests verify schema definitions, row
 * creation, and catalog configuration without requiring external infrastructure.
 */
class BeamIcebergExampleTest {

  @TempDir Path tempDir;

  @Test
  @DisplayName("Beam schema has the correct number of fields")
  void testBeamSchemaFieldCount() {
    Schema schema = BeamIcebergExample.BEAM_SCHEMA;
    assertEquals(4, schema.getFieldCount());
  }

  @Test
  @DisplayName("Beam schema contains all expected field names")
  void testBeamSchemaFieldNames() {
    Schema schema = BeamIcebergExample.BEAM_SCHEMA;
    assertTrue(schema.hasField("id"));
    assertTrue(schema.hasField("name"));
    assertTrue(schema.hasField("email"));
    assertTrue(schema.hasField("age"));
  }

  @Test
  @DisplayName("Beam schema field types are correct")
  void testBeamSchemaFieldTypes() {
    Schema schema = BeamIcebergExample.BEAM_SCHEMA;
    assertEquals(Schema.FieldType.INT64, schema.getField("id").getType());
    assertEquals(Schema.FieldType.STRING, schema.getField("name").getType());
    assertEquals(Schema.FieldType.STRING, schema.getField("email").getType());
    assertEquals(Schema.FieldType.INT32, schema.getField("age").getType());
  }

  @Test
  @DisplayName("Rows can be created with the defined schema")
  void testRowCreation() {
    Row row =
        Row.withSchema(BeamIcebergExample.BEAM_SCHEMA)
            .addValues(1L, "Alice", "alice@example.com", 30)
            .build();

    assertNotNull(row);
    assertEquals(1L, row.getInt64("id"));
    assertEquals("Alice", row.getString("name"));
    assertEquals("alice@example.com", row.getString("email"));
    assertEquals(30, row.getInt32("age"));
  }

  @Test
  @DisplayName("Iceberg table identifier is created correctly")
  void testTableIdentifier() {
    TableIdentifier tableId = TableIdentifier.of("default", "users");
    assertNotNull(tableId);
    assertEquals("default", tableId.namespace().toString());
    assertEquals("users", tableId.name());
  }

  @Test
  @DisplayName("IcebergCatalogConfig is created with HadoopCatalog properties")
  void testCatalogConfig() {
    assertDoesNotThrow(
        () ->
            IcebergCatalogConfig.builder()
                .setCatalogName("test_catalog")
                .setCatalogProperties(Map.of("type", "hadoop", "warehouse", "/tmp/test-warehouse"))
                .build());
  }

  @Test
  @DisplayName("Full write and read pipeline runs end-to-end with DirectRunner")
  void testWriteAndReadPipeline() {
    assertDoesNotThrow(() -> BeamIcebergExample.runExample(tempDir.toAbsolutePath().toString()));
  }
}
