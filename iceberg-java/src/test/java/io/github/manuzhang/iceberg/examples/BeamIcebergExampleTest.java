package io.github.manuzhang.iceberg.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

/**
 * Unit tests for the Apache Beam + Apache Iceberg example. Tests verify schema definitions, row
 * creation, and catalog configuration without requiring external infrastructure.
 */
public class BeamIcebergExampleTest {

  @Test
  public void testBeamSchemaFieldCount() {
    Schema schema = BeamIcebergExample.BEAM_SCHEMA;
    assertEquals(4, schema.getFieldCount());
  }

  @Test
  public void testBeamSchemaFieldNames() {
    Schema schema = BeamIcebergExample.BEAM_SCHEMA;
    assertTrue(schema.hasField("id"));
    assertTrue(schema.hasField("name"));
    assertTrue(schema.hasField("email"));
    assertTrue(schema.hasField("age"));
  }

  @Test
  public void testBeamSchemaFieldTypes() {
    Schema schema = BeamIcebergExample.BEAM_SCHEMA;
    assertEquals(Schema.FieldType.INT64, schema.getField("id").getType());
    assertEquals(Schema.FieldType.STRING, schema.getField("name").getType());
    assertEquals(Schema.FieldType.STRING, schema.getField("email").getType());
    assertEquals(Schema.FieldType.INT32, schema.getField("age").getType());
  }

  @Test
  public void testRowCreation() {
    Row row =
        Row.withSchema(BeamIcebergExample.BEAM_SCHEMA)
            .addValues(1L, "Alice", "alice@example.com", 30)
            .build();

    assertNotNull(row);
    assertEquals(Long.valueOf(1L), row.getInt64("id"));
    assertEquals("Alice", row.getString("name"));
    assertEquals("alice@example.com", row.getString("email"));
    assertEquals(Integer.valueOf(30), row.getInt32("age"));
  }

  @Test
  public void testTableIdentifier() {
    TableIdentifier tableId = TableIdentifier.of("default", "users");
    assertNotNull(tableId);
    assertEquals("default", tableId.namespace().toString());
    assertEquals("users", tableId.name());
  }

  @Test
  public void testCatalogConfig() {
    IcebergCatalogConfig.builder()
        .setCatalogName("test_catalog")
        .setCatalogProperties(Map.of("type", "hadoop", "warehouse", "/tmp/test-warehouse"))
        .build();
  }

  @Test
  public void testWriteAndReadPipeline() throws Exception {
    Path tempDir = Files.createTempDirectory("beam-iceberg-example-test");
    BeamIcebergExample.runExample(tempDir.toAbsolutePath().toString());
  }
}
