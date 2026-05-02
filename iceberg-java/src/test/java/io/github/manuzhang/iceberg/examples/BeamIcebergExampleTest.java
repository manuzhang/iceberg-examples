package io.github.manuzhang.iceberg.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Assume;
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
  public void testManagedIcebergConfig() {
    Map<String, Object> config =
        BeamIcebergExample.managedIcebergConfig(
            "default.users", "http://catalog:8181", "file:///tmp/warehouse");

    assertEquals("default.users", config.get("table"));
    assertEquals("rest", config.get("catalog_name"));
    assertTrue(config.containsKey("catalog_properties"));
  }

  @Test
  public void testRestCatalogProperties() {
    Map<String, String> properties =
        BeamIcebergExample.restCatalogProperties(
            "http://catalog:8181", "file:///tmp/warehouse");

    assertEquals(BeamIcebergExample.ICEBERG_REST_CATALOG_IMPL, properties.get("catalog-impl"));
    assertEquals("http://catalog:8181", properties.get("uri"));
    assertEquals("file:///tmp/warehouse", properties.get("warehouse"));
  }

  @Test
  public void testWriteAndReadPipelineWhenRestCatalogIsConfigured() {
    String catalogUri = System.getenv("ICEBERG_REST_URI");
    Assume.assumeTrue(
        "Set ICEBERG_REST_URI to run the REST catalog integration test",
        catalogUri != null && !catalogUri.isBlank());

    String warehouse = envOrDefault("ICEBERG_WAREHOUSE", BeamIcebergExample.DEFAULT_WAREHOUSE);
    String table =
        envOrDefault(
            "ICEBERG_TABLE",
            "default.beam_users_test_" + Long.toUnsignedString(System.nanoTime()));

    BeamIcebergExample.runExample(catalogUri, warehouse, table);
  }

  private static String envOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }

    return value;
  }
}
